# ruff: noqa: I001
import argparse
import glob
import heapq
import json
import logging
import multiprocessing as mp
import os
import re
import time
import types
from functools import partial
from functools import reduce
from typing import Any

import numpy as np
import pandas as pd
import yaml

from typing import Union

import crisp.common as common
from crisp.shared.models import CallPathProfile
from crisp.shared.utils import getLeafNodeFromCallPath
import crisp.storage as storage
import crisp.flamegraph as flamegraph
from crisp.graph import (
    accumulateInDict,
    bcolors,
    Graph,
)
from crisp.models import (
    ErrorCPMetrics,
    ErrorMetrics,
    Metrics,
    QuantizedMetrics,
    SavingData,
)
from crisp.shared.models import LatencyData
from crisp.constants import PARQUET_STRING_ID
from crisp.shared.constants import TOTAL_TIME

# import polars as pl  # deferred: parquet support not available in OSS build
from crisp.cct_utils import (
    cct_to_dot,
    parse_cct_file,
)
# create_protobuf_response_with_exemplars deferred — requires protobuf stub (PR 14)
from crisp.metrics.aggregators import MergeCallPathProfilesWithExemplars

# Import CSV generation functions from the new output module for backward compatibility
from crisp.output.csv_generators import (
    genSummaryCSVFile,
    genHypoLatencyCSVFile,
    genCyclesCSVFile,
    genCrossRegionCallsCSVFile,
)

# Import aggregation function from the new metrics module
from crisp.metrics.aggregators import (
    MergeCallPathProfilesWithExample,
)
# Import percentile calculation functions from the new metrics module
from crisp.metrics.percentile_calculator import (
    insertInDF,
    addPercentileColumns,
    insertInclusivePercentileInfoDF,
    genLatencyPercentile,
)

# Import formatting functions from the new output module for backward compatibility
from crisp.output.formatters import (
    makeClickable,
    addHyperLinkToTrace,
    renameSortableIcon,
    insertOccurenceCol,
    reindexDescending,
    setCellFormating,
    JAEGER_UI_URL,
    SORTABLE_COL_CLASS,
)

# Backward compatibility aliases for constants
sortabelColClass = SORTABLE_COL_CLASS


logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
debug_on = logging.getLogger(__name__).isEnabledFor(logging.DEBUG)

INVALID_PARENT_SPAN_WARNING = "invalid parent span IDs"


def check_trace_completeness(data: dict) -> tuple[bool, str]:
    """Check whether a Jaeger JSON trace is incomplete due to sampling limitations.

    A trace is considered incomplete when any span carries an
    "invalid parent span IDs" warning, which Jaeger emits when a span
    references a parent that was not captured in the trace.
    Bails out on the first match since one orphan span is enough.

    Args:
        data: Parsed Jaeger JSON trace (the top-level dict with a "data" key).

    Returns:
        Tuple of (is_incomplete, trace_id).
    """
    trace_id = ""
    for item in data.get("data", []):
        if not trace_id:
            trace_id = item.get("traceID", "")
        for span in item.get("spans", []):
            for warning in span.get("warnings") or []:
                if INVALID_PARENT_SPAN_WARNING in warning:
                    return True, trace_id
    return False, trace_id


def collect_orphan_span_services(data: dict) -> set[str]:
    """Collect service names of all orphan spans in an incomplete Jaeger JSON trace.

    Only called when the trace is already known to be incomplete.

    Args:
        data: Parsed Jaeger JSON trace.

    Returns:
        Set of service names that have at least one orphan span.
    """
    orphan_services: set[str] = set()
    for item in data.get("data", []):
        processes = item.get("processes", {})
        for span in item.get("spans", []):
            for warning in span.get("warnings") or []:
                if INVALID_PARENT_SPAN_WARNING in warning:
                    pid = span.get("processID", "")
                    service = processes.get(pid, {}).get("serviceName", "unknown")
                    orphan_services.add(service)
                    break
    return orphan_services


def get_subtree_services(root_node, process_name: dict) -> set[str]:
    """Collect all service names reachable from root_node in the parsed graph.

    Args:
        root_node: Root GraphNode of the target sub-tree.
        process_name: Graph.processName mapping pid -> service name.

    Returns:
        Set of service names present in the sub-tree.
    """
    services: set[str] = set()
    stack = [root_node]
    while stack:
        node = stack.pop()
        svc = process_name.get(node.pid)
        if svc:
            services.add(svc)
        stack.extend(node.children)
    return services


def log_incomplete_trace_stats(
    metrics: list,
    total_processed: int,
    service_name: str,
    operation_name: str,
) -> dict:
    """Log aggregated stats about incomplete traces for CRON job reporting.

    Args:
        metrics: List of Metrics objects from trace processing.
        total_processed: Total number of trace files/rows attempted.
        service_name: Service name being analyzed.
        operation_name: Operation name being analyzed.

    Returns:
        Dict with summary stats suitable for structured logging / M3 emission.
    """
    incomplete_count = sum(1 for m in metrics if m.isIncomplete)
    subtree_incomplete_count = sum(1 for m in metrics if m.isSubtreeIncomplete)
    total_metrics = len(metrics)
    pct = f"{incomplete_count / total_metrics:.2%}" if total_metrics > 0 else "N/A"

    stats = {
        "service": service_name,
        "operation": operation_name,
        "total_traces_processed": total_processed,
        "total_traces_with_metrics": total_metrics,
        "incomplete_traces": incomplete_count,
        "incomplete_trace_pct": pct,
        "subtree_incomplete_traces": subtree_incomplete_count,
    }

    if incomplete_count > 0:
        logging.warning(
            f"[IncompleteTraceReport] [{service_name}]::{operation_name} — "
            f"{incomplete_count}/{total_metrics} traces ({pct}) are INCOMPLETE. "
            f"Of these, {subtree_incomplete_count} affect the target sub-graph (downstream), "
            f"{incomplete_count - subtree_incomplete_count} are upstream/unrelated."
        )
    else:
        logging.info(
            f"[IncompleteTraceReport] [{service_name}]::{operation_name} — "
            f"all {total_metrics} traces are complete (no orphan spans detected)."
        )

    return stats


class YAMLAction(argparse.Action):
    "A YAML string with a list of Key-Value Dicts"

    def __call__(self, parser, namespace, yamlStr, option_string=None):  # noqa: ARG002
        if not isinstance(yamlStr, str):
            raise argparse.ArgumentTypeError("Invalid YAML" + str(yamlStr))
        parsed_yaml = yaml.safe_load(yamlStr)
        if not isinstance(parsed_yaml, list):
            raise argparse.ArgumentTypeError("Invalid YAML" + str(parsed_yaml))
        for d in parsed_yaml:
            if not isinstance(d, dict):
                raise argparse.ArgumentTypeError("Invalid YAML:" + str(d))
            if len(d) != len(common.TAG_KEYS):
                raise argparse.ArgumentTypeError("Invalid YAML:" + str(d))
            for k in common.TAG_KEYS:
                if k not in d.keys():
                    raise argparse.ArgumentTypeError(
                        "Key " + str(k) + "not found in: " + str(d),
                    )
            if not isinstance(d[common.TAG_NAME], str):
                raise argparse.ArgumentTypeError("Invalid YAML:" + str(d))
            if not isinstance(d[common.TAG_VALUE], str):
                raise argparse.ArgumentTypeError("Invalid YAML:" + str(d))
            if not isinstance(d[common.TAG_SEARCH_DEPTH], int):
                raise argparse.ArgumentTypeError("Invalid YAML:" + str(d))
        setattr(namespace, self.dest, parsed_yaml)


def initArgs():
    argParser = argparse.ArgumentParser()
    argParser.add_argument(
        "-o",
        "--operationName",
        action="store",
        help="operation name",
        default="",
        type=str,
    )
    argParser.add_argument(
        "-s",
        "--serviceName",
        action="store",
        help="name of the service",
        default="",
        type=str,
    )

    argParser.add_argument(
        "--rootTrace",
        dest="rootTrace",
        action="store_true",
        default=False,
        required=False,
        help="Should the service and operation be the root span of the trace (default:false).",
    )
    argParser.add_argument(
        "--mergeAllRoots",
        dest="mergeAllRoots",
        action=argparse.BooleanOptionalAction,
        default=True,
        required=False,
        help="Merge metrics from every matching root span instead of using only the first match (default=true).",
    )

    argParser.add_argument(
        "--anonymize",
        dest="anonymize",
        action="store_true",
        default=False,
        required=False,
        help="Should the service and operation names be anonymized (default:false).",
    )
    argParser.add_argument(
        "-i",
        "--inputDir",
        action="store",
        help="input path of the trace directory (mutually exclusive with --file)",
        default="traces",
        type=str,
    )
    argParser.add_argument(
        "--file",
        type=argparse.FileType("r"),
        action="store",
        help="input path of the trace file (mutually exclusivbe with --inputDir)",
        default=None,
    )
    argParser.add_argument(
        "--parallelism",
        action="store",
        help="Number of concurrent python processes.",
        default=1,
        type=int,
    )
    argParser.add_argument(
        "--topN",
        action="store",
        help="number of services to show in the summary",
        default=5,
        type=int,
    )
    argParser.add_argument(
        "--numHMTrace",
        action="store",
        help="number of traces to show in the heatmap",
        default=100,
        type=int,
    )
    argParser.add_argument(
        "--numOperation",
        action="store",
        help="number of operations to show in the heatmap",
        default=100,
        type=int,
    )
    argParser.add_argument(
        "--ignoreTestTraces",
        dest="ignoreTestTraces",
        action="store_true",
        help="Ignore traces marked as synthetic test traces.",
        default=False,
        required=False,
    )
    argParser.add_argument(
        "--doRanges",
        dest="doRanges",
        action="store_true",
        help="Compute flamegraphs for every 20 percentiles (default=false)",
        default=False,
        required=False,
    )

    argParser.add_argument(
        "--tags",
        dest="tags",
        action=YAMLAction,
        help=(
            "a YAML formated list of key-value filters to apply to the traces. e.g. "
            '--tags "[{name: TAGA, value: VALA, search_depth: 1}, {name: TAGB, value: VALB, search_depth: 20}]"'
        ),
        default=common.DEFAULT_TAGS,
        required=False,
    )
    argParser.add_argument(
        "--exclude-from-cp",
        dest="excludeFromCP",
        type=argparse.FileType("r"),
        action="store",
        help="a YAML file with a set of operations to ignore from the critical path",
        default=None,
        required=False,
    )

    argParser.add_argument(
        "--errorAnalysis",
        dest="errorAnalysis",
        action="store_true",
        default=False,
        required=False,
        help="Run error analysis"
    )

    argParser.add_argument(
        "--deltaMicroSec",
        action="store",
        help="Analyze delta time injection in micro seconds (-ve means time reduction)",
        default=0,
        type=int,
    )

    argParser.add_argument(
        "--lightMode",
        dest="lightMode",
        action="store_true",
        default=False,
        required=False,
        help="Use light mode for the analysis",
    )

    argParser.add_argument(
        "--maxExemplars",
        dest="maxExemplars",
        action="store",
        default=3,
        required=False,
        help="Maximum number of exemplars (trace/span pairs) to keep per call path in protobuf output (default=3).",
        type=int,
    )

    argParser.add_argument(
        "--deltaTargetService",
        dest="deltaTargetService",
        action="store",
        default=None,
        required=False,
        type=str,
        help=(
            "Service name to target for latency projection (context-insensitive: applies to ALL instances). "
            "Must be used with --deltaTargetOperation and a non-zero --deltaMicroSec."
        ),
    )

    argParser.add_argument(
        "--deltaTargetOperation",
        dest="deltaTargetOperation",
        action="store",
        default=None,
        required=False,
        type=str,
        help=(
            "Operation name to target for latency projection (context-insensitive: applies to ALL instances). "
            "Must be used with --deltaTargetService and a non-zero --deltaMicroSec."
        ),
    )

    argParser.add_argument(
        "--jaegerQueryUrl",
        dest="jaegerQueryUrl",
        action="store",
        default="http://localhost:16686",
        required=False,
        help="Base URL for the Jaeger query API.",
        type=str,
    )

    args = argParser.parse_args()

    if (args.deltaTargetService is None) != (args.deltaTargetOperation is None):
        argParser.error("--deltaTargetService and --deltaTargetOperation must both be provided together.")
    if args.deltaTargetService is not None and args.deltaMicroSec == 0:
        argParser.error("--deltaMicroSec must be non-zero when --deltaTargetService/--deltaTargetOperation are set.")
    operationName = args.operationName
    serviceName = args.serviceName
    tracesDir = args.inputDir
    topN = args.topN
    numOperation = args.numOperation
    numHMTrace = args.numHMTrace
    rootTrace = args.rootTrace
    anonymize = args.anonymize
    tags = args.tags
    doRanges = args.doRanges
    errorAnalysis = args.errorAnalysis
    deltaMicroSec = args.deltaMicroSec
    deltaTargetService = args.deltaTargetService
    deltaTargetOperation = args.deltaTargetOperation
    lightMode = args.lightMode
    maxExemplars = args.maxExemplars
    jaegerTraceFiles = glob.glob(os.path.join(tracesDir, "*.json"))

    if args.file:
        jaegerTraceFiles = [args.file.name]
        args.file.close()

    exclusionSet = set()
    if args.excludeFromCP:
        exclusionDict = yaml.safe_load(args.excludeFromCP)
        for srv, v in exclusionDict.items():
            for ops in v:
                exclusionSet.add((srv, ops))
        args.excludeFromCP.close()
    c = common.Config(
        operationName=operationName,
        serviceName=serviceName,
        tags=tags,
        tracesDir=tracesDir,
        topN=topN,
        numOperation=numOperation,
        numHMTrace=numHMTrace,
        rootTrace=rootTrace,
        anonymize=anonymize,
        file=args.file,
        computeParallelism=args.parallelism,
        doRanges=doRanges,
        exclusionSet=exclusionSet,
        errorAnalysis=errorAnalysis,
        deltaMicroSec=deltaMicroSec,
        deltaTargetService=deltaTargetService,
        deltaTargetOperation=deltaTargetOperation,
        lightMode=lightMode,
        mergeAllRoots=args.mergeAllRoots,
        maxExemplars=maxExemplars,
        jaegerQueryUrl=args.jaegerQueryUrl,
    )
    c.jaegerTraceFiles = jaegerTraceFiles
    return c


# ---------------------------------------------------------------------------
# HTML report templates
# ---------------------------------------------------------------------------

DATE_TIME = datetime.now().strftime("%d_%B_%Y_%H_%M_%S")

HTML_PREFIX = '''
<html>
  <head><title>CRISP: Critical Path Report</title>
  <style>
.row_heading {
  text-align: right;
}
/* Tooltip container */
.tooltip {
  position: relative;
  display: inline-block;
  border-bottom: 1px dotted black;
}
/* Tooltip text */
.tooltip .tooltiptext {
  visibility: hidden;
  width: max-content;
  background-color: black;
  color: #fff;
  text-align: left;
  padding: 5px 0;
  border-radius: 6px;
  position: absolute;
  z-index: 1;
}
.tooltip:hover .tooltiptext {
  visibility: visible;
}.table-sortable th {
  cursor: pointer;
}

.table-sortable .th-sort-asc::after {
  content: " \\003c";
}

.table-sortable .th-sort-desc::after {
  content: " \\003e";
}

.table-sortable .th-sort-asc::after,
.table-sortable .th-sort-desc::after {
  margin-left: 5px;
}

.table-sortable .th-sort-asc,
.table-sortable .th-sort-desc {
  background: rgba(0, 0, 0, 0.1);
}

</style>
<link rel="stylesheet" href="https://use.fontawesome.com/releases/v5.0.7/css/all.css">
  </head>
  <body>
  '''

HTML_GENERATION_TIME = "<h1>Critical path generated on %s </h1>" % DATE_TIME

HTML_SUFFIX = '''
  <script type = "text/javascript">
  /**
 * Sorts a HTML table.
 *
 * @param {HTMLTableElement} table The table to sort
 * @param {number} column The index of the column to sort
 * @param {boolean} asc Determines if the sorting will be in ascending order
 */
function sortTableByColumn(table, column, asc = true) {
    const dirModifier = asc ? 1 : -1;
    const tBody = table.tBodies[0];
    const rows = Array.from(tBody.querySelectorAll("tr"));

    const sortedRows = rows.sort((a, b) => {
        const aColText = Number(a.querySelector(`td:nth-child(${ column + 1 })`).textContent.trim())
        const bColText = Number(b.querySelector(`td:nth-child(${ column + 1 })`).textContent.trim());

        return aColText > bColText ? (1 * dirModifier) : (-1 * dirModifier);
    });

    while (tBody.firstChild) {
        tBody.removeChild(tBody.firstChild);
    }

    tBody.append(...sortedRows);

    table.querySelectorAll("th").forEach(th => th.classList.remove("th-sort-asc", "th-sort-desc"));
    table.querySelector(`th:nth-child(${ column + 1})`).classList.toggle("th-sort-asc", asc);
    table.querySelector(`th:nth-child(${ column + 1})`).classList.toggle("th-sort-desc", !asc);
}

document.querySelectorAll(".table-sortable th").forEach(headerCell => {
    const headerIndex = Array.prototype.indexOf.call(headerCell.parentElement.children, headerCell);
    if ((headerIndex > 1 && headerIndex) <= 4 || (headerIndex >= 8 && headerIndex <= 10)) {
        headerCell.addEventListener("click", () => {
            const tableElement = headerCell.parentElement.parentElement.parentElement;
            const currentIsAscending = headerCell.classList.contains("th-sort-asc");

            sortTableByColumn(tableElement, headerIndex, !currentIsAscending);
        });
    }
});

  </script>
  </body>
</html>
'''


# ---------------------------------------------------------------------------
# Single-trace processing
# ---------------------------------------------------------------------------

def process(filename: str, config: common.Config) -> Any:
    """Process one Jaeger JSON trace file and return its Metrics."""
    with open(filename, "r") as f:
        data = json.load(f)

    graph = Graph(
        data,
        config.serviceName,
        config.operationName,
        filename,
        config.rootTrace,
    )

    if graph.rootNode is None:
        return Metrics({}, {}, {}, {}, {}, {}, {}, 0, 0, 0)

    if config.ignoreTestTraces and graph.isTestTrace:
        return Metrics({}, {}, {}, {}, {}, {}, {}, 0, 0, 0)

    res = graph.findCriticalPath()
    logging.debug("critical path: %s", res)

    metrics = graph.getMetrics(res)
    logging.debug("opTimeExclusive: %s", metrics.opTimeExclusive)
    logging.debug("checkResults: %s", graph.checkResults(metrics.opTimeExclusive))

    # Inject a synthetic totalTime entry so percentile logic has a denominator.
    metrics.opTimeExclusive["totalTime"] = graph.rootNode.duration
    metrics.opTimeInclusive["totalTime"] = graph.rootNode.duration
    return metrics


def mapReduce(numWorkers: int, jaegerTraceFiles: list, config: common.Config) -> list:
    """Build graph + critical path for each trace file using a process pool."""
    process_fn = partial(process, config=config)
    with mp.Pool(numWorkers) as p:
        metrics = p.map(process_fn, jaegerTraceFiles)
    return metrics


# ---------------------------------------------------------------------------
# Aggregate data structures
# ---------------------------------------------------------------------------

class SummaryResult:
    """
    Holds aggregate measurements across all traces as dictionaries.

    Attributes:
        opTime: flat profile — exclusive or inclusive operation times per trace.
        callpathTime: call-path profile — per-operation call-chain times.
        exampleMap: per call-path worst-case (traceID, spanID, time).
    """

    def __init__(self, opTime: dict, callpathTime: dict, exampleMap: dict) -> None:
        self.opTime = opTime
        self.callpathTime = callpathTime
        self.exampleMap = exampleMap


# ---------------------------------------------------------------------------
# Merge helpers
# ---------------------------------------------------------------------------

def getTraceIdFromFilePath(traceFile: str) -> str:
    """Extract trace ID from a path like /foo/bar/73212187.json → '73212187'."""
    return traceFile.split("/")[-1].split(".")[0]


def mergeCallChains(callMap: dict, totalCallMap: dict) -> None:
    """Collect all call chains per operation name into totalCallMap."""
    for opName, names in callMap.items():
        if opName not in totalCallMap:
            totalCallMap[opName] = set()
        for name in names:
            totalCallMap[opName].add(name)


def mergeCallpathTime(
    callMap: dict,
    callPathMap: dict,
    totalBreakdownTime: dict,
) -> None:
    """Collect per-call-path times into totalBreakdownTime."""
    for opName, paths in callMap.items():
        if opName not in totalBreakdownTime:
            totalBreakdownTime[opName] = {}
        for p in paths:
            if p not in totalBreakdownTime[opName]:
                totalBreakdownTime[opName][p] = []
            totalBreakdownTime[opName][p].append(callPathMap[p])


def mergeExampleID(
    traceID: str,
    localExampleMap: dict,
    exampleMap: dict,
) -> None:
    """Maintain the worst-case example (traceID, spanID, time) per call path."""
    for opName in localExampleMap:
        if opName not in exampleMap:
            exampleMap[opName] = (
                traceID,
                localExampleMap[opName][0],
                localExampleMap[opName][1],
            )
        elif localExampleMap[opName][1] > exampleMap[opName][2]:
            exampleMap[opName] = (
                traceID,
                localExampleMap[opName][0],
                localExampleMap[opName][1],
            )


def aggregateMetrics(
    metrics: list,
    jaegerTraceFiles: list,
) -> tuple:
    """Compute aggregate exclusive/inclusive SummaryResult and aggregateCallMap."""
    exclusive = SummaryResult({}, {}, {})
    inclusive = SummaryResult({}, {}, {})
    aggregateCallMap: dict = {}

    for i, traceFile in enumerate(jaegerTraceFiles):
        traceID = getTraceIdFromFilePath(traceFile)

        exclusive.opTime[traceID] = metrics[i].opTimeExclusive
        inclusive.opTime[traceID] = metrics[i].opTimeInclusive

        mergeCallChains(
            callMap=metrics[i].callChain,
            totalCallMap=aggregateCallMap,
        )
        mergeCallpathTime(
            callMap=metrics[i].callChain,
            callPathMap=metrics[i].callpathTimeExlusive,
            totalBreakdownTime=exclusive.callpathTime,
        )
        mergeCallpathTime(
            callMap=metrics[i].callChain,
            callPathMap=metrics[i].callpathTimeInclusive,
            totalBreakdownTime=inclusive.callpathTime,
        )
        mergeExampleID(
            traceID=traceID,
            localExampleMap=metrics[i].exclusiveExampleMap,
            exampleMap=exclusive.exampleMap,
        )
        mergeExampleID(
            traceID=traceID,
            localExampleMap=metrics[i].inclusiveExampleMap,
            exampleMap=inclusive.exampleMap,
        )

    return exclusive, inclusive, aggregateCallMap


def getOutputDir(args: Any) -> str:
    """Return the output directory: file's parent dir if --file was given, else inputDir."""
    if args.file is not None:
        return os.path.dirname(args.file.name)
    return args.inputDir


# ---------------------------------------------------------------------------
# DataFrame / percentile helpers
# ---------------------------------------------------------------------------

class PVal:
    """Holds percentile values and percentages for one percentile level."""

    def __init__(self, percentile: float, percentileStr: str) -> None:
        self.percentile = percentile
        self.percentileStr = percentileStr
        self.pVal: dict = {}
        self.pPct: dict = {}

    def percentileWithPercentSign(self) -> str:
        return self.percentileStr + "%"


def insertInDF(
    metric: SummaryResult,
    opsStableOrder: list,
    traceIDsStableOrder: list,
) -> "pd.DataFrame":
    """Build a DataFrame[traceID × operation] from per-trace op times."""
    df = pd.DataFrame(index=traceIDsStableOrder)
    for op in opsStableOrder:
        opColumn = [
            metric.opTime[trace].get(op, 0) for trace in traceIDsStableOrder
        ]
        df.insert(len(df.columns), op, opColumn)
    return df


def addPercentileColumns(
    df: "pd.DataFrame",
    percentiles: tuple,
) -> "pd.DataFrame":
    """
    Transpose df and prepend percentile (value + pct-of-totalTime) columns.

    Input shape:  traceID  × operation
    Output shape: operation × (P50 P95 P99 P50% P95% P99%  traceID…)
    """
    columnsToAdd: dict = {}
    for p in percentiles:
        columnsToAdd[p.percentileStr] = []
        columnsToAdd[p.percentileWithPercentSign()] = []

    for p in percentiles:
        denominator = df["totalTime"].quantile(p.percentile)
        for col in df:
            nonZeros = df[col].loc[df[col] != 0]
            if len(nonZeros) == 0:
                p.pVal[col] = 0
                p.pPct[col] = 0
            else:
                p.pVal[col] = nonZeros.quantile(p.percentile)
                p.pPct[col] = (p.pVal[col] / denominator) if denominator != 0 else 0
            columnsToAdd[p.percentileStr].append(p.pVal[col])
            columnsToAdd[p.percentileWithPercentSign()].append(p.pPct[col])

    df = df.transpose()

    for i, p in enumerate(percentiles):
        df.insert(i, p.percentileStr, columnsToAdd[p.percentileStr])
    for i, p in enumerate(percentiles):
        df.insert(
            len(percentiles) + i,
            p.percentileWithPercentSign(),
            columnsToAdd[p.percentileWithPercentSign()],
        )

    return df


def insertInclusivePercentileInfoDF(
    df: "pd.DataFrame",
    percentilesInclusive: tuple,
    inclusiveDF: "pd.DataFrame",
) -> "pd.DataFrame":
    """Prepend inclusive-percentile columns from inclusiveDF into df."""
    for idx, p in enumerate(percentilesInclusive):
        df.insert(idx, p.percentileStr, inclusiveDF[p.percentileStr])
    for idx, p in enumerate(percentilesInclusive):
        df.insert(
            len(percentilesInclusive) + idx,
            p.percentileWithPercentSign(),
            inclusiveDF[p.percentileWithPercentSign()],
        )
    return df


def insertOccurenceCol(
    df: "pd.DataFrame",
    jaegerTraceFiles: list,
    nonZeros: "pd.Series",
) -> tuple:
    """Prepend an occurrence-count column (how many traces each op appears on CP)."""
    occurenceColHeader = "occurence (%s)" % len(jaegerTraceFiles)
    # Use integer placeholder so pandas StringDtype columns (pandas 3+) do not reject int counts.
    df.insert(0, occurenceColHeader, [0] * len(df))
    for row_label in df.index:
        df.at[row_label, occurenceColHeader] = int(nonZeros.get(row_label, 0))
    return df, occurenceColHeader


def reindexDescending(
    df: "pd.DataFrame",
    exclusive: SummaryResult,
    prefixColumns: list,
    traceIDIndex: list,
) -> "pd.DataFrame":
    """Sort rows by descending total op time and columns by descending trace total time."""
    opSums = df[traceIDIndex].sum(axis=1).sort_values(ascending=False)
    df = df.reindex(opSums.index.tolist())

    traceIDSorted = sorted(
        traceIDIndex,
        key=lambda x: exclusive.opTime[x].get("totalTime", 0),
        reverse=True,
    )
    return df.reindex(columns=prefixColumns + traceIDSorted)


# --- Remaining functions (makeClickable through main) will be added in PRs 13c–13e ---
