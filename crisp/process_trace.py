# ruff: noqa: I001
import time
import types
import argparse
import glob
import json
import logging
import multiprocessing as mp
import os
import re
from datetime import datetime
from functools import partial, reduce
from typing import Any

import psutil

import numpy as np
import pandas as pd
import yaml

import crisp.common as common
import crisp.flamegraph as flamegraph
from crisp.cct_utils import (
    cct_to_dot,
    create_protobuf_response_with_exemplars,
    parse_cct_file,
)
from crisp.graph import Graph, accumulateInDict
from crisp.metrics.aggregators import (
    MergeCallPathProfilesWithExample,
    MergeCallPathProfilesWithExemplars,
    mergeCallChains,
    mergeExampleID,
)
from crisp.metrics.percentile_calculator import genLatencyPercentile
from crisp.output.csv_generators import (
    genCrossRegionCallsCSVFile,
    genCyclesCSVFile,
    genHypoLatencyCSVFile,
    genSlackDragCSVFile,
    genSummaryCSVFile,
)
from crisp.output.formatters import makeClickable, renameSortableIcon
from crisp.shared.constants import JAEGER_UI_URL
from crisp.shared.models import LatencyData, QuantizedMetrics, SavingData
from crisp.shared.utils import getLeafNodeFromCallPath
from crisp.slack_drag import aggregate_drag_slack_by_callpath, merge_per_method_slack_drag


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
        "-a",
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
        "--computeSlackDrag",
        dest="computeSlackDrag",
        action="store_true",
        default=False,
        required=False,
        help=(
            "Compute per-method Drag/Slack (see slack_drag.py) and emit a slackDrag.csv "
            "output file (default=false). Opt-in: when not set, output is unchanged."
        ),
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
    argParser.add_argument(
        "-o",
        "--outputDir",
        dest="outputDir",
        action="store",
        default=None,
        required=False,
        help="Directory where output files (HTML report, flame graphs) will be written."
             " Defaults to inputDir.",
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
    computeSlackDrag = args.computeSlackDrag
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
        computeSlackDrag=computeSlackDrag,
    )
    c.jaegerTraceFiles = jaegerTraceFiles
    c.outputDir = args.outputDir if args.outputDir else tracesDir
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
    """Process one Jaeger JSON trace file and return its Metrics, or None to skip."""
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
        return None

    if config.ignoreTestTraces and graph.isTestTrace:
        return None

    traceID = getTraceIdFromFilePath(filename)
    criticalPath = graph.findCriticalPath()
    fullErrCP = graph.findErrorsOnCriticalPath()
    (
        totalWork,
        timeSavedOnWork,
        timeSavedOnCPPessimistic,
        timeSavedOnCPOptimistic,
        timeSavedOnCPAllSeries,
    ) = graph.computeTimeSaved()

    metrics = graph.getMetrics(
        traceID,
        criticalPath,
        fullErrCP,
        totalWork,
        timeSavedOnWork,
        timeSavedOnCPPessimistic,
        timeSavedOnCPOptimistic,
        timeSavedOnCPAllSeries,
        {},
    )

    if config.computeSlackDrag and metrics:
        drag = graph.calculateDrag(cp=criticalPath)
        slack = graph.calculateSlack(cp=criticalPath)
        metrics.slackDragPerCallPath = aggregate_drag_slack_by_callpath(graph, drag, slack)

    logging.debug("critical path: %s", criticalPath)
    cpp = metrics.CPMetrics.profile if metrics.CPMetrics else {}

    # Derive flat convenience maps from CPMetrics for downstream aggregation.
    metrics.opTimeExclusive = {path: mv.excl for path, mv in cpp.items()}
    metrics.opTimeInclusive = {path: mv.inc for path, mv in cpp.items()}
    metrics.callpathTimeExlusive = metrics.opTimeExclusive
    metrics.callpathTimeInclusive = metrics.opTimeInclusive
    metrics.exclusiveExampleMap = {path: (mv.exclEx, mv.excl) for path, mv in cpp.items()}
    metrics.inclusiveExampleMap = {path: (mv.incEx, mv.inc) for path, mv in cpp.items()}
    callChain: dict = {}
    for path in cpp:
        op = getLeafNodeFromCallPath(path)
        callChain.setdefault(op, set()).add(path)
    metrics.callChain = callChain

    # Inject a synthetic totalTime entry so percentile logic has a denominator.
    metrics.opTimeExclusive["totalTime"] = graph.rootNode.duration
    metrics.opTimeInclusive["totalTime"] = graph.rootNode.duration
    return metrics


# Trace-size constants used by mapReduce() to cap parallelism when individual
# trace files are large enough to risk OOM.
# Traces with 1M spans can be ~1 GB on disk and expand ~4x in memory as Python
# objects.  Loading 16 such traces concurrently would require ~64 GB of RSS.
_LARGE_TRACE_THRESHOLD_BYTES: int = 100 * 1024 * 1024   # 100 MB — trigger the guard
_IN_MEMORY_EXPANSION: int = 4   # conservative JSON → Python object expansion factor
_MEMORY_HEADROOM_FRACTION: float = 0.8   # use at most 80% of available memory


def _memory_aware_workers(num_workers: int, trace_files: list[str]) -> int:
    """Return a worker count capped so that peak RSS stays within available memory.

    For each file that actually exists, we stat its size (one syscall per
    file — negligible compared to json.load).  When the largest file exceeds
    _LARGE_TRACE_THRESHOLD_BYTES we query psutil for the current available
    memory and apply:

        capped = max(1, int(available * headroom / (max_size * expansion)))

    For small traces (below the threshold) we return num_workers unchanged,
    so the normal code path has essentially zero overhead.
    """
    if num_workers <= 1 or not trace_files:
        return num_workers

    existing = [f for f in trace_files if os.path.exists(f)]
    if not existing:
        return num_workers

    max_trace_bytes = max(os.path.getsize(f) for f in existing)
    if max_trace_bytes <= _LARGE_TRACE_THRESHOLD_BYTES:
        return num_workers

    available_bytes = psutil.virtual_memory().available
    capped = max(1, int(available_bytes * _MEMORY_HEADROOM_FRACTION / (max_trace_bytes * _IN_MEMORY_EXPANSION)))
    if capped < num_workers:
        logging.warning(
            "Large traces detected (max %.2f GB, available %.2f GB); "
            "reducing computeParallelism from %d to %d to avoid OOM",
            max_trace_bytes / (1024 ** 3),
            available_bytes / (1024 ** 3),
            num_workers,
            capped,
        )
        return capped
    return num_workers


def mapReduce(numWorkers: int, jaegerTraceFiles: list, config: common.Config) -> list:
    """Build graph + critical path for each trace file using a process pool."""
    process_fn = partial(process, config=config)
    # Cap workers to avoid OOM when individual trace files are very large.
    numWorkers = _memory_aware_workers(numWorkers, jaegerTraceFiles)
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

        if metrics[i] is None:
            continue

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


# ---------------------------------------------------------------------------
# Display helpers
# ---------------------------------------------------------------------------

def addHyperLinkToTrace(
    df: "pd.DataFrame",
    tracespanIDmap: dict,
    jaegerQueryUrl: str,
) -> "pd.DataFrame":
    """Rename each trace-ID column header to a Jaeger UI hyperlink."""
    hyperLinkHT = {}
    for k, v in tracespanIDmap.items():
        hyperLinkHT[k] = makeClickable(
            "{}/trace/{}?uiFind={}".format(jaegerQueryUrl, k, v), "#"
        )
    df.rename(columns=hyperLinkHT, inplace=True)
    return df


def setCellFormating(
    df: "pd.DataFrame",
    percentiles: tuple,
    occurenceColHeader: str,
) -> dict:
    """Return a format dict: scientific for values, % for percentages, int for occurrence."""
    precisionHT = {col: "{:.2e}" for col in df.columns.values}
    for p in percentiles:
        precisionHT[p.percentileWithPercentSign()] = "{:.2%}"
    precisionHT[occurenceColHeader] = "{:5d}"
    return precisionHT


def cssNameHandle(call_chain: str) -> str:
    """Format a call chain string (A->B->C) as indented HTML lines."""
    lst = call_chain.split("->")
    res = ""
    for i in range(len(lst)):
        for _ in range(i):
            res += " &emsp; "
        res += lst[i] + "</br>  "
    return res


def getSummaryText(
    pval: str,
    pctMap: dict,
    valMap: dict,
    totalBreakdownTime: dict,
    topN: int,
    serviceName: str,
    operationName: str,
) -> str:
    """Build an HTML summary listing top-N operations contributing most to pval."""
    summary = "<h1>Top %d operations contributing to %s of [%s] %s:</h1>" % (
        topN, pval, serviceName, operationName
    )
    res = sorted(pctMap.items(), key=lambda x: x[1], reverse=True)
    res = [item for item in res if item[0] != "totalTime"]
    for idx in range(min(topN, len(res))):
        summary += (
            "<h2>%s. %s -> %s Value: %s, %s percentage: %s, call chains are below:</h2>"
            % (
                idx + 1,
                res[idx][0],
                pval,
                "{:.2e}".format(valMap[res[idx][0]]),
                pval,
                "{:.2%}".format(pctMap[res[idx][0]]),
            )
        )
        cc = totalBreakdownTime[res[idx][0]]
        sumCC = sum(t for _, times in cc.items() for t in times)
        sortedCC = sorted(cc.items(), key=lambda x: sum(x[1]), reverse=True)
        for chain, times in sortedCC:
            summary += cssNameHandle(
                chain + "</br>" + "Contributing: {:.2%}".format(
                    sum(times) / sumCC if sumCC != 0 else 1.0
                )
            )
            summary += "</br>"
    return summary


def getTopNCCTs(
    sortedContexts: list,
    sumTime: float,
    n: int,
    exampleMap: dict,
    jaegerQueryUrl: str,
) -> str:
    """Return HTML for the top-N calling contexts with Jaeger example links."""
    res = ""
    for i in range(min(len(sortedContexts), n)):
        chain, times = sortedContexts[i]
        traceID, spanID, _ = exampleMap[chain]
        res += (
            cssNameHandle(
                chain + "</br>" + "Contributing: {:.2%}".format(
                    sum(times) / sumTime if sumTime != 0 else 0
                )
            )
            + makeClickable(
                "{}/trace/{}?uiFind={}".format(jaegerQueryUrl, traceID, spanID),
                "Example",
            )
            + "</br></br>"
        )
    return res


def sum2DCCT(cct: list) -> float:
    """Sum all time values across a list of (chain, [times]) pairs."""
    return sum(t for _, times in cct for t in times)


def addToolTip(
    df: "pd.DataFrame",
    exclusive: SummaryResult,
    inclusive: SummaryResult,
    ignoreSet: set,
    jaegerQueryUrl: str,
) -> "pd.DataFrame":
    """Replace each row-header (operation) with a tooltip showing top call chains."""
    renameRowHT = {}
    for i, idx in enumerate(df.index[:]):
        if idx in ignoreSet:
            continue
        cc = exclusive.callpathTime[idx]
        sortedCC = sorted(cc.items(), key=lambda x: sum(x[1]), reverse=True)
        ccInc = inclusive.callpathTime[idx]
        sortedCCInc = sorted(ccInc.items(), key=lambda x: sum(x[1]), reverse=True)
        sumCC = sum2DCCT(sortedCC)
        sumCCInc = sum2DCCT(sortedCCInc)

        res = "Exclusive:</br>"
        res += getTopNCCTs(sortedCC, sumCC, 5, exclusive.exampleMap, jaegerQueryUrl)
        res += "Inclusive:</br>"
        res += getTopNCCTs(sortedCCInc, sumCCInc, 5, inclusive.exampleMap, jaegerQueryUrl)
        renameRowHT[df.index[i]] = (
            '<div class="tooltip">%s '
            '<span class="tooltiptext">%s</span> </div>' % (df.index[i], res)
        )
    df.rename(index=renameRowHT, inplace=True)
    return df


def getGradientFormatFromDataframe(
    df: "pd.DataFrame",
    precisionHT: dict,
    firstSortableColumn: int,
    lastSortableColumns: int,
) -> str:
    """Apply a purple gradient background to the numeric cells and return HTML."""
    return (
        df.style.background_gradient(
            axis=0,
            cmap="BuPu",
            subset=(
                df.index.values[firstSortableColumn:],
                df.columns.values[lastSortableColumns:],
            ),
        )
        .set_table_attributes('class="table-sortable"')
        .set_properties(**{"text-align": "right"})
        .format(precisionHT)
        .to_html()
    )


def heatmapAndSummary(
    exclusive: SummaryResult,
    inclusive: SummaryResult,
    aggregateCallMap: dict,
    traceIDIndex: list,
    traceToRootspanMap: dict,
    config: common.Config,
    jaegerTraceFiles: list,
) -> tuple:
    """
    Build the heatmap HTML table, textual summary, and JSON critical-path export.

    Returns:
        (heatmap_html, summary_html, criticalPathJSONStr)
    """
    allOps = list(aggregateCallMap.keys()) + ["totalTime"]
    opsStableOrder = sorted(allOps)
    traceIDsStableOrder = sorted(traceIDIndex)

    exclusiveDF = insertInDF(exclusive, opsStableOrder, traceIDsStableOrder)
    inclusiveDF = insertInDF(inclusive, opsStableOrder, traceIDsStableOrder)

    nonZeroOpCounts = exclusiveDF.astype(bool).sum(axis=0)

    percentilesExclusive = (PVal(0.5, "P50(E)"), PVal(0.95, "P95(E)"), PVal(0.99, "P99(E)"))
    exclusiveDF = addPercentileColumns(exclusiveDF, percentilesExclusive)

    percentilesInclusive = (PVal(0.5, "P50(I)"), PVal(0.95, "P95(I)"), PVal(0.99, "P99(I)"))
    inclusiveDF = addPercentileColumns(inclusiveDF, percentilesInclusive)

    df = insertInclusivePercentileInfoDF(exclusiveDF, percentilesInclusive, inclusiveDF)
    df, occurenceColHeader = insertOccurenceCol(df, jaegerTraceFiles, nonZeroOpCounts)

    numColsToRetain = 1 + 2 * (len(percentilesExclusive) + len(percentilesInclusive))
    unmodifiedPrefix = df.columns.values.tolist()[:numColsToRetain]

    df = reindexDescending(df, exclusive, unmodifiedPrefix, traceIDIndex)

    # Truncate to configured limits.
    df = df.iloc[: config.numOperation, : numColsToRetain + config.numHMTrace]

    criticalPathJSONStr = df.to_json()

    df = addHyperLinkToTrace(df, traceToRootspanMap, config.jaegerQueryUrl)
    df = renameSortableIcon(
        df,
        [x.percentileStr for x in percentilesInclusive + percentilesExclusive],
    )
    precisionHT = setCellFormating(
        df, percentilesExclusive + percentilesInclusive, occurenceColHeader
    )
    df = addToolTip(df, exclusive, inclusive, ignoreSet={"totalTime"}, jaegerQueryUrl=config.jaegerQueryUrl)

    firstSortableColumn = 1
    lastSortableColumns = firstSortableColumn + 2 * (
        len(percentilesExclusive) + len(percentilesInclusive)
    )

    summary = ""
    for p in percentilesExclusive:
        summary += getSummaryText(
            p.percentileStr,
            p.pPct,
            p.pVal,
            exclusive.callpathTime,
            config.topN,
            config.serviceName,
            config.operationName,
        )

    heatmap = getGradientFormatFromDataframe(
        df, precisionHT, firstSortableColumn, numColsToRetain
    )
    return heatmap, summary, criticalPathJSONStr


# ---------------------------------------------------------------------------
# Anonymization helpers
# ---------------------------------------------------------------------------

# Module-level anonymization state (reset each process run via sanitizeNames).
_saniMap: dict = {"totalTime": "totalTime"}
_saniCtr: int = 0


def replaceNonAlphaNumericWithUnderscore(s: str) -> str:
    return re.sub("[^a-zA-Z0-9_]+", "_", s)


def sanitized(op: str) -> str:
    """Map each service::operation token to a generic 'ServiceN::OperationN' label."""
    global _saniCtr, _saniMap
    ret = ""
    for piece in op.split("->"):
        if ret:
            ret += "->"
        if piece in _saniMap:
            ret += _saniMap[piece]
        else:
            _saniCtr += 1
            label = "Service::Operation" + str(_saniCtr)
            _saniMap[piece] = label
            ret += label
    return ret


def sanitizeNames(metrics: list) -> None:
    """Anonymize all service/operation names in-place across a list of Metrics."""
    for r in metrics:
        for field in [
            r.opTimeExclusive,
            r.callpathTimeExlusive,
            r.exclusiveExampleMap,
            r.opTimeInclusive,
            r.callpathTimeInclusive,
            r.inclusiveExampleMap,
        ]:
            for k, v in list(field.items()):
                del field[k]
                field[sanitized(k)] = v
        for k, vals in list(r.callChain.items()):
            del r.callChain[k]
            sk = sanitized(k)
            r.callChain[sk] = {sanitized(v) for v in vals}


# ---------------------------------------------------------------------------
# HTML / JSON output
# ---------------------------------------------------------------------------

def genCriticalPathFiles(
    flameGraphPctFilePair,
    errCPFlameGraphPctFilePair,
    criticalPathJSONStr,
    tailLatencyPercentile,
    numDiscardedDueToRootError,
    numDiscardedTestTraces,
    heatMap,
    summary,
    c: common.Config,
):
    """Write criticalPaths.html and crisp.json to the output directory."""
    outputDir = c.getOutputDir()
    criticalPathHTMLFile = os.path.join(outputDir, "criticalPaths.html")
    logging.info(
        "[%s]%s critical path file %s",
        c.serviceName,
        c.operationName,
        criticalPathHTMLFile,
    )

    with open(criticalPathHTMLFile, "w") as f:
        f.write(HTML_PREFIX + heatMap)
        f.write(HTML_GENERATION_TIME)
        for pval, file in flameGraphPctFilePair:
            src = os.path.basename(file)
            f.write(f"<div> <h2>{pval} flame graph. </h2> <img src={src}></div>")
        f.write(summary)
        f.write(HTML_SUFFIX)

        for pval, file in errCPFlameGraphPctFilePair:
            src = os.path.basename(file)
            f.write(f"<div> <h2>{pval} error flame graph. </h2> <img src={src}></div>")

        f.write("<div> <h3>")
        for pval, sampleCount, latencyData in tailLatencyPercentile:
            latency = round(latencyData.latency, 2)
            hypo = round(latencyData.hypoLatency, 2)
            if latency == 0.0:
                continue
            reduction = round((1 - (hypo / latency)) * 100, 2)
            f.write(
                "Top %s&#37 latency (based on %d traces) will be reduced by %.2f&#37 (%.2f => %.2f) after error removal<br/>"  # noqa: UP031
                % (pval, sampleCount, reduction, latency, hypo),
            )
        f.write("</h3></div>")

        f.write("<div> <h3>")
        f.write(
            "Number of traces discarded due to root errors: %d"  # noqa: UP031
            % (numDiscardedDueToRootError),
        )
        f.write("<br/>")
        f.write("Number of test traces discarded: %d" % (numDiscardedTestTraces))  # noqa: UP031
        f.write("</h3></div>")

    jsonPath = os.path.join(outputDir, "crisp.json")
    with open(jsonPath, "w") as f:
        f.write(criticalPathJSONStr)

    return criticalPathHTMLFile, jsonPath


# ---------------------------------------------------------------------------
# Latency / time-saved summary
# ---------------------------------------------------------------------------

def genTimeSavedSummary(metrics, c: common.Config):
    """Compute head/tail/hypo latency percentile summaries, filtering test/root-error traces.

    Returns:
        (pointLantencyPercentileObj, headLatencyPercentile,
         tailLatencyPercentile, hypoLatencyPercentile)
    Each element is a list of (percentile, num_traces, LatencyData) tuples.
    """
    latencyHypo = []
    percentiles = [1, 5, 10, 50, 90, 95, 99, 100]

    for m in metrics:
        if m.rootReturnError or (m.isTestTrace and c.ignoreTestTraces):
            continue
        latency = m.latency
        hypoLatency = latency - m.timeSavedOnCPAllSeries
        hypoLatencyPessimistic = latency - m.timeSavedOnCPPessimistic
        hypoLatencyOptimistic = latency - m.timeSavedOnCPOptimistic
        assert hypoLatency >= 0
        latencyHypo.append(
            LatencyData(
                m.traceID,
                latency,
                hypoLatency,
                hypoLatencyOptimistic,
                hypoLatencyPessimistic,
            ),
        )

    headLatencyPercentile = genLatencyPercentile(
        latencyHypo, percentiles[3:], lambda x: x.latency, tailLatency=False,
    )
    tailLatencyPercentile = genLatencyPercentile(
        latencyHypo, percentiles, lambda x: x.latency, tailLatency=True,
    )
    hypoLatencyPercentile = genLatencyPercentile(
        latencyHypo, percentiles[3:], lambda x: x.hypoLatency, tailLatency=False,
    )

    pointLatencyVal = [x.latency for x in latencyHypo]
    pointLantencyPercentile = (
        np.percentile(pointLatencyVal, percentiles) if len(pointLatencyVal) > 0 else []
    )
    pointLantencyPercentileObj = [
        (p, len(latencyHypo), LatencyData("", x, 0, 0, 0))
        for p, x in zip(percentiles, pointLantencyPercentile)
    ]

    return (
        pointLantencyPercentileObj,
        headLatencyPercentile,
        tailLatencyPercentile,
        hypoLatencyPercentile,
    )


def computeSumPercent(df, numeratorCol, denominatorCol):
    """Return sum(numerator) / sum(denominator), or np.nan if denominator is 0."""
    denominator = df[denominatorCol].sum()
    if denominator == 0:
        return np.nan
    return df[numeratorCol].sum() / denominator


def computeAveRow(df, traceTag):
    """Build a summary row with averages and weighted-sum percentages for a subset DataFrame."""
    aveRow = df.mean(numeric_only=True)
    aveRow[common.TRACE_COL] = traceTag
    aveRow[common.TRACE_TYPE_COL] = "AVERAGE_MIN_MAX"
    aveRow[common.PERCENT_WORK_SAVED_COL] = computeSumPercent(
        df, common.WORK_SAVED_COL, common.WORK_COL,
    )
    aveRow[common.PERCENT_LATENCY_SAVED_COL] = computeSumPercent(
        df, common.TIME_SAVED_ON_CP_COL, common.LATENCY_COL,
    )
    aveRow[common.PERCENT_CP_ERRORS_COL] = computeSumPercent(
        df, common.NUM_CP_ERRORS_COL, common.NUM_ERRORS_COL,
    )
    aveRow[common.PERCENT_CONNECTED_TO_CP_ERRORS_COL] = computeSumPercent(
        df, common.NUM_CONNECTED_TO_CP_ERRORS_COL, common.NUM_ERRORS_COL,
    )
    return aveRow


# ---------------------------------------------------------------------------
# Per-trace CSV
# ---------------------------------------------------------------------------

def _emitNonTransitiveData(traceStatsDf, config):
    """No-op stub — internal metric emission is not available in the OSS build."""


def genTraceCSVFile(metrics, c: common.Config):
    """Write traceStats.csv and return error-percentage lists for downstream callers.

    Returns:
        (traceStatsCSV, numDiscardedDueToRootError, numDiscardedTestTraces,
         percCPErrList, percConnectedToCPErrList)
    """
    outputDir = c.getOutputDir()
    traceStatsCSV = os.path.join(outputDir, common.TRACE_STATS_CSV)
    logging.info(
        "[%s]%s trace info file %s",
        c.serviceName,
        c.operationName,
        traceStatsCSV,
    )
    headerList = [
        common.TRACE_COL,
        common.TRACE_TYPE_COL,
        common.WORK_COL,
        common.WORK_SAVED_COL,
        common.PERCENT_WORK_SAVED_COL,
        common.LATENCY_COL,
        common.TIME_SAVED_ON_CP_COL,
        common.PERCENT_LATENCY_SAVED_COL,
        common.NUM_ERRORS_COL,
        common.NUM_CP_ERRORS_COL,
        common.PERCENT_CP_ERRORS_COL,
        common.NUM_CONNECTED_TO_CP_ERRORS_COL,
        common.PERCENT_CONNECTED_TO_CP_ERRORS_COL,
        common.NUM_NODES_ON_CP_COL,
        common.NUM_NODES_COL,
        common.MAX_DEPTH_COL,
        common.NUM_SELF_ERRORS_COL,
        common.MAX_ERR_DEPTH_PROP_TO_ROOT_COL,
        common.MIN_DEPTH_NON_ROOT_SELF_ERRORS_COL,
        common.MAX_DEPTH_NON_ROOT_SELF_ERRORS_COL,
        common.P50_DEPTH_NON_ROOT_SELF_ERRORS_COL,
        common.P90_DEPTH_NON_ROOT_SELF_ERRORS_COL,
        common.P95_DEPTH_NON_ROOT_SELF_ERRORS_COL,
        common.P99_DEPTH_NON_ROOT_SELF_ERRORS_COL,
        common.MIN_PERCENT_DEPTH_NON_ROOT_SELF_ERRORS_COL,
        common.MAX_PERCENT_DEPTH_NON_ROOT_SELF_ERRORS_COL,
        common.P50_PERCENT_DEPTH_NON_ROOT_SELF_ERRORS_COL,
        common.P90_PERCENT_DEPTH_NON_ROOT_SELF_ERRORS_COL,
        common.P95_PERCENT_DEPTH_NON_ROOT_SELF_ERRORS_COL,
        common.P99_PERCENT_DEPTH_NON_ROOT_SELF_ERRORS_COL,
    ]
    numDiscardedDueToRootError = 0
    numDiscardedTestTraces = 0

    metricsSorted = sorted(
        metrics,
        key=lambda x: (x.timeSavedOnCPAllSeries / x.latency) if x.latency != 0 else 0,
        reverse=True,
    )

    rowList = []
    for m in metricsSorted:
        traceID = m.traceID
        totalWork = m.totalWork
        if totalWork == 0:
            continue
        timeSavedOnWork = m.timeSavedOnWork
        workSavedPct = timeSavedOnWork / totalWork
        latency = m.latency
        timeSavedOnCP = m.timeSavedOnCPAllSeries
        CPSavedPct = timeSavedOnCP / latency
        traceLink = '=HYPERLINK("' + JAEGER_UI_URL + traceID + '", "' + traceID + '")'

        numAllErrors = m.errMetrics.numAllErrors
        numCPErrors = m.errCPMetrics.numCPErrors
        numRelatedToCPErrors = m.errCPMetrics.numRelatedToCPErrors
        if numAllErrors > 0:
            percentCPErrors = numCPErrors / numAllErrors
            percentConnectedToCPErrors = numRelatedToCPErrors / numAllErrors
        else:
            percentCPErrors = np.nan
            percentConnectedToCPErrors = np.nan

        if m.isTestTrace and c.ignoreTestTraces:
            traceType = "TEST_TRACE"
            numDiscardedTestTraces += 1
        elif m.rootReturnError:
            traceType = "ROOT_ERROR"
            numDiscardedDueToRootError += 1
        else:
            traceType = "NON_ROOT_ERROR"

        depthList = m.errMetrics.selfErrDepthList
        if traceType == "ROOT_ERROR":
            depthList = depthList[1:]
        if len(depthList) > 0:
            minV = np.percentile(depthList, 0)
            medianV = np.percentile(depthList, 50)
            p90V = np.percentile(depthList, 90)
            p95V = np.percentile(depthList, 95)
            p99V = np.percentile(depthList, 99)
            maxV = np.percentile(depthList, 100)
            percMinV = minV / m.depth
            percMedianV = medianV / m.depth
            percP90V = p90V / m.depth
            percP95V = p95V / m.depth
            percP99V = p99V / m.depth
            percMaxV = maxV / m.depth
        else:
            minV = medianV = p90V = p95V = p99V = maxV = np.nan
            percMinV = percMedianV = percP90V = percP95V = percP99V = percMaxV = np.nan

        maxErrDepthPropToRoot = m.errMetrics.maxErrDepthPropToRoot
        if maxErrDepthPropToRoot == -1:
            maxErrDepthPropToRoot = np.nan

        row = [
            traceLink,
            traceType,
            totalWork,
            timeSavedOnWork,
            workSavedPct,
            latency,
            timeSavedOnCP,
            CPSavedPct,
            numAllErrors,
            numCPErrors,
            percentCPErrors,
            numRelatedToCPErrors,
            percentConnectedToCPErrors,
            m.numNodesOnCP,
            m.numNodes,
            m.depth,
            len(depthList),
            maxErrDepthPropToRoot,
            minV,
            maxV,
            medianV,
            p90V,
            p95V,
            p99V,
            percMinV,
            percMaxV,
            percMedianV,
            percP90V,
            percP95V,
            percP99V,
        ]
        rowList.append(row)

    df = pd.DataFrame(rowList, columns=headerList)
    percCPErrList = percConnectedToCPErrList = []
    nonTestDf = df.loc[df[common.TRACE_TYPE_COL] != "TEST_TRACE"]
    rootErrDf = df.loc[df[common.TRACE_TYPE_COL] == "ROOT_ERROR"]
    nonRootErrDf = df.loc[df[common.TRACE_TYPE_COL] == "NON_ROOT_ERROR"]

    _emitNonTransitiveData(traceStatsDf=df, config=c)

    if len(rootErrDf.index) > 0:
        rootErrAveRow = computeAveRow(rootErrDf, common.ROOT_ERROR_TRACES_STATS_TAG)
        df.loc[len(df)] = rootErrAveRow

    if len(nonRootErrDf.index) > 0:
        nonTestAveRow = computeAveRow(nonTestDf, common.NON_TEST_TRACES_STATS_TAG)
        nonRootErrAveRow = computeAveRow(
            nonRootErrDf, common.NON_ROOT_ERROR_TRACES_STATS_TAG,
        )

        percCPErrCol = nonRootErrDf[common.PERCENT_CP_ERRORS_COL]
        percCPErrList = percCPErrCol[pd.notna(percCPErrCol)].tolist()
        percConnectedToCPErrCol = nonRootErrDf[common.PERCENT_CONNECTED_TO_CP_ERRORS_COL]
        percConnectedToCPErrList = percConnectedToCPErrCol[pd.notna(percCPErrCol)].tolist()

        df.loc[len(df)] = nonTestAveRow
        df.loc[len(df)] = nonRootErrAveRow

    df.to_csv(traceStatsCSV, index=False)

    return (
        traceStatsCSV,
        numDiscardedDueToRootError,
        numDiscardedTestTraces,
        percCPErrList,
        percConnectedToCPErrList,
    )


# ---------------------------------------------------------------------------
# Error analysis helpers
# ---------------------------------------------------------------------------

def genPercentErrorFile(percCPErrList, percConnectedToCPErrList, c: common.Config):
    """Write percentError.csv — histogram of CP-error and connected-to-CP-error rates."""
    outputDir = c.getOutputDir()
    percentErrorFile = os.path.join(outputDir, common.PERCENT_ERROR_CSV)
    logging.info(
        "[%s]%s percent error file %s",
        c.serviceName, c.operationName, percentErrorFile,
    )
    fixedBins = [0.0, 1e-10, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    hist1, bins = np.histogram(percCPErrList, bins=fixedBins)
    hist2, bins = np.histogram(percConnectedToCPErrList, bins=fixedBins)
    df = pd.DataFrame(
        {
            common.PERCENT_ERRORS_COL: bins[0:11],
            common.NUM_TRACES_FOR_CP_ERRORS_COL: hist1,
            common.NUM_TRACES_FOR_CONNECTED_TO_CP_ERRORS_COL: hist2,
        },
    )
    df = df.sort_values(by=[common.PERCENT_ERRORS_COL])
    df.to_csv(percentErrorFile, index=False)
    return percentErrorFile


def genSavingPotential(metrics, c: common.Config):
    """Write savingPotential.csv — per-op error-removal saving potential."""
    outputDir = c.getOutputDir()
    savingPotentialFile = os.path.join(outputDir, common.SAVING_POTENTIAL_CSV)
    logging.info(
        "[%s]%s saving potential file %s",
        c.serviceName, c.operationName, savingPotentialFile,
    )
    finalSavingMap = {}
    for m in metrics:
        if m.rootReturnError or (m.isTestTrace and c.ignoreTestTraces):
            continue
        latency = m.latency
        saving = m.errCPMetrics.savingPotential
        for k, v in saving.items():
            accumulateInDict(finalSavingMap, k, SavingData(v.timeSaved, latency, v.opCount))

    savingHeader = [
        common.OP_NAME_COL,
        common.TIME_SAVED_ON_CP_COL,
        common.LATENCY_COL,
        common.NUM_OP_COL,
    ]
    df = pd.DataFrame(
        [(k, v.timeSaved, v.latency, v.opCount) for k, v in finalSavingMap.items()],
        columns=savingHeader,
    )
    df[common.PERCENT_LATENCY_SAVED_COL] = (
        df[common.TIME_SAVED_ON_CP_COL] / df[common.LATENCY_COL]
    )
    df = df.sort_values(by=[common.PERCENT_LATENCY_SAVED_COL])
    df.to_csv(savingPotentialFile, index=False)
    return savingPotentialFile


def genErrStatsFiles(metrics, c: common.Config):
    """Write five error-stats CSVs: errDepth, percentErrDepth, errPropLength, resiliency, perTraceErrInfo."""
    outputDir = c.getOutputDir()
    errDepthFile = os.path.join(outputDir, common.ERROR_DEPTH_CSV)
    percentErrDepthFile = os.path.join(outputDir, common.PERCENT_ERROR_DEPTH_CSV)
    errPropLengthFile = os.path.join(outputDir, common.ERROR_PROP_LENGTH_CSV)
    resiliencyFile = os.path.join(outputDir, common.RESILIENCY_CSV)
    perTraceErrInfoFile = os.path.join(outputDir, common.PER_TRACE_ERR_INFO_CSV)
    for label, path in [
        ("error depth", errDepthFile),
        ("percent error depth", percentErrDepthFile),
        ("error prop length", errPropLengthFile),
        ("resiliency", resiliencyFile),
        ("perTraceErrInfo", perTraceErrInfoFile),
    ]:
        logging.info("[%s]%s %s file %s", c.serviceName, c.operationName, label, path)

    finalDepthMap = {}
    finalPropLengthMap = {}
    finalResiliencyMap = {}
    percentSelfErrDepth = []
    percentStoppedErrDepth = []
    propagationPerTraceHistoAll = []

    for m in metrics:
        if m.isTestTrace and c.ignoreTestTraces:
            continue
        hasErr = common.HAS_ROOT_ERR if m.rootReturnError else common.NO_ROOT_ERR
        for propType, quantizedErr in zip(
            [
                common.PRORP_TO_ROOT,
                common.NOT_PRORP_TO_ROOT,
                common.PRORP_TO_ROOT_ON_CP,
                common.NOT_PRORP_TO_ROOT_ON_CP,
                common.SUPRESSED_ERR,
                common.SUPRESSED_ERR_ON_CP,
            ],
            [
                m.errMetrics.propToRootHistoQuantized,
                m.errMetrics.notPropToRootHistoQuantized,
                m.errMetrics.propToRootOnCPHistoQuantized,
                m.errMetrics.notPropToRootOnCPHistoQuantized,
                m.errMetrics.supressHistoQuantized,
                m.errMetrics.supressOnCPHistoQuantized,
            ],
        ):
            if quantizedErr.isValid:
                propagationPerTraceHistoAll.append(
                    [
                        m.traceID,
                        *[hasErr, propType, str(m.numNodes), str(m.depth), str(m.numNodesOnCP)],
                        *quantizedErr.getRow(),
                    ],
                )
        if m.rootReturnError:
            continue
        trace_depth = m.depth
        for key, val in m.errMetrics.errDepthMap.items():
            accumulateInDict(finalDepthMap, key, val)
        percentSelfErrDepth = [d / trace_depth for d in m.errMetrics.selfErrDepthList]
        percentStoppedErrDepth = [d / trace_depth for d in m.errMetrics.stoppedErrDepthList]
        for key, val in m.errMetrics.errPropLengthMap.items():
            accumulateInDict(finalPropLengthMap, key, val)
        for key, val in m.errMetrics.resiliencyMap.items():
            accumulateInDict(finalResiliencyMap, key, val)

    propagationPerTraceHeader = [
        common.TRACE_COL,
        common.ROOT_HAS_ERR_COL,
        common.METRIC_COL,
        common.NUM_NODES_COL,
        common.MAX_DEPTH_COL,
        common.NUM_NODES_ON_CP_COL,
        *QuantizedMetrics.headers,
    ]
    pd.DataFrame(propagationPerTraceHistoAll, columns=propagationPerTraceHeader).to_csv(
        perTraceErrInfoFile, index=False,
    )

    errDepthHeader = [
        common.DEPTH_COL, common.NUM_SELF_ERRORS_COL,
        common.NUM_PROPAGATED_ERRORS_COL, common.NUM_STOPPED_ERRORS_COL,
    ]
    df = pd.DataFrame(
        [(k, v.selfErrors, v.propagatedErrors, v.stoppedErrors) for k, v in finalDepthMap.items()],
        columns=errDepthHeader,
    )
    df.sort_values(by=[common.DEPTH_COL]).to_csv(errDepthFile, index=False)

    fixedBins = [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0]
    hist1, bins = np.histogram(percentSelfErrDepth, bins=fixedBins)
    hist2, bins = np.histogram(percentStoppedErrDepth, bins=fixedBins)
    pd.DataFrame(
        {common.PERCENT_DEPTH_COL: bins[0:10], common.NUM_SELF_ERRORS_COL: hist1, common.NUM_STOPPED_ERRORS_COL: hist2},
    ).sort_values(by=[common.PERCENT_DEPTH_COL]).to_csv(percentErrDepthFile, index=False)

    pd.DataFrame(list(finalPropLengthMap.items()), columns=[common.PROPAGATION_LENGTH_COL, common.NUM_SELF_ERRORS_COL]).sort_values(
        by=[common.PROPAGATION_LENGTH_COL],
    ).to_csv(errPropLengthFile, index=False)

    resiliencyHeader = [common.OP_NAME_COL, common.NUM_STOPPED_ERRORS_COL, common.NUM_PROPAGATED_ERRORS_COL]
    df = pd.DataFrame(
        [(k, v.stoppedErrors, v.propagatedErrors) for k, v in finalResiliencyMap.items()],
        columns=resiliencyHeader,
    )
    df[common.RESILIENCY_COL] = df[common.NUM_STOPPED_ERRORS_COL] / (
        df[common.NUM_STOPPED_ERRORS_COL] + df[common.NUM_PROPAGATED_ERRORS_COL]
    )
    df.to_csv(resiliencyFile, index=False)

    return errDepthFile, percentErrDepthFile, errPropLengthFile, resiliencyFile, perTraceErrInfoFile


def computeSelfErrDepthMaps(depthMap, percentDepthMap, percentile, depthList, traceDepth):
    """Compute percentile depth and update both absolute and relative depth maps."""
    depth = np.percentile(depthList, percentile)
    percDepth = np.round(depth / traceDepth, decimals=1)
    accumulateInDict(depthMap, np.round(depth, decimals=0), 1)
    accumulateInDict(percentDepthMap, np.round(percDepth, decimals=1), 1)


def combineSelfErrToNumTracesData(mapList, first_col, colList):
    """Outer-join a list of {value: count} dicts into a single sorted DataFrame."""
    dfList = [
        pd.DataFrame(list(mapList[i].items()), columns=[first_col, colList[i]])
        for i in range(len(mapList))
    ]
    df = reduce(lambda x, r: pd.merge(x, r, on=first_col, how="outer"), dfList)
    df = df.sort_values(by=[first_col])
    return df.reindex(columns=[first_col, *colList])


def genMaxErrDepthPropToRootToNumTracesFiles(metrics, c: common.Config):
    """Write maxErrDepthPropToRoot and percentMaxErrDepthPropToRoot CSVs."""
    outputDir = c.getOutputDir()
    maxErrDepthFile = os.path.join(outputDir, common.MAX_ERROR_DEPTH_PROP_TO_ROOT_TO_NUM_TRACES_CSV)
    percentMaxErrDepthFile = os.path.join(outputDir, common.PERCENT_MAX_ERROR_DEPTH_PROP_TO_ROOT_TO_NUM_TRACES_CSV)
    logging.info("[%s]%s max error depth propagated to root file %s", c.serviceName, c.operationName, maxErrDepthFile)
    logging.info("[%s]%s percent max error depth propagated to root file %s", c.serviceName, c.operationName, percentMaxErrDepthFile)

    depthMap = {}
    percDepthMap = {}
    for m in metrics:
        if (not m.rootReturnError) or (m.isTestTrace and c.ignoreTestTraces):
            continue
        maxDepthPropToRoot = m.errMetrics.maxErrDepthPropToRoot
        accumulateInDict(depthMap, maxDepthPropToRoot, 1)
        accumulateInDict(percDepthMap, np.round(maxDepthPropToRoot / m.depth, decimals=1), 1)

    pd.DataFrame(list(depthMap.items()), columns=[common.MAX_ERR_DEPTH_PROP_TO_ROOT_COL, common.NUM_TRACES_COL]).sort_values(
        by=[common.MAX_ERR_DEPTH_PROP_TO_ROOT_COL],
    ).to_csv(maxErrDepthFile, index=False)
    pd.DataFrame(list(percDepthMap.items()), columns=[common.PERCENT_MAX_ERR_DEPTH_PROP_TO_ROOT_COL, common.NUM_TRACES_COL]).sort_values(
        by=[common.PERCENT_MAX_ERR_DEPTH_PROP_TO_ROOT_COL],
    ).to_csv(percentMaxErrDepthFile, index=False)

    return maxErrDepthFile, percentMaxErrDepthFile


def genSelfErrDepthToNumTracesFiles(metrics, c: common.Config):
    """Write selfErrDepthToNumTraces and percentSelfErrDepthToNumTraces CSVs."""
    outputDir = c.getOutputDir()
    selfErrDepthFile = os.path.join(outputDir, common.SELF_ERROR_DEPTH_TO_NUM_TRACES_CSV)
    percentSelfErrDepthFile = os.path.join(outputDir, common.PERCENT_SELF_ERROR_DEPTH_TO_NUM_TRACES_CSV)
    logging.info("[%s]%s self error depth to # traces file %s", c.serviceName, c.operationName, selfErrDepthFile)
    logging.info("[%s]%s percent self error depth to # traces file %s", c.serviceName, c.operationName, percentSelfErrDepthFile)

    mapList = [{}, {}, {}, {}, {}, {}]
    percMapList = [{}, {}, {}, {}, {}, {}]
    percentile = [0, 50, 90, 95, 99, 100]

    for m in metrics:
        if m.rootReturnError or (m.isTestTrace and c.ignoreTestTraces):
            continue
        depthList = m.errMetrics.selfErrDepthList
        if len(depthList) > 0:
            for i in range(len(percentile)):
                computeSelfErrDepthMaps(mapList[i], percMapList[i], percentile[i], depthList, m.depth)

    colList = [
        common.NUM_TRACES_FOR_MIN_COL, common.NUM_TRACES_FOR_P50_COL,
        common.NUM_TRACES_FOR_P90_COL, common.NUM_TRACES_FOR_P95_COL,
        common.NUM_TRACES_FOR_P99_COL, common.NUM_TRACES_FOR_MAX_COL,
    ]
    combineSelfErrToNumTracesData(mapList, common.DEPTH_COL, colList).to_csv(selfErrDepthFile, index=False)
    combineSelfErrToNumTracesData(percMapList, common.PERCENT_DEPTH_COL, colList).to_csv(percentSelfErrDepthFile, index=False)

    return selfErrDepthFile, percentSelfErrDepthFile


# ---------------------------------------------------------------------------
# Flamegraph filter + tag YAML helpers
# ---------------------------------------------------------------------------

def GetFilteredMetrics(metrics, filter):
    """Return the subset of metrics whose tags list contains the given filter dict."""
    return [metric for metric in metrics if filter in metric.tags]


def TagToStr(tag):
    """Convert a tag dict to a filesystem-safe prefix string, e.g. 'env:prod'."""
    name = tag[common.TAG_NAME]
    value = tag[common.TAG_VALUE]
    return common.replaceNonAlphaNumericWithUnderscore(name + ":" + value)


def ProduceFlameGraphsForEachFilter(metrics, c: common.Config):
    """Generate one FlameGraphSet per tag filter defined in config.tags."""
    fgs = []
    for filter in c.tags:
        logging.info("Starting flameGraph for tag %s", filter)
        filteredMetrics = GetFilteredMetrics(metrics, filter)
        prefix = TagToStr(filter) + "_"
        logging.info(
            "Starting flameGraph for prefix %s, filteredMetrics=%d",
            prefix, len(filteredMetrics),
        )
        fg = flamegraph.flameGraph(
            filteredMetrics,
            c.getOutputDir(),
            c.serviceName,
            c.operationName,
            c.ignoreTestTraces,
            prefix,
            doRanges=c.doRanges,
        )
        fgs.append(fg)
    return fgs


def GetAllFlameGraphFiles(fgs):
    """Collect all non-error flamegraph output files from a list of FlameGraphSets."""
    files = []
    for f in fgs:
        files += f.GetAllFiles()
    return files


def GetAllErrorFlameGraphFiles(fgs):
    """Collect all error flamegraph output files from a list of FlameGraphSets."""
    files = []
    for f in fgs:
        files += f.GetAllErrorFiles()
    return files


def genTagYAML(c: common.Config):
    """Write tags.yaml listing the tag name/value pairs from config."""
    tagYAMLFile = os.path.join(c.getOutputDir(), "tags.yaml")
    logging.info(
        "Producing [%s]%s tag yaml file %s",
        c.serviceName, c.operationName, tagYAMLFile,
    )
    newC = {}
    for d in c.tags:
        if common.TAG_NAME not in d:
            raise ValueError(common.TAG_NAME + " not present in: " + str(d))
        if common.TAG_VALUE not in d:
            raise ValueError(common.TAG_VALUE + " not present in: " + str(d))
        tag = d[common.TAG_NAME]
        value = d[common.TAG_VALUE]
        if tag in newC:
            newC[tag].append(value)
        else:
            newC[tag] = [value]
    for k in newC:
        newC[k].sort()
    with open(tagYAMLFile, "w") as outfile:
        yaml.dump({"version": common.TAG_YAML_VERSION, "tags": newC}, outfile)
    return tagYAMLFile


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def seqProcess(c: common.Config):
    """Single-threaded fallback for mapReduce — useful for debugging."""
    metrics = []
    for traceFile in c.jaegerTraceFiles:
        traceMetrics = process(traceFile, c)
        if traceMetrics:
            metrics.append(traceMetrics)
    return metrics


def getProcessedMetrics(c: common.Config):
    """Load, process, and (optionally) anonymize all traces; return valid Metrics list."""
    logging.info("Starting mapReduce")
    if c.computeParallelism == 1:
        metrics = seqProcess(c)
    else:
        metrics = mapReduce(c.computeParallelism, c.jaegerTraceFiles, c)

    if c.anonymize:
        sanitizeNames(metrics)

    valid_metrics = [m for m in metrics if m]
    total_attempted = (
        len(c.jaegerTraceFiles)
        if hasattr(c, "jaegerTraceFiles") and c.jaegerTraceFiles
        else len(valid_metrics)
    )
    log_incomplete_trace_stats(valid_metrics, total_attempted, c.serviceName, c.operationName)
    return valid_metrics


def buildFlamegraphsFromMetrics(metrics, c: common.Config):
    """Run the main flamegraph plus one per tag filter. Returns (fg, filteredFGs)."""
    logging.info("Starting flameGraph")
    fg = flamegraph.flameGraph(
        metrics,
        c.getOutputDir(),
        c.serviceName,
        c.operationName,
        c.ignoreTestTraces,
        doRanges=c.doRanges,
    )
    filteredFGs = ProduceFlameGraphsForEachFilter(metrics, c)
    return fg, filteredFGs


def logMetrics(c: common.Config, metrics, fg: flamegraph.FlameGraphSet):
    """Log aggregate size/depth statistics for a completed analysis run."""
    maxNodes = totalNodes = maxDepth = 0
    for m in metrics:
        totalNodes += m.numNodes
        maxNodes = max(maxNodes, m.numNodes)
        maxDepth = max(maxDepth, m.depth)
    logging.info("maxNodes = %d, totalNodes=%d, maxDepth=%d", maxNodes, totalNodes, maxDepth)

    traceSzs = sorted([m.fileSz for m in metrics])
    cpSzs = sorted([m.cpSize for m in metrics])
    errCPSzs = sorted([m.errCPMetrics.errCPSize for m in metrics])
    ln = len(cpSzs)
    percentiles = [0.5, 0.9, 0.95, 0.99]
    if traceSzs:
        logging.info(
            "[%s]%s,traceSzs,P50,%d,P90,%d,P95,%d,P99,%d",
            c.serviceName, c.operationName,
            *[traceSzs[int(ln * i)] for i in percentiles],
        )
    if cpSzs:
        logging.info(
            "[%s]%s,cpSzs,P50,%d,P90,%d,P95,%d,P99,%d",
            c.serviceName, c.operationName,
            *[cpSzs[int(ln * i)] for i in percentiles],
        )
    if errCPSzs:
        logging.info(
            "[%s]%s,errCPSzs,P50,%d,P90,%d,P95,%d,P99=%d",
            c.serviceName, c.operationName,
            *[errCPSzs[int(ln * i)] for i in percentiles],
        )
    logging.info("[%s]%s,TotTraceSz,%d", c.serviceName, c.operationName, sum(traceSzs))
    logging.info(
        "[%s]%s TotCPSz:%d", c.serviceName, c.operationName,
        fg.GetCCTSz(os.path.join(c.getOutputDir(), "flame-graph-P99.cct")),
    )
    logging.info(
        "[%s]%s TotErrCPSz:%d", c.serviceName, c.operationName,
        fg.GetCCTSz(os.path.join(c.getOutputDir(), "err-flame-graph-P99.cct")),
    )


def performErrorAnalysis(c: common.Config) -> int:
    """Run error-only flamegraph analysis and populate c.filesToUpload."""
    logging.info("Starting errorAnalysis")
    metrics = getProcessedMetrics(c)
    if not metrics:
        logging.warning("No metrics found.")
        return 1
    fg, filteredFGs = buildFlamegraphsFromMetrics(metrics=metrics, c=c)
    fgFiles = GetAllErrorFlameGraphFiles([fg, *filteredFGs])
    c.filesToUpload = [*fgFiles]
    logMetrics(c, metrics, fg)
    return 0


def performCriticalPathAnalysis(c: common.Config) -> int:
    """Run the full critical-path analysis pipeline and populate c.filesToUpload."""
    # Process traces — keep the raw (possibly-None) list aligned with jaegerTraceFiles
    # for the flat-dict aggregation path, and the filtered list for CSV generators.
    logging.info("Starting mapReduce")
    if c.computeParallelism == 1:
        rawMetrics = seqProcess(c)
    else:
        rawMetrics = mapReduce(c.computeParallelism, c.jaegerTraceFiles, c)

    if c.anonymize:
        sanitizeNames(rawMetrics)
    metrics = [m for m in rawMetrics if m]
    log_incomplete_trace_stats(metrics, len(c.jaegerTraceFiles), c.serviceName, c.operationName)

    if not metrics:
        logging.warning("No metrics found.")
        return 1

    fg, filteredFGs = buildFlamegraphsFromMetrics(metrics=metrics, c=c)

    # traceIDIndex matches the jaegerTraceFiles ordering (used by the heatmap).
    traceIDIndex = [getTraceIdFromFilePath(f) for f in c.jaegerTraceFiles]
    traceToRootspanMap = {
        getTraceIdFromFilePath(c.jaegerTraceFiles[i]): rawMetrics[i].rootSpanID
        for i in range(len(rawMetrics))
        if rawMetrics[i] is not None
    }

    logging.info("Starting aggregateMetrics")
    exclusive, inclusive, aggregateCallMap = aggregateMetrics(rawMetrics, c.jaegerTraceFiles)

    logging.info("Starting heatmapAndSummary")
    heatMap, summary, criticalPathJSONStr = heatmapAndSummary(
        exclusive, inclusive, aggregateCallMap, traceIDIndex, traceToRootspanMap,
        c, c.jaegerTraceFiles,
    )

    logging.info("Starting genTimeSavedSummary")
    pointLatencyPercentiles, headLatencyPercentile, tailLatencyPercentile, hypoLatencyPercentile = genTimeSavedSummary(metrics, c)

    logging.info("Starting genTraceCSVFile")
    traceStatsCSV, numDiscardedDueToRootError, numDiscardedTestTraces, percCPErrList, percConnectedToCPErrList = genTraceCSVFile(metrics, c)

    logging.info("Starting genPercentErrorFile")
    percentErrorFile = genPercentErrorFile(percCPErrList, percConnectedToCPErrList, c)
    logging.info("Starting genErrStatsFiles")
    errDepthFile, percentErrDepthFile, errPropLengthFile, resiliencyFile, perTraceErrInfoFile = genErrStatsFiles(metrics, c)
    logging.info("Starting genMaxErrDepthPropToRootToNumTracesFiles")
    maxErrDepthToRootFile, percMaxErrDepthToRootFile = genMaxErrDepthPropToRootToNumTracesFiles(metrics, c)
    logging.info("Starting genSelfErrDepthToNumTracesFiles")
    selfErrDepthFile, percentSelfErrDepthFile = genSelfErrDepthToNumTracesFiles(metrics, c)
    logging.info("Starting genSavingPotential")
    savingPotentialFile = genSavingPotential(metrics, c)

    logging.info("Starting genSummaryCSVFile")
    summaryCSVFile = genSummaryCSVFile(pointLatencyPercentiles, headLatencyPercentile, tailLatencyPercentile, c)
    logging.info("Starting genCyclesCSVFile")
    cyclesCSVFile = genCyclesCSVFile(metrics, c, filename=common.CYCLES_CSV)
    logging.info("Starting genCrossRegionCallsCSVFile")
    crossRegionCallsCSVFile = genCrossRegionCallsCSVFile(metrics, c, filename=common.CROSS_REGION_CALLS_CSV)

    slackDragCSVFile = None
    if c.computeSlackDrag:
        logging.info("Starting genSlackDragCSVFile")
        mergedSlackDragPerCallPath = merge_per_method_slack_drag(m.slackDragPerCallPath for m in metrics if m.slackDragPerCallPath)
        slackDragCSVFile = genSlackDragCSVFile(mergedSlackDragPerCallPath, c, filename=common.SLACK_DRAG_CSV)

    logging.info("Starting genHypoLatencyCSVFile")
    hypoLatencyCSVFile = genHypoLatencyCSVFile(headLatencyPercentile, hypoLatencyPercentile, c)

    logging.info("Starting genCriticalPathFiles")
    criticalPathHTMLFile, jsonPath = genCriticalPathFiles(
        fg.fgPctFilePair,
        fg.errCPFGPctFilePair,
        criticalPathJSONStr,
        tailLatencyPercentile,
        numDiscardedDueToRootError,
        numDiscardedTestTraces,
        heatMap,
        summary,
        c,
    )

    logging.info("Starting GetAllFlameGraphFiles")
    fgFiles = GetAllFlameGraphFiles([fg, *filteredFGs])
    tagYAMLFile = genTagYAML(c)

    c.filesToUpload = [
        criticalPathHTMLFile, jsonPath, traceStatsCSV, percentErrorFile,
        errDepthFile, percentErrDepthFile, selfErrDepthFile, percentSelfErrDepthFile,
        maxErrDepthToRootFile, percMaxErrDepthToRootFile, errPropLengthFile,
        resiliencyFile, perTraceErrInfoFile, savingPotentialFile,
        summaryCSVFile, hypoLatencyCSVFile, tagYAMLFile, *fgFiles,
    ]
    if cyclesCSVFile:
        c.filesToUpload.append(cyclesCSVFile)
    if crossRegionCallsCSVFile:
        c.filesToUpload.append(crossRegionCallsCSVFile)
    if slackDragCSVFile:
        c.filesToUpload.append(slackDragCSVFile)
    logMetrics(c, metrics, fg)
    return 0


def _writeCCTOutputs(cctFile: str, flameGraphStr: str, merged_cpp, max_exemplars: int = 3) -> None:
    """Write .cct, .dot, and .pb outputs for a flamegraph string."""
    with open(cctFile, "w") as f:
        f.write(flameGraphStr)
    logging.info("Wrote CCT file: %s", cctFile)

    summaries = parse_cct_file(cctFile)
    dot_str = cct_to_dot(summaries)
    dot_file = os.path.splitext(cctFile)[0] + ".dot"
    with open(dot_file, "w", encoding="utf-8") as f_dot:
        f_dot.write(dot_str)
    logging.info("Wrote DOT file: %s", dot_file)

    pb_response = create_protobuf_response_with_exemplars(summaries, merged_cpp, max_exemplars)
    pb_file = os.path.splitext(cctFile)[0] + ".pb"
    with open(pb_file, "wb") as f_pb:
        f_pb.write(pb_response.SerializeToString())
    logging.info("Wrote protobuf file: %s", pb_file)


def lightProcess(c: common.Config) -> int:
    """Light mode: process traces and generate only the P0-100 call-path CCT file."""
    metrics = getProcessedMetrics(c)
    validMetrics = [m for m in metrics if m]

    merged_cpp = MergeCallPathProfilesWithExemplars(validMetrics, c.maxExemplars)
    cpps = [(m.latency, m.CPMetrics) for m in validMetrics]
    flameGraphStr = flamegraph.aggregateCCTs(cpps, [], average=True)
    outputDir = c.getOutputDir()
    cctFile = os.path.join(outputDir, "light-flame-graph-P100.cct")
    _writeCCTOutputs(cctFile, flameGraphStr, merged_cpp, c.maxExemplars)

    if c.projectionEnabled:
        projected_metrics = [
            types.SimpleNamespace(CPMetrics=m.projectedCPMetrics, traceID=m.traceID)
            for m in validMetrics
            if m.projectedCPMetrics is not None
        ]
        projected_merged = (
            MergeCallPathProfilesWithExemplars(projected_metrics, c.maxExemplars)
            if projected_metrics
            else None
        )
        projectedCpps = [
            (m.latency, m.projectedCPMetrics)
            for m in validMetrics
            if m.projectedCPMetrics is not None
        ]
        if projectedCpps:
            projectedFlameGraphStr = flamegraph.aggregateCCTs(projectedCpps, [], average=True)
            projectedCctFile = os.path.join(outputDir, "light-projected-flame-graph-P100.cct")
            _writeCCTOutputs(projectedCctFile, projectedFlameGraphStr, projected_merged, c.maxExemplars)
            latencyChanges = [m.projectedLatency for m in validMetrics if m.projectedLatency is not None]
            if latencyChanges:
                avgLatencyChange = sum(latencyChanges) / len(latencyChanges)
                logging.info(
                    "Projection summary: avg latency change = %.1f μs across %d trace(s)",
                    avgLatencyChange, len(latencyChanges),
                )
    return 0


def processReal(c: common.Config) -> int:
    """Dispatch to performErrorAnalysis or performCriticalPathAnalysis based on config."""
    if c.errorAnalysis:
        return performErrorAnalysis(c)
    return performCriticalPathAnalysis(c)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> int:
    c = initArgs()
    if c.lightMode:
        return lightProcess(c)
    return processReal(c)


if __name__ == "__main__":
    main()
