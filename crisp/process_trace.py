# ruff: noqa: I001
import argparse
import glob
import json
import logging
import multiprocessing as mp
import os
import re
from datetime import datetime
from functools import partial
from typing import Any

import pandas as pd
import yaml

import crisp.common as common
import crisp.flamegraph as flamegraph
from crisp.graph import Graph
from crisp.metrics.aggregators import mergeCallChains, mergeExampleID
from crisp.output.formatters import makeClickable, renameSortableIcon
from crisp.shared.utils import getLeafNodeFromCallPath


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
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    config = initArgs()
    jaegerTraceFiles = config.jaegerTraceFiles

    logging.info("Starting mapReduce over %d trace files", len(jaegerTraceFiles))
    metrics = mapReduce(config.computeParallelism, jaegerTraceFiles, config)

    valid = [m for m in metrics if m is not None]
    maxNodes = max((m.numNodes for m in valid), default=0)
    totalNodes = sum(m.numNodes for m in valid)
    maxDepth = max((m.depth for m in valid), default=0)
    logging.info(
        "mapReduce complete: maxNodes=%d totalNodes=%d maxDepth=%d",
        maxNodes, totalNodes, maxDepth,
    )

    if config.anonymize:
        sanitizeNames(valid)

    logging.info("Starting aggregateMetrics")
    exclusive, inclusive, aggregateCallMap = aggregateMetrics(metrics, jaegerTraceFiles)

    traceIDIndex = [os.path.splitext(os.path.basename(f))[0] for f in jaegerTraceFiles]
    traceToRootspanMap = {
        traceIDIndex[i]: metrics[i].rootSpanID
        for i in range(len(traceIDIndex))
        if metrics[i] is not None
    }

    outputDir = config.outputDir
    logging.info("Starting flameGraph, outputDir=%s", outputDir)
    flameGraphResult = flamegraph.flameGraph(
        valid,
        outputDir,
        config.serviceName,
        config.operationName,
        config.ignoreTestTraces,
        doRanges=config.doRanges,
    )
    flameGraphPctFilePair = flameGraphResult.fgPctFilePair

    logging.info("Starting heatmapAndSummary")
    heatMap, summary, criticalPathJSONStr = heatmapAndSummary(
        exclusive, inclusive, aggregateCallMap, traceIDIndex,
        traceToRootspanMap, config, jaegerTraceFiles,
    )

    criticalPathHTMLFile = os.path.join(outputDir, "criticalPaths.html")
    logging.info(
        "[%s] %s critical path file: %s",
        config.serviceName, config.operationName, criticalPathHTMLFile,
    )

    with open(criticalPathHTMLFile, "w") as f:
        f.write(HTML_PREFIX + heatMap)
        f.write(HTML_GENERATION_TIME)
        for pval, file in flameGraphPctFilePair:
            src = os.path.basename(file)
            f.write(
                "<div> <h2>%s flame graph. </h2> <img src=%s></div>" % (pval, src)
            )
        f.write(summary)
        f.write(HTML_SUFFIX)


if __name__ == "__main__":
    main()
