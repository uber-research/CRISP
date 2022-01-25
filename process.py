#Copyright (c) 2021  Uber Technologies, Inc.
#
#Licensed under the Uber Non-Commercial License (the "License");
#you may not use this file except in compliance with the License.
#You may obtain a copy of the License at the root directory of this project.
#
#See the License for the specific language governing permissions and
#limitations under the License.

import json
import glob
import os
from multiprocessing import Pool, Value
from graph import *
import argparse
import re
import pandas as pd
import sys
from datetime import datetime
import logging
import subprocess

DATE_TIME = datetime.now().strftime("%d_%B_%Y_%H_%M_%S")
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')
debug_on = logging.getLogger(__name__).isEnabledFor(logging.DEBUG)
JAEGER_UI_URL = "https://jaeger-ui.yourserver.com/trace"


def dirPathCheck(path):
    if os.path.isdir(path):
        return path
    else:
        raise argparse.ArgumentTypeError(
            f"readable_dir:{path} is not a valid path")


argParser = argparse.ArgumentParser()
argParser.add_argument('-a',
                       '--operationName',
                       action='store',
                       help='operation name',
                       required=True,
                       type=str)
argParser.add_argument('-s',
                       '--serviceName',
                       action='store',
                       help='name of the service',
                       required=True,
                       type=str)
argParser.add_argument(
    '--rootTrace',
    dest='rootTrace',
    action='store_true',
    default=False,
    required=False,
    help=
    "Should the service and operation be the root span of the trace (default:false)."
)

argParser.add_argument(
    '--anonymize',
    dest='anonymize',
    action='store_true',
    default=False,
    required=False,
    help="Should the service and operation names be anonymized (default:false)."
)

argParser.add_argument(
    '-t',
    '--traceDir',
    action='store',
    type=dirPathCheck,
    help='path of the trace directory (mutually exclusive with --file)',
    default=None)
argParser.add_argument(
    '--file',
    type=argparse.FileType('r'),
    action='store',
    help='input path of the trace file (mutually exclusivbe with --traceDir)',
    default=None)
argParser.add_argument('-o',
                       '--outputDir',
                       required=True,
                       action='store',
                       help='directory where output will be produced',
                       type=dirPathCheck)
argParser.add_argument('--parallelism',
                       action='store',
                       help="number of concurrent python processes.",
                       default=1,
                       type=int)
argParser.add_argument('--topN',
                       action='store',
                       help='number of services to show in the summary',
                       default=5,
                       type=int)
argParser.add_argument('--numTrace',
                       action='store',
                       help='number of traces to show in the heatmap',
                       default=100,
                       type=int)
argParser.add_argument('--numOperation',
                       action='store',
                       help='number of operations to show in the heatmap',
                       default=100,
                       type=int)

args = argParser.parse_args()
operationName = args.operationName
serviceName = args.serviceName
tracesDir = args.traceDir
topN = args.topN
numOperation = args.numOperation
numTrace = args.numTrace
rootTrace = args.rootTrace
anonymize = args.anonymize

if args.file == None and args.traceDir == None:
    print("One of --inpiut/--file should be set.")
    sys.exit(-1)

if args.file != None and args.traceDir != None:
    print("Only one of --inpiut/--file should be set.")
    sys.exit(-1)

jaegerTraceFiles = []

if args.file != None:
    jaegerTraceFiles = [args.file.name]
else:
    jaegerTraceFiles = glob.glob(os.path.join(tracesDir, '*.json'))

htmlPrefixStr = '''
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
  border-bottom: 1px dotted black; /* If you want dots under the hoverable text */
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
  /* Position the tooltip text - see examples below! */
  position: absolute;
  z-index: 1;
}
/* Show the tooltip text when you mouse over the tooltip container */
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

htmlGenerationTime = '''<h1>Critical path generated on %s </h1>''' % DATE_TIME

htmlSuffixStr = '''
  <script type = "text/javascript">
  /**
 * Sorts a HTML table.
 *
 * @param {HTMLTableElement} table The table to sort
 * @param {number} column The index of the column to sort
 * @param {boolean} asc Determines if the sorting will be in ascending
 */
function sortTableByColumn(table, column, asc = true) {
    const dirModifier = asc ? 1 : -1;
    const tBody = table.tBodies[0];
    const rows = Array.from(tBody.querySelectorAll("tr"));

    // Sort each row
    const sortedRows = rows.sort((a, b) => {
        const aColText = Number(a.querySelector(`td:nth-child(${ column + 1 })`).textContent.trim())
        const bColText = Number(b.querySelector(`td:nth-child(${ column + 1 })`).textContent.trim());

        return aColText > bColText ? (1 * dirModifier) : (-1 * dirModifier);
    });

    // Remove all existing TRs from the table
    while (tBody.firstChild) {
        tBody.removeChild(tBody.firstChild);
    }

    // Re-add the newly sorted rows
    tBody.append(...sortedRows);

    // Remember how the column is currently sorted
    table.querySelectorAll("th").forEach(th => th.classList.remove("th-sort-asc", "th-sort-desc"));
    table.querySelector(`th:nth-child(${ column + 1})`).classList.toggle("th-sort-asc", asc);
    table.querySelector(`th:nth-child(${ column + 1})`).classList.toggle("th-sort-desc", !asc);
}

document.querySelectorAll(".table-sortable th").forEach(headerCell => {
    const headerIndex = Array.prototype.indexOf.call(headerCell.parentElement.children, headerCell);
    // only sortable on the first 6 columns
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


def process(filename):
    # process one Jaeger JSON trace file
    with open(os.path.join(filename), 'r') as f:
        data = json.load(f)
        graph = Graph(data, serviceName, operationName, filename, rootTrace)
        if graph.rootNode == None:
            return Metrics({}, {}, {}, {}, {}, {}, {}, 0, 0, 0)

        res = graph.findCriticalPath()
        debug_on and logging.debug("critical path:" + str(res))

        metrics = graph.getMetrics(res)
        debug_on and logging.debug(metrics.opTimeExclusive)

        debug_on and logging.debug(
            "Test result = " +
            str(graph.checkResults(metrics.opTimeExclusive)))

        # artifically introduce the totalTime entry
        metrics.opTimeExclusive['totalTime'] = graph.rootNode.duration
        metrics.opTimeInclusive['totalTime'] = graph.rootNode.duration
        return metrics


def mapReduce(numWorkers, jaegerTraceFiles):
    # Build graph for each trace file and compute its critical path.
    # Use python multiprocessing to split work on to numWorkers.
    metrics = None
    with Pool(numWorkers) as p:
        metrics = p.map(process, jaegerTraceFiles)
    return metrics


class SummaryResult:
    """
    SummaryResult holds the following measurements as dictionaries
    1. opTime: the flat profile with exclusive operation times.
    2. callpathTime: the call-path profile with callpath times.
    3. exampleMap: per callpath worst case example.
    """
    def __init__(self, opTime, callpathTime, exampleMap):
        self.opTime = opTime
        self.callpathTime = callpathTime
        self.exampleMap = exampleMap


def getTraceIdFromFilePath(traceFile):
    # A trace file path will look like /foo/bar/73212187.json
    # We need to return 73212187
    return traceFile.split('/')[-1].split('.')[0]


def mergeCallChains(callMap, totalCallMap):
    # Collect all call chains per opName
    for opName in callMap:
        if opName not in totalCallMap:
            totalCallMap[opName] = set()
        for name in callMap[opName]:
            totalCallMap[opName].add(name)


def mergeCallpathTime(callMap, callPathMap, totalBreakdownTime):
    # Collect all call paths and thier corresponding time
    for opName, paths in callMap.items():
        if opName not in totalBreakdownTime:
            totalBreakdownTime[opName] = {}
        for p in paths:
            if p not in totalBreakdownTime[opName]:
                totalBreakdownTime[opName][p] = []
            totalBreakdownTime[opName][p].append(callPathMap[p])


def mergeExampleID(traceID, localExampleMap, exampleMap):
    # Maintain the worst case example per call path.
    for opName in localExampleMap:
        if opName not in exampleMap:
            exampleMap[opName] = (traceID, localExampleMap[opName][0],
                                  localExampleMap[opName][1])
        elif localExampleMap[opName][1] > exampleMap[opName][2]:
            exampleMap[opName] = (traceID, localExampleMap[opName][0],
                                  localExampleMap[opName][1])


def aggregateMetrics(metrics, jaegerTraceFiles):
    # Compute aggregate metrics from individual metrics.
    exclusive, inclusive = SummaryResult({}, {}, {}), SummaryResult({}, {}, {})
    exclusive.opTime = {}
    inclusive.opTime = {}
    aggregateCallMap = {}
    exclusive.callpathTime = {}
    inclusive.callpathTime = {}
    exclusive.exampleMap = {
    }  # stores the most time consuming traceID and spanID for each [serviceName] opName pair: (traceID, spanID, time)
    inclusive.exampleMap = {}

    for i in range(len(jaegerTraceFiles)):
        traceID = getTraceIdFromFilePath(jaegerTraceFiles[i])

        # remember per-trace info
        exclusive.opTime[traceID] = metrics[i].opTimeExclusive
        inclusive.opTime[traceID] = metrics[i].opTimeInclusive
        mergeCallChains(callMap=metrics[i].callChain,
                        totalCallMap=aggregateCallMap)

        mergeCallpathTime(callMap=metrics[i].callChain,
                          callPathMap=metrics[i].callpathTimeExlusive,
                          totalBreakdownTime=exclusive.callpathTime)

        mergeCallpathTime(callMap=metrics[i].callChain,
                          callPathMap=metrics[i].callpathTimeInclusive,
                          totalBreakdownTime=inclusive.callpathTime)

        mergeExampleID(traceID=traceID,
                       localExampleMap=metrics[i].exclusiveExampleMap,
                       exampleMap=exclusive.exampleMap)

        mergeExampleID(traceID=traceID,
                       localExampleMap=metrics[i].inclusiveExampleMap,
                       exampleMap=inclusive.exampleMap)

    return exclusive, inclusive, aggregateCallMap


def flameGraph(metrics, outputDir):
    # Produce SVG flame graphs from critical paths for different percentiles.
    # Returns a list of tuples [(percentile value, path to SVG file), ...]
    ccts = {}

    cctsAndtime = []
    for r in metrics:
        if not 'totalTime' in r.opTimeExclusive:
            continue
        cctsAndtime.append(
            (r.opTimeExclusive['totalTime'], r.callpathTimeExlusive))

    cctsAndtime = sorted(cctsAndtime, key=lambda x: x[0])
    percentilesExclusive = sorted([50, 95, 99])
    flameGraphPctFilePair = []
    differentialFlameGraphFiles = []
    for p in percentilesExclusive:
        limit = int(round(len(cctsAndtime) * p / 100))
        if limit == 0:
            logging.info(f"not enough samples for P" + str(p) + " flamegraph")
            continue

        for time, ccts in cctsAndtime[:limit]:
            for k, v in ccts.items():
                if not k in ccts:
                    ccts[k] = v
                else:
                    ccts[k] += v
        flameGraph = ''
        for k, v in ccts.items():
            flameGraph += k.replace('->', ';') + ' ' + str(v) + '\n'

        cctFile = 'flame-graph-P' + str(p) + '.cct'
        flamegraphPath = os.path.join(outputDir, cctFile)
        with open(flamegraphPath, 'w') as f:
            f.write(flameGraph)

        svgFile = flamegraphPath + '.svg'
        flameGraphPctFilePair.append(('P' + str(p), svgFile))
        with open(svgFile, 'w') as f:
            subprocess.check_call(('./flamegraph.pl', flamegraphPath),
                                  stdout=f)

        # if there are predecessors, do a differential analysis with them
        for predPct, predFile in flameGraphPctFilePair[:-1]:
            diffCCTFile = 'flame-graph-' + predPct + 'vsP' + str(p) + '.cct'
            diffFilePath = os.path.join(outputDir, diffCCTFile)
            # produce diff CCT
            with open(diffFilePath, 'w') as f:
                subprocess.check_call(
                    ('./difffolded.pl', '-n', predFile.rstrip('.svg'),
                     flamegraphPath),
                    stdout=f)
            # produce diff SVG
            diffSVGFile = diffFilePath + '.svg'
            with open(diffSVGFile, 'w') as f:
                subprocess.check_call(('./flamegraph.pl', diffFilePath),
                                      stdout=f)
            differentialFlameGraphFiles.append(diffSVGFile)

    return flameGraphPctFilePair, differentialFlameGraphFiles


def getOutputDir():
    # Override if we have a file.
    if args.file != None:
        return os.path.dirname(args.file.name)
    return tracesDir


class PVal:
    def __init__(self, percentile, percentileStr):
        self.percentile = percentile
        self.percentileStr = percentileStr
        self.pVal = {}
        self.pPct = {}

    def percentileWithPercentSign(self):
        return self.percentileStr + '%'


def insertInDF(metric, opsStableOrder, traceIDsStableOrder):
    df = pd.DataFrame(index=traceIDsStableOrder)
    # updates df by inserting the operation times for each operation
    for op in opsStableOrder:
        opColumn = []
        for trace in traceIDsStableOrder:
            if op in metric.opTime[trace]:
                opColumn.append(metric.opTime[trace][op])
            else:
                opColumn.append(0)
        # insert the column
        df.insert(len(df.columns), op, opColumn)
    return df


def addPercentileColumns(df, percentiles):
    # Here a data frame looks like this:
    # traceId       Op1     Op2     Op3 totalTime
    #   687216      99      1       30    130
    #   287382      89      2       20    111
    #   79827       90      3       40    133

    columnsToAdd = {}
    for p in percentiles:
        columnsToAdd[p.percentileStr] = []
        columnsToAdd[p.percentileWithPercentSign()] = []

    for p in percentiles:
        denominator = df['totalTime'].quantile(p.percentile)
        for i in df:
            # Compute the quantile of non-zero values of operations
            nonZeros = df[i].loc[df[i] != 0]
            if len(nonZeros) == 0:
                p.pVal[i] = 0
                p.pPct[i] = 0
            else:
                p.pVal[i] = nonZeros.quantile(p.percentile)
                p.pPct[i] = (p.pVal[i] /
                             denominator) if denominator != 0 else 0
            columnsToAdd[p.percentileStr].append(p.pVal[i])
            columnsToAdd[p.percentileWithPercentSign()].append(p.pPct[i])

    df = df.transpose()

    # Here a data frame looks like this:
    #      687216 287382 79827
    # op1   ?      ?       ?
    # op2   ?      ?       ?
    # op3   ?      ?       ?

    for i, p in enumerate(percentiles):
        df.insert(i, p.percentileStr, columnsToAdd[p.percentileStr])
    for i, p in enumerate(percentiles):
        df.insert(
            len(percentiles) + i, p.percentileWithPercentSign(),
            columnsToAdd[p.percentileWithPercentSign()])

    # Here a data frame looks like this:
    #       p50 P95 P99  P50% P95% P99%  687216 287382 79827
    # op1    ?    ?   ?   ?    ?    ?     ?      ?       ?
    # op2    ?    ?   ?   ?    ?    ?     ?      ?       ?
    # op3    ?    ?   ?   ?    ?    ?     ?      ?       ?

    return df


def insertInclusivePercentileInfoDF(df, percentilesInclusive, inclusiveDF):
    # Insert percentileStr columns.
    for idx, p in enumerate(percentilesInclusive):
        df.insert(idx, p.percentileStr, inclusiveDF[p.percentileStr])

    # Insert percentileWithPercentSign columns.
    for idx, p in enumerate(percentilesInclusive):
        df.insert(
            len(percentilesInclusive) + idx, p.percentileWithPercentSign(),
            inclusiveDF[p.percentileWithPercentSign()])
    return df


def insertOccurenceCol(df, jaegerTraceFiles, nonZeros):
    # Insert one column that counts the number of times the operation is seen on the critical path
    occurenceColHeader = 'occurence (%s)' % len(jaegerTraceFiles)
    df.insert(0, occurenceColHeader, "")
    for i in range(len(df)):
        df.at[df.index[i],
              'occurence (%s)' % len(jaegerTraceFiles)] = int(nonZeros[i])
    return df, occurenceColHeader


def reindexDescending(df, exclusive, prefixColumns, traceIDIndex):
    # Sort the rows descending total time per op.
    opSums = df[traceIDIndex].sum(axis=1).sort_values(ascending=False)
    df = df.reindex(opSums.index.tolist())

    # traceIds orders by total execution time.
    traceIDSorted = sorted(traceIDIndex,
                           key=lambda x: exclusive.opTime[x]['totalTime']
                           if 'totalTime' in exclusive.opTime[x] else 0,
                           reverse=True)
    # Sort the columns descending total time per trace.
    return df.reindex(columns=prefixColumns + traceIDSorted)


def makeClickable(url, name):
    return '<a href="{}" rel="noopener noreferrer" target="_blank">{}</a>'.format(
        url, name)


def addHyperLinkToTrace(df, tracespanIDmap):
    # Make each trace column header navigatable to Jaeger UI
    hyperLinkHT = {}
    for k, v in tracespanIDmap.items():
        hyperLinkHT[k] = makeClickable(JAEGER_UI_URL + "%s?uiFind=%s" % (k, v),
                                       '#')
    df.rename(columns=hyperLinkHT, inplace=True)
    return df


def renameSortableIcon(df, columns):
    # Use fas fa-sort script to make columns sortable.
    sortableRenameHT = {}
    for col in columns:
        sortableRenameHT[col] = col + ' <i class="fas fa-sort"></i>'
    df.rename(columns=sortableRenameHT, inplace=True)
    return df


def setCellFormating(df, percentiles, occurenceColHeader):
    precisionHT = {}
    for i in df.columns.values:
        # All columns except percentiles with % sign will be in scientific.
        precisionHT[i] = "{:.2e}"
    for p in percentiles:
        # Columns with % sign.
        precisionHT[p.percentileWithPercentSign()] = "{:.2%}"
    # occurence column will be in decimal
    precisionHT[occurenceColHeader] = "{:5d}"
    return precisionHT


def cssNameHandle(str):
    # Given the call chain, change it into css format with indentation.
    lst = str.split('->')
    res = ""
    for i in range(len(lst)):
        for j in range(i):
            res += ' &emsp; '
        res += lst[i] + '</br>  '
    return res


def getSummaryText(pval, pctMap, valMap, totalBreakdownTime):
    summary = ''
    summary += '<h1>Top %d operations contributing to %s of [%s] %s:</h1>' % (
        topN, pval, serviceName, operationName)

    res = sorted(pctMap.items(), key=lambda x: x[1], reverse=True)
    for i in res:
        if i[0] == 'totalTime':
            res.remove(i)
            break
    for idx in range(0, min(topN, len(res))):
        summary += '<h2>%s. %s -> %s Value: %s, %s percentage: %s, call chains are below:</h2>' % (
            idx + 1, res[idx][0], pval, '{:.2e}'.format(valMap[res[idx][0]]),
            pval, '{:.2%}'.format(pctMap[res[idx][0]]))
        cc = totalBreakdownTime[res[idx][0]]
        sumCC = 0
        sortedCC = sorted(cc.items(), key=lambda x: sum(x[1]), reverse=True)
        for i in sortedCC:
            for j in i[1]:
                sumCC += j
        for i in range(len(sortedCC)):
            summary += cssNameHandle(sortedCC[i][0] + '</br>' +
                                     'Contributing: {:.2%}'.format(
                                         sum(sortedCC[i][1]) /
                                         sumCC if sumCC != 0 else 1.0))
            summary += '</br>'
    return summary


def getTopNCCTs(sortedContexts, sumTime, n, exampleMap):
    res = ''
    for i in range(min(len(sortedContexts), n)):
        res += cssNameHandle(
            sortedContexts[i][0] + '</br>' + 'Contributing: {:.2%}'.format(
                sum(sortedContexts[i][1]) /
                sumTime if sumTime != 0 else 0)) + makeClickable(
                    JAEGER_UI_URL + "/%s?uiFind=%s" %
                    (exampleMap[sortedContexts[i][0]][0],
                     exampleMap[sortedContexts[i][0]][1]),
                    "Example") + '</br>' + '</br>'
    return res


def sum2DCCT(cct):
    total = 0
    for i in cct:
        for j in i[1]:
            total += j
    return total


def addToolTip(df, exclusive, inclusive, ignoreSet):
    # Add tooltip and example url to each opName.
    renameRowHT = {}
    for i, idx in enumerate(df.index[:]):  # copy
        if idx in ignoreSet:
            continue
        res = ""
        cc = exclusive.callpathTime[idx]
        sortedCC = sorted(cc.items(), key=lambda x: sum(x[1]), reverse=True)
        ccInc = inclusive.callpathTime[idx]
        sortedCCInc = sorted(ccInc.items(),
                             key=lambda x: sum(x[1]),
                             reverse=True)
        sumCC = sum2DCCT(sortedCC)
        sumCCInc = sum2DCCT(sortedCCInc)

        res += "Exclusive:</br>"

        res += getTopNCCTs(sortedCC, sumCC, 5, exclusive.exampleMap)
        res += "Inclusive:</br>"
        res += getTopNCCTs(sortedCCInc, sumCCInc, 5, inclusive.exampleMap)
        renameRowHT[df.index[
            i]] = '<div class="tooltip">%s <span class="tooltiptext">%s</span> </div>' % (
                df.index[i], res)
    df.rename(index=renameRowHT, inplace=True)
    return df


def getGradientFormatFromDataframe(df, precisionHT, firstSorableCoulmn,
                                   lastSortableColumns):
    return (df.style.background_gradient(
        axis=0,
        cmap='BuPu',
        subset=(df.index.values[firstSorableCoulmn:],
                df.columns.values[lastSortableColumns:])).set_table_attributes(
                    'class="table-sortable"').set_properties(
                        **{
                            'text-align': 'right'
                        }).format(precisionHT).render())


def heatmapAndSummary(exclusive, inclusive, aggregateCallMap, traceIDIndex,
                      traceToRootspanMap):
    # Create a dataframe of traces and operations.
    # Insert percentile columns.
    # Compute a heatmap.
    # return HTML form of the heatmap as a table and textual summary.

    allOps = [k for k in aggregateCallMap.keys()]
    allOps.append('totalTime')
    opsStableOrder = sorted(allOps)
    traceIDsStableOrder = sorted(traceIDIndex)

    exclusiveDF = insertInDF(exclusive, opsStableOrder, traceIDsStableOrder)
    inclusiveDF = insertInDF(inclusive, opsStableOrder, traceIDsStableOrder)

    # Here a data frame looks like this:
    # traceId       Op1     Op2     Op3
    #   687216      ?      ?       ?
    #   287382      ?      ?       ?
    #   79827       ?      ?       ?

    # Count the non-zeros in each column. These are the number of times an operation appears on the critical path.
    nonZeroOpCounts = exclusiveDF.astype(bool).sum(axis=0)
    # Now inject the percentile columns.
    percentilesExclusive = (PVal(.5, 'P50(E)'), PVal(.95, 'P95(E)'),
                            PVal(.99, 'P99(E)'))
    exclusiveDF = addPercentileColumns(exclusiveDF, percentilesExclusive)

    percentilesInclusive = (PVal(.5, 'P50(I)'), PVal(.95, 'P95(I)'),
                            PVal(.99, 'P99(I)'))
    inclusiveDF = addPercentileColumns(inclusiveDF, percentilesInclusive)

    # Insert inclusive percentiles into exclusiveDF and call it df
    df = insertInclusivePercentileInfoDF(exclusiveDF, percentilesInclusive,
                                         inclusiveDF)

    # Insert the occurences column as the first column.
    df, occurenceColHeader = insertOccurenceCol(df, jaegerTraceFiles,
                                                nonZeroOpCounts)

    # 1 for occurence column, 2*(len(percentilesExclusive) + len(percentilesInclusive) for the percentile columns
    numColsToRetains = 1 + 2 * (len(percentilesExclusive) +
                                len(percentilesInclusive))
    unmodifiedPrefix = df.columns.values.tolist()[:numColsToRetains]

    # Order the df in descending order of total trace time and total operation time.
    df = reindexDescending(df, exclusive, unmodifiedPrefix, traceIDIndex)

    # Truncate df to max of numOperation rows and numColsToRetains + numTrace columns.
    df = df.iloc[:numOperation, :numColsToRetains + numTrace]

    # Add hyperlinks to the column heads of each trace.
    df = addHyperLinkToTrace(df, traceToRootspanMap)

    # Make percentile columns and occurence column sortable.
    df = renameSortableIcon(
        df,
        [x.percentileStr for x in percentilesInclusive + percentilesExclusive])

    # Make each cell in scientific value
    precisionHT = setCellFormating(df,
                                   percentilesExclusive + percentilesInclusive,
                                   occurenceColHeader)

    # Add tool tip to each row header (operation) to show the top calling contexts.
    df = addToolTip(df, exclusive, inclusive, ignoreSet={'totalTime'})

    firstSorableCoulmn = 1
    lastSortableColumns = firstSorableCoulmn + 2 * (len(percentilesExclusive) +
                                                    len(percentilesInclusive))

    # Obtain the textual summary.
    summary = ''
    for p in percentilesExclusive:
        summary += getSummaryText(p.percentileStr, p.pPct, p.pVal,
                                  exclusive.callpathTime)

    # Color the heapmap with the gradient.
    heatmap = getGradientFormatFromDataframe(df, precisionHT,
                                             firstSorableCoulmn,
                                             numColsToRetains)
    return heatmap, summary


def replaceNonAlphaNumericWithUnderscore(str):
    return re.sub('[^a-zA-Z0-9_]+', '_', str)


saniMap = {'totalTime': 'totalTime'}
saniCtr = 0


def sanitized(op):
    global saniCtr
    global saniMap
    ret = ''
    pieces = op.split('->')
    for piece in pieces:
        if ret != '':
            ret += '->'
        if piece in saniMap:
            ret += saniMap[piece]
        else:
            saniCtr += 1
            ret += 'Service::Operation' + str(saniCtr)
            saniMap[piece] = 'Service::Operation' + str(saniCtr)
    return ret


def sanitizeNames(metric):
    for r in metric:
        for field in [
                r.opTimeExclusive, r.callpathTimeExlusive,
                r.exclusiveExampleMap, r.opTimeInclusive,
                r.callpathTimeInclusive, r.inclusiveExampleMap
        ]:
            for k, v in field.copy().items():
                del field[k]
                field[sanitized(k)] = v
        for k, vals in r.callChain.copy().items():
            del r.callChain[k]
            sk = sanitized(k)
            r.callChain[sk] = set()
            for v in vals:
                r.callChain[sk].add(sanitized(v))


if __name__ == '__main__':
    logging.info("Starting mapReduce")
    metrics = mapReduce(args.parallelism, jaegerTraceFiles)

    maxNodes = 0
    totalNodes = 0
    maxDepth = 0
    for i in metrics:
        totalNodes = totalNodes + i.numNodes
        maxNodes = i.numNodes if i.numNodes > maxNodes else maxNodes
        maxDepth = i.depth if i.depth > maxDepth else maxDepth
    logging.info(
        f"maxNodes = {maxNodes}, totalNodes={totalNodes}, maxDepth={maxDepth}")

    if anonymize:
        sanitizeNames(metrics)
    logging.info("Starting aggregateMetrics")
    exclusive, inclusive, aggregateCallMap = aggregateMetrics(
        metrics, jaegerTraceFiles)
    traceIDIndex = [
        os.path.splitext(os.path.basename(i))[0] for i in jaegerTraceFiles
    ]
    # create a map of from traceID to the corresponding spanID.
    traceToRootspanMap = {}
    for i in range(len(traceIDIndex)):
        traceID = traceIDIndex[i]
        spanID = metrics[i].rootSpanID
        traceToRootspanMap[traceID] = spanID

    logging.info("Starting flameGraph")
    flameGraphPctFilePair, differentialFlameGraphFiles = flameGraph(
        metrics, getOutputDir())

    logging.info("Starting heatmapAndSummary")
    heatMap, summary = heatmapAndSummary(exclusive, inclusive,
                                         aggregateCallMap, traceIDIndex,
                                         traceToRootspanMap)

    criticalPathHTMLFile = os.path.join(args.outputDir, 'criticalPaths.html')

    logging.info("[%s]%s critical path file %s", args.serviceName,
                 args.operationName, criticalPathHTMLFile)

    with open(criticalPathHTMLFile, 'w') as f:
        f.write(htmlPrefixStr + heatMap)
        f.write(htmlGenerationTime)
        for pval, file in flameGraphPctFilePair:
            src = os.path.basename(file)
            f.write('<div> <h2>%s flame graph. </h2> <img src=%s></div>' %
                    (pval, src))
        f.write(summary)
        f.write(htmlSuffixStr)
