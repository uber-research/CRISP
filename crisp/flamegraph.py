import logging
import os
import subprocess

from crisp.shared.models import CallPathProfile

MIN_TIME_METRIC_VALUE = 1
STANDARD_PERCENTILES = [(0, 50), (0, 95), (0, 99), (0, 100)]
PROP_TO_ROOT_ERROR_PERCENTILES = [(0, 100)]
ADDITIONAL_RANGES = [(20, 40), (40, 60), (60, 80), (80, 90), (90, 95), (90, 99)]
# This is added for comparing latencies with Muttley
RANGES_SKIP_DIFF = [(94, 96)]


class FlameGraphSet:
    def __init__(
        self,
        fgPctFilePair,
        diffFGFiles,
        errCPFGPctFilePair,
        diffErrCPFGFiles,
        errRootCountFgPctFilePair,
        diffErrRootCountFGFiles,
        errPropToRootCountFgPctFilePair,
        diffErrPropToRootCountFgPctFilePair,
    ):
        self.fgPctFilePair = fgPctFilePair
        self.diffFGFiles = diffFGFiles
        self.errCPFGPctFilePair = errCPFGPctFilePair
        self.diffErrCPFGFiles = diffErrCPFGFiles
        self.errRootCountFgPctFilePair = errRootCountFgPctFilePair
        self.diffErrRootCountFGFiles = diffErrRootCountFGFiles
        self.errPropToRootCountFgPctFilePair = errPropToRootCountFgPctFilePair
        self.diffErrPropToRootCountFgPctFilePair = diffErrPropToRootCountFgPctFilePair


    def __eq__(self, other):
        return (
            self.fgPctFilePair == other.fgPctFilePair
            and self.diffFGFiles == other.diffFGFiles
            and self.errCPFGPctFilePair == other.errCPFGPctFilePair
            and self.diffErrCPFGFiles == other.diffErrCPFGFiles
            and self.errRootCountFgPctFilePair == other.errRootCountFgPctFilePair
            and self.diffErrRootCountFGFiles == other.diffErrRootCountFGFiles
            and self.errPropToRootCountFgPctFilePair == other.errPropToRootCountFgPctFilePair
            and self.diffErrPropToRootCountFgPctFilePair == other.diffErrPropToRootCountFgPctFilePair
        )

    def GetAllFiles(self):
        svgFiles = (
            [f[1] for f in self.fgPctFilePair]
            + list(self.diffFGFiles)
            + [f[1] for f in self.errCPFGPctFilePair]
            + list(self.diffErrCPFGFiles)
            + [f[1] for f in self.errRootCountFgPctFilePair]
            + list(self.diffErrRootCountFGFiles)
        )
        cctFiles = [os.path.splitext(f)[0] for f in svgFiles]
        return svgFiles + cctFiles

    def GetAllErrorFiles(self):
        svgFiles = (
            [f[1] for f in self.errPropToRootCountFgPctFilePair]
            + list(self.diffErrPropToRootCountFgPctFilePair)
        )
        cctFiles = [os.path.splitext(f)[0] for f in svgFiles]
        return svgFiles + cctFiles

    def GetCCTSz(self, filepath) -> int:
        return os.path.getsize(filepath) if os.path.exists(filepath) else 0


# Expected input format: callpath separated by '->'
def getParentCallPath(callpath):
    res = callpath.rsplit("->", 1)
    if len(res) == 1:
        # if "->" is not in the string there is nothing to split
        return ""
    return res[0]


# walk through the map and set any entries with a key that is a leaf call path
# and a value of zero to have a small value (set to one for now)


def sanitizeAggregatedMap(aggregatedMap):
    parentCallpaths = set()

    # iterate through keys and collect all parent callpaths
    for k in aggregatedMap:
        parentPath = getParentCallPath(k)
        if parentPath != "":
            parentCallpaths.add(parentPath)

    for k, v in aggregatedMap.items():
        if k not in parentCallpaths and v == 0:
            # k is a leaf callpath and has value 0; change the value to 1
            aggregatedMap[k] = MIN_TIME_METRIC_VALUE


def aggregateTimeMapList(timeMapList, average):
    aggregatedMap = {}

    for _, myMap in timeMapList:
        for k, v in myMap.items():
            if k not in aggregatedMap:
                aggregatedMap[k] = v
            else:
                aggregatedMap[k] += v
    if average:
        # count those traces that contribute at least one call path.
        denominator = sum([1 if len(x[1]) > 0 else 0 for x in timeMapList])
        for k, v in aggregatedMap.items():
            # Floors the fraction. The data is in microsec, we don't care <1usec yet.
            aggregatedMap[k] = v // denominator

    sanitizeAggregatedMap(aggregatedMap)
    return aggregatedMap


def aggregateCallPathProfiles(cpps):
    cpp = CallPathProfile({}, 0, None)
    for i in range(len(cpps)):
        cpp += cpps[i]

    cpp.NormalizeField("excl")
    parentCallpaths = set()
    # iterate through keys and collect all parent callpaths
    for k in cpp.profile:
        parentPath = getParentCallPath(k)
        if parentPath != "":
            parentCallpaths.add(parentPath)

    for k, v in cpp.profile.items():
        if k not in parentCallpaths and v.excl == 0:
            # k is a leaf callpath and has value 0; change the value to 1
            v.excl = MIN_TIME_METRIC_VALUE

    flameGraph = ""
    for k, v in cpp.profile.items():
        newKey = k.replace(";", "_").replace("->", ";")
        flameGraph += newKey + " " + str(v.excl) + " <<" + str(v.freq) + ">>\n"

    return flameGraph


def aggregateCCTs(cctsAndTime, errCounts, average=True):
    # TODO: migrate error analysis also to using callpath profiles.
    if (len(cctsAndTime) > 0) and (
        isinstance(cctsAndTime[0][1], CallPathProfile)
    ):
        return aggregateCallPathProfiles([x[1] for x in cctsAndTime])

    # use the average for time-based flamegraphs but not for count-based ones
    # aggregates ccts from all traces
    aggregatedCcts = aggregateTimeMapList(cctsAndTime, average=average)
    # aggregates error counts from all traces
    aggregatedErrCounts = aggregateTimeMapList(errCounts, average=False)

    # there is no guarantee that keys from one dictionary is a superset of
    # the other, so take the union of their keys
    allkeys = set().union(aggregatedCcts.keys(), aggregatedErrCounts.keys())

    flameGraph = ""
    for k in allkeys:
        errTime = aggregatedCcts[k] if k in aggregatedCcts else 0
        newKey = k.replace(";", "_").replace("->", ";")
        flameGraph += newKey + " " + str(errTime) + "\n"
    return flameGraph


def genFlameGraph(
    percentilesExclusive,
    cctsAndTime,
    errCounts,
    fileNamePrefix: str,
    outputDir: str,
    service: str,
    operation: str,
    numTraces: int,
    average: bool = True,
    doDiffGraph: bool = True,
):
    fgPctFilePair = []
    differentialFlameGraphFiles = []
    countname = "usec avg" if average else "counts"

    for start, end in percentilesExclusive:
        rangeStr = str(start) + "-" + str(end)
        s = round(len(cctsAndTime) * start / 100)
        e = round(len(cctsAndTime) * end / 100)
        if e - s == 0:
            logging.info(
                "not enough samples for P" + rangeStr + f" flamegraph {fileNamePrefix}",
            )
            continue

        flameGraph = aggregateCCTs(cctsAndTime[s:e], errCounts[s:e], average)
        if flameGraph == "":
            logging.info(
                "nothing to generate for "
                + fileNamePrefix
                + "P"
                + str(start)
                + "-"
                + str(end)
                + f" flamegraph {fileNamePrefix}",
            )
            continue

        # For backward compatibility file with P0-x is named as Px.
        if s == 0:
            cctFile = fileNamePrefix + "P" + str(end) + ".cct"
        else:
            cctFile = fileNamePrefix + "P" + rangeStr + ".cct"

        flamegraphPath = os.path.join(outputDir, cctFile)
        with open(flamegraphPath, "w") as f:
            f.write(flameGraph)

        svgFile = flamegraphPath + ".svg"
        fgPctFilePair.append(("P" + rangeStr, svgFile))

        flameGraphPerl = os.path.join(os.path.dirname(__file__), "flamegraph.pl")
        diffFoldPerl = os.path.join(os.path.dirname(__file__), "difffolded.pl")
        with open(svgFile, "w") as f:
            title = "[" + service + "]" + operation
            subtitle = (
                "P"
                + rangeStr
                + " over "
                + str(e - s)
                + "/"
                + str(numTraces)
                + " traces"
            )
            if average:
                subtitle = "Avg of " + subtitle
            try:
                subprocess.check_call(
                    (
                        flameGraphPerl,
                        "--title",
                        title,
                        "--subtitle",
                        subtitle,
                        "--countname",
                        countname,
                        flamegraphPath,
                    ),
                    stdout=f,
                )
            except FileNotFoundError:
                logging.info("flamegraph.pl not found; wrote .cct file but skipped SVG")
            except subprocess.CalledProcessError as e:
                logging.warning("flamegraph.pl exited %d; skipped SVG for %s", e.returncode, flamegraphPath)

        # if there are predecessors, do a differential analysis with them
        if doDiffGraph:
            for predPct, predFile in fgPctFilePair[:-1]:
                diffCCTFile = fileNamePrefix + predPct + "vsP" + rangeStr + ".cct"
                diffFilePath = os.path.join(outputDir, diffCCTFile)
                # produce diff CCT
                with open(diffFilePath, "w") as f:
                    print((diffFoldPerl, flamegraphPath, predFile))
                    try:
                        subprocess.check_call(
                            (diffFoldPerl, predFile.rstrip(".svg"), flamegraphPath),
                            stdout=f,
                        )
                    except FileNotFoundError:
                        logging.info("difffolded.pl not found; skipping diff CCT")
                        continue
                    except subprocess.CalledProcessError as e:
                        logging.warning("difffolded.pl exited %d; skipping diff CCT", e.returncode)
                        continue
                # produce diff SVG
                diffSVGFile = diffFilePath + ".svg"
                with open(diffSVGFile, "w") as f:
                    try:
                        subprocess.check_call((flameGraphPerl, diffFilePath), stdout=f)
                    except FileNotFoundError:
                        logging.info("flamegraph.pl not found; skipping diff SVG")
                    except subprocess.CalledProcessError as e:
                        logging.warning("flamegraph.pl exited %d; skipping diff SVG for %s", e.returncode, diffFilePath)
                differentialFlameGraphFiles.append(diffSVGFile)

    return fgPctFilePair, differentialFlameGraphFiles


def flameGraph(
    metrics,
    outputDir,
    service: str,
    operation: str,
    ignoreTestTraces: bool,
    filePrefix: str = "",
    doRanges: bool = False,
):
    # Produce SVG flame graphs from critical paths for different percentiles.
    # Returns a list of tuples [(percentile value, path to SVG file), ...]

    cpps = []
    errCPCctsAndTime = []
    errCPCounts = []
    # for generating count-based flamegraphs for requests that errored out at roots only
    errRootCctsAndCounts = []
    errRootErrCounts = []

    # for generating count-based flamegraphs for errors that propagated to root
    errPropToRootCctsAndCounts = []

    for r in metrics:
        if not r:
            continue
        totalTime = r.latency

        cpps.append((totalTime, r.CPMetrics))

        if r.isTestTrace and ignoreTestTraces:
            continue  # skip the metric where the root span itself returns error

        if r.rootReturnError:
            errRootCctsAndCounts.append((totalTime, r.errMetrics.errCallChainCounts))
            errRootErrCounts.append((totalTime, r.errMetrics.errCounts))

            errPropToRootCctsAndCounts.append((totalTime, r.propToRootErrCCT))
        else:
            destList = [
                errCPCctsAndTime,
                errCPCounts,
            ]
            valList = [
                r.errCPMetrics.errCPCallpathTimeExclusive,
                r.errCPMetrics.errCPErrCounts,
            ]
            for dst, val in zip(destList, valList):
                dst.append((totalTime, val))

    cpps = sorted(cpps, key=lambda x: x[0])
    errCPCctsAndTime = sorted(errCPCctsAndTime, key=lambda x: x[0])
    errCPCounts = sorted(errCPCounts, key=lambda x: x[0])
    errRootCctsAndCounts = sorted(errRootCctsAndCounts, key=lambda x: x[0])
    errRootErrCounts = sorted(errRootErrCounts, key=lambda x: x[0])

    percentilesExclusive = sorted(STANDARD_PERCENTILES)
    fgPctFilePair, diffFGFiles = genFlameGraph(
        percentilesExclusive,
        cpps,
        cpps,
        filePrefix + "flame-graph-",
        outputDir,
        service,
        operation,
        len(metrics),
        average=True,
    )

    errCPFGPctFilePair, diffErrCPFGFiles = genFlameGraph(
        percentilesExclusive,
        errCPCctsAndTime,
        errCPCounts,
        filePrefix + "err-flame-graph-",
        outputDir,
        service,
        operation,
        len(errCPCctsAndTime),
        average=True,
    )

    # generate count-based flamegraphs for requests that errored out at root only
    errRootCountFgPctFilePair, diffErrRootCountFGFiles = genFlameGraph(
        percentilesExclusive,
        errRootCctsAndCounts,
        errRootErrCounts,
        filePrefix + "errored-API-flame-graph-",
        outputDir,
        service,
        operation,
        len(errRootCctsAndCounts),
        average=False,
    )

    errPropToRootCountFgPctFilePair, diffErrPropToRootCountFgPctFilePair = genFlameGraph(
        PROP_TO_ROOT_ERROR_PERCENTILES,
        errPropToRootCctsAndCounts,
        [], # no frequency info
        filePrefix + "errorsPropToRoot-flame-graph-",
        outputDir,
        service,
        operation,
        len(errRootCctsAndCounts),
        average=False,
        doDiffGraph=False, # no differential flamegraphs for now
    )

    if doRanges:
        ranges = sorted(ADDITIONAL_RANGES)
        rangeFGPctFilePair, rangeDiffFGFiles = genFlameGraph(
            ranges,
            cpps,
            cpps,
            filePrefix + "flame-graph-",
            outputDir,
            service,
            operation,
            len(metrics),
            average=True,
        )

        # Create more flamegraphs of custom types w/o doing a differential FG.
        moreFGPctFilePair, _ = genFlameGraph(
            RANGES_SKIP_DIFF,
            cpps,
            cpps,
            filePrefix + "flame-graph-",
            outputDir,
            service,
            operation,
            len(metrics),
            average=True,
            doDiffGraph=False,
        )

        result = FlameGraphSet(
            fgPctFilePair + rangeFGPctFilePair + moreFGPctFilePair,
            diffFGFiles + rangeDiffFGFiles,
            errCPFGPctFilePair,
            diffErrCPFGFiles,
            errRootCountFgPctFilePair,
            diffErrRootCountFGFiles,
            errPropToRootCountFgPctFilePair,
            diffErrPropToRootCountFgPctFilePair,
        )
        # No ranges for errors for now until we see the need.
    else:
        result = FlameGraphSet(
            fgPctFilePair,
            diffFGFiles,
            errCPFGPctFilePair,
            diffErrCPFGFiles,
            errRootCountFgPctFilePair,
            diffErrRootCountFGFiles,
            errPropToRootCountFgPctFilePair,
            diffErrPropToRootCountFgPctFilePair,
        )

    return result
