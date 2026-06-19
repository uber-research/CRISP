# ruff: noqa: I001
import argparse
import datetime
import logging
import multiprocessing as mp
import os
import re
import time
import traceback
import typing
from collections.abc import Callable

import dateutil.tz

from crisp.shared.constants import TAG_NAME, TAG_VALUE, TAG_SEARCH_DEPTH


DEFAULT_TAGS = []
TAG_YAML_VERSION = "1.0"
TAG_KEYS = [TAG_NAME, TAG_VALUE, TAG_SEARCH_DEPTH]
DEFAULT_TAG_SEARCH_DEPTH = 1
DEFAULT_TAG = {
    TAG_NAME: "*",
    TAG_VALUE: "*",
    TAG_SEARCH_DEPTH: DEFAULT_TAG_SEARCH_DEPTH,
}
DISK_CHECK_FREQUENCY = 1000
MAX_DOWNLOAD_RETRY = 2
MAX_TRACE_PER_QUERY = 1000

# generated csv file names
SUMMARY_CSV = "summary.csv"
HYPO_LATENCY_CSV = "hypoLatency.csv"
TRACE_STATS_CSV = "traceStats.csv"
ERROR_DEPTH_CSV = "errDepth.csv"
PERCENT_ERROR_DEPTH_CSV = "percentErrDepth.csv"
SELF_ERROR_DEPTH_TO_NUM_TRACES_CSV = "selfErrDepthToNumTraces.csv"
PERCENT_SELF_ERROR_DEPTH_TO_NUM_TRACES_CSV = "percentSelfErrDepthToNumTraces.csv"
MAX_ERROR_DEPTH_PROP_TO_ROOT_TO_NUM_TRACES_CSV = "maxErrDepthPropToRootToNumTraces.csv"
PERCENT_MAX_ERROR_DEPTH_PROP_TO_ROOT_TO_NUM_TRACES_CSV = (
    "percentMaxErrDepthPropToRootToNumTraces.csv"
)
ERROR_PROP_LENGTH_CSV = "errPropLength.csv"
RESILIENCY_CSV = "resiliency.csv"
PERCENT_ERROR_CSV = "percentError.csv"
SAVING_POTENTIAL_CSV = "savingPotential.csv"
PER_TRACE_ERR_INFO_CSV = "perTraceErrInfo.csv"
CYCLES_CSV = "cycles.csv"
CROSS_REGION_CALLS_CSV = "crossRegionCalls.csv"

# various header names used in generated CSV files
PRORP_TO_ROOT = "PropToRoot"
NOT_PRORP_TO_ROOT = "NotPropToRoot"
PRORP_TO_ROOT_ON_CP = "PropToRootOnCP"
NOT_PRORP_TO_ROOT_ON_CP = "NotPropToRootOnCP"
SUPRESSED_ERR = "SupressedErr"
SUPRESSED_ERR_ON_CP = "SupressedErrOnCP"
METRIC_COL = "metric"
ROOT_HAS_ERR_COL = "hasRootErr"
HAS_ROOT_ERR = "1"
NO_ROOT_ERR = "0"
TRACE_COL = "trace"
TRACE_TYPE_COL = "traceType"
WORK_COL = "work"
WORK_SAVED_COL = "workSaved"
PERCENT_WORK_SAVED_COL = "%workSaved"
LATENCY_COL = "latency"
TIME_SAVED_ON_CP_COL = "timeSavedOnCP"
PERCENT_LATENCY_SAVED_COL = "%latencySaved"
NUM_CP_ERRORS_COL = "#CPErrors"
PERCENT_CP_ERRORS_COL = "%CPErrors"
NUM_CONNECTED_TO_CP_ERRORS_COL = "#connectedToCPErrors"
PERCENT_CONNECTED_TO_CP_ERRORS_COL = "%connectToCPErrors"
NUM_ERRORS_COL = "#errors"
PERCENT_ERRORS_COL = "%errors"
NUM_NODES_ON_CP_COL = "#nodesOnCP"
NUM_NODES_COL = "#nodes"
MAX_DEPTH_COL = "maxDepth"
NUM_SELF_ERRORS_COL = "#selfErrors"
MAX_ERR_DEPTH_PROP_TO_ROOT_COL = "maxErrDepthPropagatedToRoot"
PERCENT_MAX_ERR_DEPTH_PROP_TO_ROOT_COL = "%maxErrDepthPropagatedToRoot"
MIN_DEPTH_NON_ROOT_SELF_ERRORS_COL = "minDepthNonRootSelfErrors"
MAX_DEPTH_NON_ROOT_SELF_ERRORS_COL = "maxDepthNonRootSelfErrors"
P50_DEPTH_NON_ROOT_SELF_ERRORS_COL = "p50DepthNonRootSelfErrors"
P90_DEPTH_NON_ROOT_SELF_ERRORS_COL = "p90DepthNonRootSelfErrors"
P95_DEPTH_NON_ROOT_SELF_ERRORS_COL = "p95DepthNonRootSelfErrors"
P99_DEPTH_NON_ROOT_SELF_ERRORS_COL = "p99DepthNonRootSelfErrors"
MIN_PERCENT_DEPTH_NON_ROOT_SELF_ERRORS_COL = "min%DepthNonRootSelfErrors"
P50_PERCENT_DEPTH_NON_ROOT_SELF_ERRORS_COL = "p50%DepthNonRootSelfErrors"
P90_PERCENT_DEPTH_NON_ROOT_SELF_ERRORS_COL = "p90%DepthNonRootSelfErrors"
P95_PERCENT_DEPTH_NON_ROOT_SELF_ERRORS_COL = "p95%DepthNonRootSelfErrors"
P99_PERCENT_DEPTH_NON_ROOT_SELF_ERRORS_COL = "p99%DepthNonRootSelfErrors"
MAX_PERCENT_DEPTH_NON_ROOT_SELF_ERRORS_COL = "max%DepthNonRootSelfErrors"

DEPTH_COL = "depth"
PERCENT_DEPTH_COL = "%depth"
PROPAGATION_LENGTH_COL = "propagationLength"
NUM_PROPAGATED_ERRORS_COL = "#propagatedErrors"
NUM_STOPPED_ERRORS_COL = "#stoppedErrors"
OP_NAME_COL = "opName"
RESILIENCY_COL = "resiliency"
NUM_TRACES_COL = "#traces"
NUM_TRACES_FOR_MIN_COL = "#tracesForMin"
NUM_TRACES_FOR_MAX_COL = "#tracesForMax"
NUM_TRACES_FOR_P50_COL = "#tracesForP50"
NUM_TRACES_FOR_P90_COL = "#tracesForP90"
NUM_TRACES_FOR_P95_COL = "#tracesForP95"
NUM_TRACES_FOR_P99_COL = "#tracesForP99"
NUM_TRACES_FOR_CP_ERRORS_COL = "#tracesForCPErrors"
NUM_TRACES_FOR_CONNECTED_TO_CP_ERRORS_COL = "#tracesForConnectedToCPErrors"
POTENTIAL_SAVING_COL = "potentialSaving"
NUM_OP_COL = "#ops"
PERCENTILE_COL = "percentile"
# TODO: UPDATE ME
LATENCY_REDUCTION_COL = "latency_reduction"

NON_TEST_TRACES_STATS_TAG = "statsAcrossNonTestTraces"
ROOT_ERROR_TRACES_STATS_TAG = "statsAcrossRootErrTraces"
NON_ROOT_ERROR_TRACES_STATS_TAG = "statsAcrossNonRootErrTraces"


class Config:
    def __init__(
        self,
        operationName: str = "",
        serviceName: str = "",
        tags: list = DEFAULT_TAGS,
        output: str = "traces",
        numTrace: int = 1000,
        ioParallelism: int = 1,
        computeParallelism: int = 1,
        lookbackDays: int = 1,
        startTimestamp: typing.Optional[int] = None,
        endTimestamp: typing.Optional[int] = None,
        ignoreLastNMinutes: int = 10,
        timeoutSec: int = 60,
        jaegerQueryUrl: str = "http://localhost:16686",
        useMidnightTime: bool = False,
        rootTrace: bool = False,
        anonymize: bool = False,
        inputDir: str = "traces",
        filterProxy: bool = False,
        file: typing.Optional[argparse.FileType] = None,
        topN: int = 5,
        numHMTrace: int = 100,
        numOperation: int = 100,
        tracesDir: str = "traces",
        downloadRetry: int = 10,
        diskFreeWaitTime: int = 60,
        diskRequirement: int = 5,
        doRanges: bool = False,
        endpointDiskGBLimit: int = 500,
        dryRun: bool = False,
        exclusionSet: typing.Optional[set] = None,
        qps: int = 500,
        numShards: int = 1,
        shardId: int = 0,
        errorAnalysis: bool = False,
        ignoreTestTraces: bool = False,
        deltaMicroSec: int = 0,
        deltaTargetService: typing.Optional[str] = None,
        deltaTargetOperation: typing.Optional[str] = None,
        lightMode: bool = False,
        mergeAllRoots: bool = True,
        maxExemplars: int = 3,
    ):
        self.operationName = operationName
        self.serviceName = serviceName
        self.tags = tags
        self.output = output
        self.numTrace = numTrace
        self.ioParallelism = ioParallelism
        self.computeParallelism = computeParallelism
        self.lookbackDays = lookbackDays
        self.ignoreLastNMinutes = ignoreLastNMinutes
        self.timeoutSec = timeoutSec
        self.jaegerQueryUrl = jaegerQueryUrl
        self.useMidnightTime = useMidnightTime
        self.rootTrace = rootTrace
        self.anonymize = anonymize
        self.inputDir = inputDir
        self.filterProxy = filterProxy
        self.file = file
        self.topN = topN
        self.numHMTrace = numHMTrace
        self.numOperation = numOperation
        self.jaegerTraceFiles = []
        self.tracesDir = tracesDir
        self.filesToUpload = None
        self.traceIDs = []
        self.downloadRetry = downloadRetry
        self.failed = False
        self.failedLog = []
        self.diskFreeWaitTime = diskFreeWaitTime
        self.diskRequirement = diskRequirement
        self.doRanges = doRanges
        self.endpointDiskGBLimit = endpointDiskGBLimit
        self.dryRun = dryRun
        self.exclusionSet = exclusionSet if exclusionSet else set()
        self.qps = qps
        self.numShards = numShards
        self.shardId = shardId
        self.callsPerWorker = max(
            1,
            int(self.qps / (self.ioParallelism * self.numShards)),
        )
        self.errorAnalysis = errorAnalysis
        self.ignoreTestTraces = ignoreTestTraces
        self.lightMode = lightMode
        self.mergeAllRoots = mergeAllRoots
        self.maxExemplars = maxExemplars
        # Compute the start and end UTC times for trace query.
        initialTimeStamp = getMidnightTimeStamp() if self.useMidnightTime else time.time() * 1000 * 1000
        self.startTimestamp = int(initialTimeStamp - (self.lookbackDays * 24 * 60 * 60) * 1000 * 1000) if not startTimestamp else startTimestamp
        self.endTimestamp = int(initialTimeStamp - (self.ignoreLastNMinutes * 60) * 1000 * 1000) if not endTimestamp else endTimestamp
        self.deltaMicroSec = deltaMicroSec
        self.deltaTargetService = deltaTargetService
        self.deltaTargetOperation = deltaTargetOperation

    @property
    def projectionEnabled(self) -> bool:
        return self.deltaTargetService is not None and self.deltaTargetOperation is not None

    def getOutputDir(self):
        if self.file:
            return os.path.dirname(self.file.name)
        return self.tracesDir


class PipelinePhase:
    def __init__(
        self,
        name: str,
        func: Callable[[Config, mp.Queue], None],
        blocking: bool,
    ):
        self.name = name
        self.func = func
        self.blocking = blocking


def templateHandler(
    message: str,
    realHandler: Callable[[Config], None],
    preStart: Callable[[Config], None],
    postFinish: Callable[[Config], None],
    c: Config,
    resultQ: mp.Queue,
) -> Config:
    logging.info(f"{message} for {c.serviceName}::{c.operationName}")
    try:
        if not c.failed:
            if preStart:
                preStart(c)
            realHandler(c)
            if postFinish:
                postFinish(c)
        else:
            logging.info(f"Skipping {message} for {c.serviceName}::{c.operationName}")
    except Exception as ex:
        exceptionStr = "".join(traceback.TracebackException.from_exception(ex).format())
        logging.warning(exceptionStr)
        c.failedLog.append(exceptionStr)
        c.failedLog.append(f"{message} failed for {c.serviceName}::{c.operationName}")
        c.failed = True
    if resultQ:
        resultQ.put(c)
        resultQ.close()

    logging.info(f"Finished {message} for {c.serviceName}::{c.operationName}")
    return c


def replaceNonAlphaNumericWithUnderscore(str):
    return re.sub("[^a-zA-Z0-9_]+", "_", str)


def getMidnightTimeStamp():
    midnight = (
        datetime.datetime.now(dateutil.tz.gettz("UTC"))
        .replace(hour=0, minute=0, second=0, microsecond=0)
        .astimezone(dateutil.tz.tzutc())
    )
    return midnight.timestamp() * 1000 * 1000


# Helper function to convert signed int to hex string
def intToHexString(value):
    return value.to_bytes(8, byteorder='big', signed=True).hex()


def serviceOperationToTBPath(service, operation, blobPath, suffix):
    return os.path.join(
        blobPath,
        replaceNonAlphaNumericWithUnderscore(service),
        replaceNonAlphaNumericWithUnderscore(operation),
        suffix,
    )


SERVICE_TAG_NAME = "service_name"
OPERATION_TAG_NAME = "operation_name"


def getServiceOperationTags(config: "Config") -> dict[str, str]:
    return {
        SERVICE_TAG_NAME: replaceNonAlphaNumericWithUnderscore(config.serviceName),
        OPERATION_TAG_NAME: replaceNonAlphaNumericWithUnderscore(config.operationName),
    }
