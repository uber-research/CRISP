import argparse
import copy
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
from tb_client import TBClient

# these two packages are needed for plotting
import jinja2
import matplotlib as mpl

TOTAL_TIME = "totalTime"
DEFAULT_TAGS = []
TAG_YAML_VERSION = "1.0"
TAG_NAME = "name"
TAG_VALUE = "value"
TAG_SEARCH_DEPTH = "search_depth"
TAG_KEYS = [TAG_NAME, TAG_VALUE, TAG_SEARCH_DEPTH]
DEFAULT_TAG_SEARCH_DEPTH = 1
DEFAULT_TAG = {
    TAG_NAME: "*",
    TAG_VALUE: "*",
    TAG_SEARCH_DEPTH: DEFAULT_TAG_SEARCH_DEPTH,
}
DISK_CHECK_FREQUENCY = 1000
MAX_DOWNLOAD_RETRY = 2
MAX_TRACE_PER_QUERY = 5000
PARQUET_FILE_NAME = "traces.parquet"

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

LATENCY_REDUCTION_COL = "latency_reduction"

NON_TEST_TRACES_STATS_TAG = "statsAcrossNonTestTraces"
ROOT_ERROR_TRACES_STATS_TAG = "statsAcrossRootErrTraces"
NON_ROOT_ERROR_TRACES_STATS_TAG = "statsAcrossNonRootErrTraces"

CRISP_LOCAL_NAME = "crisp"
CRISP_LOCAL_PORT = "12345" # TODO: placeholder, please change on your own
CRISP_CI_AGENT = "example@domain.com" # TODO: placeholder, please change on your own

APPLICATION_IDENTIFIER = "crisp-service"
APPLICATION_ENVIRONMENT = os.getenv("CRISP_ENV", "default-env") # TODO: placeholder, please change on your own


class MetricVals:
    def __init__(self, inc, excl, freq, sid):
        self.inc = inc
        self.excl = excl
        self.freq = freq
        self.incExVal, self.incEx = inc, sid
        self.exclExVal, self.exclEx = excl, sid

    def __str__(self) -> str:
        return f"self.inc={self.inc}, self.excl={self.excl}, self.freq={self.freq}, self.incEx={self.incEx}, self.exclEx={self.exclEx}"

    # Adds the values of metrics ignoring the sid.
    def __add__(self, other):
        result = MetricVals(0, 0, 0, -1)
        result += self
        result += other
        return result

    # Adds the values of metrics ignoring the sid.
    def __iadd__(self, metric):
        self.inc += metric.inc
        self.excl += metric.excl
        self.freq += metric.freq
        if metric.incExVal > self.incExVal:
            self.incEx = metric.incEx
            self.incExVal = metric.incExVal
        if metric.exclExVal > self.exclExVal:
            self.exclEx = metric.exclEx
            self.exclExVal = metric.exclExVal
        return self

    # Divides each metric value by val. Ignores the sids.
    def __floordiv__(self, val):
        result = MetricVals(0, 0, 0, -1)
        result.inc = self.inc
        result.excl = self.excl
        result.freq = self.freq
        result //= val
        return result

    # Divides each metric value by val. Ignores the sids.
    def __ifloordiv__(self, val):
        self.inc //= val
        self.excl //= val
        self.freq //= val
        return self


class CallPathProfile:
    def __init__(self, kv: dict[str, any], count: int, traceId):
        self.profile = kv
        self.count = count
        self.traceId = traceId

    def Get(self):
        return self.profile

    # Returns the profile metrics divided by the count. Ignores SID.
    def GetNormalized(self):
        if self.count == 0:
            raise Exception("GetNormalized called with zero count.")  # noqa: TRY002
        result = {}
        for k, v in self.profile.items():
            result[k] = v // self.count
        return result

    # Divides profile metrics by the count. Ignores SID.
    def Normalize(self):
        if self.count == 0:
            raise Exception("Normalize called with zero count.")  # noqa: TRY002
        for k in self.profile:
            self.profile[k] = self.profile[k] // self.count

    # Divides the specified field of the metrics in profiles by the count.
    def NormalizeField(self, field):
        if self.count == 0:
            raise Exception("NormalizeField called with zero count.")  # noqa: TRY002
        for k in self.profile:
            v = getattr(self.profile[k], field) // self.count
            setattr(self.profile[k], field, v)

    # Updates (via addition) or inserts a new callpath metric without changing the count.
    def Upsert(self, path, metric):
        if path in self.profile:
            self.profile[path] += metric
        else:
            self.profile[path] = copy.copy(metric)

    # Makes the field zero if it is negative.
    def Sanitize(self, field, debug_on):
        for k, v in self.profile.items():
            if hasattr(v, field):
                if getattr(v, field) < 0:
                    debug_on and logging.debug(
                        f"In {self.filename} zero out time entry for key {k} field {field}",
                    )
                    setattr(self.profile[k], field, 0)

    # Merges two call path profiles returning the summation. The SIDs are not propagated.
    def __add__(self, other):
        result = CallPathProfile({}, 0, -1)
        result += self
        result += other
        return result

    # Merges two call path profiles returning the summation. The SIDs are not propagated.
    def __iadd__(self, other):
        for call_path, metric in other.profile.items():
            self.Upsert(call_path, metric)
        self.count += other.count
        return self

    def __repr__(self):
        return str(self.profile)


class Config:
    def __init__(
        self,
        operationName: str = "default-operation",
        serviceName: str = "default-service",
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
        useUSSO: bool = False,
        useMidnightTime: bool = False,
        rootTrace: bool = False,
        anonymize: bool = False,
        uploadToTB: bool = False,
        uploadToCrispRiTB: bool = False,
        inputDir: str = "traces",
        filterProxy: bool = False,
        file: typing.Optional[argparse.FileType] = None,
        topN: int = 5,
        numHMTrace: int = 100,
        numOperation: int = 100,
        ignoreCtfTests: bool = False,
        tracesDir: str = "traces",
        jaegerOfflineToken: typing.Optional[str] = None,
        terrablobOfflineToken: typing.Optional[str] = None,
        downloadRetry: int = 10,
        diskFreeWaitTime: int = 60,
        diskRequirement: int = 5,
        doRanges: bool = False,
        uploadTar: bool = False,
        endpointDiskGBLimit: int = 500,
        noOverwriteUpload: bool = False,
        dryRun: bool = False,
        exclusionSet: typing.Optional[set] = None,
        qps: int = 500,
        numShards: int = 1,
        shardId: int = 0,
        serviceMode: bool = False,
        enableM3Metrics: bool = False,
        jobTag: typing.Optional[str] = None,
        errorAnalysis: bool = False,
        useParquet: bool = False,
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
        self.useUSSO = useUSSO
        self.useMidnightTime = useMidnightTime
        self.rootTrace = rootTrace
        self.anonymize = anonymize
        self.uploadToTB = uploadToTB
        self.uploadToCrispRiTB = uploadToCrispRiTB
        self.inputDir = inputDir
        self.filterProxy = filterProxy
        self.file = file
        self.topN = topN
        self.numHMTrace = numHMTrace
        self.numOperation = numOperation
        self.ignoreCtfTests = ignoreCtfTests
        self.jaegerTraceFiles = []
        self.tracesDir = tracesDir
        self.filesToUpload = None
        self.traceIDs = []
        self.jaegerOfflineToken = jaegerOfflineToken
        self.terrablobOfflineToken = terrablobOfflineToken
        self.downloadRetry = downloadRetry
        self.failed = False
        self.failedLog = []
        self.diskFreeWaitTime = diskFreeWaitTime
        self.diskRequirement = diskRequirement
        self.doRanges = doRanges
        self.uploadTar = uploadTar
        self.endpointDiskGBLimit = endpointDiskGBLimit
        self.noOverwriteUpload = noOverwriteUpload
        self.dryRun = dryRun
        self.exclusionSet = exclusionSet if exclusionSet else set()
        self.qps = qps
        self.numShards = numShards
        self.shardId = shardId
        self.callsPerWorker = max(
            1,
            int(self.qps / (self.ioParallelism * self.numShards)),
        )
        self.serviceMode = serviceMode
        self.enableM3Metrics = enableM3Metrics
        self.jobTag = jobTag
        self.errorAnalysis = errorAnalysis
        self.useParquet = useParquet

        # Compute the start and end UTC times for trace query.
        initialTimeStamp = getMidnightTimeStamp() if self.useMidnightTime else time.time() * 1000 * 1000
        self.startTimestamp = int(initialTimeStamp - (self.lookbackDays * 24 * 60 * 60) * 1000 * 1000) if not startTimestamp else startTimestamp
        self.endTimestamp = int(initialTimeStamp - (self.ignoreLastNMinutes * 60) * 1000 * 1000) if not endTimestamp else endTimestamp

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


def serviceOperationToTBPath(service, operation, blobPath, suffix):
    # Given a service name and operation name, get its path in terrablob.
    return os.path.join(
        blobPath,
        replaceNonAlphaNumericWithUnderscore(service),
        replaceNonAlphaNumericWithUnderscore(operation),
        suffix,
    )


def getTBClient(localDev, timeout):
    if localDev:
        client = TBClient(service_name=CRISP_LOCAL_NAME, port=CRISP_LOCAL_PORT, timeout=timeout)
    else:
        client = TBClient(service_name=CRISP_CI_AGENT, timeout=timeout)
    return client

def dirExistsOnTB(dirName, localDev=False):
    client = getTBClient(localDev, 60)
    return client.check_if_dir_exists(path=dirName)


def blobExistsOnTB(path, localDev=False):
    client = getTBClient(localDev, 60)
    return client.check_if_file_exists(path=path)


def downloadFromTerrablob(tbFilePath, destFilePath, localDev=False):
    client = getTBClient(localDev, 2 * 60)
    # Conversion to string is needed in case the Path object will be passed
    return client.download_file_from_tb(tb_file_path=str(tbFilePath), local_file_path=destFilePath)


def uploadToTerrablob(filePath, tbPath, localDev=False):
    tbFile = os.path.join(tbPath, os.path.basename(filePath))
    client = getTBClient(localDev, 2 * 60)
    # Conversion to string is needed in case the Path object will be passed
    return client.upload_file_to_tb(tb_file_path=str(tbFile), local_file_path=filePath)


def getLeafNodeFromCallPath(path):
    return path.rsplit("->", 1)[-1]


# upload filePath to Terrablob (TB).  The TB file path is constructed based on
# blobPathPrefix, service, operation, filePath, dateTime; optionally, the same file may be
# published and uploaded to TB as 'latest' data.
# The function returns the TB file path (tbFilePath) uploaded and the TB file path
# uploaded as the latest
def constructPathAndUploadToTerrablob(
    blobPathPrefix,
    service,
    operation,
    filePath,
    dateTime,
    publishAsLatest,
):
    # Put the data blob for a given service and operation into Terrablob
    tbPath = os.path.join(
        serviceOperationToTBPath(service, operation, blobPathPrefix, dateTime),
    )
    tbFile = uploadToTerrablob(filePath, tbPath)

    if not publishAsLatest:
        return tbFile, None

    tbPath = os.path.join(
        serviceOperationToTBPath(service, operation, blobPathPrefix, suffix="latest"),
    )
    latestTbFile = uploadToTerrablob(filePath, tbPath)
    return tbFile, latestTbFile


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
