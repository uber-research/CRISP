import argparse
import json
import logging
import multiprocessing as mp
import os
import re
import shutil
import subprocess
import time
from http import HTTPStatus

import requests
from ratelimit import limits
from tenacity import (
    retry,
    retry_if_result,
    stop_after_attempt,
    stop_after_delay,
    wait_random,
)

import crisp.common as common
from crisp.exceptions import (  # noqa: F401
    NoTracesDownloadedException,
    NoTraceIDsFoundException,
)


logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)


def initArgs():
    my_parser = argparse.ArgumentParser()
    my_parser.add_argument(
        "--operationName",
        action="store",
        help="name of the operation",
        default="",
        type=str,
    )
    my_parser.add_argument(
        "-s",
        "--serviceName",
        action="store",
        help="name of the service",
        default="",
        type=str,
    )
    my_parser.add_argument(
        "--output",
        action="store",
        help="where to store the trace",
        default="traces",
        type=str,
    )
    my_parser.add_argument(
        "--numTrace",
        action="store",
        help="number of traces to download",
        default=1000,
        type=int,
    )
    my_parser.add_argument(
        "--parallelism",
        action="store",
        help="Number of concurrent python processes.",
        default=1,
        type=int,
    )
    my_parser.add_argument(
        "--lookbackDays",
        action="store",
        help="Number of days to look back for trace collection.",
        default=1,
        type=int,
    )
    my_parser.add_argument(
        "--ignoreLastNMinutes",
        action="store",
        help="Number of last N minutes to ignore for trace collection.",
        default=10,
        type=int,
    )
    my_parser.add_argument(
        "--timeoutSec",
        action="store",
        help="Timeout in secs for fetching traces.",
        default=60,
        type=int,
    )
    my_parser.add_argument(
        "--useMidnightTime",
        dest="useMidnightTime",
        action="store_true",
        default=False,
        required=False,
        help="Use the start time as the midnight today UTC.",
    )
    my_parser.add_argument(
        "--diskRequirement",
        dest="diskRequirement",
        default=5,
        type=int,
        help="disk requirement to download in Gigabyte.",
        action="store",
    )
    my_parser.add_argument(
        "--qps",
        dest="qps",
        default=500,
        type=int,
        help="QPS for downloading traces.",
        action="store",
    )
    my_parser.add_argument(
        "--numShards",
        dest="numShards",
        default=1,
        type=int,
        help="Number of shards for downloading traces.",
        action="store",
    )
    my_parser.add_argument(
        "--errorAnalysis",
        dest="errorAnalysis",
        action="store_true",
        default=False,
        required=False,
        help="Run error analysis",
    )
    my_parser.add_argument(
        "--startTimestamp",
        dest="startTimestamp",
        action="store",
        default=None,
        required=False,
        help="Start timestamp in Unix epoch time.",
        type=int,
    )
    my_parser.add_argument(
        "--endTimestamp",
        dest="endTimestamp",
        action="store",
        default=None,
        required=False,
        help="End timestamp in Unix epoch time.",
        type=int,
    )
    my_parser.add_argument(
        "--jaegerQueryUrl",
        dest="jaegerQueryUrl",
        action="store",
        default="http://localhost:16686",
        required=False,
        help="Base URL for the Jaeger query API.",
        type=str,
    )
    args = my_parser.parse_args()
    operationName = args.operationName
    serviceName = args.serviceName
    numTrace = args.numTrace
    outputDir = args.output
    lookbackDays = args.lookbackDays
    ignoreLastNMinutes = args.ignoreLastNMinutes
    parallelism = args.parallelism
    timeoutSec = args.timeoutSec
    useMidnightTime = args.useMidnightTime
    diskRequirement = args.diskRequirement
    qps = args.qps
    numShards = args.numShards
    errorAnalysis = args.errorAnalysis
    startTimestamp = args.startTimestamp
    endTimestamp = args.endTimestamp
    jaegerQueryUrl = args.jaegerQueryUrl
    return common.Config(
        operationName=operationName,
        serviceName=serviceName,
        output=outputDir,
        numTrace=numTrace,
        ioParallelism=parallelism,
        lookbackDays=lookbackDays,
        ignoreLastNMinutes=ignoreLastNMinutes,
        timeoutSec=timeoutSec,
        useMidnightTime=useMidnightTime,
        diskRequirement=diskRequirement,
        qps=qps,
        numShards=numShards,
        errorAnalysis=errorAnalysis,
        startTimestamp=startTimestamp,
        endTimestamp=endTimestamp,
        jaegerQueryUrl=jaegerQueryUrl,
    )


def getTraceIDs(startTime: int, endTime: int, c: common.Config, flags: int = 1) -> list[str]:
    # Query the trace ID.
    # To get top-level trace, add "num_references = 0" to where clause.

    if c.dryRun:
        return []

    headers = {}

    # Build the tags dict dynamically
    tags_dict = {"jaeger.flags": str(flags)}
    if c.errorAnalysis:
        tags_dict["error"] = "true"

    params = {
        "start": startTime,
        "end": endTime,
        "limit": c.numTrace,
        "service": c.serviceName,
        "operation": c.operationName,
        "tags": json.dumps(tags_dict),
    }

    result = requests.get(
        f"{c.jaegerQueryUrl}/api/traceids",
        params=params,
        headers=headers,
        timeout=(c.timeoutSec, c.timeoutSec),
    )
    # Handle the case, when response payload is non-JSON
    try:
        json_response = result.json()
    except json.JSONDecodeError:
        logging.warning(
            "Trace IDs fetching failed: '/api/traceids' endpoint returned non-JSON encoded response",
        )
        return []

    if result.status_code != HTTPStatus.OK:
        errors = json_response.get("errors")

        # Extract the error message if present and enclose it into single quotes to inject into log message
        error_message = f"'{errors[0].get('msg')}'" if errors else None
        logging.warning(
            f"Trace IDs fetching failed: '/api/traceids' endpoint returned '{result.status_code}' status code"
            # Include error message if present
            f"{f' with {error_message} error message' if error_message else ''}",
        )
        return []

    return json_response.get("data") or []


# checkStatusCode returns true only on valid retry status code

RETRYABLE_STATUS_CODES = {
    HTTPStatus.TOO_MANY_REQUESTS,
    HTTPStatus.REQUEST_TIMEOUT,
    HTTPStatus.GATEWAY_TIMEOUT,
}

TRACE_DOWNLOAD_FAILED_MSG = "Trace download failed: trace_id=%s status=%s reason=%s url=%s%s"


def checkStatusCode(value):
    return value in RETRYABLE_STATUS_CODES


def parseSize(sizeStr):
    units = {"K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4}
    match = re.match(r"^(\d+(?:\.\d+)?)\s*([KMGT]?)$", sizeStr)
    if match:
        size = float(match.group(1))
        unit = match.group(2)
        if unit in units:
            sizeBytes = size * units[unit]
            return sizeBytes
    logging.error(f"Failed to parse size string: {sizeStr}")
    # TODO: create custom exceptions and use them everywhere instead of pure `Exception` class
    raise Exception(f"Failed to parse size string: {sizeStr}")  # noqa: TRY002


def getOutputDirectorySize(dirPath):
    """
    Example output of 'du -sh /tmp':
    output is: 458M(tab)/tmp (it is '\'t' in the real output, replaced by 'tab' here to avoid linter error)
    outputLines is: ['458M(tab)/tmp']
    sizeLine is: 458M(tab)/tmp
    sizeStr is: 458M
    Folder size: 480247808.0 bytes
    """
    try:
        command = ["du", "-sh", dirPath]
        output = subprocess.check_output(command, text=True, stderr=subprocess.DEVNULL)
        outputLines = output.strip().split("\n")
        sizeLine = outputLines[-1]
        sizeStr = re.split(r"\s+", sizeLine)[0]
        sizeBytes = parseSize(sizeStr)
    except subprocess.CalledProcessError as e:
        logging.error(f"Error occurred while executing 'du' command: {e.output}")
        return 0
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        return 0
    else:
        return sizeBytes


def diskLimitReached(path, limit):
    # comparing size in Gigabyte
    in_usage = getOutputDirectorySize(path) / 1024 / 1024 / 1024
    return in_usage >= limit


# Fetch the trace and move to the folder


def download(id, c: common.Config):
    last_retryable_failure = None

    @retry(
        retry=retry_if_result(checkStatusCode),
        stop=(stop_after_delay(10) | stop_after_attempt(5)),
        wait=wait_random(min=1, max=5),
    )
    @limits(calls=c.callsPerWorker, period=1)
    def downloadHelper():
        nonlocal last_retryable_failure
        try:
            headers = {
                # this is to only get the error and span kind and reduce bandwidth pressure on Jaeger side
                # To get span tags in processes, do something like "resource.attributes.host.name"
                # Also include internal.splittrace tags to detect split traces for merging
                "tracing-span-fields": "attributes.error,attributes.span.kind,resource.attributes.region,resource.attributes.zone,attributes.internal.splittrace.traceID,attributes.internal.splittrace.spanID",
            }
            receive = requests.get(
                f"{c.jaegerQueryUrl}/api/traces/{id}",
                params={"raw": "true"},
                headers=headers,
                timeout=(c.timeoutSec, c.timeoutSec),
            )

            if receive is not None:
                if receive.status_code == HTTPStatus.OK:
                    last_retryable_failure = None
                    with open(f"{c.output}/{id}.json", "w") as f:
                        f.write(receive.content.decode("utf-8"))
                else:
                    response_snippet = ""
                    if receive.text:
                        snippet = receive.text.strip().replace("\n", " ")
                        if len(snippet) > 200:
                            snippet = f"{snippet[:200]}..."
                        response_snippet = f" response_snippet={snippet}"

                    failure_payload = (
                        id,
                        receive.status_code,
                        receive.reason or "unknown",
                        receive.url,
                        response_snippet,
                    )

                    if receive.status_code in RETRYABLE_STATUS_CODES:
                        last_retryable_failure = {
                            "trace_id": id,
                            "status": receive.status_code,
                            "reason": receive.reason or "unknown",
                            "url": receive.url,
                            "response_snippet": response_snippet,
                        }
                        logging.debug(
                            TRACE_DOWNLOAD_FAILED_MSG,
                            *failure_payload,
                        )
                    else:
                        last_retryable_failure = None
                        logging.warning(
                            TRACE_DOWNLOAD_FAILED_MSG,
                            *failure_payload,
                        )
                return receive.status_code
            else:
                logging.warning("requests.get returned no response for trace_id=%s", id)
        except Exception as e:
            logging.warning("Trace download raised exception for trace_id=%s: %s", id, e)
        return -1

    res = downloadHelper()
    if res in RETRYABLE_STATUS_CODES and last_retryable_failure:
        logging.warning(
            "Trace download exhausted retries: trace_id=%s status=%s reason=%s url=%s%s",
            last_retryable_failure["trace_id"],
            last_retryable_failure["status"],
            last_retryable_failure["reason"],
            last_retryable_failure["url"],
            last_retryable_failure["response_snippet"],
        )
    return res


def getTraceIDWrapper(c: common.Config, resultQ: mp.Queue) -> common.Config:
    return common.templateHandler(
        message="getTraceID step",
        realHandler=getTraceIDReal,
        preStart=None,
        postFinish=None,
        c=c,
        resultQ=resultQ,
    )


def getTraceIDReal(c: common.Config):
    logging.info(
        f"Fetching trace ids from UTC microsec: {c.startTimestamp} to {c.endTimestamp}",
    )

    # compute the number of queries to issue
    numQueries = int(c.numTrace / common.MAX_TRACE_PER_QUERY)
    if c.numTrace % common.MAX_TRACE_PER_QUERY != 0:
        numQueries += 1
    logging.info("Issuing %d queries to fetch %d traces" % (numQueries, c.numTrace))  # noqa: UP031
    # compute the time range for each query
    timeRange = int((c.endTimestamp - c.startTimestamp) / numQueries)
    # compute the start and end time for each query
    startTimeList = [c.startTimestamp + i * timeRange for i in range(numQueries)]
    endTimeList = [c.startTimestamp + (i + 1) * timeRange for i in range(numQueries)]
    # the last query should end at c.endTimestamp
    endTimeList[-1] = c.endTimestamp
    # fetch trace ids for each query
    traceIDsKV = {}
    for i in range(numQueries):
        traceIDs = []
        # The flags is a bit mask; sampled traces have bit-0 set,
        # debug traces have bit-1 set.
        # Producton trace has flag 1 whereas ctf test trace has flag 9.
        # Some endpoints (e.g., SIA service) have both "sampled traces" and "debug traces" => 0b11 => decimal 3 set.
        # We first try flag=1; if it returns no traces, we try flag=3.
        for flag in [1, 3]:
            try:
                traceIDs = getTraceIDs(startTimeList[i], endTimeList[i], c, flag)
            except Exception:
                logging.info("failed to get traceIds with flag: %d" % (flag,))  # noqa: UP031
            else:
                if len(traceIDs) > 0:
                    traceIDsKV[i] = traceIDs
                    break  # end the loop.

    # if the total traces in traceIDsKV > c.numTrace, we need to remove some traces.
    # retain each traceIDsKV[i] by its proportion to the total number of traces.
    totalTrace = sum([len(traceIDsKV[i]) for i in traceIDsKV])

    if not totalTrace:
        logging.error(
            f"No trace IDs were found for {c.serviceName}::{c.operationName} within ({c.startTimestamp} - {c.endTimestamp}) time range",
        )
        raise NoTraceIDsFoundException

    # Log the distribution of traces in each query along with the % of total traces.
    logging.info("Trace distribution in each query:")
    for i in traceIDsKV:  # noqa: PLC0206
        logging.info(
            "%d: %d (%.2f%%)"  # noqa: UP031
            % (i, len(traceIDsKV[i]), len(traceIDsKV[i]) * 100 / c.numTrace),
        )
    if totalTrace > c.numTrace:
        for i in traceIDsKV:
            traceIDsKV[i] = traceIDsKV[i][
                : int(len(traceIDsKV[i]) * c.numTrace / totalTrace)
            ]

    # flatten the traceIDsKV to traceIDs
    traceIDs = []
    for i in traceIDsKV:  # noqa: PLC0206
        traceIDs.extend(traceIDsKV[i])
    logging.info("Total %d trace ids fetched" % len(traceIDs))  # noqa: UP031
    c.traceIDs = traceIDs

    return 0


# isDiskEnough check if the current disk space has more than threshold free space in Gigabyte (200 by default)
def isDiskEnough(threshold: int = 5) -> bool:
    # disk_usage returns a tuple like
    # usage(total=994662584320, used=248365891584, free=746296692736)
    stat = shutil.disk_usage(".")
    return stat[2] >= threshold * 1024 * 1024 * 1024


def downloadWrapper(args):
    traceId, c = args
    result = download(traceId, c)
    return result


def traceDownloadWrapper(c: common.Config, resultQ: mp.Queue) -> common.Config:
    return common.templateHandler(
        message="traceDownload step",
        realHandler=traceDownloadReal,
        preStart=None,
        postFinish=None,
        c=c,
        resultQ=resultQ,
    )


def traceDownloadReal(c: common.Config):
    counter = 0
    while counter < c.downloadRetry:
        if not isDiskEnough(c.diskRequirement):
            counter += 1
            time.sleep(c.diskFreeWaitTime)
        else:
            break

    if counter == c.downloadRetry:
        logging.warning(
            f"{c.serviceName}::{c.operationName} download failed due to disk space limit",
        )
        logging.warning(f"Free disk: {shutil.disk_usage('.')[2]} bytes")
        return 1

    logging.info("enough disk")

    # Have enough disk space, create the output directory if it does not exist.
    os.makedirs(c.output, exist_ok=True)

    funcExecutionStartTime = time.time()

    logging.info(f"Begin trace download for {c.serviceName}::{c.operationName}")
    metrics = {}
    if c.ioParallelism == 1:
        for idx in range(0, len(c.traceIDs), common.DISK_CHECK_FREQUENCY):
            if diskLimitReached(c.output, c.endpointDiskGBLimit):
                logging.warning(
                    f"Disk full for {c.output} during {c.serviceName}::{c.operationName} at {idx}",
                )
                break
            for i in range(
                idx,
                min(idx + common.DISK_CHECK_FREQUENCY, len(c.traceIDs)),
            ):
                res = download(c.traceIDs[i], c)
                if res in metrics:
                    metrics[res] += 1
                else:
                    metrics[res] = 1
    else:
        with mp.Pool(c.ioParallelism) as p:
            for idx in range(0, len(c.traceIDs), common.DISK_CHECK_FREQUENCY):
                if diskLimitReached(c.output, c.endpointDiskGBLimit):
                    logging.warning(
                        f"Disk full for {c.output} during {c.serviceName}::{c.operationName} at {idx}",
                    )
                    break
                iterEnd = min(idx + common.DISK_CHECK_FREQUENCY, len(c.traceIDs))
                wrappedData = [(traceId, c) for traceId in c.traceIDs[idx:iterEnd]]
                logging.info(f"Downloading {c.serviceName}::{c.operationName} at {idx}")

                # This code sporadically fails with
                # multiprocessing.pool.MaybeEncodingError: Error sending result:
                #  '<multiprocessing.pool.ExceptionWithTraceback object at 0x7f4793a73070>'.
                # Reason: 'TypeError("cannot pickle '_thread.RLock' object")'
                # This is a known issue with multiprocessing module and we do use any RLock objects explictly.
                # As a workaround, we catch multiprocessing.pool.MaybeEncodingError and retry the download.
                for i in range(common.MAX_DOWNLOAD_RETRY):
                    try:
                        for result in p.imap(
                            downloadWrapper,
                            wrappedData,
                            chunksize=100,
                        ):
                            if result in metrics:
                                metrics[result] += 1
                            else:
                                metrics[result] = 1
                        break
                    except mp.pool.MaybeEncodingError as e:
                        # if we have reached the max retry, we give up and raise the exception
                        if i == common.MAX_DOWNLOAD_RETRY - 1:
                            raise
                        logging.warning(f"Error occurred while downloading traces: {e}")
                        logging.warning(
                            f"Retrying download for {c.serviceName}::{c.operationName} at {idx}",
                        )
                        continue

    # Log the metrics in sorted order of keys.
    success = 0
    for k in sorted(metrics.keys()):
        logging.info(
            f"{c.serviceName}::{c.operationName} DownloadStatusCode{k}={metrics[k]}",
        )
        if k == HTTPStatus.OK:
            success += metrics[k]

    logging.info(
        "%d/%d fetches succeeded for %s::%s"  # noqa: UP031
        % (success, len(c.traceIDs), c.serviceName, c.operationName),
    )

    return 0


def main():
    c = initArgs()
    getTraceIDReal(c)
    traceDownloadReal(c)
    return 0


if __name__ == "__main__":
    main()

