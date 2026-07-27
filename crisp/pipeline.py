import logging
import multiprocessing as mp
import os
import shutil
import time
import traceback
import typing
from collections.abc import Callable

import crisp.common as common

# TODO: Dynamically compute the max workers based on the pipeline steps, number of cores, and intra-step parallelism.
MAX_WORKERS = 16

# Default ceiling on how long a single work item is allowed to run before we
# forcibly terminate its worker process. Chosen from production experience:
# legitimately slow (but successful) work items have taken up to ~90
# minutes, while a genuinely hung one can block the pipeline for many hours
# with no further progress. 120 minutes gives meaningful headroom over the
# slowest known-good case while still bounding the pipeline to a sane
# wall-clock limit. Overridable via env var so it can be tuned without a
# code change. Only applies to non-serialize (multiprocess) phases: a
# serialize=True handler runs synchronously in-process before the
# timeout-aware wait loop is ever entered, so it is not bounded by this value.
WORKITEM_TIMEOUT_SEC = int(os.environ.get("CRISP_WORKITEM_TIMEOUT_SEC", 120 * 60))


class WorkItem:
    def __init__(self, id: int, config: common.Config, isLast: bool = False):
        self.isLast = isLast
        self.itemId = id
        self.log = []
        self.state = 0
        self.config = config


def cleanupReal(c: common.Config) -> common.Config:
    # Start the timer
    funcExecutionStartTime = time.time()

    shutil.rmtree(c.output)

    funcExecutionEndTime = time.time()
    funcExecutionTime = (funcExecutionEndTime - funcExecutionStartTime) * 1000
    common.emitDurationMetric(key=common.M3_CLEANUP_PHASE_DURATION, duration=funcExecutionTime, config=c)

    return c


def cleanupWrapper(c: common.Config, resultQ: mp.Queue) -> common.Config:
    return common.templateHandler(
        message="cleanup step",
        realHandler=cleanupReal,
        preStart=None,
        postFinish=None,
        c=c,
        resultQ=resultQ,
    )


# TODO: Use error handling to prevent an outright crash (e.g. OOM-kill, segfault) in a
# single workitem's process from bringing down the entire pipeline.
class Worker:
    def __init__(
        self,
        name: str,
        c: common.Config,
        isLast: bool,
        handler: Callable[[common.Config, mp.Queue], None],
        serialize=False,
    ):
        if not name:
            raise ValueError("name cannot be None")
        if not c:
            raise ValueError("c cannot be None")
        if not handler:
            raise ValueError("handler cannot be None")
        self.name = name
        self.c = c
        self.isLast = isLast
        self.handler = handler
        self.serialize = serialize
        self.storedResult = None
        self.startTime = time.time()
        # Nop for last item.
        if isLast:
            return

        # Serial executions happen in-process.
        if serialize:
            self.storedResult = handler(c, resultQ=None)
        else:
            # Launch handler as a Multiprocess.Process.
            self.q = mp.Queue(maxsize=1)
            self.process = mp.Process(target=handler, args=(c, self.q))
            self.process.start()

    def getWaitable(self) -> mp.Queue:
        if self.serialize:
            raise ValueError("getWaitable called on a serialized worker")
        if self.isLast:
            raise ValueError("getWaitable called on a last worker")
        return self.q._reader

    def elapsedSec(self) -> float:
        return time.time() - self.startTime

    def getResult(self) -> common.Config:
        if self.isLast:
            return self.c
        if self.serialize:
            return self.storedResult
        if self.storedResult:
            return self.storedResult
        # always finish the queue before joining.
        # This order of operations is important, otherwise a deadlock can happen.
        self.storedResult = self.q.get()
        self.process.join()
        self.q.close()
        return self.storedResult

    def terminateForTimeout(self, workItemTimeoutSec: float = WORKITEM_TIMEOUT_SEC) -> common.Config:
        """Force-terminate a worker process that exceeded workItemTimeoutSec.

        Prevents a single stuck work item (e.g. an oversized or edge-case
        trace) from blocking the entire pipeline stage indefinitely. If the
        worker happened to finish right as it was being reaped, its real
        result is returned unmodified; otherwise a Config marked as failed
        is returned so it can flow through the pipeline like any other
        (unsuccessful) result. Either way, the underlying process is
        guaranteed to be terminated before this method returns -- a worker
        that wrote its result but then hangs afterwards (e.g. in cleanup
        code) would otherwise leak a zombie process.
        """
        logging.error(
            f"{self.name}: work item for {self.c.serviceName}::{self.c.operationName} "
            f"exceeded the {workItemTimeoutSec}s timeout after running for "
            f"{self.elapsedSec():.0f}s; terminating its worker process.",
        )
        # Courtesy check: the worker may have finished right as we decided to
        # reap it. Grab the result if it's already there instead of discarding it.
        try:
            self.storedResult = self.q.get_nowait()
        except Exception:  # noqa: BLE001 - queue.Empty or a closed/broken queue, both mean "not ready".
            self.storedResult = None

        if self.storedResult is None:
            self.c.failed = True
            self.c.failedLog.append(
                f"{self.name}: timed out after {self.elapsedSec():.0f}s (limit {workItemTimeoutSec}s)",
            )
            self.storedResult = self.c

        if self.process.is_alive():
            self.process.terminate()
            self.process.join(timeout=10)
        if self.process.is_alive():
            self.process.kill()
            self.process.join(timeout=10)
        self.q.close()
        return self.storedResult


def _flushReadyItems(
    name: str,
    finishedRequests: dict,
    outputQ: mp.Queue,
    bottomMark: int,
    processingOrder: int,
) -> int:
    """Push completed items to outputQ in their original input order.

    Items may finish out of order (parallel workers), so we only emit a
    contiguous prefix starting at bottomMark, buffering the rest until their
    turn comes up. Returns the updated bottomMark.
    """
    for i in range(bottomMark, processingOrder):
        if i in finishedRequests and i == bottomMark:
            logging.info(name + ": enqueuing index " + str(i) + " to outputQ")
            outputQ.put(finishedRequests[i])
            del finishedRequests[i]
            bottomMark += 1
        else:
            break
    return bottomMark


def _nextDeadlineSec(outstandingRequests: dict, workItemTimeoutSec: float = WORKITEM_TIMEOUT_SEC) -> typing.Optional[float]:
    """Seconds until the soonest outstanding item hits workItemTimeoutSec.

    Returns None if there's nothing outstanding to bound (mp.connection.wait
    can then block indefinitely, matching the pre-timeout behavior for the
    "just waiting on new input" case).
    """
    if not outstandingRequests:
        return None
    now = time.time()
    remaining = [worker.startTime + workItemTimeoutSec - now for worker, _item, _order in outstandingRequests.values()]
    return max(0.0, min(remaining))


def _reapTimedOutWorkers(
    name: str,
    outstandingRequests: dict,
    workItemTimeoutSec: float = WORKITEM_TIMEOUT_SEC,
) -> list[tuple[int, "WorkItem"]]:
    """Force-terminate and remove any outstanding item over workItemTimeoutSec."""
    timedOutKeys = [f for f, (worker, _item, _order) in outstandingRequests.items() if worker.elapsedSec() >= workItemTimeoutSec]
    if timedOutKeys:
        logging.error(name + f": reaping {len(timedOutKeys)} timed-out work item(s)")
    reaped = []
    for f in timedOutKeys:
        worker, item, order = outstandingRequests.pop(f)
        item.config = worker.terminateForTimeout(workItemTimeoutSec)
        reaped.append((order, item))
    return reaped


def pipelineWorker(
    name: str,
    inputQ: mp.Queue,
    outputQ: mp.Queue,
    errorQ: mp.Queue,
    handler: Callable[[common.Config, mp.Queue], None],
    serialize=False,
    workItemTimeoutSec: float = WORKITEM_TIMEOUT_SEC,
):
    try:
        pipelineWorkerReal(name, inputQ, outputQ, handler, serialize, workItemTimeoutSec)
    except Exception as ex:
        exceptionStr = "".join(traceback.TracebackException.from_exception(ex).format())
        logging.error(f"Exception in pipelineWorker {name}: {exceptionStr}")
        errorQ.put(name)


# gets items from inputQ, works on it, and puts items in outputQ.
# Serializes the item poping and item processing if serialize=True.
# qGet just stores handles for tasks that wait for the input, and it seems that the code ensures only one thing is added into it at any given time.  # noqa: E501
# outstandingRequests is the one that stores handles for tasks that do the heavy lifting work.
# the pipelineWorker is the one that manages these queues such that results get inserted into the outputQ in the same order as they come in in the inputQ.  # noqa: E501
def pipelineWorkerReal(
    name: str,
    inputQ: mp.Queue,
    outputQ: mp.Queue,
    handler: Callable[[common.Config, mp.Queue], None],
    serialize=False,
    workItemTimeoutSec: float = WORKITEM_TIMEOUT_SEC,
):
    if not name:
        raise ValueError("name cannot be None or empty")
    if not inputQ or not outputQ:
        raise ValueError("inputQ and outputQ cannot be None")
    if not handler:
        raise ValueError("handler cannot be None")

    logging.info("starting: " + name)
    processingOrder = 0
    outstandingRequests = {}
    finishedRequests = {}
    bottomMark = 0
    qGet = [inputQ._reader]
    lastItem = None

    # Until we have seen a) the last item, and b) all outstanding requests have been processed.
    while True:
        logging.info(
            name
            + ": waiting for :"
            + str(len(outstandingRequests.keys()))
            + " outstandingRequests items",
        )

        waitList = list(outstandingRequests.keys())

        # If we have enough pending requests, wait for one of them to finish, otherwise, ok to fetch for an item in the inputQ.
        if len(outstandingRequests) < MAX_WORKERS:
            waitList = waitList + qGet

        # Terminate if we have seen the last item and there are no more outstanding requests.
        if len(waitList) == 0:
            logging.info(name + ": has nothing to wait for")
            break

        # Bound the wait so a hung work item can't block this stage forever;
        # wake up in time to reap anything that has exceeded workItemTimeoutSec.
        finished = mp.connection.wait(waitList, timeout=_nextDeadlineSec(outstandingRequests, workItemTimeoutSec))

        if not finished:
            # Nothing completed before the deadline: reap whatever timed out and
            # loop back around to recompute waitList/deadlines from scratch.
            finishedRequests.update(dict(_reapTimedOutWorkers(name, outstandingRequests, workItemTimeoutSec)))
            bottomMark = _flushReadyItems(name, finishedRequests, outputQ, bottomMark, processingOrder)
            continue

        for f in finished:
            if len(qGet) > 0 and f == qGet[0]:  # new item in inputQ
                logging.info(name + ": has new item in the inputQ")
                item = inputQ.get()
                item.log.append((name, processingOrder))
                if item.isLast:
                    logging.info(name + ": encountered the last item")
                    lastItem = item
                    qGet = []  # from now on, we'll not wait for an item in the inputQ.
                    continue  # skip the rest of the body.

                # if serialize is True, issue in-process blocking call to the workItem.
                if serialize:
                    assert 0 == len(outstandingRequests)
                    logging.info(
                        name
                        + ": performing serial work on index: "
                        + str(processingOrder),
                    )
                    # Update the config with the result of the workItem.
                    newCfg = Worker(
                        name,
                        item.config,
                        item.isLast,
                        handler,
                        serialize,
                    ).getResult()
                    item.config = newCfg
                    logging.info(
                        name
                        + ": enqueuing index "
                        + str(processingOrder)
                        + " to outputQ",
                    )
                    outputQ.put(item)
                else:
                    # Start the workItem in another process.
                    worker = Worker(name, item.config, item.isLast, handler, serialize)
                    outstandingRequests[worker.getWaitable()] = (
                        worker,
                        item,
                        processingOrder,
                    )
                processingOrder = processingOrder + 1
            else:  # some outstanding work finished.
                logging.info(name + ": has new item in outstandingRequests")
                assert serialize is False
                assert f in outstandingRequests
                worker, itemToPush, order = outstandingRequests[f]
                logging.info(
                    name + ": next outstanding finished index is " + str(order),
                )

                itemToPush.config = worker.getResult()
                # remove the finished item from the outstandingRequests.
                del outstandingRequests[f]
                finishedRequests[order] = itemToPush
                bottomMark = _flushReadyItems(name, finishedRequests, outputQ, bottomMark, processingOrder)
    outputQ.put(lastItem)
    outputQ.close()
    logging.info(name + ": finished")


def Pipeline(
    workItemsIn: list[WorkItem],
    lst: list[common.PipelinePhase],
    allSerilized=False,
    workItemTimeoutSec: float = WORKITEM_TIMEOUT_SEC,
):
    # Create the last item.
    w = WorkItem(len(workItemsIn), common.Config(numTrace=0), True)
    workItems = workItemsIn.copy()  # shallow copy since we append to it.
    workItems.append(w)
    sz = len(workItems)

    if len(lst) == 0:
        raise ValueError("Pipeline must have at least one phase!")

    # Ensure unique names for the phases.
    ht = {}
    for i in range(len(lst)):
        if lst[i].name in ht:
            raise ValueError(f"Duplicate phase name {lst[i].name}!")
        ht[lst[i].name] = 1

    # Queue creation: N phases will have N+1 queues.
    queues = [mp.Queue(maxsize=sz) for _ in range(len(lst) + 1)]
    # An error queue for any phase to indicate error condition.
    errQ = mp.Queue(maxsize=len(lst))

    # Pipeline creation.
    workers = []
    for i in range(len(lst)):
        shouldSerialize = lst[i].blocking or allSerilized
        worker = mp.Process(
            target=pipelineWorker,
            name=lst[i].name,
            args=(
                lst[i].name,
                queues[i],
                queues[i + 1],
                errQ,
                lst[i].func,
                shouldSerialize,
                workItemTimeoutSec,
            ),
        )
        workers.append(worker)

    logging.info("Starting the pipeline workers")
    for p in workers:
        p.start()

    logging.info("Inserting work items into the first queue")
    # Item insertion.
    for i in workItems:
        queues[0].put(i)

    # get all configs from the last queue handle any error that might have occurred.
    results = []
    try:
        while len(results) < sz:
            logging.info(
                "Getting results from the last queue: "
                + str(len(results))
                + " out of "
                + str(sz)
                + " results",
            )
            # Wait either for an error or for a result.
            finished = mp.connection.wait([errQ._reader, queues[-1]._reader])
            if errQ._reader in finished:
                logging.error("Error in pipeline: " + str(errQ.get()))
                raise RuntimeError("Error in pipeline")
            results.append(queues[-1].get())
        logging.info("All results are in")
    except RuntimeError:
        # Terminate all workers.
        logging.error("Terminating all workers")
        for p in workers:
            p.terminate()
        raise
    else:
        # Successfully finished the pipeline execution.
        # The last WorkItem was inserted by us; prune it.
        return results[:-1]
    finally:
        # shutdown the queues and do the cleanup.
        logging.info("Shutting down the queues")
        for n, q in enumerate(queues):
            logging.info("Shutting down queue: " + str(n))
            q.close()
        logging.info("Shutting down errQ")
        errQ.close()
        logging.info("All queues are closed")

        logging.info("Waiting for the pipeline workers to finish")
        # Wait for the pipeline to finish.
        for p in workers:
            logging.info("Waiting for: " + p.name)
            p.join()
            logging.info("PipelineProc done for: " + p.name)
        logging.info("Pipeline workers finished")
