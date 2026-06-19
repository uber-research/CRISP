import logging
import multiprocessing as mp
import shutil
import time
import traceback
from collections.abc import Callable

import crisp.common as common

# TODO: Dynamically compute the max workers based on the pipeline steps, number of cores, and intra-step parallelism.
MAX_WORKERS = 16


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


# TODO: Use timeouts to prevent waiting forever on an multiprocess handler.
# TODO: Use error handling to prevent a crash in a single workitem bringing down the entire pipeline.
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


def pipelineWorker(
    name: str,
    inputQ: mp.Queue,
    outputQ: mp.Queue,
    errorQ: mp.Queue,
    handler: Callable[[common.Config, mp.Queue], None],
    serialize=False,
):
    try:
        pipelineWorkerReal(name, inputQ, outputQ, handler, serialize)
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

        finished = mp.connection.wait(waitList)
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
                # push all items from the bottomMark out until sequential order is preserved in the outputQ.
                for i in range(bottomMark, processingOrder):
                    if i in finishedRequests and i == bottomMark:
                        logging.info(
                            name + ": enqueuing index " + str(i) + " to outputQ",
                        )
                        outputQ.put(finishedRequests[i])
                        del finishedRequests[i]
                        bottomMark += 1
                    else:
                        break
    outputQ.put(lastItem)
    outputQ.close()
    logging.info(name + ": finished")


def Pipeline(
    workItemsIn: list[WorkItem],
    lst: list[common.PipelinePhase],
    allSerilized=False,
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
