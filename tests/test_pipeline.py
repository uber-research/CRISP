import functools
import multiprocessing as mp
import signal
import time
from unittest import TestCase
import unittest
from unittest.mock import MagicMock, patch

import crisp.common as common
import crisp.pipeline as pipeline
from crisp.common import PipelinePhase
from crisp.process_trace import (
    mapReduce,
    _memory_aware_workers,
    _LARGE_TRACE_THRESHOLD_BYTES,
    _IN_MEMORY_EXPANSION,
    _MEMORY_HEADROOM_FRACTION,
)


def dummyWorker(c: common.Config, resultQ: mp.Queue) -> common.Config:
    if resultQ:
        resultQ.put(c)
        resultQ.close()
    return c


def hangingWorker(c: common.Config, resultQ: mp.Queue) -> common.Config:
    """Simulates a work item that never returns in time (e.g. a genuine hang)."""
    time.sleep(30)
    if resultQ:
        resultQ.put(c)
        resultQ.close()
    return c


def sigtermIgnoringWorker(c: common.Config, resultQ: mp.Queue, readyEvent=None) -> common.Config:
    """Simulates a stuck work item whose process doesn't die from SIGTERM alone,
    forcing terminateForTimeout to escalate to SIGKILL. Signals readyEvent once
    the SIGTERM handler is installed so callers can synchronize on that instead
    of sleeping for an arbitrary guess at how long process startup takes."""
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    if readyEvent is not None:
        readyEvent.set()
    time.sleep(30)
    if resultQ:
        resultQ.put(c)
        resultQ.close()
    return c


class RecordingQueue:
    """Minimal outputQ stand-in for calling pipelineWorkerReal/pipelineWorker
    directly instead of via mp.Process.

    Both functions only ever call put()/close() on outputQ, never get(). A
    real mp.Queue's close() makes it unreadable via that same object -- fine
    when the two calls happen from different processes/handles (as in
    production), but not when calling the function directly in the test's
    own process. A plain in-memory list sidesteps that entirely and lets the
    test inspect what was produced after the call returns.
    """

    def __init__(self):
        self.items = []

    def put(self, item):
        self.items.append(item)

    def close(self):
        pass


def dummyWorkerList():
    lst = []
    lst.append(PipelinePhase("w1", dummyWorker, False))
    lst.append(PipelinePhase("w2", dummyWorker, False))
    return lst


def runPipelineOld(serial):
    allItems = [pipeline.WorkItem(i, common.Config()) for i in range(10)]

    result = pipeline.Pipeline(allItems, dummyWorkerList(), serial)

    for i in range(len(allItems)):
        expected = [
            ("w1", i),
            ("w2", i),
        ]
        assert expected == result[i].log


class AsyncIOTestCaseOld(TestCase):
    def test_defaultPipelineOld(self):
        allItems = [pipeline.WorkItem(i, common.Config()) for i in range(10)]

        results = pipeline.Pipeline(allItems, dummyWorkerList())

        for i in range(len(allItems)):
            expected = [
                ("w1", i),
                ("w2", i),
            ]
            assert expected == results[i].log

    def test_serialPipelineOld(self):
        runPipelineOld(True)

    def test_parallelPipelineOld(self):
        runPipelineOld(False)


class PipelineWorkerTestCase(unittest.TestCase):
    def test_pipelineWorker_normal_execution(self):
        """Test pipelineWorker when pipelineWorkerReal runs without exceptions."""
        # Set up queues
        inputQ = mp.Queue()
        outputQ = RecordingQueue()
        errorQ = mp.Queue()

        # Create a work item and put it in the input queue
        workItem = pipeline.WorkItem(0, common.Config())
        workItem.isLast = True  # To signal the last item
        inputQ.put(workItem)

        # pipelineWorker/pipelineWorkerReal are plain blocking functions -- no
        # need for an extra mp.Process wrapper here (the actual per-item work,
        # if any, is already isolated in its own process by Worker).
        pipeline.pipelineWorker("test_worker", inputQ, outputQ, errorQ, dummyWorker, False)

        # Check the output queue
        self.assertEqual(len(outputQ.items), 1, "Output queue should have exactly one item.")
        result_item = outputQ.items[0]
        self.assertEqual(result_item.itemId, 0)
        self.assertEqual(result_item.isLast, True)
        self.assertIsInstance(result_item.config, common.Config)

        # The error queue should be empty
        self.assertTrue(errorQ.empty(), "Error queue should be empty.")

    def test_pipelineWorker_exception_handling(self):
        """Test pipelineWorker when pipelineWorkerReal raises an exception."""
        # Set up queues
        inputQ = mp.Queue()
        outputQ = mp.Queue()
        errorQ = mp.Queue()

        # Create a work item and put it in the input queue
        workItem = pipeline.WorkItem(1, common.Config())
        workItem.isLast = True  # To signal the last item
        inputQ.put(workItem)

        # Mock pipelineWorkerReal to raise an exception. Calling pipelineWorker
        # directly (rather than via mp.Process) means the mock reliably applies
        # regardless of the platform's multiprocessing start method.
        with patch("crisp.pipeline.pipelineWorkerReal", side_effect=Exception("Intentional Exception for testing")):
            pipeline.pipelineWorker("faulty_worker", inputQ, outputQ, errorQ, None, False)

            # Ensure the output queue is empty, as the exception should prevent any output
            self.assertTrue(outputQ.empty(), "Output queue should be empty due to exception.")

            # The error queue should contain the worker's name. Use a blocking get()
            # with a timeout rather than checking empty() first: mp.Queue.put() hands
            # off to a background feeder thread, so an empty()/get() check immediately
            # after put() (now that pipelineWorker runs directly in this process rather
            # than in a separate mp.Process we'd join() first) can race and see "empty"
            # before the feeder thread has actually written to the underlying pipe.
            try:
                error_name = errorQ.get(timeout=5)
            except Exception:
                self.fail("Error queue should contain the worker's name.")
            self.assertEqual(error_name, "faulty_worker")

class PipelineWorkerRealTestCase(unittest.TestCase):
    def test_pipelineWorkerReal_invalid_name(self):
        """Test pipelineWorkerReal raises ValueError when name is invalid."""
        with self.assertRaises(ValueError):
            pipeline.pipelineWorkerReal(
                name="",
                inputQ=mp.Queue(),
                outputQ=mp.Queue(),
                handler=lambda c, _: c,
                serialize=False,
            )

    def test_pipelineWorkerReal_invalid_queues(self):
        """Test pipelineWorkerReal raises ValueError for invalid queues."""
        with self.assertRaises(ValueError):
            pipeline.pipelineWorkerReal(
                name="worker",
                inputQ=None,
                outputQ=mp.Queue(),
                handler=lambda c, _: c,
                serialize=False,
            )

        with self.assertRaises(ValueError):
            pipeline.pipelineWorkerReal(
                name="worker",
                inputQ=mp.Queue(),
                outputQ=None,
                handler=lambda c, _: c,
                serialize=False,
            )

    def test_pipelineWorkerReal_invalid_handler(self):
        """Test pipelineWorkerReal raises ValueError when handler is None."""
        with self.assertRaises(ValueError):
            pipeline.pipelineWorkerReal(
                name="worker",
                inputQ=mp.Queue(),
                outputQ=mp.Queue(),
                handler=None,
                serialize=False,
            )

    def test_pipelineWorkerReal_serial_execution(self):
        """Test pipelineWorkerReal in serial mode."""
        inputQ = mp.Queue()
        outputQ = RecordingQueue()

        # Create a work item and put it in the input queue
        workItem = pipeline.WorkItem(0, common.Config())
        workItem.isLast = True  # Signal the last item
        inputQ.put(workItem)

        # pipelineWorkerReal is a plain blocking function; call it directly.
        pipeline.pipelineWorkerReal("test_worker_serial", inputQ, outputQ, dummyWorker, True)

        # Check the output queue
        self.assertEqual(len(outputQ.items), 1, "Output queue should have exactly one item.")
        result_item = outputQ.items[0]
        self.assertEqual(result_item.itemId, 0)
        self.assertTrue(result_item.isLast)
        self.assertIsInstance(result_item.config, common.Config)

        # Clean up queue
        inputQ.close()

    def test_pipelineWorkerReal_serial_execution_processes_real_item(self):
        """Test pipelineWorkerReal in serial mode actually runs the handler for a real (non-last) item."""
        inputQ = mp.Queue()
        outputQ = RecordingQueue()

        workItem = pipeline.WorkItem(0, common.Config())
        inputQ.put(workItem)
        lastItem = pipeline.WorkItem(1, common.Config(), isLast=True)
        inputQ.put(lastItem)

        pipeline.pipelineWorkerReal("test_worker_serial", inputQ, outputQ, dummyWorker, True)

        self.assertEqual(len(outputQ.items), 2)
        result_item = outputQ.items[0]
        self.assertEqual(result_item.itemId, 0)
        self.assertFalse(result_item.isLast)
        self.assertIn(("test_worker_serial", 0), result_item.log)

        last_output_item = outputQ.items[1]
        self.assertTrue(last_output_item.isLast)

        inputQ.close()

    def test_pipelineWorkerReal_parallel_execution(self):
        """Test pipelineWorkerReal in parallel mode."""
        inputQ = mp.Queue()
        outputQ = RecordingQueue()

        # Create multiple work items and put them in the input queue
        workItems = [pipeline.WorkItem(i, common.Config()) for i in range(3)]
        for item in workItems:
            inputQ.put(item)

        # Add the last item to signal completion
        lastItem = pipeline.WorkItem(len(workItems), common.Config(), isLast=True)
        inputQ.put(lastItem)

        # Run pipelineWorkerReal in parallel mode. Each item's actual handler
        # still runs in its own process via Worker; only the driving loop runs
        # directly here.
        pipeline.pipelineWorkerReal("test_worker_parallel", inputQ, outputQ, dummyWorker, False)

        # Check that the output queue contains all items in the correct order
        output_items = outputQ.items
        self.assertEqual(len(output_items), len(workItems) + 1)
        for i in range(len(workItems)):
            result_item = output_items[i]
            self.assertEqual(result_item.itemId, i)
            self.assertIsInstance(result_item.config, common.Config)

        # Check the last item
        result_item = output_items[-1]
        self.assertEqual(result_item.itemId, len(workItems))
        self.assertTrue(result_item.isLast)

        # Clean up queue
        inputQ.close()

    def test_pipelineWorkerReal_order_preservation(self):
        """Test pipelineWorkerReal maintains input order in output queue."""
        inputQ = mp.Queue()
        outputQ = RecordingQueue()

        # Create work items in ascending order
        workItems = [pipeline.WorkItem(i, common.Config()) for i in range(3)]
        for item in workItems:
            inputQ.put(item)

        # Add the last item to signal completion
        lastItem = pipeline.WorkItem(len(workItems), common.Config(), isLast=True)
        inputQ.put(lastItem)

        # Run pipelineWorkerReal with serialize=False to allow parallel execution.
        pipeline.pipelineWorkerReal("test_worker_order", inputQ, outputQ, dummyWorker, False)

        # Ensure that items are processed in the correct order (0, 1, 2)
        output_items = outputQ.items
        for idx, item in enumerate(output_items[:-1]):  # Exclude the last item
            self.assertEqual(item.itemId, idx)
            self.assertIsInstance(item.config, common.Config)
            self.assertIn(("test_worker_order", idx), item.log)

        # Verify the last item
        last_output_item = output_items[-1]
        self.assertTrue(last_output_item.isLast)
        self.assertEqual(last_output_item.itemId, 3)

        # Clean up queue
        inputQ.close()


def test_nextDeadlineSec_returns_none_when_no_outstanding_requests():
    assert pipeline._nextDeadlineSec({}) is None


def test_nextDeadlineSec_returns_soonest_remaining_time():
    timeoutSec = 100
    soonWorker = pipeline.Worker("w", common.Config(), True, dummyWorker)
    soonWorker.startTime = time.time() - 90  # 10s left until timeout
    laterWorker = pipeline.Worker("w", common.Config(), True, dummyWorker)
    laterWorker.startTime = time.time() - 10  # 90s left until timeout

    outstanding = {1: (soonWorker, None, 0), 2: (laterWorker, None, 1)}

    remaining = pipeline._nextDeadlineSec(outstanding, timeoutSec)

    assert 0 <= remaining <= 10


def test_terminateForTimeout_marks_config_failed_and_kills_process():
    worker = pipeline.Worker("test_worker", common.Config(), False, hangingWorker)
    assert worker.process.is_alive()

    result = worker.terminateForTimeout()

    assert result.failed is True
    assert any("timed out" in msg for msg in result.failedLog)
    worker.process.join(timeout=5)
    assert not worker.process.is_alive()


def test_terminateForTimeout_escalates_to_kill_when_terminate_is_ignored():
    """If the worker process survives SIGTERM (terminate()), we must escalate to SIGKILL
    rather than leaving a zombie process behind."""
    readyEvent = mp.Event()
    handler = functools.partial(sigtermIgnoringWorker, readyEvent=readyEvent)
    worker = pipeline.Worker("test_worker", common.Config(), False, handler)
    assert worker.process.is_alive()
    # Wait for the child to actually confirm it has installed the SIGTERM
    # handler, rather than guessing how long that takes with a fixed sleep.
    assert readyEvent.wait(timeout=5), "child did not install its SIGTERM handler in time"

    result = worker.terminateForTimeout()

    assert result.failed is True
    worker.process.join(timeout=5)
    assert not worker.process.is_alive(), "process ignoring SIGTERM should still be killed via SIGKILL"


def test_reapTimedOutWorkers_removes_only_expired_items():
    timeoutSec = 60
    expiredWorker = pipeline.Worker("test_worker", common.Config(), False, hangingWorker)
    expiredWorker.startTime = time.time() - (timeoutSec + 60)
    freshWorker = pipeline.Worker("test_worker", common.Config(), False, dummyWorker)

    expiredItem = pipeline.WorkItem(0, common.Config())
    freshItem = pipeline.WorkItem(1, common.Config())
    outstanding = {
        expiredWorker.getWaitable(): (expiredWorker, expiredItem, 0),
        freshWorker.getWaitable(): (freshWorker, freshItem, 1),
    }

    reaped = pipeline._reapTimedOutWorkers("test_worker", outstanding, timeoutSec)

    assert [order for order, _item in reaped] == [0]
    assert reaped[0][1].config.failed is True
    assert list(outstanding.keys()) == [freshWorker.getWaitable()]

    freshWorker.process.join(timeout=5)


def test_pipelineWorkerReal_reaps_hung_worker_instead_of_blocking_forever():
    """A single hung work item should be killed and marked failed, not block the whole stage."""
    inputQ = mp.Queue()
    outputQ = RecordingQueue()

    workItem = pipeline.WorkItem(0, common.Config())
    inputQ.put(workItem)
    lastItem = pipeline.WorkItem(1, common.Config(), isLast=True)
    inputQ.put(lastItem)

    # pipelineWorkerReal is a plain blocking function: calling it directly
    # with a short workItemTimeoutSec means this call returns on its own once
    # the hung item is reaped, rather than hanging forever, with no need for
    # an outer mp.Process/join/is_alive dance to bound the wait from the test
    # side. The hung item's own handler still runs in a real subprocess via
    # Worker, so the actual termination/kill logic under test is exercised faithfully.
    pipeline.pipelineWorkerReal("test_worker_timeout", inputQ, outputQ, hangingWorker, False, 1)

    assert len(outputQ.items) == 2
    result_item = outputQ.items[0]
    assert result_item.itemId == 0
    assert result_item.config.failed is True

    last_output_item = outputQ.items[1]
    assert last_output_item.isLast

    inputQ.close()


def test_flushReadyItems_emits_contiguous_prefix_and_buffers_the_rest():
    outputQ = mp.Queue()
    finishedRequests = {0: "item-0", 2: "item-2"}

    bottomMark = pipeline._flushReadyItems("test_worker", finishedRequests, outputQ, bottomMark=0, processingOrder=3)

    # Only index 0 is contiguous from bottomMark; index 2 is buffered since index 1 hasn't finished yet.
    assert bottomMark == 1
    assert finishedRequests == {2: "item-2"}
    assert outputQ.get(timeout=5) == "item-0"
    assert outputQ.empty()

    # Once index 1 arrives, both 1 and 2 should flush through in order.
    finishedRequests[1] = "item-1"
    bottomMark = pipeline._flushReadyItems("test_worker", finishedRequests, outputQ, bottomMark=bottomMark, processingOrder=3)

    assert bottomMark == 3
    assert finishedRequests == {}
    assert outputQ.get(timeout=5) == "item-1"
    assert outputQ.get(timeout=5) == "item-2"


class TestMemoryAwareWorkers(TestCase):
    """Tests for _memory_aware_workers() — the OOM-guard helper in mapReduce."""

    def _mock_psutil(self, available_bytes: int):
        mock_vm = MagicMock()
        mock_vm.available = available_bytes
        return patch("psutil.virtual_memory", return_value=mock_vm)

    def test_small_traces_below_threshold_no_cap(self):
        """Traces well below the threshold leave num_workers unchanged."""
        small_size = _LARGE_TRACE_THRESHOLD_BYTES // 2  # 50 MB
        with patch("os.path.exists", return_value=True), \
             patch("os.path.getsize", return_value=small_size):
            result = _memory_aware_workers(16, ["trace1.json", "trace2.json"])
        self.assertEqual(result, 16)

    def test_traces_at_threshold_no_cap(self):
        """Traces exactly at the threshold (not exceeding) leave num_workers unchanged."""
        with patch("os.path.exists", return_value=True), \
             patch("os.path.getsize", return_value=_LARGE_TRACE_THRESHOLD_BYTES):
            result = _memory_aware_workers(16, ["trace.json"])
        self.assertEqual(result, 16)

    def test_large_trace_caps_workers(self):
        """A 1 GB trace file with 4 GB available caps to 1 worker.

        budget = 4 GB * 0.8 = 3.2 GB
        per_worker_cost = 1 GB * 4 = 4 GB
        capped = max(1, int(3.2 / 4)) = 1
        """
        one_gb = 1024 * 1024 * 1024
        four_gb = 4 * one_gb
        with patch("os.path.exists", return_value=True), \
             patch("os.path.getsize", return_value=one_gb), \
             self._mock_psutil(four_gb):
            result = _memory_aware_workers(16, ["huge_trace.json"])
        expected = max(1, int(four_gb * _MEMORY_HEADROOM_FRACTION / (one_gb * _IN_MEMORY_EXPANSION)))
        self.assertEqual(result, expected)
        self.assertEqual(result, 1)

    def test_medium_large_trace_partial_cap(self):
        """A 512 MB trace with 8 GB available caps to 3 workers.

        budget = 8 GB * 0.8 = 6.4 GB
        per_worker_cost = 512 MB * 4 = 2 GB
        capped = max(1, int(6.4 / 2)) = 3
        """
        half_gb = 512 * 1024 * 1024
        eight_gb = 8 * 1024 * 1024 * 1024
        with patch("os.path.exists", return_value=True), \
             patch("os.path.getsize", return_value=half_gb), \
             self._mock_psutil(eight_gb):
            result = _memory_aware_workers(16, ["trace.json"])
        expected = max(1, int(eight_gb * _MEMORY_HEADROOM_FRACTION / (half_gb * _IN_MEMORY_EXPANSION)))
        self.assertEqual(result, expected)
        self.assertEqual(result, 3)

    def test_cap_never_exceeds_requested_workers(self):
        """If budget allows more workers than requested, returns the original count."""
        small_but_above_threshold = _LARGE_TRACE_THRESHOLD_BYTES + 1
        sixty_four_gb = 64 * 1024 * 1024 * 1024
        with patch("os.path.exists", return_value=True), \
             patch("os.path.getsize", return_value=small_but_above_threshold), \
             self._mock_psutil(sixty_four_gb):
            result = _memory_aware_workers(2, ["trace.json"])
        self.assertEqual(result, 2)

    def test_single_worker_unchanged(self):
        """num_workers=1 is returned immediately with no stat calls."""
        with patch("os.path.getsize") as mock_stat:
            result = _memory_aware_workers(1, ["trace.json"])
        self.assertEqual(result, 1)
        mock_stat.assert_not_called()

    def test_empty_file_list_unchanged(self):
        """Empty file list returns num_workers unchanged."""
        result = _memory_aware_workers(16, [])
        self.assertEqual(result, 16)

    def test_nonexistent_files_skipped(self):
        """Files that do not exist are excluded from the size check."""
        with patch("os.path.exists", return_value=False):
            result = _memory_aware_workers(16, ["ghost.json"])
        self.assertEqual(result, 16)

    def test_warning_logged_when_capping(self):
        """A warning is emitted when parallelism is reduced."""
        one_gb = 1024 * 1024 * 1024
        with patch("os.path.exists", return_value=True), \
             patch("os.path.getsize", return_value=one_gb), \
             self._mock_psutil(4 * one_gb), \
             patch("crisp.process_trace.logging") as mock_log:
            _memory_aware_workers(16, ["big.json"])
        mock_log.warning.assert_called_once()
        warning_msg = mock_log.warning.call_args[0][0]
        self.assertIn("Large traces detected", warning_msg)

    def test_no_warning_for_small_traces(self):
        """No warning is emitted when traces are below the threshold."""
        small_size = _LARGE_TRACE_THRESHOLD_BYTES // 2
        with patch("os.path.exists", return_value=True), \
             patch("os.path.getsize", return_value=small_size), \
             patch("crisp.process_trace.logging") as mock_log:
            _memory_aware_workers(16, ["small.json"])
        mock_log.warning.assert_not_called()


class TestMapReduceOomGuard(TestCase):
    """Tests that mapReduce() applies the memory-aware worker cap."""

    @patch("multiprocessing.Pool")
    @patch("crisp.process_trace.process")
    def test_mapReduce_small_traces_unchanged(self, _mock_process, mock_pool):
        """mapReduce uses the requested pool size for small traces."""
        mock_pool_instance = MagicMock()
        mock_pool.return_value.__enter__.return_value = mock_pool_instance
        mock_pool_instance.map.return_value = []

        mock_config = MagicMock()
        with patch("os.path.exists", return_value=True), \
             patch("os.path.getsize", return_value=1024):  # 1 KB — well below threshold
            mapReduce(4, ["trace.json"], mock_config)

        actual_workers = mock_pool.call_args[0][0]
        self.assertEqual(actual_workers, 4)

    @patch("multiprocessing.Pool")
    @patch("crisp.process_trace.process")
    def test_mapReduce_caps_workers_for_large_traces(self, _mock_process, mock_pool):
        """mapReduce uses a smaller pool when trace files are very large.

        1 GB file, 4 GB available → capped = max(1, int(4*0.8/4)) = 1
        """
        mock_pool_instance = MagicMock()
        mock_pool.return_value.__enter__.return_value = mock_pool_instance
        mock_pool_instance.map.return_value = []

        mock_config = MagicMock()
        one_gb = 1024 * 1024 * 1024
        mock_vm = MagicMock()
        mock_vm.available = 4 * one_gb
        with patch("os.path.exists", return_value=True), \
             patch("os.path.getsize", return_value=one_gb), \
             patch("psutil.virtual_memory", return_value=mock_vm):
            mapReduce(16, ["big_trace.json"], mock_config)

        actual_workers = mock_pool.call_args[0][0]
        self.assertEqual(actual_workers, 1)
