import multiprocessing as mp
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
        outputQ = mp.Queue()
        errorQ = mp.Queue()

        # Create a work item and put it in the input queue
        workItem = pipeline.WorkItem(0, common.Config())
        workItem.isLast = True  # To signal the last item
        inputQ.put(workItem)

        # Start the pipeline worker in a process
        worker_process = mp.Process(
            target=pipeline.pipelineWorker,
            args=("test_worker", inputQ, outputQ, errorQ, dummyWorker, False),
        )
        worker_process.start()

        # Wait for the worker process to finish
        worker_process.join()

        # Check the output queue
        self.assertFalse(outputQ.empty(), "Output queue should not be empty.")
        result_item = outputQ.get()
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

        # Mock pipelineWorkerReal to raise an exception
        with patch("crisp.pipeline.pipelineWorkerReal", side_effect=Exception("Intentional Exception for testing")):
            # Start the pipeline worker in a process
            worker_process = mp.Process(
                target=pipeline.pipelineWorker,
                args=("faulty_worker", inputQ, outputQ, errorQ, None, False),
            )
            worker_process.start()

            # Wait for the worker process to finish
            worker_process.join()

            # Ensure the output queue is empty, as the exception should prevent any output
            self.assertTrue(outputQ.empty(), "Output queue should be empty due to exception.")

            # The error queue should contain the worker's name
            self.assertFalse(errorQ.empty(), "Error queue should contain the worker's name.")
            error_name = errorQ.get()
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
        outputQ = mp.Queue()

        # Create a work item and put it in the input queue
        workItem = pipeline.WorkItem(0, common.Config())
        workItem.isLast = True  # Signal the last item
        inputQ.put(workItem)

        # Start pipelineWorkerReal in a separate process for serial execution
        process = mp.Process(
            target=pipeline.pipelineWorkerReal,
            args=("test_worker_serial", inputQ, outputQ, dummyWorker, True),
        )
        process.start()
        process.join()

        # Check the output queue
        self.assertFalse(outputQ.empty(), "Output queue should not be empty.")
        result_item = outputQ.get()
        self.assertEqual(result_item.itemId, 0)
        self.assertTrue(result_item.isLast)
        self.assertIsInstance(result_item.config, common.Config)

        # Clean up queues
        inputQ.close()
        outputQ.close()

    def test_pipelineWorkerReal_parallel_execution(self):
        """Test pipelineWorkerReal in parallel mode."""
        inputQ = mp.Queue()
        outputQ = mp.Queue()

        # Create multiple work items and put them in the input queue
        workItems = [pipeline.WorkItem(i, common.Config()) for i in range(3)]
        for item in workItems:
            inputQ.put(item)

        # Add the last item to signal completion
        lastItem = pipeline.WorkItem(len(workItems), common.Config(), isLast=True)
        inputQ.put(lastItem)

        # Run pipelineWorkerReal in parallel mode in a separate process
        process = mp.Process(
            target=pipeline.pipelineWorkerReal,
            args=("test_worker_parallel", inputQ, outputQ, dummyWorker, False),
        )
        process.start()
        process.join()

        # Drain the output queue and collect items
        output_items = []
        while True:
            try:
                output_items.append(outputQ.get_nowait())
            except Exception:
                break

        # Check that the output queue contains all items in the correct order
        self.assertEqual(len(output_items), len(workItems) + 1)
        for i in range(len(workItems)):
            result_item = output_items[i]
            self.assertEqual(result_item.itemId, i)
            self.assertIsInstance(result_item.config, common.Config)

        # Check the last item
        result_item = output_items[-1]
        self.assertEqual(result_item.itemId, len(workItems))
        self.assertTrue(result_item.isLast)

        # Clean up queues
        inputQ.close()
        outputQ.close()

    def test_pipelineWorkerReal_order_preservation(self):
        """Test pipelineWorkerReal maintains input order in output queue."""
        inputQ = mp.Queue()
        outputQ = mp.Queue()

        # Create work items in ascending order
        workItems = [pipeline.WorkItem(i, common.Config()) for i in range(3)]
        for item in workItems:
            inputQ.put(item)

        # Add the last item to signal completion
        lastItem = pipeline.WorkItem(len(workItems), common.Config(), isLast=True)
        inputQ.put(lastItem)

        # Run pipelineWorkerReal with serialize=False to allow parallel execution
        process = mp.Process(
            target=pipeline.pipelineWorkerReal,
            args=("test_worker_order", inputQ, outputQ, dummyWorker, False),
        )
        process.start()
        process.join()

        # Retrieve and check the processing order from each item's log
        output_items = []
        while not outputQ.empty():
            output_items.append(outputQ.get())

        # Ensure that items are processed in the correct order (0, 1, 2)
        for idx, item in enumerate(output_items[:-1]):  # Exclude the last item
            self.assertEqual(item.itemId, idx)
            self.assertIsInstance(item.config, common.Config)
            self.assertIn(("test_worker_order", idx), item.log)

        # Verify the last item
        last_output_item = output_items[-1]
        self.assertTrue(last_output_item.isLast)
        self.assertEqual(last_output_item.itemId, 3)

        # Clean up queues
        inputQ.close()
        outputQ.close()


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
