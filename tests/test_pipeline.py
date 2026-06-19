import multiprocessing as mp
from unittest import TestCase
import unittest
from unittest.mock import patch

import crisp.common as common
import crisp.pipeline as pipeline
from crisp.common import PipelinePhase


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
