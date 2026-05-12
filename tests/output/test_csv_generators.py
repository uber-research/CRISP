"""Tests for CSV generation functions.

Note on porting scope (see crisp/LAYERS.md "Deferred tests" table):

Internal critical_path/output/tests/test_csv_generators.py contains 14
test methods. This module ports 10 of them verbatim. The other 4
(TestGenCyclesCSVFile.* and TestGenSummaryCSVFile.*) use a
``@patch("...common.Config")`` decorator whose patch-target module
(crisp.common) does not exist yet. Per the layer-crossing-tests rule
those 4 tests are deferred to PR 8a (when crisp.common lands), at which
point the helper getDummyMetric and its dependency on Metrics from
crisp.shared.models will be brought back along with them.
"""

import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from crisp.output.csv_generators import (
    computeLatencyReduction,
    computeMultipleLatencyReduction,
    genCrossRegionCallsCSVFile,
    genEmptyCSVFile,
    genHypoLatencyCSVFile,
)
from crisp.shared.models import LatencyData


class TestComputeLatencyReduction(unittest.TestCase):
    def test_computeLatencyReduction(self):
        input_df = pd.DataFrame(
            {
                "observed_latency": [100, 200, 0],
                "no_err_latency": [80, 150, 0],
            }
        )

        result_df = computeLatencyReduction(input_df)

        # Check that latency_reduction column was added
        self.assertIn("latency_reduction", result_df.columns)

        # Check the calculations
        expected_reductions = [20.0, 25.0, 0.0]  # (1 - 80/100)*100, (1 - 150/200)*100, (1 - 0/0)*100
        self.assertEqual(result_df["latency_reduction"].tolist(), expected_reductions)


class TestComputeMultipleLatencyReduction(unittest.TestCase):
    def test_computeMultipleLatencyReduction(self):
        """Test computeMultipleLatencyReduction with optimistic and pessimistic values."""
        input_df = pd.DataFrame({
            "observed_latency": [100, 200, 0, 50],
            "no_err_latency": [80, 150, 0, 40],
            "no_err_latency_optimistic": [70, 140, 0, 35],
            "no_err_latency_pessimistic": [85, 160, 0, 42],
        })

        result_df = computeMultipleLatencyReduction(input_df)

        # Check that all latency_reduction columns were added
        self.assertIn("latency_reduction", result_df.columns)
        self.assertIn("latency_reduction_optimistic", result_df.columns)
        self.assertIn("latency_reduction_pessimistic", result_df.columns)

        # Check regular latency reduction calculations
        expected_regular = [20.0, 25.0, 0.0, 20.0]  # (1 - no_err/observed)*100
        self.assertEqual(result_df["latency_reduction"].tolist(), expected_regular)

        # Check optimistic latency reduction calculations
        expected_optimistic = [30.0, 30.0, 0.0, 30.0]  # (1 - optimistic/observed)*100
        self.assertEqual(result_df["latency_reduction_optimistic"].tolist(), expected_optimistic)

        # Check pessimistic latency reduction calculations
        expected_pessimistic = [15.0, 20.0, 0.0, 16.0]  # (1 - pessimistic/observed)*100
        self.assertEqual(result_df["latency_reduction_pessimistic"].tolist(), expected_pessimistic)

        # Verify data is rounded to 2 decimal places
        for col in ["latency_reduction", "latency_reduction_optimistic", "latency_reduction_pessimistic"]:
            for val in result_df[col]:
                self.assertEqual(val, round(val, 2))

    def test_computeMultipleLatencyReduction_zero_division(self):
        """Test computeMultipleLatencyReduction handles zero division correctly."""
        input_df = pd.DataFrame({
            "observed_latency": [0, 100],
            "no_err_latency": [0, 80],
            "no_err_latency_optimistic": [0, 70],
            "no_err_latency_pessimistic": [0, 85],
        })

        result_df = computeMultipleLatencyReduction(input_df)

        # When observed_latency is 0, all reductions should be 0
        self.assertEqual(result_df["latency_reduction"].tolist()[0], 0.0)
        self.assertEqual(result_df["latency_reduction_optimistic"].tolist()[0], 0.0)
        self.assertEqual(result_df["latency_reduction_pessimistic"].tolist()[0], 0.0)

        # Normal case should work
        self.assertEqual(result_df["latency_reduction"].tolist()[1], 20.0)
        self.assertEqual(result_df["latency_reduction_optimistic"].tolist()[1], 30.0)
        self.assertEqual(result_df["latency_reduction_pessimistic"].tolist()[1], 15.0)


class TestGenEmptyCSVFile(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        # Cleanup the temporary directory after tests
        for filename in os.listdir(self.test_dir):
            os.remove(os.path.join(self.test_dir, filename))
        os.rmdir(self.test_dir)

    def test_genEmptyCSVFile_basic_columns(self):
        """Test genEmptyCSVFile with basic column set."""
        filename = os.path.join(self.test_dir, "test_empty.csv")
        columns = ["col1", "col2", "col3"]

        genEmptyCSVFile(filename, columns)

        # Verify file exists
        self.assertTrue(os.path.exists(filename))

        # Verify content
        df = pd.read_csv(filename)
        self.assertEqual(list(df.columns), columns)
        self.assertEqual(len(df), 0)  # Should be empty

    def test_genEmptyCSVFile_latency_columns(self):
        """Test genEmptyCSVFile with latency-specific columns."""
        filename = os.path.join(self.test_dir, "test_latency.csv")
        columns = [
            "percentile",
            "num_traces",
            "observed_latency",
            "no_err_latency",
            "latency_reduction"
        ]

        genEmptyCSVFile(filename, columns)

        # Verify file exists and has correct structure
        self.assertTrue(os.path.exists(filename))
        df = pd.read_csv(filename)
        self.assertEqual(list(df.columns), columns)
        self.assertEqual(len(df), 0)


class TestGenHypoLatencyCSVFile(unittest.TestCase):
    def setUp(self):
        self.mock_config = MagicMock()
        self.mock_config.getOutputDir.return_value = "/test/output"
        self.mock_config.serviceName = "TestService"
        self.mock_config.operationName = "TestOperation"

    @patch("crisp.output.csv_generators.genEmptyCSVFile")
    @patch("crisp.output.csv_generators.computeLatencyReduction")
    def test_genHypoLatencyCSVFile_empty(
        self, _mock_computeLatencyReduction, mock_genEmptyCSVFile
    ):
        """Test genHypoLatencyCSVFile with empty hypoLatencyPercentile."""
        latencyPercentile = [(50, 100, LatencyData("trace1", 100, 80, 70, 90))]
        hypoLatencyPercentile = []
        config = self.mock_config

        with patch("os.path.join", return_value="/test/output/hypoLatency.csv"):
            result = genHypoLatencyCSVFile(latencyPercentile, hypoLatencyPercentile, config)

        # Verify that genEmptyCSVFile was called when hypoLatencyPercentile is empty
        mock_genEmptyCSVFile.assert_called_once()

        # Verify return value
        self.assertEqual(result, "/test/output/hypoLatency.csv")

    @patch("crisp.output.csv_generators.computeLatencyReduction")
    def test_genHypoLatencyCSVFile_with_data(
        self, mock_computeLatencyReduction
    ):
        """Test genHypoLatencyCSVFile with actual data."""
        # Setup test data
        latencyPercentile = [
            (50, 100, LatencyData("trace1", 100, 80, 70, 90)),
            (95, 50, LatencyData("trace2", 200, 160, 140, 180)),
        ]
        hypoLatencyPercentile = [
            (50, 100, LatencyData("trace1", 100, 80, 70, 90)),
            (95, 50, LatencyData("trace2", 200, 160, 140, 180)),
        ]
        config = self.mock_config

        # Mock computeLatencyReduction
        mock_df = MagicMock()
        mock_computeLatencyReduction.return_value = mock_df

        with patch("os.path.join", return_value="/test/output/hypoLatency.csv"), \
             patch("pandas.DataFrame"), \
             patch("pandas.merge"):

            result = genHypoLatencyCSVFile(latencyPercentile, hypoLatencyPercentile, config)

        # Verify that computeLatencyReduction was called
        mock_computeLatencyReduction.assert_called_once()

        # Verify return value
        self.assertEqual(result, "/test/output/hypoLatency.csv")


class TestGenCrossRegionCallsCSVFile(unittest.TestCase):
    def setUp(self):
        self.mock_config = MagicMock()
        self.mock_config.getOutputDir.return_value = "/test/output"
        self.mock_config.serviceName = "TestService"
        self.mock_config.operationName = "TestOperation"

    def test_genCrossRegionCallsCSVFile_empty_metrics(self):
        """Test genCrossRegionCallsCSVFile with metrics that have no cross region calls."""
        metrics_list = [MagicMock(crossRegionCalls=None)]

        with patch("os.path.join", return_value="/test/output/crossRegionCalls.csv"):
            result = genCrossRegionCallsCSVFile(metrics_list, self.mock_config)

        self.assertIsNone(result)

    def test_genCrossRegionCallsCSVFile_with_data(self):
        """Test genCrossRegionCallsCSVFile with actual cross region calls data."""
        # Create mock metrics with cross region calls
        mock_metric = MagicMock()
        mock_metric.traceID = "service/trace123"
        mock_metric.crossRegionCalls = {
            "call1": {
                "parentSpanId": "span1",
                "childSpanId": "span2",
                "operationName": "test_op",
                "parentRegion": "us-east-1",
                "childRegion": "us-west-2",
                "parentService": "service1",
                "childService": "service2",
                "parentDuration": 100,
                "childDuration": 50,
                "durationRatio": 0.5,
                "callPath": "service1->service2"
            }
        }

        metrics_list = [mock_metric]

        with patch("os.path.join", return_value="/test/output/crossRegionCalls.csv"), \
             patch("pandas.DataFrame") as mock_dataframe:

            mock_df = MagicMock()
            mock_dataframe.return_value = mock_df

            result = genCrossRegionCallsCSVFile(metrics_list, self.mock_config)

        # Verify DataFrame was created and to_csv was called
        mock_dataframe.assert_called_once()
        mock_df.to_csv.assert_called_once_with("/test/output/crossRegionCalls.csv", index=False)

        # Verify return value
        self.assertEqual(result, "/test/output/crossRegionCalls.csv")

    def test_genCrossRegionCallsCSVFile_no_data(self):
        """Test genCrossRegionCallsCSVFile when no cross region calls are found."""
        metrics_list = []

        with patch("os.path.join", return_value="/test/output/crossRegionCalls.csv"):
            result = genCrossRegionCallsCSVFile(metrics_list, self.mock_config)

        self.assertIsNone(result)


class TestComputeLatencyReductionOriginal(unittest.TestCase):
    """Test from original test_process.py - moved here after refactoring."""

    def test_computeLatencyReduction(self):
        # Input DataFrame
        input_df = pd.DataFrame(
            {
                "observed_latency": [100, 200, 0, 150],
                "no_err_latency": [50, 100, 50, 100],
            }
        )

        # Call the function
        result_df = computeLatencyReduction(input_df)

        # Check that latency_reduction column was added
        self.assertIn("latency_reduction", result_df.columns)

        # Check the calculated values
        expected_values = [50.0, 50.0, 0.0, 33.33]
        actual_values = result_df["latency_reduction"].tolist()

        for expected, actual in zip(expected_values, actual_values):
            self.assertAlmostEqual(expected, actual, places=2)


if __name__ == "__main__":
    unittest.main()
