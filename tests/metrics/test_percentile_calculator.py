"""Tests for percentile calculation functions.

This module contains unit tests for the percentile calculation functions
used in critical path analysis.
"""

import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

from crisp.metrics.percentile_calculator import (
    addPercentileColumns,
    genLatencyPercentile,
    insertInDF,
    insertInclusivePercentileInfoDF,
    processMetricChunk,
)
from crisp.shared.models import LatencyData


class TestPercentileCalculator(unittest.TestCase):
    """Tests for percentile calculation functions."""

    def test_processMetricChunk(self):
        """Test processing a metric chunk into a DataFrame."""
        chunk = [
            MagicMock(traceID="trace1", latency=100, CPMetrics=MagicMock(profile={"op1": MagicMock(time=30)})),
            MagicMock(traceID="trace2", latency=200, CPMetrics=MagicMock(profile={"op1": MagicMock(time=40)}))
        ]
        with patch("crisp.shared.constants.TOTAL_TIME", "totalTime"):
            df = processMetricChunk(chunk, "time")
            expected_df = pd.DataFrame(
                {"totalTime": [100, 200], "op1": [30, 40]},
                index=["trace1", "trace2"]
            )
            pd.testing.assert_frame_equal(df, expected_df)

    def test_insertInDF(self):
        """Test inserting chunks of metrics into a DataFrame."""
        metrics = [
            MagicMock(traceID="trace1", latency=100, CPMetrics=MagicMock(profile={"op1": MagicMock(time=30)})),
            MagicMock(traceID="trace2", latency=200, CPMetrics=MagicMock(profile={"op1": MagicMock(time=40)})),
            MagicMock(traceID="trace3", latency=300, CPMetrics=MagicMock(profile={"op1": MagicMock(time=50)})),
        ]
        with patch("crisp.metrics.percentile_calculator.processMetricChunk") as mock_processMetricChunk:
            mock_processMetricChunk.side_effect = lambda chunk, _: pd.DataFrame(
                [{"totalTime": metric.latency, "op1": metric.CPMetrics.profile["op1"].time} for metric in chunk],
                index=[metric.traceID for metric in chunk]
            )
            df = insertInDF(metrics, "time")
            self.assertEqual(df.shape[0], 3)

    def test_addPercentileColumns(self):
        """Test adding percentile columns to a DataFrame."""
        df = pd.DataFrame({"op1": [10, 20, 30], "totalTime": [10, 20, 30]})
        percentiles = [
            MagicMock(
                percentile=0.5,
                percentileWithMaxPrefix=MagicMock(return_value="P50"),
                percentageWithAvgPrefix=MagicMock(return_value="P50%"),
            )
        ]
        with patch("crisp.shared.constants.TOTAL_TIME", "totalTime"):
            df_result = addPercentileColumns(df, percentiles)
            self.assertIn("P50", df_result.columns)

    def test_insertInclusivePercentileInfoDF(self):
        """Test inserting inclusive percentile info from one DataFrame to another."""
        df = pd.DataFrame(columns=["op1", "op2"])
        inclusiveDF = pd.DataFrame({
            "P50": [10, 20],
            "P90": [30, 40],
            "P50%": [0.5, 0.6],
            "P90%": [0.7, 0.8]
        }, index=["op1", "op2"])

        percentilesInclusive = [
            MagicMock(percentileWithMaxPrefix=MagicMock(return_value="P50"), percentageWithAvgPrefix=MagicMock(return_value="P50%")),
            MagicMock(percentileWithMaxPrefix=MagicMock(return_value="P90"), percentageWithAvgPrefix=MagicMock(return_value="P90%"))
        ]

        df_result = insertInclusivePercentileInfoDF(df, percentilesInclusive, inclusiveDF)

        # Check that columns have been inserted correctly
        expected_columns = ["P50", "P90", "P50%", "P90%", "op1", "op2"]
        self.assertEqual(df_result.columns.tolist(), expected_columns)
        self.assertEqual(df_result["P50"].tolist(), [10, 20])
        self.assertEqual(df_result["P90%"].tolist(), [0.7, 0.8])

    def compare_latency_data_lists(self, list1, list2):
        """Helper function to compare lists of (percentile, limit, LatencyData) tuples."""
        self.assertEqual(len(list1), len(list2), "Lists have different lengths")
        for (p1, limit1, latency1), (p2, limit2, latency2) in zip(list1, list2):
            self.assertEqual(p1, p2, "Percentiles differ")
            self.assertEqual(limit1, limit2, "Limits differ")
            # Compare each attribute in LatencyData
            self.assertEqual(latency1.traceID, latency2.traceID, "Trace IDs differ")
            self.assertAlmostEqual(latency1.latency, latency2.latency, places=2, msg="Latencies differ")
            self.assertAlmostEqual(latency1.hypoLatency, latency2.hypoLatency, places=2, msg="Hypothetical latencies differ")
            self.assertAlmostEqual(latency1.hypoLatencyOptimistic, latency2.hypoLatencyOptimistic,
                                   places=2, msg="Optimistic hypothetical latencies differ")
            self.assertAlmostEqual(latency1.hypoLatencyPessimistic, latency2.hypoLatencyPessimistic,
                                   places=2, msg="Pessimistic hypothetical latencies differ")

    def test_genLatencyPercentile(self):
        """Test the genLatencyPercentile function with sample data."""
        # Create sample LatencyData objects
        latencyHypo = [
            LatencyData("trace1", 100, 80, 70, 90),
            LatencyData("trace2", 200, 180, 170, 190),
            LatencyData("trace3", 300, 280, 270, 290),
            LatencyData("trace4", 400, 380, 370, 390),
            LatencyData("trace5", 500, 480, 470, 490),
        ]
        percentiles = [20, 40, 60, 80, 100]

        # Test sorting by latency, tailLatency=False
        result = genLatencyPercentile(latencyHypo, percentiles, lambda x: x.latency, tailLatency=False)
        expected = []
        for p in percentiles:
            limit = round(len(latencyHypo) * p / 100)
            selected = latencyHypo[:limit]
            avg_latency = sum(selected, LatencyData("", 0, 0, 0, 0))
            avg_latency.average(limit)
            expected.append((p, limit, avg_latency))
        self.compare_latency_data_lists(result, expected)

        # Test sorting by hypoLatency, tailLatency=True
        result = genLatencyPercentile(latencyHypo, percentiles, lambda x: x.hypoLatency, tailLatency=True)
        expected = []
        latencySorted = sorted(latencyHypo, key=lambda x: x.hypoLatency)
        for p in percentiles:
            limit = round(len(latencyHypo) * p / 100)
            if limit == 0:
                continue
            start = len(latencySorted) - limit
            selected = latencySorted[start:]
            avg_latency = sum(selected, LatencyData("", 0, 0, 0, 0))
            avg_latency.average(limit)
            expected.append((p, limit, avg_latency))
        self.compare_latency_data_lists(result, expected)
