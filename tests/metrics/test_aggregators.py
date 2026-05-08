"""Tests for metrics aggregation functions.

This module contains unit tests for the aggregation and merging functions
used in critical path analysis.
"""

import unittest
from unittest.mock import MagicMock

import pytest

from crisp.metrics.aggregators import (
    MergeCallPathProfilesWithExample,
    MergeCallPathProfilesWithExemplars,
    MergeMetricValsWithTrace,
    mergeCallChains,
    mergeCallpathTime,
    mergeExampleID,
)
from crisp.shared.models import (
    CallPathProfile,
    ErrorCPMetrics,
    ErrorMetrics,
    Metrics,
    MetricVals,
    QuantizedMetrics,
)


def getDummyMetric(**kwargs):
    """Helper function to create a dummy Metrics object for testing."""
    traceID = kwargs.get("traceID", "trace1")
    CPMetrics = kwargs.get("CPMetrics", CallPathProfile({}, 0, None))
    errCPMetrics = kwargs.get("errCPMetrics", ErrorCPMetrics(
        errCPCallpathTimeExclusive={},
        errCPErrCounts={},
        savingPotential={},
        numCPErrors=0,
        numRelatedToCPErrors=0,
    ))
    errMetrics = kwargs.get("errMetrics", ErrorMetrics(
        numAllErrors=0,
        errCounts={},
        errCallChainCounts={},
        selfErrDepthList=[],
        stoppedErrDepthList=[],
        errDepthMap={},
        errPropLengthMap={},
        resiliencyMap={},
        maxErrDepthPropToRoot=-1,
        propToRootHistoQuantized=QuantizedMetrics({}),
        notPropToRootHistoQuantized=QuantizedMetrics({}),
        propToRootOnCPHistoQuantized=QuantizedMetrics({}),
        notPropToRootOnCPHistoQuantized=QuantizedMetrics({}),
        supressHistoQuantized=QuantizedMetrics({}),
        supressOnCPHistoQuantized=QuantizedMetrics({}),
    ))

    return Metrics(
        traceID=traceID,
        traceSz=0,
        CPMetrics=CPMetrics,
        errCPMetrics=errCPMetrics,
        errMetrics=errMetrics,
        totalWork=0,
        timeSavedOnWork=0,
        latency=0,
        timeSavedOnCPPessimistic=0,
        timeSavedOnCPOptimistic=0,
        timeSavedOnCPAllSeries=0,
        rootSpanID="span1",
        descendants=0,
        depth=0,
        numNodesOnCP=0,
        rootReturnError=False,
        propToRootErrCCT={},
        isCtfTest=False,
        numProxyRoots=0,
        tags=[],
        cycles={},
        crossRegionCalls={},
    )


class TestMergeCallChains(unittest.TestCase):
    """Test cases for mergeCallChains function."""

    def test_merge_call_chains_basic(self):
        """Test merging callMap into an empty totalCallMap."""
        callMap = {
            "operation1": {"callA", "callB"},
            "operation2": {"callC"},
        }
        totalCallMap = {}

        # Call the function
        mergeCallChains(callMap, totalCallMap)

        # Check that totalCallMap contains the merged data
        expected_totalCallMap = {
            "operation1": {"callA", "callB"},
            "operation2": {"callC"},
        }
        self.assertEqual(totalCallMap, expected_totalCallMap)

    def test_merge_call_chains_with_existing_data(self):
        """Test merging callMap into a non-empty totalCallMap."""
        callMap = {
            "operation1": {"callC"},
            "operation2": {"callD"},
        }
        totalCallMap = {
            "operation1": {"callA", "callB"},
        }

        # Call the function
        mergeCallChains(callMap, totalCallMap)

        # Check that existing data is preserved and new data is added
        expected_totalCallMap = {
            "operation1": {"callA", "callB", "callC"},
            "operation2": {"callD"},
        }
        self.assertEqual(totalCallMap, expected_totalCallMap)


class TestMergeCallpathTime(unittest.TestCase):
    """Test cases for mergeCallpathTime function."""

    def test_merge_callpath_time(self):
        """Test mergeCallpathTime covers basic branch conditions."""
        # Set up inputs
        callMap = {
            "operation1": ["path1", "path2"]
        }
        callPathMap = {
            "path1": MagicMock(field=10),
            "path2": MagicMock(field=20),
        }
        totalBreakdownTime = {}
        field = "field"

        # Call the function
        mergeCallpathTime(callMap, callPathMap, field, totalBreakdownTime)

        # Verify the output
        expected_totalBreakdownTime = {
            "operation1": {
                "path1": [10],
                "path2": [20],
            }
        }
        self.assertEqual(totalBreakdownTime, expected_totalBreakdownTime)

    def test_merge_callpath_time_with_existing_data(self):
        """Test merging into existing totalBreakdownTime."""
        # Set up inputs
        callMap = {
            "operation1": ["path1"],
        }
        callPathMap = {
            "path1": MagicMock(field=30),
        }
        totalBreakdownTime = {
            "operation1": {
                "path1": [10, 20],
            }
        }
        field = "field"

        # Call the function
        mergeCallpathTime(callMap, callPathMap, field, totalBreakdownTime)

        # Verify that the new value is appended
        expected_totalBreakdownTime = {
            "operation1": {
                "path1": [10, 20, 30],
            }
        }
        self.assertEqual(totalBreakdownTime, expected_totalBreakdownTime)


class TestMergeExampleID(unittest.TestCase):
    """Test cases for mergeExampleID function."""

    def test_merge_example_id(self):
        """Test mergeExampleID to cover branch conditions for new and existing entries."""
        # Set up inputs
        traceID = "traceA"
        localExampleMap = {
            "operation1": ("path1", 15),
            "operation2": ("path2", 10),
        }
        exampleMap = {
            "operation1": ("traceB", "path1", 10),  # Lower value, should update
            "operation3": ("traceC", "path3", 20),  # Not in localExampleMap, should stay the same
        }

        # Call the function
        mergeExampleID(traceID, localExampleMap, exampleMap)

        # Verify the output
        expected_exampleMap = {
            "operation1": ("traceA", "path1", 15),  # Updated because 15 > 10
            "operation2": ("traceA", "path2", 10),  # New entry
            "operation3": ("traceC", "path3", 20),  # Unchanged
        }
        self.assertEqual(exampleMap, expected_exampleMap)

    def test_merge_example_id_no_update_lower_value(self):
        """Test that existing entry is not updated when new value is lower."""
        # Set up inputs
        traceID = "traceA"
        localExampleMap = {
            "operation1": ("path1", 5),
        }
        exampleMap = {
            "operation1": ("traceB", "path1", 10),  # Higher value, should not update
        }

        # Call the function
        mergeExampleID(traceID, localExampleMap, exampleMap)

        # Verify that exampleMap is unchanged
        expected_exampleMap = {
            "operation1": ("traceB", "path1", 10),
        }
        self.assertEqual(exampleMap, expected_exampleMap)


class TestMergeMetricValsWithTrace(unittest.TestCase):
    """Test cases for MergeMetricValsWithTrace function."""

    def test_merge_metric_vals_with_trace(self):
        """Test basic merging of metric values."""
        metric_a = MetricVals(1, 2, 3, 100)
        metric_b = MetricVals(4, 5, 6, 200)
        b_trace = "trace_b"

        result = MergeMetricValsWithTrace(
            metric_a,
            None,
            metric_b,
            b_trace,
        )

        # Check if the metrics are correctly added
        self.assertEqual(result.inc, 5)
        self.assertEqual(result.excl, 7)
        self.assertEqual(result.freq, 9)

        # Check if the trace is correctly updated
        self.assertEqual(result.incEx, 200)
        self.assertEqual(result.incTrace, b_trace)
        self.assertEqual(result.exclEx, 200)
        self.assertEqual(result.exclTrace, b_trace)

    def test_merge_metric_vals_no_trace_update(self):
        """Test that trace is not updated when b has lower values."""
        metric_a = MetricVals(1, 2, 3, 100)
        metric_a.incExVal = 300
        metric_a.incEx = 300
        metric_a.incTrace = "trace_a"
        metric_a.exclExVal = 300
        metric_a.exclEx = 300
        metric_a.exclTrace = "trace_a"

        metric_b = MetricVals(4, 5, 6, 50)
        b_trace = "trace_b"

        result = MergeMetricValsWithTrace(
            metric_a,
            None,
            metric_b,
            b_trace,
        )

        # Check that values are added
        self.assertEqual(result.inc, 5)
        self.assertEqual(result.excl, 7)
        self.assertEqual(result.freq, 9)

        # Check that trace is NOT updated (original trace_a should remain)
        self.assertEqual(result.incTrace, "trace_a")
        self.assertEqual(result.exclTrace, "trace_a")


class TestMergeCallPathProfilesWithExample(unittest.TestCase):
    """Test cases for MergeCallPathProfilesWithExample function."""

    def test_merge_call_path_profiles_with_example(self):
        """Test merging multiple call path profiles."""
        metric1 = MetricVals(1, 2, 3, 100)
        m1 = getDummyMetric(
            traceID="trace1",
            CPMetrics=CallPathProfile({"path1": metric1}, 2, "trace1"),
        )

        metric2 = MetricVals(4, 5, 6, 200)
        m2 = getDummyMetric(
            traceID="trace2",
            CPMetrics=CallPathProfile({"path2": metric2}, 3, "trace2"),
        )

        metric3 = MetricVals(7, 1, 9, 400)
        metric4 = MetricVals(3, 3, 3, 500)
        profile3 = CallPathProfile(
            {"path1": metric3, "path2": metric4},
            1,
            "trace3",
        )
        m3 = getDummyMetric(traceID="trace3", CPMetrics=profile3)

        metrics = [m1, m2, m3]
        result = MergeCallPathProfilesWithExample(metrics)

        # Check if the call paths are correctly merged
        self.assertEqual(len(result.profile), 2)
        self.assertIn("path1", result.profile)
        self.assertIn("path2", result.profile)

        # Check merged values for path1 (metric1 + metric3)
        self.assertEqual(result.profile["path1"].inc, 8)  # 1 + 7
        self.assertEqual(result.profile["path1"].excl, 3)  # 2 + 1
        self.assertEqual(result.profile["path1"].freq, 12)  # 3 + 9

        # Check merged values for path2 (metric2 + metric4)
        self.assertEqual(result.profile["path2"].inc, 7)  # 4 + 3
        self.assertEqual(result.profile["path2"].excl, 8)  # 5 + 3
        self.assertEqual(result.profile["path2"].freq, 9)  # 6 + 3

        # Check that count is correctly summed
        self.assertEqual(result.count, 6)  # 2 + 3 + 1

    def test_merge_call_path_profiles_empty_list(self):
        """Test merging with an empty list of metrics."""
        metrics = []
        result = MergeCallPathProfilesWithExample(metrics)

        # Should return an empty profile
        self.assertEqual(len(result.profile), 0)
        self.assertEqual(result.count, 0)

@pytest.fixture
def _make_metric():
    """Factory fixture that builds a Metrics object with a given call-path profile."""

    def _factory(trace_id, profile_dict, count=1):
        return getDummyMetric(
            traceID=trace_id,
            CPMetrics=CallPathProfile(profile_dict, count, trace_id),
        )

    return _factory


def test_merge_exemplars_top_n_by_excl_time(_make_metric):
    """Merge 5 metrics with max_exemplars=3 -> keep the 3 highest by exclExVal."""
    metrics = [
        _make_metric("t1", {"pathA": MetricVals(10, 100, 1, "s1")}),
        _make_metric("t2", {"pathA": MetricVals(10, 200, 1, "s2")}),
        _make_metric("t3", {"pathA": MetricVals(10, 50, 1, "s3")}),
        _make_metric("t4", {"pathA": MetricVals(10, 300, 1, "s4")}),
        _make_metric("t5", {"pathA": MetricVals(10, 150, 1, "s5")}),
    ]

    result = MergeCallPathProfilesWithExemplars(metrics, max_exemplars=3)

    assert len(result.profile) == 1
    exemplars = result.profile["pathA"].exemplars
    assert len(exemplars) == 3
    assert exemplars == [("t4", "s4"), ("t2", "s2"), ("t5", "s5")]


def test_merge_exemplars_single_metric(_make_metric):
    """A single metric produces exactly 1 exemplar."""
    metrics = [
        _make_metric("t1", {"pathA": MetricVals(10, 42, 1, "s1")}),
    ]

    result = MergeCallPathProfilesWithExemplars(metrics, max_exemplars=3)

    exemplars = result.profile["pathA"].exemplars
    assert exemplars == [("t1", "s1")]


def test_merge_exemplars_empty_input():
    """Empty metrics list yields an empty profile."""
    result = MergeCallPathProfilesWithExemplars([], max_exemplars=3)

    assert len(result.profile) == 0
    assert result.count == 0


def test_merge_exemplars_per_call_path_independent(_make_metric):
    """Exemplars are tracked independently per call path."""
    metrics = [
        _make_metric("t1", {
            "pathA": MetricVals(10, 300, 1, "sA1"),
            "pathB": MetricVals(10, 50, 1, "sB1"),
        }),
        _make_metric("t2", {
            "pathA": MetricVals(10, 100, 1, "sA2"),
            "pathB": MetricVals(10, 200, 1, "sB2"),
        }),
        _make_metric("t3", {
            "pathA": MetricVals(10, 200, 1, "sA3"),
            "pathB": MetricVals(10, 150, 1, "sB3"),
        }),
    ]

    result = MergeCallPathProfilesWithExemplars(metrics, max_exemplars=2)

    a_exemplars = result.profile["pathA"].exemplars
    assert len(a_exemplars) == 2
    assert a_exemplars == [("t1", "sA1"), ("t3", "sA3")]

    b_exemplars = result.profile["pathB"].exemplars
    assert len(b_exemplars) == 2
    assert b_exemplars == [("t2", "sB2"), ("t3", "sB3")]


def test_merge_exemplars_preserves_backward_compat_fields(_make_metric):
    """exclTrace/incTrace/exclEx/incEx are still set like MergeCallPathProfilesWithExample."""
    metrics = [
        _make_metric("t1", {"pathA": MetricVals(10, 20, 1, "s1")}),
        _make_metric("t2", {"pathA": MetricVals(50, 80, 1, "s2")}),
    ]

    result = MergeCallPathProfilesWithExemplars(metrics, max_exemplars=3)

    mv = result.profile["pathA"]
    assert mv.inc == 60
    assert mv.excl == 100
    assert mv.freq == 2
    assert mv.exclTrace == "t2"
    assert mv.incTrace == "t2"
    assert mv.exclEx == "s2"
    assert mv.incEx == "s2"


def test_merge_exemplars_count_accumulated(_make_metric):
    """Counts from all CPMetrics are accumulated."""
    metrics = [
        _make_metric("t1", {"pathA": MetricVals(1, 2, 3, "s1")}, count=5),
        _make_metric("t2", {"pathA": MetricVals(4, 5, 6, "s2")}, count=7),
    ]

    result = MergeCallPathProfilesWithExemplars(metrics, max_exemplars=3)

    assert result.count == 12


def test_merge_exemplars_max_zero_gives_no_exemplars(_make_metric):
    """max_exemplars=0 means no exemplars are collected."""
    metrics = [
        _make_metric("t1", {"pathA": MetricVals(10, 100, 1, "s1")}),
        _make_metric("t2", {"pathA": MetricVals(10, 200, 1, "s2")}),
    ]

    result = MergeCallPathProfilesWithExemplars(metrics, max_exemplars=0)

    assert result.profile["pathA"].exemplars == []


def test_merge_exemplars_fewer_metrics_than_cap(_make_metric):
    """When there are fewer metrics than max_exemplars, all are kept."""
    metrics = [
        _make_metric("t1", {"pathA": MetricVals(10, 100, 1, "s1")}),
        _make_metric("t2", {"pathA": MetricVals(10, 200, 1, "s2")}),
    ]

    result = MergeCallPathProfilesWithExemplars(metrics, max_exemplars=5)

    exemplars = result.profile["pathA"].exemplars
    assert len(exemplars) == 2
    assert exemplars == [("t2", "s2"), ("t1", "s1")]


if __name__ == "__main__":
    unittest.main()
