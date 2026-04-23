# ruff: noqa: I001
"""Tests for shared data models.

This module contains unit tests for MetricVals, CallPathProfile, and LatencyData classes.
"""

from unittest import TestCase

from crisp.shared.models import (
    MetricVals,
    CallPathProfile,
    LatencyData,
)


class TestMetricVals(TestCase):
    def test_addition(self):
        metric1 = MetricVals(1, 2, 3, 100)
        metric2 = MetricVals(4, 5, 6, 200)
        result = metric1 + metric2
        self.assertEqual(result.inc, 5)
        self.assertEqual(result.excl, 7)
        self.assertEqual(result.freq, 9)

    def test_in_place_addition(self):
        metric = MetricVals(1, 2, 3, 100)
        metric += MetricVals(4, 5, 6, 200)
        self.assertEqual(metric.inc, 5)
        self.assertEqual(metric.excl, 7)
        self.assertEqual(metric.freq, 9)

    def test_floordiv(self):
        metric = MetricVals(10, 20, 30, 100)
        result = metric // 2
        self.assertEqual(result.inc, 5)
        self.assertEqual(result.excl, 10)
        self.assertEqual(result.freq, 15)

    def test_in_place_floordiv(self):
        metric = MetricVals(10, 20, 30, 100)
        metric //= 2
        self.assertEqual(metric.inc, 5)
        self.assertEqual(metric.excl, 10)
        self.assertEqual(metric.freq, 15)


class TestCallPathProfile(TestCase):
    def setUp(self):
        self.metric1 = MetricVals(1, 2, 3, 100)
        self.metric2 = MetricVals(4, 5, 6, 200)
        self.profile1 = CallPathProfile({"path1": self.metric1}, 2, 1)
        self.profile2 = CallPathProfile({"path2": self.metric2}, 3, 2)

    def test_get_normalized(self):
        result = self.profile1.GetNormalized()
        self.assertEqual(result["path1"].inc, 0)
        self.assertEqual(result["path1"].excl, 1)
        self.assertEqual(result["path1"].freq, 1)

    def test_normalize(self):
        self.profile1.Normalize()
        self.assertEqual(self.profile1.profile["path1"].inc, 0)
        self.assertEqual(self.profile1.profile["path1"].excl, 1)
        self.assertEqual(self.profile1.profile["path1"].freq, 1)

    def test_normalize_field(self):
        self.profile1.NormalizeField("inc")
        self.assertEqual(self.profile1.profile["path1"].inc, 0)

    def test_upsert_existing(self):
        self.profile1.Upsert("path1", MetricVals(1, 1, 1, 300))
        self.assertEqual(self.profile1.profile["path1"].inc, 2)
        self.assertEqual(self.profile1.profile["path1"].excl, 3)
        self.assertEqual(self.profile1.profile["path1"].freq, 4)

    def test_upsert_new(self):
        self.profile1.Upsert("path3", MetricVals(1, 1, 1, 300))
        self.assertEqual(self.profile1.profile["path3"].inc, 1)
        self.assertEqual(self.profile1.profile["path3"].excl, 1)
        self.assertEqual(self.profile1.profile["path3"].freq, 1)

    def test_add_profiles(self):
        result = self.profile1 + self.profile2
        self.assertIn("path1", result.profile)
        self.assertIn("path2", result.profile)
        self.assertEqual(result.count, 5)

    def test_in_place_add_profiles(self):
        self.profile1 += self.profile2
        self.assertIn("path1", self.profile1.profile)
        self.assertIn("path2", self.profile1.profile)
        self.assertEqual(self.profile1.count, 5)


class TestLatencyData(TestCase):
    def test_addition_and_average(self):
        """Test LatencyData addition using sum() and average() method."""
        d1 = LatencyData("1", 100, 10, 1, 2)
        d2 = LatencyData("2", 100, 20, 3, 4)
        d3 = LatencyData("3", 100, 30, 5, 6)

        process_traces = sum(
            [d1, d2, d3],
            start=LatencyData("", 0, 0, 0, 0),
        )

        self.assertEqual(process_traces.latency, 300)
        self.assertEqual(process_traces.hypoLatency, 60)
        self.assertEqual(process_traces.hypoLatencyOptimistic, 9)
        self.assertEqual(process_traces.hypoLatencyPessimistic, 12)

        process_traces.average(3)

        self.assertEqual(process_traces.latency, 100)
        self.assertEqual(process_traces.hypoLatency, 20)
        self.assertEqual(process_traces.hypoLatencyOptimistic, 3)
        self.assertEqual(process_traces.hypoLatencyPessimistic, 4)
