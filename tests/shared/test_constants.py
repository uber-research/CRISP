"""Tests for shared constants to ensure coverage."""

import unittest

from crisp.shared.constants import (
    DEFAULT_MAX_DEPTH,
    DEFAULT_MIN_DEPTH,
    DEFAULT_PROPAGATED_ERRORS,
    DEFAULT_SELF_ERRORS,
    DEFAULT_STOPPED_ERRORS,
    JAEGER_UI_URL,
    PERCENTILE_50,
    PERCENTILE_90,
    PERCENTILE_95,
    PERCENTILE_99,
    SORTABLE_COL_CLASS,
    TOTAL_TIME,
    SpanKindValues,
)


class TestConstants(unittest.TestCase):
    """Test constants are properly defined."""

    def test_jaeger_ui_url(self):
        """JAEGER_UI_URL is an http(s) URL and does not leak an internal host."""
        self.assertIsInstance(JAEGER_UI_URL, str)
        # Accept either scheme so the default can change (e.g. local Jaeger
        # at http://localhost:16686/ or a user-configured https endpoint)
        # without having to update this assertion.
        self.assertRegex(JAEGER_UI_URL, r"^https?://")
        # Guard against accidentally re-introducing an Uber-internal host.
        self.assertNotIn("uberinternal", JAEGER_UI_URL)

    def test_sortable_col_class(self):
        """Test SORTABLE_COL_CLASS constant."""
        self.assertIsInstance(SORTABLE_COL_CLASS, str)
        self.assertIn("fas fa-sort", SORTABLE_COL_CLASS)

    def test_total_time(self):
        """Test TOTAL_TIME constant."""
        self.assertEqual(TOTAL_TIME, "totalTime")
        self.assertIsInstance(TOTAL_TIME, str)


class TestDepthSentinels(unittest.TestCase):
    """Test the depth-sentinel constants used by QuantizedMetrics."""

    def test_min_depth_is_large_positive_int(self):
        # DEFAULT_MIN_DEPTH is seeded into a running min(), so any real
        # observed depth should beat it. Any sufficiently large int works;
        # we pin the specific internal value to catch accidental edits.
        self.assertIsInstance(DEFAULT_MIN_DEPTH, int)
        self.assertEqual(DEFAULT_MIN_DEPTH, 1_000_000_000)

    def test_max_depth_is_minus_one(self):
        # DEFAULT_MAX_DEPTH is seeded into a running max(), so any real
        # observed depth (>= 0) should beat it.
        self.assertIsInstance(DEFAULT_MAX_DEPTH, int)
        self.assertEqual(DEFAULT_MAX_DEPTH, -1)

    def test_min_beats_max_when_no_data(self):
        # Property check: with no data points, the "min" sentinel stays
        # larger than the "max" sentinel, which callers rely on to detect
        # the empty-histogram case.
        self.assertGreater(DEFAULT_MIN_DEPTH, DEFAULT_MAX_DEPTH)


class TestPercentiles(unittest.TestCase):
    """Test percentile constants are fractions in [0, 1]."""

    def test_values(self):
        self.assertEqual(PERCENTILE_50, 0.5)
        self.assertEqual(PERCENTILE_90, 0.9)
        self.assertEqual(PERCENTILE_95, 0.95)
        self.assertEqual(PERCENTILE_99, 0.99)

    def test_all_in_unit_interval(self):
        for p in (PERCENTILE_50, PERCENTILE_90, PERCENTILE_95, PERCENTILE_99):
            self.assertGreaterEqual(p, 0.0)
            self.assertLessEqual(p, 1.0)

    def test_monotonic(self):
        # Guard against a swap or typo in a future edit.
        self.assertLess(PERCENTILE_50, PERCENTILE_90)
        self.assertLess(PERCENTILE_90, PERCENTILE_95)
        self.assertLess(PERCENTILE_95, PERCENTILE_99)


class TestDefaultErrorCounts(unittest.TestCase):
    """Test error-count zero-initializers."""

    def test_all_zero(self):
        self.assertEqual(DEFAULT_SELF_ERRORS, 0)
        self.assertEqual(DEFAULT_PROPAGATED_ERRORS, 0)
        self.assertEqual(DEFAULT_STOPPED_ERRORS, 0)


class TestSpanKindValues(unittest.TestCase):
    """Test the raw numeric span-kind values."""

    def test_values(self):
        self.assertEqual(SpanKindValues.CLIENT, 0)
        self.assertEqual(SpanKindValues.SERVER, 1)
        self.assertEqual(SpanKindValues.UNKNOWN, 2)

    def test_distinct(self):
        values = {
            SpanKindValues.CLIENT,
            SpanKindValues.SERVER,
            SpanKindValues.UNKNOWN,
        }
        self.assertEqual(len(values), 3)


if __name__ == "__main__":
    unittest.main()
