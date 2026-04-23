"""Tests for shared constants to ensure coverage."""

import unittest

from crisp.shared.constants import (
    JAEGER_UI_URL,
    SORTABLE_COL_CLASS,
    TOTAL_TIME,
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


if __name__ == "__main__":
    unittest.main()
