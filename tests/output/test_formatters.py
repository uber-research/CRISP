"""Tests for formatter functions."""

import unittest
from unittest.mock import MagicMock

import pandas as pd

from crisp.output.formatters import (
    JAEGER_UI_URL,
    SORTABLE_COL_CLASS,
    TOTAL_TIME,
    addHyperLinkToTrace,
    insertOccurenceCol,
    makeClickable,
    reindexDescending,
    renameSortableIcon,
    setCellFormating,
    sortableColHeader,
)


class TestFormatters(unittest.TestCase):

    def test_makeClickable(self):
        url = "http://example.com"
        name = "Example"
        result = makeClickable(url, name)
        expected = '<a href="http://example.com" rel="noopener noreferrer" target="_blank">Example</a>'
        self.assertEqual(result, expected)

    def test_sortableColHeader(self):
        result = sortableColHeader("col1")
        expected = "col1" + SORTABLE_COL_CLASS
        self.assertEqual(result, expected)

    def test_addHyperLinkToTrace(self):
        df = pd.DataFrame({'trace1': [1, 2], 'trace2': [3, 4]})
        tracespanIDmap = {'trace1': 'span1', 'trace2': 'span2'}

        result_df = addHyperLinkToTrace(df, tracespanIDmap)

        expected_cols = [
            f'<a href="{JAEGER_UI_URL}trace1?uiFind=span1" rel="noopener noreferrer" target="_blank">#</a>',
            f'<a href="{JAEGER_UI_URL}trace2?uiFind=span2" rel="noopener noreferrer" target="_blank">#</a>'
        ]
        self.assertEqual(list(result_df.columns), expected_cols)

    def test_renameSortableIcon(self):
        df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})
        columns = ['col1', 'col2']

        result_df = renameSortableIcon(df, columns)

        expected_cols = [
            'col1' + SORTABLE_COL_CLASS,
            'col2' + SORTABLE_COL_CLASS
        ]
        self.assertEqual(list(result_df.columns), expected_cols)

    def test_insertOccurenceCol(self):
        df = pd.DataFrame({'op1': [1, 2], 'op2': [3, 4]})
        jaegerTraceFiles = ['file1.json', 'file2.json', 'file3.json']
        nonZeros = pd.Series([10, 20])

        result_df, occurenceColHeader = insertOccurenceCol(df, jaegerTraceFiles, nonZeros)

        expected_header = "occurence (3)"
        self.assertEqual(occurenceColHeader, expected_header)
        self.assertEqual(result_df.columns[0], expected_header)
        self.assertEqual(result_df.iloc[0, 0], 10)
        self.assertEqual(result_df.iloc[1, 0], 20)

    def test_reindexDescending(self):
        # Create test dataframe with TOTAL_TIME row
        df = pd.DataFrame({
            'trace1': [10, 5, 15],  # sum = 30
            'trace2': [8, 3, 12],   # sum = 23
            'trace3': [12, 7, 20]   # sum = 39
        }, index=['op1', 'op2', TOTAL_TIME])

        prefixColumns = ['prefix1']
        traceIDIndex = ['trace1', 'trace2', 'trace3']

        result_df = reindexDescending(df, prefixColumns, traceIDIndex)

        # Should be sorted by TOTAL_TIME row values: trace3 (20), trace1 (15), trace2 (12)
        expected_cols = ['prefix1', 'trace3', 'trace1', 'trace2']
        self.assertEqual(list(result_df.columns), expected_cols)

    def test_setCellFormating(self):
        df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})

        # Mock percentiles with required methods
        percentiles = [
            MagicMock(percentageWithAvgPrefix=MagicMock(return_value="P50%"))
        ]
        occurenceColHeader = "occurence (100)"

        result = setCellFormating(df, percentiles, occurenceColHeader)

        # Check that it returns a dictionary with formatting
        self.assertIsInstance(result, dict)
        self.assertIn(sortableColHeader("P50%"), result)
        self.assertEqual(result[sortableColHeader("P50%")], "{:.2f}")
        self.assertIn(sortableColHeader(occurenceColHeader), result)
        self.assertEqual(result[sortableColHeader(occurenceColHeader)], "{:5d}")


if __name__ == '__main__':
    unittest.main()
