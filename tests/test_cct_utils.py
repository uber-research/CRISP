"""Unit tests for cct_utils.py module."""

import os
import tempfile
import unittest
from unittest.mock import patch, mock_open

import pytest

from crisp import cct_utils


class TestParseCallPathPart(unittest.TestCase):
    """Test parse_call_path_part function."""

    def test_valid_service_operation(self):
        """Test parsing valid service and operation."""
        result = cct_utils.parse_call_path_part("[feedservice]GetStreamingFeedByID")
        expected = {
            'service': 'feedservice',
            'operation_name': 'GetStreamingFeedByID'
        }
        self.assertEqual(result, expected)

    def test_service_only(self):
        """Test parsing service without operation."""
        result = cct_utils.parse_call_path_part("[feedservice]")
        expected = {
            'service': 'feedservice',
            'operation_name': ''
        }
        self.assertEqual(result, expected)

    def test_operation_with_spaces(self):
        """Test parsing with whitespace around operation."""
        result = cct_utils.parse_call_path_part("[service]  operation_name  ")
        expected = {
            'service': 'service',
            'operation_name': 'operation_name'
        }
        self.assertEqual(result, expected)

    def test_complex_service_name(self):
        """Test parsing complex service names."""
        result = cct_utils.parse_call_path_part("[com.example.feedservice.feedmaker]GetFeed")
        expected = {
            'service': 'com.example.feedservice.feedmaker',
            'operation_name': 'GetFeed'
        }
        self.assertEqual(result, expected)

    def test_invalid_format_no_brackets(self):
        """Test invalid format without brackets."""
        result = cct_utils.parse_call_path_part("feedservice GetStreamingFeedByID")
        self.assertEqual(result, {})

    def test_invalid_format_no_closing_bracket(self):
        """Test invalid format without closing bracket."""
        result = cct_utils.parse_call_path_part("[feedservice GetStreamingFeedByID")
        self.assertEqual(result, {})

    def test_empty_string(self):
        """Test parsing empty string."""
        result = cct_utils.parse_call_path_part("")
        self.assertEqual(result, {})

    def test_only_brackets(self):
        """Test parsing only brackets."""
        result = cct_utils.parse_call_path_part("[]")
        expected = {
            'service': '',
            'operation_name': ''
        }
        self.assertEqual(result, expected)


class TestParseCCTLine(unittest.TestCase):
    """Test parse_cct_line function."""

    def test_valid_single_service_line(self):
        """Test parsing valid line with single service."""
        line = "[feedservice]GetStreamingFeedByID 1500 <<100>>"
        result = cct_utils.parse_cct_line(line)
        expected = {
            'call_path': [{'service': 'feedservice', 'operation_name': 'GetStreamingFeedByID'}],
            'duration': 1500,
            'frequency': 100
        }
        self.assertEqual(result, expected)

    def test_valid_multi_service_line(self):
        """Test parsing valid line with multiple services."""
        line = "[service1]operation1;[service2]operation2;[service3]operation3 2000 <<50>>"
        result = cct_utils.parse_cct_line(line)
        expected = {
            'call_path': [
                {'service': 'service1', 'operation_name': 'operation1'},
                {'service': 'service2', 'operation_name': 'operation2'},
                {'service': 'service3', 'operation_name': 'operation3'}
            ],
            'duration': 2000,
            'frequency': 50
        }
        self.assertEqual(result, expected)

    def test_complex_real_world_line(self):
        """Test parsing complex real-world CCT line."""
        line = "[com.example.feedservice.feedmaker]FeedService::GetStreamingFeedByID;[cache]GetFromCache;[db]QueryDatabase 750 <<25>>"
        result = cct_utils.parse_cct_line(line)
        expected = {
            'call_path': [
                {'service': 'com.example.feedservice.feedmaker', 'operation_name': 'FeedService::GetStreamingFeedByID'},
                {'service': 'cache', 'operation_name': 'GetFromCache'},
                {'service': 'db', 'operation_name': 'QueryDatabase'}
            ],
            'duration': 750,
            'frequency': 25
        }
        self.assertEqual(result, expected)

    def test_line_with_whitespace(self):
        """Test parsing line with extra whitespace."""
        line = "  [service]operation  3000 <<10>>  "
        result = cct_utils.parse_cct_line(line)
        expected = {
            'call_path': [{'service': 'service', 'operation_name': 'operation'}],
            'duration': 3000,
            'frequency': 10
        }
        self.assertEqual(result, expected)

    def test_invalid_timing_format(self):
        """Test invalid timing format."""
        line = "[service]operation invalid_timing"
        result = cct_utils.parse_cct_line(line)
        self.assertEqual(result, {})

    def test_missing_frequency(self):
        """Test missing frequency in timing."""
        line = "[service]operation 1500"
        result = cct_utils.parse_cct_line(line)
        self.assertEqual(result, {})

    def test_malformed_frequency(self):
        """Test malformed frequency brackets."""
        line = "[service]operation 1500 <100>"
        result = cct_utils.parse_cct_line(line)
        self.assertEqual(result, {})

    def test_empty_line(self):
        """Test parsing empty line."""
        result = cct_utils.parse_cct_line("")
        self.assertEqual(result, {})

    def test_line_with_invalid_service_parts(self):
        """Test line with some invalid service parts."""
        line = "[valid]op1;invalid_part;[valid2]op2 1000 <<5>>"
        result = cct_utils.parse_cct_line(line)
        expected = {
            'call_path': [
                {'service': 'valid', 'operation_name': 'op1'},
                {'service': 'valid2', 'operation_name': 'op2'}
            ],
            'duration': 1000,
            'frequency': 5
        }
        self.assertEqual(result, expected)

    def test_no_valid_call_path_parts(self):
        """Test line with no valid call path parts."""
        line = "invalid;invalid2;invalid3 1000 <<5>>"
        result = cct_utils.parse_cct_line(line)
        self.assertEqual(result, {})


class TestParseCCTFile(unittest.TestCase):
    """Test parse_cct_file function."""

    def test_valid_file_parsing(self):
        """Test parsing valid CCT file."""
        file_content = """[service1]operation1 1000 <<10>>
[service2]operation2;[service3]operation3 2000 <<20>>

[service4]operation4 500 <<5>>"""

        with patch("builtins.open", mock_open(read_data=file_content)):
            result = cct_utils.parse_cct_file("test.cct")

        expected = [
            {
                'call_path': [{'service': 'service1', 'operation_name': 'operation1'}],
                'duration': 1000,
                'frequency': 10
            },
            {
                'call_path': [
                    {'service': 'service2', 'operation_name': 'operation2'},
                    {'service': 'service3', 'operation_name': 'operation3'}
                ],
                'duration': 2000,
                'frequency': 20
            },
            {
                'call_path': [{'service': 'service4', 'operation_name': 'operation4'}],
                'duration': 500,
                'frequency': 5
            }
        ]
        self.assertEqual(result, expected)

    def test_file_with_invalid_lines(self):
        """Test parsing file with some invalid lines."""
        file_content = """[valid]operation 1000 <<10>>
invalid line
[another_valid]op2 500 <<5>>
"""

        with patch("builtins.open", mock_open(read_data=file_content)):
            result = cct_utils.parse_cct_file("test.cct")

        expected = [
            {
                'call_path': [{'service': 'valid', 'operation_name': 'operation'}],
                'duration': 1000,
                'frequency': 10
            },
            {
                'call_path': [{'service': 'another_valid', 'operation_name': 'op2'}],
                'duration': 500,
                'frequency': 5
            }
        ]
        self.assertEqual(result, expected)

    def test_empty_file(self):
        """Test parsing empty file."""
        with patch("builtins.open", mock_open(read_data="")):
            result = cct_utils.parse_cct_file("empty.cct")
        self.assertEqual(result, [])

    def test_file_read_error(self):
        """Test file read error handling."""
        with patch("builtins.open", side_effect=FileNotFoundError("File not found")):
            with patch("crisp.cct_utils.logger") as mock_logger:
                result = cct_utils.parse_cct_file("nonexistent.cct")
                mock_logger.error.assert_called_once()
                self.assertEqual(result, [])


@pytest.mark.parametrize("label,excl,incl,freq,expected", [
    ("svc op", 100, 500, 10, "svc op\\nincl: 500\u00b5s\\nexcl: 100\u00b5s\\nfreq: 10"),
    ("svc op", 0, 500, 10, "svc op\\nincl: 500\u00b5s\\nfreq: 10"),
    ("svc op", 100, 0, 10, "svc op\\nexcl: 100\u00b5s\\nfreq: 10"),
    ("svc op", 100, 500, 0, "svc op\\nincl: 500\u00b5s\\nexcl: 100\u00b5s"),
    ("svc op", 0, 0, 0, "svc op"),
    ('has"quote', 1, 1, 1, 'has\\"quote\\nincl: 1\u00b5s\\nexcl: 1\u00b5s\\nfreq: 1'),
    ("has\\back", 1, 1, 1, "has\\\\back\\nincl: 1\u00b5s\\nexcl: 1\u00b5s\\nfreq: 1"),
])
def test_make_node_label(label, excl, incl, freq, expected):
    result = cct_utils._make_node_label(label, excl, incl, freq)
    assert result == expected


def test_cct_to_dot_empty_summaries():
    result = cct_utils.cct_to_dot([])
    assert result == "digraph CCT {\n}\n"


def test_cct_to_dot_single_leaf():
    summaries = [
        {
            'call_path': [{'service': 'svcA', 'operation_name': 'opA'}],
            'duration': 1000,
            'frequency': 5,
        }
    ]
    dot = cct_utils.cct_to_dot(summaries)
    assert dot.startswith("digraph CCT {")
    assert "[svcA] opA" in dot
    assert "incl: 1000" in dot
    assert "excl: 1000" in dot
    assert "freq: 5" in dot
    assert "->" not in dot


def test_cct_to_dot_simple_chain():
    summaries = [
        {
            'call_path': [
                {'service': 'svcA', 'operation_name': 'opA'},
                {'service': 'svcB', 'operation_name': 'opB'},
            ],
            'duration': 200,
            'frequency': 3,
        }
    ]
    dot = cct_utils.cct_to_dot(summaries)
    assert "n0 -> n1;" in dot
    assert "[svcA] opA" in dot
    assert "[svcB] opB" in dot


def test_cct_to_dot_branching_tree():
    summaries = [
        {
            'call_path': [
                {'service': 'root', 'operation_name': 'main'},
                {'service': 'svcA', 'operation_name': 'opA'},
            ],
            'duration': 100,
            'frequency': 1,
        },
        {
            'call_path': [
                {'service': 'root', 'operation_name': 'main'},
                {'service': 'svcB', 'operation_name': 'opB'},
            ],
            'duration': 200,
            'frequency': 2,
        },
    ]
    dot = cct_utils.cct_to_dot(summaries)

    assert dot.count("->") == 2
    assert "[root] main" in dot
    assert "[svcA] opA" in dot
    assert "[svcB] opB" in dot


def test_cct_to_dot_inclusive_time_propagation():
    """Inclusive time of a parent equals the sum of exclusive times in its subtree."""
    summaries = [
        {
            'call_path': [
                {'service': 'root', 'operation_name': 'main'},
                {'service': 'child', 'operation_name': 'work'},
            ],
            'duration': 300,
            'frequency': 1,
        },
        {
            'call_path': [
                {'service': 'root', 'operation_name': 'main'},
                {'service': 'child2', 'operation_name': 'io'},
            ],
            'duration': 700,
            'frequency': 1,
        },
    ]
    dot = cct_utils.cct_to_dot(summaries)

    assert "incl: 1000" in dot
    assert "incl: 300" in dot
    assert "incl: 700" in dot


def test_cct_to_dot_shared_prefix():
    """Two chains sharing a prefix should share the same intermediate node."""
    summaries = [
        {
            'call_path': [
                {'service': 'A', 'operation_name': 'a'},
                {'service': 'B', 'operation_name': 'b'},
                {'service': 'C', 'operation_name': 'c'},
            ],
            'duration': 50,
            'frequency': 1,
        },
        {
            'call_path': [
                {'service': 'A', 'operation_name': 'a'},
                {'service': 'B', 'operation_name': 'b'},
                {'service': 'D', 'operation_name': 'd'},
            ],
            'duration': 150,
            'frequency': 2,
        },
    ]
    dot = cct_utils.cct_to_dot(summaries)

    assert dot.count("[A] a") == 1
    assert dot.count("[B] b") == 1

    assert dot.count("->") == 3


def test_cct_to_dot_full_pipeline_with_file():
    """Integration: parse a CCT file then produce DOT output."""
    cct_content = (
        "[gateway]handleRequest;[auth]validate;[db]lookup 500 <<10>>\n"
        "[gateway]handleRequest;[auth]validate 100 <<10>>\n"
        "[gateway]handleRequest;[cache]get 300 <<8>>\n"
    )

    with tempfile.NamedTemporaryFile(mode='w', suffix='.cct', delete=False) as f:
        f.write(cct_content)
        temp_file = f.name

    try:
        summaries = cct_utils.parse_cct_file(temp_file)
        dot = cct_utils.cct_to_dot(summaries)

        assert dot.startswith("digraph CCT {")
        assert dot.strip().endswith("}")
        assert "[gateway] handleRequest" in dot
        assert "[auth] validate" in dot
        assert "[db] lookup" in dot
        assert "[cache] get" in dot

        assert "incl: 900" in dot
        assert "incl: 600" in dot
    finally:
        os.unlink(temp_file)


if __name__ == '__main__':
    unittest.main()
