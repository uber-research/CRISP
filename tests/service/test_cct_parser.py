"""Unit tests for crisp/service/cct_parser.py."""
import os
import tempfile

import aiofiles
import pytest

from crisp.proto import analyzer_pb2
from crisp.service.cct_parser import (
    create_protobuf_response,
    parse_call_path_part,
    parse_cct_file,
    parse_cct_line,
    process_cct,
)


# ── parse_call_path_part ──────────────────────────────────────────────────────


def test_parse_call_path_part_with_space():
    r = parse_call_path_part("[svc1] op1")
    assert r == {"service": "svc1", "operation_name": "op1"}


def test_parse_call_path_part_without_space():
    r = parse_call_path_part("[svc2]op2")
    assert r == {"service": "svc2", "operation_name": "op2"}


def test_parse_call_path_part_no_brackets():
    assert parse_call_path_part("svc op") == {}


def test_parse_call_path_part_incomplete_bracket():
    assert parse_call_path_part("[svc op") == {}


def test_parse_call_path_part_empty():
    assert parse_call_path_part("") == {}


# ── parse_cct_line ────────────────────────────────────────────────────────────


def test_parse_cct_line_valid_multi_segment():
    line = "[svc1] op1;[svc2] op2;[svc3] op3 100 <<5>>"
    r = parse_cct_line(line)
    assert r["frequency"] == 5
    assert r["duration"] == 100
    assert len(r["call_path"]) == 3
    assert r["call_path"][0] == {"service": "svc1", "operation_name": "op1"}
    assert r["call_path"][2] == {"service": "svc3", "operation_name": "op3"}


def test_parse_cct_line_single_segment():
    r = parse_cct_line("[svc1] op1 42 <<1>>")
    assert r["duration"] == 42
    assert r["frequency"] == 1
    assert len(r["call_path"]) == 1


def test_parse_cct_line_empty():
    assert parse_cct_line("") == {}


def test_parse_cct_line_no_freq():
    assert parse_cct_line("[svc1] op1;[svc2] op2") == {}


def test_parse_cct_line_invalid_freq_format():
    assert parse_cct_line("[svc1] op1 100") == {}


def test_parse_cct_line_invalid_call_path():
    assert parse_cct_line("svc1 op1 100 <<5>>") == {}


# ── parse_cct_file ────────────────────────────────────────────────────────────


def test_parse_cct_file_valid():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".cct", delete=False) as f:
        f.write("[svc1] op1;[svc2] op2 100 <<5>>\n")
        f.write("[svc3] op3 200 <<3>>\n")
        f.write("invalid line\n")
        path = f.name
    try:
        results = parse_cct_file(path)
        assert len(results) == 2
        assert results[0]["duration"] == 100
        assert results[0]["frequency"] == 5
        assert results[1]["duration"] == 200
    finally:
        os.unlink(path)


def test_parse_cct_file_not_found():
    assert parse_cct_file("/nonexistent/path.cct") == []


# ── create_protobuf_response ──────────────────────────────────────────────────


def test_create_protobuf_response_basic():
    summaries = [
        {
            "call_path": [
                {"service": "svc1", "operation_name": "op1"},
                {"service": "svc2", "operation_name": "op2"},
            ],
            "duration": 100_000,
            "frequency": 5,
        },
        {
            "call_path": [{"service": "svc3", "operation_name": "op3"}],
            "duration": 200_000,
            "frequency": 3,
        },
    ]
    response = create_protobuf_response(summaries)
    assert len(response.report_window_1) == 2
    assert response.report_window_1[0].call_path[0].service == "svc1"
    assert response.report_window_1[0].base.duration.ToMicroseconds() == 100_000
    assert response.report_window_1[0].base.frequency == 5
    assert response.report_window_1[1].call_path[0].service == "svc3"
    assert response.report_window_1[1].base.duration.ToMicroseconds() == 200_000


def test_create_protobuf_response_empty():
    response = create_protobuf_response([])
    assert len(response.report_window_1) == 0


# ── process_cct (async) ───────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_process_cct_valid():
    with tempfile.TemporaryDirectory() as tmpdir:
        cct_path = os.path.join(tmpdir, "flame-graph-P100.cct")
        async with aiofiles.open(cct_path, "w") as f:
            await f.write("[svc1] op1;[svc2] op2 100 <<5>>\n")
            await f.write("[svc3] op3 200 <<3>>\n")
            await f.write("invalid line\n")

        response = await process_cct(tmpdir)
        assert isinstance(response, analyzer_pb2.AnalyzeResponse)
        assert len(response.report_window_1) == 2
        assert response.report_window_1[0].base.duration.ToMicroseconds() == 100
        assert response.report_window_1[0].base.frequency == 5


@pytest.mark.asyncio
async def test_process_cct_missing_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        response = await process_cct(tmpdir)
        assert len(response.report_window_1) == 0


@pytest.mark.asyncio
async def test_process_cct_empty_file():
    with tempfile.TemporaryDirectory() as tmpdir:
        cct_path = os.path.join(tmpdir, "flame-graph-P100.cct")
        async with aiofiles.open(cct_path, "w") as f:
            await f.write("")
        response = await process_cct(tmpdir)
        assert len(response.report_window_1) == 0
