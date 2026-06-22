"""Unit tests for crisp/server.py routes (mocked streaming layer)."""
from unittest.mock import AsyncMock, patch

import pytest
from fastapi.testclient import TestClient

from crisp.proto import analyzer_pb2
from crisp.server import app

client = TestClient(app, raise_server_exceptions=False)


# ── /health ───────────────────────────────────────────────────────────────────


def test_health_ok():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "OK"}


# ── /v2/trace/analysis/stream ─────────────────────────────────────────────────


@patch(
    "crisp.service.streaming.process_trace_stream",
    new_callable=AsyncMock,
)
def test_stream_returns_protobuf_bytes(mock_stream):
    expected = analyzer_pb2.AnalyzeResponse()
    chain = expected.report_window_1.add()
    chain.base.frequency = 3
    mock_stream.return_value = expected

    r = client.post(
        "/v2/trace/analysis/stream",
        content=b"\x00",
        headers={"Content-Type": "application/octet-stream"},
    )
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/octet-stream"
    decoded = analyzer_pb2.AnalyzeResponse()
    decoded.ParseFromString(r.content)
    assert len(decoded.report_window_1) == 1
    assert decoded.report_window_1[0].base.frequency == 3


@patch(
    "crisp.service.streaming.process_trace_stream",
    new_callable=AsyncMock,
    side_effect=RuntimeError("boom"),
)
def test_stream_error_returns_empty_response(mock_stream):
    r = client.post(
        "/v2/trace/analysis/stream",
        content=b"\x00",
        headers={"Content-Type": "application/octet-stream"},
    )
    assert r.status_code == 200
    decoded = analyzer_pb2.AnalyzeResponse()
    decoded.ParseFromString(r.content)
    assert len(decoded.report_window_1) == 0


# ── /v2/trace/analysis/compare ───────────────────────────────────────────────


@patch(
    "crisp.service.streaming.process_trace_comparison",
    new_callable=AsyncMock,
)
def test_compare_returns_diff_response(mock_compare):
    expected = analyzer_pb2.AnalyzeResponse()
    d = expected.report_window_diff.add()
    d.diff.frequency = 2
    mock_compare.return_value = expected

    r = client.post(
        "/v2/trace/analysis/compare",
        content=b"\x00",
        headers={"Content-Type": "application/octet-stream"},
    )
    assert r.status_code == 200
    decoded = analyzer_pb2.AnalyzeResponse()
    decoded.ParseFromString(r.content)
    assert len(decoded.report_window_diff) == 1
    assert decoded.report_window_diff[0].diff.frequency == 2


@patch(
    "crisp.service.streaming.process_trace_comparison",
    new_callable=AsyncMock,
    side_effect=Exception("unexpected"),
)
def test_compare_error_returns_empty_response(mock_compare):
    r = client.post(
        "/v2/trace/analysis/compare",
        content=b"\x00",
        headers={"Content-Type": "application/octet-stream"},
    )
    assert r.status_code == 200
    decoded = analyzer_pb2.AnalyzeResponse()
    decoded.ParseFromString(r.content)
    assert len(decoded.report_window_diff) == 0
