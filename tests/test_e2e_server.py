"""End-to-end integration tests for the CRISP HTTP service.

These tests start a real in-process FastAPI server using httpx's AsyncClient
and send actual Jaeger trace fixtures through the streaming wire protocol,
asserting that the response contains valid protobuf analysis results.

The tests run against the fixtures already used by test_e2e.py (test_cases/).
They do NOT mock processReal — a full analysis pipeline runs for each fixture.
"""
import json
import os

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from crisp.proto import analyzer_pb2
from crisp.server import app
from crisp.service.trace_processor import encode_message

# Fixtures live under test_cases/ relative to the repo root.
_FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "..", "test_cases")

# Use the simplest single-trace fixtures (1.json and 2.json) so the e2e
# test stays fast and deterministic.
_SIMPLE_FIXTURES = ["1.json", "2.json"]


# ── helpers ───────────────────────────────────────────────────────────────────


def _build_stream(service: str, operation: str, trace_jsons: list[bytes]) -> bytes:
    """Encode a complete length-prefixed AnalyzeRequest stream."""
    # First message: metadata
    meta_req = analyzer_pb2.AnalyzeRequest()
    meta_req.metadata.service_name = service
    meta_req.metadata.operation_name = operation
    stream = encode_message(meta_req)

    # Subsequent messages: one per trace
    for tj in trace_jsons:
        data_req = analyzer_pb2.AnalyzeRequest()
        data_req.traces.trace_json = tj
        stream += encode_message(data_req)

    return stream


def _load_fixture(name: str) -> bytes:
    path = os.path.join(_FIXTURES_DIR, name)
    with open(path, "rb") as f:
        return f.read()


# ── fixtures ──────────────────────────────────────────────────────────────────


@pytest_asyncio.fixture
async def ac():
    """Async HTTP client wired to the FastAPI app."""
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client


# ── health ────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_health(ac):
    r = await ac.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "OK"


# ── /v2/trace/analysis/stream ─────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_stream_single_fixture(ac):
    """Send one real trace fixture and verify the protobuf response is valid."""
    trace_json = _load_fixture("1.json")
    stream_body = _build_stream("S1", "O1", [trace_json])

    r = await ac.post(
        "/v2/trace/analysis/stream",
        content=stream_body,
        headers={"Content-Type": "application/octet-stream"},
    )
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/octet-stream"

    response = analyzer_pb2.AnalyzeResponse()
    response.ParseFromString(r.content)

    # At least one call chain should have been extracted.
    assert len(response.report_window_1) > 0, (
        "Expected at least one call chain summary in the response"
    )
    # Verify structure: each entry has a non-empty call path and positive stats.
    for chain in response.report_window_1:
        assert len(chain.call_path) > 0
        assert chain.base.frequency > 0


@pytest.mark.asyncio
@pytest.mark.parametrize("fixture_name", _SIMPLE_FIXTURES)
async def test_stream_multiple_fixtures(ac, fixture_name):
    """Each simple fixture independently produces a non-empty response."""
    trace_json = _load_fixture(fixture_name)
    stream_body = _build_stream("S1", "O1", [trace_json])

    r = await ac.post(
        "/v2/trace/analysis/stream",
        content=stream_body,
        headers={"Content-Type": "application/octet-stream"},
    )
    assert r.status_code == 200
    response = analyzer_pb2.AnalyzeResponse()
    response.ParseFromString(r.content)
    assert len(response.report_window_1) > 0, f"Empty response for {fixture_name}"


@pytest.mark.asyncio
async def test_stream_no_data_returns_empty(ac):
    """An empty stream body should return an empty (but valid) AnalyzeResponse."""
    r = await ac.post(
        "/v2/trace/analysis/stream",
        content=b"",
        headers={"Content-Type": "application/octet-stream"},
    )
    assert r.status_code == 200
    response = analyzer_pb2.AnalyzeResponse()
    response.ParseFromString(r.content)
    # No crash, empty result
    assert len(response.report_window_1) == 0


# ── /v2/trace/analysis/compare ───────────────────────────────────────────────


@pytest.mark.asyncio
async def test_compare_two_windows(ac):
    """Send fixture 1 as BASELINE and fixture 2 as COMPARISON."""
    trace_w1 = _load_fixture("1.json")
    trace_w2 = _load_fixture("2.json")

    # Build a single stream: metadata first, then BASELINE trace, then COMPARISON trace.
    meta_req = analyzer_pb2.AnalyzeRequest()
    meta_req.metadata.service_name = "S1"
    meta_req.metadata.operation_name = "O1"
    stream = encode_message(meta_req)

    for tj, ctype in [
        (trace_w1, analyzer_pb2.BASELINE),
        (trace_w2, analyzer_pb2.COMPARISON),
    ]:
        req = analyzer_pb2.AnalyzeRequest()
        req.traces.trace_json = tj
        req.traces.type = ctype
        stream += encode_message(req)

    r = await ac.post(
        "/v2/trace/analysis/compare",
        content=stream,
        headers={"Content-Type": "application/octet-stream"},
    )
    assert r.status_code == 200
    response = analyzer_pb2.AnalyzeResponse()
    response.ParseFromString(r.content)

    # Both windows should be populated.
    assert len(response.report_window_1) > 0, "window 1 is empty"
    assert len(response.report_window_2) > 0, "window 2 is empty"
    # Diff should be computed (may be empty if both windows have identical traces).
    # We just verify the response is well-formed.
    for chain in response.report_window_diff:
        assert len(chain.call_path) > 0
