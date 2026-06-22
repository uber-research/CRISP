"""Unit tests for crisp/service/streaming.py (mocked dependencies)."""
import json
import os
import tempfile
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from crisp.proto import analyzer_pb2
from crisp.service.streaming import (
    _validate_processor_state,
    analyze_streaming_traces,
)
from crisp.service.trace_processor import TraceStreamProcessor


# ── _validate_processor_state ─────────────────────────────────────────────────


def _processor(service="svc", operation="op", total_traces=5):
    p = TraceStreamProcessor()
    meta = analyzer_pb2.StreamMetadata()
    meta.service_name = service
    meta.operation_name = operation
    p.metadata = meta
    p.stats.total_traces = total_traces
    return p


def test_validate_state_valid():
    assert _validate_processor_state(_processor()) is True


def test_validate_state_no_metadata():
    p = TraceStreamProcessor()
    assert _validate_processor_state(p) is False


def test_validate_state_missing_service():
    assert _validate_processor_state(_processor(service="")) is False


def test_validate_state_missing_operation():
    assert _validate_processor_state(_processor(operation="")) is False


def test_validate_state_no_traces():
    assert _validate_processor_state(_processor(total_traces=0)) is False


# ── analyze_streaming_traces ──────────────────────────────────────────────────


@patch("crisp.service.streaming.processReal")
@patch("crisp.service.streaming.create_analysis_config")
def test_analyze_streaming_traces_ok(mock_cfg, mock_process, tmp_path):
    (tmp_path / "t1.json").write_text("{}")
    fake_cfg = MagicMock()
    fake_cfg.inputDir = str(tmp_path)
    fake_cfg.tracesDir = str(tmp_path)
    fake_cfg.output = str(tmp_path)
    mock_cfg.return_value = fake_cfg

    out = analyze_streaming_traces("svc", "op", str(tmp_path), request_id="req-1")
    assert os.path.isdir(out)
    mock_process.assert_called_once_with(fake_cfg)


@patch("crisp.service.streaming.processReal")
def test_analyze_streaming_traces_no_json_raises(mock_process, tmp_path):
    with pytest.raises(ValueError, match="No JSON trace files"):
        analyze_streaming_traces("svc", "op", str(tmp_path))
    mock_process.assert_not_called()


@patch("crisp.service.streaming.processReal", side_effect=RuntimeError("boom"))
@patch("crisp.service.streaming.create_analysis_config")
def test_analyze_streaming_traces_process_error(mock_cfg, mock_process, tmp_path):
    (tmp_path / "t1.json").write_text("{}")
    fake_cfg = MagicMock()
    fake_cfg.inputDir = str(tmp_path)
    fake_cfg.tracesDir = str(tmp_path)
    fake_cfg.output = str(tmp_path)
    mock_cfg.return_value = fake_cfg

    with pytest.raises(RuntimeError, match="Failed to process traces"):
        analyze_streaming_traces("svc", "op", str(tmp_path))


# ── process_trace_stream (async, mocked) ─────────────────────────────────────


async def _fake_stream(chunks):
    for c in chunks:
        yield c


@pytest.mark.asyncio
@patch("crisp.service.streaming.process_cct", new_callable=AsyncMock)
@patch("crisp.service.streaming._run_trace_analysis")
@patch("crisp.service.streaming.process_trace_chunks", new_callable=AsyncMock)
async def test_process_trace_stream_ok(
    mock_chunks, mock_run, mock_cct
):
    from crisp.service.streaming import process_trace_stream

    mock_run.return_value = "/fake/output"
    expected = analyzer_pb2.AnalyzeResponse()
    chain = expected.report_window_1.add()
    chain.base.frequency = 7
    mock_cct.return_value = expected

    # Set up a processor with valid state after chunks are processed.
    def _set_processor(req, proc, tmpdir):
        meta = analyzer_pb2.StreamMetadata()
        meta.service_name = "svc"
        meta.operation_name = "op"
        proc.metadata = meta
        proc.stats.total_traces = 3

    mock_chunks.side_effect = _set_processor

    fake_req = MagicMock()
    result = await process_trace_stream(fake_req)
    assert len(result.report_window_1) == 1
    assert result.report_window_1[0].base.frequency == 7


@pytest.mark.asyncio
@patch("crisp.service.streaming.process_trace_chunks", new_callable=AsyncMock)
async def test_process_trace_stream_no_metadata_raises(mock_chunks):
    from crisp.service.streaming import process_trace_stream

    # processor remains empty — no metadata set
    fake_req = MagicMock()
    with pytest.raises(ValueError, match="No metadata"):
        await process_trace_stream(fake_req)


@pytest.mark.asyncio
@patch("crisp.service.streaming.process_trace_chunks", new_callable=AsyncMock)
async def test_process_trace_stream_no_traces_raises(mock_chunks):
    from crisp.service.streaming import process_trace_stream

    def _set_meta(req, proc, tmpdir):
        meta = analyzer_pb2.StreamMetadata()
        meta.service_name = "svc"
        meta.operation_name = "op"
        proc.metadata = meta
        proc.stats.total_traces = 0

    mock_chunks.side_effect = _set_meta
    fake_req = MagicMock()
    with pytest.raises(ValueError, match="No traces"):
        await process_trace_stream(fake_req)
