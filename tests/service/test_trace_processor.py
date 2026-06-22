"""Unit tests for crisp/service/trace_processor.py."""
import json

import pytest

from crisp.proto import analyzer_pb2
from crisp.service.trace_processor import (
    TraceMessage,
    TraceStreamProcessor,
    _parse_varint,
    encode_message,
)


# ── _parse_varint ─────────────────────────────────────────────────────────────


def test_parse_varint_single_byte():
    assert _parse_varint(b"\x05") == (5, 1)


def test_parse_varint_multi_byte():
    # 300 encodes as 0xAC 0x02
    value, consumed = _parse_varint(b"\xac\x02")
    assert value == 300
    assert consumed == 2


def test_parse_varint_incomplete():
    assert _parse_varint(b"\x80") == (0, 0)


def test_parse_varint_empty():
    assert _parse_varint(b"") == (0, 0)


# ── encode_message / round-trip ───────────────────────────────────────────────


def _make_metadata_req(service="svc", operation="op"):
    req = analyzer_pb2.AnalyzeRequest()
    req.metadata.service_name = service
    req.metadata.operation_name = operation
    return req


def _make_trace_req(trace_json: bytes, ctype=analyzer_pb2.BASELINE):
    req = analyzer_pb2.AnalyzeRequest()
    req.traces.trace_json = trace_json
    req.traces.type = ctype
    return req


def test_encode_decode_metadata():
    req = _make_metadata_req("my-svc", "my.op")
    framed = encode_message(req)
    processor = TraceStreamProcessor()
    messages = list(processor.process_chunk(framed))
    assert len(messages) == 1
    assert messages[0].metadata is not None
    assert messages[0].metadata.service_name == "my-svc"
    assert messages[0].metadata.operation_name == "my.op"


def test_encode_decode_trace():
    trace = json.dumps({"data": [{"traceID": "abc123", "spans": []}]}).encode()
    req = _make_trace_req(trace)
    framed = encode_message(req)
    processor = TraceStreamProcessor()
    messages = list(processor.process_chunk(framed))
    assert len(messages) == 1
    assert messages[0].trace_json == trace


# ── TraceStreamProcessor ──────────────────────────────────────────────────────


def test_processor_multi_message():
    stream = b""
    stream += encode_message(_make_metadata_req("svc", "op"))
    trace = json.dumps({"data": [{"traceID": "t1", "spans": []}]}).encode()
    stream += encode_message(_make_trace_req(trace))
    stream += encode_message(_make_trace_req(trace))

    processor = TraceStreamProcessor()
    messages = list(processor.process_chunk(stream))
    assert len(messages) == 3
    assert messages[0].metadata is not None
    assert processor.metadata.service_name == "svc"
    assert processor.stats.messages_processed == 3


def test_processor_chunked_delivery():
    """Simulate the stream arriving in small arbitrary chunks."""
    trace = json.dumps({"data": [{"traceID": "t2", "spans": []}]}).encode()
    full_stream = encode_message(_make_metadata_req()) + encode_message(_make_trace_req(trace))

    processor = TraceStreamProcessor()
    messages = []
    for i in range(0, len(full_stream), 3):
        messages.extend(processor.process_chunk(full_stream[i : i + 3]))
    assert len(messages) == 2


def test_processor_stats():
    processor = TraceStreamProcessor()
    stream = encode_message(_make_metadata_req("s", "o"))
    list(processor.process_chunk(stream))
    assert processor.stats.total_chunks == 1
    assert processor.stats.messages_processed == 1


# ── TraceMessage.save_traces_to_json ─────────────────────────────────────────


def test_save_traces_to_json(tmp_path):
    trace_data = {"data": [{"traceID": "myid123", "spans": []}]}
    req = _make_trace_req(json.dumps(trace_data).encode())
    msg = TraceMessage(req, 1)
    saved = msg.save_traces_to_json(str(tmp_path))
    assert saved == 1
    files = list(tmp_path.glob("*.json"))
    assert len(files) == 1
    content = json.loads(files[0].read_text())
    assert content["data"][0]["traceID"] == "myid123"


def test_save_traces_to_json_metadata_message_saves_nothing(tmp_path):
    req = _make_metadata_req()
    msg = TraceMessage(req, 1)
    saved = msg.save_traces_to_json(str(tmp_path))
    assert saved == 0
    assert list(tmp_path.glob("*.json")) == []


def test_save_traces_to_json_bad_json(tmp_path):
    req = _make_trace_req(b"not-json")
    msg = TraceMessage(req, 1)
    saved = msg.save_traces_to_json(str(tmp_path))
    assert saved == 0
