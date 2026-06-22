"""Length-prefixed protobuf stream parser for AnalyzeRequest messages."""
import io
import json
import logging
import os
from collections.abc import Iterator
from dataclasses import dataclass, field
from typing import Optional

from crisp.proto import analyzer_pb2

logger = logging.getLogger(__name__)


@dataclass
class ProcessingStats:
    total_chunks: int = 0
    total_bytes: int = 0
    messages_processed: int = 0
    traces_saved: int = 0
    errors: int = 0


class TraceMessage:
    """One decoded AnalyzeRequest from the stream."""

    def __init__(self, message: analyzer_pb2.AnalyzeRequest, message_number: int):
        self.message_number = message_number
        self.metadata: Optional[analyzer_pb2.StreamMetadata] = None
        self.trace_json: Optional[bytes] = None
        self.comparison_type: analyzer_pb2.ComparisonType = (
            analyzer_pb2.COMPARISON_TYPE_UNSPECIFIED
        )

        which = message.WhichOneof("data")
        if which == "metadata":
            self.metadata = message.metadata
            logger.debug(
                "Message %d: StreamMetadata service=%s operation=%s",
                message_number,
                message.metadata.service_name,
                message.metadata.operation_name,
            )
        elif which == "traces":
            self.trace_json = message.traces.trace_json
            self.comparison_type = message.traces.type
            logger.debug(
                "Message %d: AnalysisData %d bytes type=%s",
                message_number,
                len(self.trace_json),
                self.comparison_type,
            )

    def save_traces_to_json(self, output_dir: str) -> int:
        """Write the trace JSON to output_dir as a .json file.

        Returns 1 on success, 0 if this message has no trace payload.
        """
        if not self.trace_json:
            return 0
        try:
            data = json.loads(self.trace_json)
            # Extract trace ID from first span of first trace for a stable filename.
            trace_id = (
                data.get("data", [{}])[0].get("traceID", "")
                or f"trace_{self.message_number}"
            )
            out_path = os.path.join(output_dir, f"{trace_id}.json")
            with open(out_path, "w") as f:
                json.dump(data, f)
            return 1
        except Exception as exc:
            logger.error(
                "Failed to save trace from message %d: %s", self.message_number, exc
            )
            return 0

    def log_summary(self) -> None:
        if self.metadata:
            logger.debug(
                "Message %d: metadata service=%s operation=%s",
                self.message_number,
                self.metadata.service_name,
                self.metadata.operation_name,
            )
        elif self.trace_json is not None:
            logger.debug(
                "Message %d: trace_json %d bytes", self.message_number, len(self.trace_json)
            )


class TraceStreamProcessor:
    """Stateful parser for a length-prefixed AnalyzeRequest stream.

    Wire format per message:
        [varint: serialised byte-length][serialised AnalyzeRequest bytes]
    """

    def __init__(self) -> None:
        self.stats = ProcessingStats()
        self.metadata: Optional[analyzer_pb2.StreamMetadata] = None
        self._buf = bytearray()
        self._message_count = 0

    # ── public ────────────────────────────────────────────────────────────────

    def process_chunk(self, chunk: bytes) -> Iterator[TraceMessage]:
        """Feed raw bytes; yield zero or more decoded TraceMessages."""
        self._buf.extend(chunk)
        self.stats.total_chunks += 1
        self.stats.total_bytes += len(chunk)
        yield from self._drain()

    def log_progress(self) -> None:
        logger.debug(
            "Stream progress: chunks=%d bytes=%d messages=%d traces=%d",
            self.stats.total_chunks,
            self.stats.total_bytes,
            self.stats.messages_processed,
            self.stats.traces_saved,
        )

    def log_summary(self) -> None:
        logger.info(
            "Stream done: chunks=%d bytes=%d messages=%d traces=%d errors=%d",
            self.stats.total_chunks,
            self.stats.total_bytes,
            self.stats.messages_processed,
            self.stats.traces_saved,
            self.stats.errors,
        )

    # ── internals ─────────────────────────────────────────────────────────────

    def _drain(self) -> Iterator[TraceMessage]:
        """Yield complete messages from the internal buffer."""
        buf = memoryview(self._buf)
        pos = 0

        while pos < len(buf):
            # Parse the leading varint (message byte-length).
            msg_len, varint_bytes = _parse_varint(bytes(buf[pos:]))
            if varint_bytes == 0:
                break  # not enough bytes for a complete varint yet
            pos += varint_bytes

            if pos + msg_len > len(buf):
                pos -= varint_bytes  # rewind — incomplete message
                break

            raw = bytes(buf[pos : pos + msg_len])
            pos += msg_len

            try:
                req = analyzer_pb2.AnalyzeRequest()
                req.ParseFromString(raw)
                self._message_count += 1
                self.stats.messages_processed += 1

                msg = TraceMessage(req, self._message_count)
                if msg.metadata:
                    self.metadata = msg.metadata
                    # metadata messages don't increment trace count
                else:
                    self.stats.total_traces = getattr(self.stats, "total_traces", 0) + 1

                yield msg
            except Exception as exc:
                logger.error("Failed to parse message at offset %d: %s", pos - msg_len, exc)
                self.stats.errors += 1

        # Keep only unconsumed bytes.
        self._buf = bytearray(buf[pos:])


def _parse_varint(data: bytes) -> tuple[int, int]:
    """Decode a base-128 varint from the start of data.

    Returns (value, bytes_consumed). bytes_consumed == 0 means not enough data.
    """
    result = 0
    for i, byte in enumerate(data):
        result |= (byte & 0x7F) << (7 * i)
        if not (byte & 0x80):
            return result, i + 1
    return 0, 0  # incomplete varint


def encode_message(msg: "analyzer_pb2.AnalyzeRequest") -> bytes:
    """Serialise msg with a varint length prefix (for use in tests/clients)."""
    raw = msg.SerializeToString()
    length = len(raw)
    varint = bytearray()
    while True:
        bits = length & 0x7F
        length >>= 7
        if length:
            varint.append(bits | 0x80)
        else:
            varint.append(bits)
            break
    return bytes(varint) + raw
