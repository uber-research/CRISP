"""CCT file parsing and protobuf response construction."""
import logging
import os
import re
from typing import Any, Optional

import aiofiles

from crisp.proto import analyzer_pb2

logger = logging.getLogger(__name__)

_CCT_FILE_NAME = "flame-graph-P100.cct"

# CCT line format produced by crisp/flamegraph.py:
#   [svc1] op1;[svc2] op2 <duration_usec> <<freq>>
_FREQ_RE = re.compile(r"<<(\d+)>>$")


def parse_call_path_part(part: str) -> dict[str, str]:
    """Parse a single '[service] operation' segment.

    Returns a dict with 'service' and 'operation_name', or {} on failure.
    """
    match = re.match(r"^\[([^\]]+)\]\s*(.+)$", part.strip())
    if not match:
        return {}
    return {"service": match.group(1), "operation_name": match.group(2).strip()}


def parse_cct_line(line: str) -> dict[str, Any]:
    """Parse one CCT line into a summary dict.

    Expected format:
        [svc1] op1;[svc2] op2 <duration> <<freq>>

    Returns a dict with keys call_path, duration, frequency on success,
    or {} if the line cannot be parsed.
    """
    line = line.strip()
    if not line:
        return {}

    freq_match = _FREQ_RE.search(line)
    if not freq_match:
        return {}

    frequency = int(freq_match.group(1))
    rest = line[: freq_match.start()].strip()

    parts = rest.rsplit(" ", 1)
    if len(parts) != 2:
        return {}

    call_path_str, duration_str = parts
    try:
        duration = int(duration_str)
    except ValueError:
        return {}

    call_path = []
    for segment in call_path_str.split(";"):
        parsed = parse_call_path_part(segment)
        if not parsed:
            return {}
        call_path.append(parsed)

    if not call_path:
        return {}

    return {"call_path": call_path, "duration": duration, "frequency": frequency}


def parse_cct_file(cct_path: str) -> list[dict[str, Any]]:
    """Parse a CCT file synchronously and return call chain summaries."""
    summaries: list[dict[str, Any]] = []
    try:
        with open(cct_path) as f:
            for line in f:
                summary = parse_cct_line(line)
                if summary:
                    summaries.append(summary)
    except Exception as exc:
        logger.error("Error parsing CCT file %s: %s", cct_path, exc)
    return summaries


def create_protobuf_response(
    call_chain_summaries: list[dict[str, Any]],
) -> analyzer_pb2.AnalyzeResponse:
    """Build an AnalyzeResponse from a list of parsed CCT summaries."""
    response = analyzer_pb2.AnalyzeResponse()
    for summary in call_chain_summaries:
        chain = response.report_window_1.add()
        for path in summary["call_path"]:
            cp = chain.call_path.add()
            cp.service = path["service"]
            cp.operation_name = path["operation_name"]
        chain.base.duration.FromMicroseconds(summary["duration"])
        chain.base.frequency = summary["frequency"]

    if not response.IsInitialized():
        logger.error("Failed to initialise protobuf AnalyzeResponse")
        return analyzer_pb2.AnalyzeResponse()
    return response


async def process_cct(output_dir: str) -> analyzer_pb2.AnalyzeResponse:
    """Async: read the P100 CCT file from output_dir and return an AnalyzeResponse."""
    cct_path = os.path.join(output_dir, _CCT_FILE_NAME)
    if not os.path.exists(cct_path):
        logger.warning("CCT file not found at: %s", cct_path)
        return analyzer_pb2.AnalyzeResponse()

    try:
        async with aiofiles.open(cct_path) as f:
            content = await f.read()
    except Exception as exc:
        logger.error("Error parsing CCT file %s: %s", cct_path, exc)
        return analyzer_pb2.AnalyzeResponse()

    summaries = []
    for line in content.splitlines():
        summary = parse_cct_line(line)
        if summary:
            summaries.append(summary)

    return create_protobuf_response(summaries)
