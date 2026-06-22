"""Streaming analysis logic: decode → analyse → return AnalyzeResponse."""
import logging
import os
import tempfile
import traceback
import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import Request

from crisp.proto import analyzer_pb2
from crisp.process_trace import processReal
from crisp.service.cct_parser import process_cct
from crisp.service.data_converter import create_analysis_config, process_trace_chunks
from crisp.service.file_io import (
    count_json_files,
    create_output_directory,
    ensure_absolute_paths,
    validate_trace_analysis_inputs,
)
from crisp.service.trace_processor import TraceStreamProcessor

logger = logging.getLogger(__name__)


# ── validation ────────────────────────────────────────────────────────────────


def _validate_processor_state(processor: TraceStreamProcessor) -> bool:
    if not processor.metadata:
        logger.error("No metadata in processor state")
        return False
    if not processor.metadata.service_name or not processor.metadata.operation_name:
        logger.error("metadata missing service_name or operation_name")
        return False
    total = getattr(processor.stats, "total_traces", 0)
    if total == 0:
        logger.error("No traces received from stream")
        return False
    return True


def _extract_metadata(
    processor: TraceStreamProcessor,
) -> tuple[Optional[str], Optional[str]]:
    if not processor.metadata:
        return None, None
    return processor.metadata.service_name, processor.metadata.operation_name


# ── core analysis helpers ─────────────────────────────────────────────────────


def analyze_streaming_traces(
    service_name: str,
    method_name: str,
    traces_dir: str,
    request_id: str = "",
    compute_parallelism: int = 4,
    io_parallelism: int = 1,
) -> str:
    """Run the CRISP pipeline on traces already saved under traces_dir.

    Returns the path to the output directory.
    """
    validate_trace_analysis_inputs(service_name, method_name, traces_dir)

    if not request_id:
        request_id = str(uuid.uuid4())
        logger.info("Generated new request ID: %s", request_id)

    output_dir = create_output_directory(
        traces_dir=traces_dir,
        request_id=request_id,
        service_name=service_name,
        method_name=method_name,
    )

    num_traces = count_json_files(traces_dir)
    if num_traces == 0:
        raise ValueError(f"No JSON trace files found in directory: {traces_dir}")

    config = create_analysis_config(
        service_name=service_name,
        method_name=method_name,
        traces_dir=traces_dir,
        output_dir=output_dir,
        num_traces=num_traces,
        compute_parallelism=compute_parallelism,
        io_parallelism=io_parallelism,
        request_id=request_id,
    )
    ensure_absolute_paths(config)

    try:
        processReal(config)
    except Exception as exc:
        logger.error("processReal failed: %s\n%s", exc, traceback.format_exc())
        raise RuntimeError(f"Failed to process traces: {exc}") from exc

    return output_dir


def _run_trace_analysis(
    temp_dir: str,
    request_id: str,
    processor: TraceStreamProcessor,
) -> str:
    if not processor.metadata:
        raise ValueError("No metadata available in processor")
    service_name = processor.metadata.service_name
    method_name = processor.metadata.operation_name
    if not service_name or not method_name:
        raise ValueError("Missing service_name or method_name in metadata")

    output_dir = analyze_streaming_traces(
        service_name=service_name,
        method_name=method_name,
        traces_dir=temp_dir,
        request_id=request_id,
    )
    logger.info("Analysis complete; results at %s", output_dir)
    return output_dir


# ── HTTP endpoint helpers ─────────────────────────────────────────────────────


async def process_trace_stream(request: Request) -> analyzer_pb2.AnalyzeResponse:
    """Handle the /v2/trace/analysis/stream endpoint."""
    processor = TraceStreamProcessor()

    with tempfile.TemporaryDirectory() as temp_dir:
        try:
            await process_trace_chunks(request, processor, temp_dir)
        except Exception as exc:
            raise RuntimeError(f"Failed to process trace chunks: {exc}") from exc

        if not _validate_processor_state(processor):
            service = getattr(processor.metadata, "service_name", None) or "?"
            op = getattr(processor.metadata, "operation_name", None) or "?"
            total = getattr(processor.stats, "total_traces", 0)
            if not processor.metadata:
                raise ValueError("No metadata available in processor")
            if not service or not op:
                raise ValueError("Missing service_name or method_name in metadata")
            if total == 0:
                raise ValueError("No traces received from stream")

        request_id = f"stream_{int(datetime.now(timezone.utc).timestamp())}"

        try:
            output_dir = _run_trace_analysis(temp_dir, request_id, processor)
        except Exception as exc:
            raise RuntimeError(f"Failed to run trace analysis: {exc}") from exc

        try:
            return await process_cct(output_dir)
        except Exception as exc:
            raise RuntimeError(f"Failed to process CCT: {exc}") from exc


async def _run_window_analysis(
    window_dir: str,
    request_id: str,
    processor: TraceStreamProcessor,
) -> Optional[str]:
    try:
        return _run_trace_analysis(window_dir, request_id, processor)
    except Exception as exc:
        logger.error("Window analysis failed for %s: %s", request_id, exc)
        return None


def _get_call_path_key(call_path) -> str:
    return "->".join(f"{p.service}:{p.operation_name}" for p in call_path)


def _create_combined_response(
    r1: analyzer_pb2.AnalyzeResponse,
    r2: analyzer_pb2.AnalyzeResponse,
) -> analyzer_pb2.AnalyzeResponse:
    combined = analyzer_pb2.AnalyzeResponse()
    for s in r1.report_window_1:
        combined.report_window_1.add().CopyFrom(s)
    for s in r2.report_window_1:
        combined.report_window_2.add().CopyFrom(s)
    return combined


def _calculate_diff_stats(
    diff_summary: analyzer_pb2.CallChainSummary,
    w1: Optional[analyzer_pb2.PathStats],
    w2: Optional[analyzer_pb2.PathStats],
) -> None:
    if w1 and w2:
        diff_us = w2.duration.ToMicroseconds() - w1.duration.ToMicroseconds()
        diff_summary.diff.duration.FromMicroseconds(diff_us)
        diff_summary.diff.frequency = w2.frequency - w1.frequency
    elif w1:
        diff_summary.diff.duration.FromMicroseconds(-w1.duration.ToMicroseconds())
        diff_summary.diff.frequency = -w1.frequency
    elif w2:
        diff_summary.diff.duration.FromMicroseconds(w2.duration.ToMicroseconds())
        diff_summary.diff.frequency = w2.frequency


def _calculate_differences(
    r1: analyzer_pb2.AnalyzeResponse,
    r2: analyzer_pb2.AnalyzeResponse,
    combined: analyzer_pb2.AnalyzeResponse,
) -> None:
    w1_map = {_get_call_path_key(s.call_path): s.base for s in r1.report_window_1}
    w2_map = {_get_call_path_key(s.call_path): s.base for s in r2.report_window_1}
    all_keys = set(w1_map) | set(w2_map)

    # Build a lookup for call_path objects.
    path_lookup: dict[str, list] = {}
    for s in list(r1.report_window_1) + list(r2.report_window_1):
        key = _get_call_path_key(s.call_path)
        if key not in path_lookup:
            path_lookup[key] = list(s.call_path)

    for key in all_keys:
        diff_s = combined.report_window_diff.add()
        for p in path_lookup.get(key, []):
            new_p = diff_s.call_path.add()
            new_p.service = p.service
            new_p.operation_name = p.operation_name
        _calculate_diff_stats(diff_s, w1_map.get(key), w2_map.get(key))


async def _generate_comparison_report(
    output_dir1: str,
    output_dir2: str,
) -> analyzer_pb2.AnalyzeResponse:
    r1 = await process_cct(output_dir1)
    r2 = await process_cct(output_dir2)
    combined = _create_combined_response(r1, r2)
    _calculate_differences(r1, r2, combined)
    return combined


async def process_trace_comparison(request: Request) -> analyzer_pb2.AnalyzeResponse:
    """Handle the /v2/trace/analysis/compare endpoint.

    Splits incoming traces by ComparisonType (BASELINE vs COMPARISON),
    runs analysis on each window, and returns a combined diff response.
    """
    processor_w1 = TraceStreamProcessor()
    processor_w2 = TraceStreamProcessor()
    # Single combined processor to track metadata from the first message.
    meta_processor = TraceStreamProcessor()

    with (
        tempfile.TemporaryDirectory() as window1_dir,
        tempfile.TemporaryDirectory() as window2_dir,
    ):
        received_data = False
        chunk_count = 0

        async for chunk in request.stream():
            if not chunk:
                continue
            received_data = True
            chunk_count += 1

            for msg in meta_processor.process_chunk(chunk):
                if msg.metadata:
                    # Propagate metadata to both window processors.
                    import io as _io
                    req = analyzer_pb2.AnalyzeRequest()
                    req.metadata.CopyFrom(msg.metadata)
                    raw = req.SerializeToString()
                    from crisp.service.trace_processor import encode_message
                    framed = encode_message(req)
                    for m in processor_w1.process_chunk(framed):
                        pass
                    for m in processor_w2.process_chunk(framed):
                        pass
                elif msg.trace_json is not None:
                    # Route trace to the right window.
                    req = analyzer_pb2.AnalyzeRequest()
                    req.traces.trace_json = msg.trace_json
                    req.traces.type = msg.comparison_type
                    from crisp.service.trace_processor import encode_message
                    framed = encode_message(req)
                    if msg.comparison_type == analyzer_pb2.COMPARISON:
                        for m in processor_w2.process_chunk(framed):
                            saved = m.save_traces_to_json(window2_dir)
                            processor_w2.stats.traces_saved += saved
                    else:
                        for m in processor_w1.process_chunk(framed):
                            saved = m.save_traces_to_json(window1_dir)
                            processor_w1.stats.traces_saved += saved

        if not received_data:
            logger.error("No data received for comparison")
            return analyzer_pb2.AnalyzeResponse()

        request_id = f"compare_{int(datetime.now(timezone.utc).timestamp())}"

        output_dir1 = await _run_window_analysis(
            window1_dir, f"{request_id}_w1", processor_w1
        )
        output_dir2 = await _run_window_analysis(
            window2_dir, f"{request_id}_w2", processor_w2
        )

        if not output_dir1 or not output_dir2:
            return analyzer_pb2.AnalyzeResponse()

        try:
            return await _generate_comparison_report(output_dir1, output_dir2)
        except Exception as exc:
            logger.error("Comparison report failed: %s", exc)
            return analyzer_pb2.AnalyzeResponse()
