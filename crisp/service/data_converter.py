"""Config construction and request-stream chunk processing."""
import logging
import uuid
from typing import Optional

from fastapi import Request

from crisp.common import Config
from crisp.service.trace_processor import TraceStreamProcessor

logger = logging.getLogger(__name__)


def create_analysis_config(
    service_name: str,
    method_name: str,
    traces_dir: str,
    output_dir: str,
    num_traces: int,
    compute_parallelism: int = 4,
    io_parallelism: int = 1,
    request_id: Optional[str] = None,
) -> Config:
    """Return a Config wired for an in-process streaming analysis run."""
    if not request_id:
        request_id = str(uuid.uuid4())
        logger.info("Generated new request ID: %s", request_id)

    return Config(
        operationName=method_name,
        serviceName=service_name,
        tracesDir=output_dir,
        output=output_dir,
        inputDir=traces_dir,
        numTrace=num_traces,
        computeParallelism=compute_parallelism,
        ioParallelism=io_parallelism,
    )


async def process_trace_chunks(
    request: Request,
    processor: TraceStreamProcessor,
    temp_dir: str,
) -> None:
    """Drain the HTTP request body, hand each chunk to the processor,
    and save decoded traces as JSON files under temp_dir."""
    logger.info("Starting to process trace stream into %s", temp_dir)
    try:
        async for chunk in request.stream():
            if not chunk:
                continue
            for message in processor.process_chunk(chunk):
                saved = message.save_traces_to_json(temp_dir)
                processor.stats.traces_saved += saved
                message.log_summary()
                processor.log_progress()
        processor.log_summary()
        logger.info("All chunks processed; traces written to %s", temp_dir)
    except Exception as exc:
        logger.error("Error processing trace chunks: %s", exc)
        raise
