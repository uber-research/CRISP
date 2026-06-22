"""File I/O helpers for trace analysis."""
import glob
import logging
import os
import re
import tempfile

logger = logging.getLogger(__name__)

_NON_ALPHANUM_RE = re.compile(r"[^a-zA-Z0-9]")


def _sanitize(name: str) -> str:
    return _NON_ALPHANUM_RE.sub("_", name)


def validate_trace_analysis_inputs(
    service_name: str,
    method_name: str,
    traces_dir: str,
) -> None:
    """Raise on bad inputs before starting analysis."""
    if not service_name or not isinstance(service_name, str):
        raise ValueError("service_name must be a non-empty string")
    if not method_name or not isinstance(method_name, str):
        raise ValueError("method_name must be a non-empty string")
    if not traces_dir or not isinstance(traces_dir, str):
        raise ValueError("traces_dir must be a non-empty string")
    if not os.path.exists(traces_dir):
        raise FileNotFoundError(f"Traces directory does not exist: {traces_dir}")
    if not os.access(traces_dir, os.R_OK):
        raise PermissionError(f"No read access to traces directory: {traces_dir}")


def get_json_files(directory: str) -> list[str]:
    return glob.glob(os.path.join(directory, "*.json"))


def count_json_files(directory: str) -> int:
    return len(get_json_files(directory))


def create_output_directory(
    traces_dir: str,
    request_id: str,
    service_name: str,
    method_name: str,
) -> str:
    output_dir = os.path.join(
        traces_dir,
        request_id,
        _sanitize(service_name),
        _sanitize(method_name),
        "output",
    )
    os.makedirs(output_dir, exist_ok=True)
    logger.info("Created output directory: %s", output_dir)
    return output_dir


def ensure_absolute_paths(config) -> None:
    """Make all path attributes on config absolute and inject jaegerTraceFiles."""
    config.inputDir = os.path.abspath(config.inputDir)
    config.tracesDir = os.path.abspath(config.tracesDir)
    config.output = os.path.abspath(config.output)
    config.jaegerTraceFiles = [
        os.path.join(config.inputDir, f)
        for f in os.listdir(config.inputDir)
        if f.endswith(".json")
    ]
