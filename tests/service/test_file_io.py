"""Unit tests for crisp/service/file_io.py."""
import os
import tempfile

import pytest

from crisp.service.file_io import (
    count_json_files,
    create_output_directory,
    ensure_absolute_paths,
    get_json_files,
    validate_trace_analysis_inputs,
)


def test_validate_ok(tmp_path):
    validate_trace_analysis_inputs("svc", "op", str(tmp_path))


def test_validate_empty_service(tmp_path):
    with pytest.raises(ValueError, match="service_name"):
        validate_trace_analysis_inputs("", "op", str(tmp_path))


def test_validate_empty_method(tmp_path):
    with pytest.raises(ValueError, match="method_name"):
        validate_trace_analysis_inputs("svc", "", str(tmp_path))


def test_validate_missing_dir():
    with pytest.raises(FileNotFoundError):
        validate_trace_analysis_inputs("svc", "op", "/no/such/dir")


def test_get_json_files(tmp_path):
    (tmp_path / "a.json").write_text("{}")
    (tmp_path / "b.json").write_text("{}")
    (tmp_path / "c.txt").write_text("x")
    files = get_json_files(str(tmp_path))
    assert len(files) == 2
    assert all(f.endswith(".json") for f in files)


def test_count_json_files(tmp_path):
    (tmp_path / "t1.json").write_text("{}")
    (tmp_path / "t2.json").write_text("{}")
    assert count_json_files(str(tmp_path)) == 2


def test_create_output_directory(tmp_path):
    out = create_output_directory(
        traces_dir=str(tmp_path),
        request_id="req-1",
        service_name="my-svc",
        method_name="my.op",
    )
    assert os.path.isdir(out)
    assert "req-1" in out
    assert "my_svc" in out
    assert "my_op" in out


def test_create_output_directory_idempotent(tmp_path):
    # calling twice should not raise
    for _ in range(2):
        create_output_directory(str(tmp_path), "req-2", "svc", "op")


class _FakeConfig:
    def __init__(self, input_dir):
        self.inputDir = input_dir
        self.tracesDir = input_dir
        self.output = input_dir
        self.jaegerTraceFiles = []


def test_ensure_absolute_paths(tmp_path):
    (tmp_path / "t1.json").write_text("{}")
    (tmp_path / "t2.json").write_text("{}")
    cfg = _FakeConfig(str(tmp_path))
    ensure_absolute_paths(cfg)
    assert os.path.isabs(cfg.inputDir)
    assert os.path.isabs(cfg.tracesDir)
    assert os.path.isabs(cfg.output)
    assert len(cfg.jaegerTraceFiles) == 2
