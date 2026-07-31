# ruff: noqa: I001
"""Golden and determinism tests for the conformance output mode.

Intentional behavior changes must regenerate goldens via
scripts/generate_goldens.py in the same PR. See CONFORMANCE.md.
"""
import json
import shutil
import types
from pathlib import Path

import pytest

import crisp.common as common
from crisp.conformance import (
    CONFORMANCE_CCT_FILE,
    CONFORMANCE_JSON_FILE,
    canonical_cct,
    derive_root_span,
)
from crisp.process_trace import lightProcess

REPO_ROOT = Path(__file__).resolve().parent.parent
TEST_CASES = REPO_ROOT / "test_cases"
GOLDEN_DIR = TEST_CASES / "golden"


def discover_fixtures() -> dict[str, Path]:
    """Map golden fixture name -> fixture path (same naming as generate_goldens.py)."""
    fixtures = sorted(TEST_CASES.glob("*.json")) + sorted(TEST_CASES.glob("err_pattern*/*.json"))
    return {"_".join(f.relative_to(TEST_CASES).with_suffix("").parts): f for f in fixtures}


FIXTURES = discover_fixtures()
GOLDEN_NAMES = sorted(d.name for d in GOLDEN_DIR.iterdir() if d.is_dir()) if GOLDEN_DIR.exists() else []


def run_conformance_in_process(fixture: Path, out_dir: Path) -> dict[str, bytes]:
    """Run the light+conformance pipeline in-process; return conformance file bytes."""
    trace_path = out_dir / fixture.name
    shutil.copyfile(fixture, trace_path)
    with open(fixture, encoding="utf-8") as f:
        service, operation = derive_root_span(json.load(f))

    c = common.Config(
        serviceName=service,
        operationName=operation,
        rootTrace=True,
        lightMode=True,
        conformance=True,
        file=types.SimpleNamespace(name=str(trace_path)),
    )
    c.jaegerTraceFiles = [str(trace_path)]
    assert lightProcess(c) == 0

    return {
        name: (out_dir / name).read_bytes()
        for name in (CONFORMANCE_CCT_FILE, CONFORMANCE_JSON_FILE)
    }


def test_all_fixtures_have_goldens():
    assert GOLDEN_NAMES, "no goldens found; run scripts/generate_goldens.py"
    assert set(GOLDEN_NAMES) == set(FIXTURES), (
        "fixtures and goldens out of sync; run scripts/generate_goldens.py"
    )


@pytest.mark.parametrize("name", GOLDEN_NAMES)
def test_conformance_matches_golden(name, tmp_path):
    outputs = run_conformance_in_process(FIXTURES[name], tmp_path)
    for fname, data in outputs.items():
        golden = (GOLDEN_DIR / name / fname).read_bytes()
        assert data == golden, (
            f"{name}/{fname} diverged from golden. If this change is intentional, "
            f"regenerate goldens with scripts/generate_goldens.py"
        )


@pytest.mark.parametrize("name", ["18", "err_pattern4_err1"])
def test_conformance_is_deterministic(name, tmp_path):
    run1, run2 = tmp_path / "run1", tmp_path / "run2"
    run1.mkdir()
    run2.mkdir()
    first = run_conformance_in_process(FIXTURES[name], run1)
    second = run_conformance_in_process(FIXTURES[name], run2)
    assert first == second


def test_canonical_cct_sorts_and_terminates():
    raw = "b 2 <<1>>\na 1 <<1>>\n"
    assert canonical_cct(raw) == "a 1 <<1>>\nb 2 <<1>>\n"


def test_canonical_cct_empty():
    assert canonical_cct("") == ""
    assert canonical_cct("\n\n") == ""


def test_derive_root_span_picks_unparented_span():
    trace = {
        "data": [
            {
                "processes": {"p1": {"serviceName": "svcA"}, "p2": {"serviceName": "svcB"}},
                "spans": [
                    {
                        "spanID": "child",
                        "operationName": "opChild",
                        "processID": "p2",
                        "startTime": 5,
                        "references": [{"refType": "CHILD_OF", "spanID": "root"}],
                    },
                    {
                        "spanID": "root",
                        "operationName": "opRoot",
                        "processID": "p1",
                        "startTime": 10,
                        "references": [],
                    },
                ],
            }
        ]
    }
    assert derive_root_span(trace) == ("svcA", "opRoot")
