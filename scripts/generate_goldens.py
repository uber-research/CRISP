#!/usr/bin/env python3
"""Regenerate conformance goldens under test_cases/golden/.

Runs each fixture through the CLI with --conformance twice, in fresh
subprocesses, and refuses to write goldens unless both runs are
byte-identical. See CONFORMANCE.md.

Usage:
    python scripts/generate_goldens.py [fixture-name ...]

No arguments regenerates all fixtures. Names are relative to test_cases/
without .json, with '/' replaced by '_' (e.g. "1", "err_pattern1_err1").
"""
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
TEST_CASES = REPO_ROOT / "test_cases"
GOLDEN_DIR = TEST_CASES / "golden"
CONFORMANCE_FILES = ("conformance.cct", "conformance.json")

sys.path.insert(0, str(REPO_ROOT))

from crisp.conformance import derive_root_span  # noqa: E402

import json  # noqa: E402


def discover_fixtures() -> list[Path]:
    fixtures = sorted(TEST_CASES.glob("*.json"))
    fixtures += sorted(TEST_CASES.glob("err_pattern*/*.json"))
    return fixtures


def fixture_name(path: Path) -> str:
    rel = path.relative_to(TEST_CASES).with_suffix("")
    return "_".join(rel.parts)


def run_conformance(fixture: Path, service: str, operation: str) -> dict[str, bytes]:
    """Run the CLI on a copy of the fixture in a temp dir; return output bytes."""
    with tempfile.TemporaryDirectory(prefix="crisp-golden-") as tmp:
        trace_path = Path(tmp) / fixture.name
        shutil.copyfile(fixture, trace_path)
        cmd = [
            sys.executable, "-m", "crisp.process_trace",
            "--file", str(trace_path),
            "-s", service,
            "-a", operation,
            "--rootTrace",
            "--conformance",
        ]
        result = subprocess.run(
            cmd, cwd=REPO_ROOT, capture_output=True, text=True, check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"CLI failed for {fixture} (exit {result.returncode}):\n"
                f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
            )
        outputs = {}
        for name in CONFORMANCE_FILES:
            out_path = Path(tmp) / name
            if not out_path.exists():
                raise RuntimeError(f"CLI did not produce {name} for {fixture}")
            outputs[name] = out_path.read_bytes()
        return outputs


def main() -> int:
    only = set(sys.argv[1:])
    fixtures = discover_fixtures()
    if only:
        fixtures = [f for f in fixtures if fixture_name(f) in only]
        missing = only - {fixture_name(f) for f in fixtures}
        if missing:
            print(f"error: unknown fixture(s): {sorted(missing)}", file=sys.stderr)
            return 1

    failures = []
    for fixture in fixtures:
        name = fixture_name(fixture)
        try:
            with open(fixture, encoding="utf-8") as f:
                service, operation = derive_root_span(json.load(f))
        except Exception as e:  # noqa: BLE001
            print(f"SKIP {name}: cannot derive root span ({e})")
            failures.append(name)
            continue

        try:
            first = run_conformance(fixture, service, operation)
            second = run_conformance(fixture, service, operation)
        except RuntimeError as e:
            print(f"FAIL {name}: {e}")
            failures.append(name)
            continue

        if first != second:
            print(f"FAIL {name}: outputs differ between two runs (nondeterminism!)")
            failures.append(name)
            continue

        out_dir = GOLDEN_DIR / name
        out_dir.mkdir(parents=True, exist_ok=True)
        for fname, data in first.items():
            (out_dir / fname).write_bytes(data)
        cct_lines = first["conformance.cct"].count(b"\n")
        print(f"OK   {name}: [{service}] {operation} -> {cct_lines} CCT lines")

    print(f"\n{len(fixtures) - len(failures)}/{len(fixtures)} goldens written to {GOLDEN_DIR}")
    if failures:
        print(f"failed: {failures}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
