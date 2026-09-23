#!/usr/bin/env python3
"""Regenerate conformance goldens under test_cases/golden/ and
test_cases/error_breakdown/golden/.

Runs each fixture through the CLI twice per output, in fresh subprocesses,
and refuses to write goldens unless both runs are byte-identical. See
CONFORMANCE.md.

Usage:
    python scripts/generate_goldens.py [fixture-name ...]

No arguments regenerates all fixtures. Names are relative to test_cases/
without .json, with '/' replaced by '_' (e.g. "1", "err_pattern1_err1",
"error_breakdown_orphans").
"""
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
TEST_CASES = REPO_ROOT / "test_cases"
GOLDEN_DIR = TEST_CASES / "golden"
ERROR_BREAKDOWN_DIR = TEST_CASES / "error_breakdown"
ERROR_BREAKDOWN_GOLDEN_DIR = ERROR_BREAKDOWN_DIR / "golden"
CONFORMANCE_FILES = ("conformance.cct", "conformance.json")
ERROR_BREAKDOWN_FILE = "error-breakdown.json"
ERROR_BREAKDOWN_MODES = ("origins", "propToRoot")
ERROR_BREAKDOWN_ROOTS = ("trace", "analysis")

sys.path.insert(0, str(REPO_ROOT))

from crisp.conformance import derive_root_span  # noqa: E402

import json  # noqa: E402


def discover_fixtures() -> list[Path]:
    fixtures = sorted(TEST_CASES.glob("*.json"))
    fixtures += sorted(TEST_CASES.glob("err_pattern*/*.json"))
    return fixtures


def discover_error_breakdown_fixtures() -> list[Path]:
    return sorted(ERROR_BREAKDOWN_DIR.glob("*.json"))


def fixture_name(path: Path) -> str:
    rel = path.relative_to(TEST_CASES).with_suffix("")
    return "_".join(rel.parts)


def run_cli(fixture: Path, args: list[str], outputs: dict[str, str]) -> dict[str, bytes]:
    """Run the CLI on a copy of the fixture in a temp dir.

    outputs maps golden file name -> CLI output file name; returns golden
    file name -> output bytes.
    """
    with tempfile.TemporaryDirectory(prefix="crisp-golden-") as tmp:
        trace_path = Path(tmp) / fixture.name
        shutil.copyfile(fixture, trace_path)
        cmd = [sys.executable, "-m", "crisp.process_trace", "--file", str(trace_path), *args]
        result = subprocess.run(
            cmd, cwd=REPO_ROOT, capture_output=True, text=True, check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"CLI failed for {fixture} (exit {result.returncode}):\n"
                f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
            )
        data = {}
        for golden_name, out_name in outputs.items():
            out_path = Path(tmp) / out_name
            if not out_path.exists():
                raise RuntimeError(f"CLI did not produce {out_name} for {fixture}")
            data[golden_name] = out_path.read_bytes()
        return data


def conformance_runs(service: str, operation: str) -> list[tuple[list[str], dict[str, str]]]:
    """(CLI args, outputs) for a test_cases/ fixture's goldens."""
    base = ["-s", service, "-a", operation, "--rootTrace"]
    runs = [(base + ["--conformance"], {name: name for name in CONFORMANCE_FILES})]
    for mode in ERROR_BREAKDOWN_MODES:
        runs.append((
            base + ["--lightMode", "--errorBreakdown", mode],
            {f"error-breakdown-{mode}.json": ERROR_BREAKDOWN_FILE},
        ))
    return runs


def error_breakdown_runs(service: str, operation: str) -> list[tuple[list[str], dict[str, str]]]:
    """(CLI args, outputs) for a test_cases/error_breakdown/ fixture's goldens.

    rootTrace is off so the analysis root may be any span matching the
    derived service/operation, not only a lone root.
    """
    runs = []
    for mode in ERROR_BREAKDOWN_MODES:
        for root in ERROR_BREAKDOWN_ROOTS:
            runs.append((
                ["-s", service, "-a", operation, "--lightMode",
                 "--errorBreakdown", mode, "--errorBreakdownRoot", root],
                {f"{mode}-{root}.json": ERROR_BREAKDOWN_FILE},
            ))
    return runs


def main() -> int:
    only = set(sys.argv[1:])
    fixtures = [(f, GOLDEN_DIR, conformance_runs) for f in discover_fixtures()]
    fixtures += [(f, ERROR_BREAKDOWN_GOLDEN_DIR, error_breakdown_runs) for f in discover_error_breakdown_fixtures()]
    if only:
        fixtures = [entry for entry in fixtures if fixture_name(entry[0]) in only]
        missing = only - {fixture_name(entry[0]) for entry in fixtures}
        if missing:
            print(f"error: unknown fixture(s): {sorted(missing)}", file=sys.stderr)
            return 1

    failures = []
    for fixture, golden_dir, runs_for in fixtures:
        name = fixture_name(fixture)
        try:
            with open(fixture, encoding="utf-8") as f:
                service, operation = derive_root_span(json.load(f))
        except Exception as e:  # noqa: BLE001
            print(f"SKIP {name}: cannot derive root span ({e})")
            failures.append(name)
            continue

        outputs: dict[str, bytes] = {}
        try:
            for args, files in runs_for(service, operation):
                first = run_cli(fixture, args, files)
                second = run_cli(fixture, args, files)
                if first != second:
                    raise RuntimeError(f"outputs differ between two runs of {args} (nondeterminism!)")
                outputs.update(first)
        except RuntimeError as e:
            print(f"FAIL {name}: {e}")
            failures.append(name)
            continue

        # test_cases/error_breakdown/x.json -> error_breakdown/golden/x/.
        out_dir = golden_dir / (fixture.stem if golden_dir == ERROR_BREAKDOWN_GOLDEN_DIR else name)
        out_dir.mkdir(parents=True, exist_ok=True)
        for fname, data in outputs.items():
            (out_dir / fname).write_bytes(data)
        print(f"OK   {name}: [{service}] {operation} -> {len(outputs)} files")

    print(f"\n{len(fixtures) - len(failures)}/{len(fixtures)} fixtures' goldens written")
    if failures:
        print(f"failed: {failures}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
