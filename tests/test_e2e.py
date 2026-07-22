"""End-to-end tests for the CRISP critical-path pipeline.

Each test runs process_trace.main() against real Jaeger JSON fixtures,
exercising the full pipeline from trace ingestion through output file
generation.  The fixtures live in test_cases/ at the repository root.
"""

import glob
import json
import os
import re
import shutil
import sys
import tempfile
from unittest import TestCase, mock

import crisp.process_trace as process_trace
from crisp.proto import analyzer_pb2

# Absolute path to the test_cases directory at the repo root.
_FIXTURES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "test_cases")


def _run_single(json_path: str, extra_args: list[str] | None = None) -> int:
    """Run the full pipeline against one JSON trace file in a temp directory."""
    with tempfile.TemporaryDirectory() as tmpdir:
        dest = os.path.join(tmpdir, os.path.basename(json_path))
        shutil.copyfile(json_path, dest)
        argv = ["process_trace.py", "--operationName", "O1", "--serviceName", "S1",
                "--file", dest]
        if extra_args:
            argv.extend(extra_args)
        with mock.patch.object(sys, "argv", argv):
            return process_trace.main()


def _run_dir(traces_dir: str, extra_args: list[str] | None = None) -> int:
    """Run the full pipeline against a directory of JSON trace files."""
    argv = ["process_trace.py", "--operationName", "O1", "--serviceName", "S1",
            "--inputDir", traces_dir]
    if extra_args:
        argv.extend(extra_args)
    with mock.patch.object(sys, "argv", argv):
        return process_trace.main()


class TestCriticalPathE2E(TestCase):

    def test_criticalpath_all_fixtures(self):
        """All 25 regular trace fixtures must process without error."""
        jsons = sorted(glob.glob(os.path.join(_FIXTURES_DIR, "*.json")))
        self.assertGreater(len(jsons), 0, "No fixture files found")
        for json_path in jsons:
            with self.subTest(fixture=os.path.basename(json_path)):
                rc = _run_single(json_path)
                self.assertEqual(rc, 0, f"Non-zero exit for {json_path}")

    def test_errorpath1(self):
        for json_path in sorted(glob.glob(os.path.join(_FIXTURES_DIR, "err_pattern1", "*.json"))):
            with self.subTest(fixture=os.path.basename(json_path)):
                self.assertEqual(0, _run_single(json_path))

    def test_errorpath2(self):
        for json_path in sorted(glob.glob(os.path.join(_FIXTURES_DIR, "err_pattern2", "*.json"))):
            with self.subTest(fixture=os.path.basename(json_path)):
                self.assertEqual(0, _run_single(json_path))

    def test_errorpath3(self):
        for json_path in sorted(glob.glob(os.path.join(_FIXTURES_DIR, "err_pattern3", "*.json"))):
            with self.subTest(fixture=os.path.basename(json_path)):
                self.assertEqual(0, _run_single(json_path))

    def test_errorpath4(self):
        for json_path in sorted(glob.glob(os.path.join(_FIXTURES_DIR, "err_pattern4", "*.json"))):
            with self.subTest(fixture=os.path.basename(json_path)):
                self.assertEqual(0, _run_single(json_path))

    def test_light_mode_cct_proto_parity(self):
        """CCT and proto must carry identical (averaged) durations and frequencies.

        Regression test for a bug where the .pb carried raw summed exclusive
        durations (N × latency) instead of per-trace averages, while the .cct
        correctly stored averages.  Creates N copies of 3.json with distinct
        traceIDs so the same call paths are aggregated N times in light mode.
        """
        source = os.path.join(_FIXTURES_DIR, "3.json")
        self.assertTrue(os.path.exists(source), f"Fixture missing: {source}")

        with open(source) as f:
            base_trace = json.load(f)

        num_copies = 5
        with tempfile.TemporaryDirectory() as tmpdir:
            for i in range(num_copies):
                trace = json.loads(json.dumps(base_trace))
                tid = f"trace{i:04d}"
                trace["data"][0]["traceID"] = tid
                for span in trace["data"][0]["spans"]:
                    span["traceID"] = tid
                    span["spanID"] = f"{span['spanID']}_{i}"
                    for ref in span.get("references", []):
                        ref["traceID"] = tid
                        ref["spanID"] = f"{ref['spanID']}_{i}"
                with open(os.path.join(tmpdir, f"{tid}.json"), "w") as f:
                    json.dump(trace, f)

            argv = [
                "process_trace.py",
                "--operationName", "O1", "--serviceName", "S1",
                "--lightMode",
                "--inputDir", tmpdir,
            ]
            with mock.patch.object(sys, "argv", argv):
                rc = process_trace.main()
            self.assertEqual(rc, 0)

            cct_path = os.path.join(tmpdir, "light-flame-graph-P100.cct")
            pb_path = os.path.join(tmpdir, "light-flame-graph-P100.pb")
            self.assertTrue(os.path.exists(cct_path), "CCT file not written")
            self.assertTrue(os.path.exists(pb_path), "Protobuf file not written")

            # Parse the CCT: each line is  <path> <excl_us> <<freq>>
            timing_re = re.compile(r'(\d+)\s*<<(\d+)>>$')
            cct_map: dict[str, tuple[int, int]] = {}
            with open(cct_path) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    m = timing_re.search(line)
                    if not m:
                        continue
                    excl, freq = int(m.group(1)), int(m.group(2))
                    path = line[:m.start()].strip()
                    cct_map[path] = (excl, freq)

            # Parse the proto
            resp = analyzer_pb2.AnalyzeResponse()
            with open(pb_path, "rb") as f:
                resp.ParseFromString(f.read())

            proto_map: dict[str, tuple[int, int]] = {}
            for entry in resp.report_window_1:
                parts = [
                    f"[{node.service}] {node.operation_name}"
                    for node in entry.call_path
                ]
                path = ";".join(parts)
                proto_map[path] = (
                    entry.base.duration.ToMicroseconds(),
                    entry.base.frequency,
                )

            self.assertGreater(len(cct_map), 0, "Expected at least one CCT entry")
            self.assertEqual(
                len(cct_map), len(proto_map),
                f"Entry count mismatch: CCT={len(cct_map)} proto={len(proto_map)}",
            )

            for path, (cct_excl, cct_freq) in cct_map.items():
                self.assertIn(path, proto_map, f"Path missing from proto: {path}")
                proto_excl, proto_freq = proto_map[path]
                self.assertEqual(
                    cct_excl, proto_excl,
                    f"Duration mismatch for '{path}': CCT={cct_excl} proto={proto_excl}"
                    f" (if proto is {num_copies}× CCT, the sum-vs-avg bug is back)",
                )
                self.assertEqual(
                    cct_freq, proto_freq,
                    f"Frequency mismatch for '{path}': CCT={cct_freq} proto={proto_freq}",
                )

            # Each call path should appear in all num_copies traces
            for path, (_excl, freq) in cct_map.items():
                self.assertEqual(
                    freq, num_copies,
                    f"Expected freq={num_copies} for '{path}' (all {num_copies} copies "
                    f"share this path), got {freq}",
                )


# --- --computeSlackDrag: opt-in flag, default off. ---


def _synthetic_two_root_child_trace(trace_id: str, scale: int = 1) -> dict:
    """Two-service, two-root-child trace (R -> A, B) with non-trivial drag/slack.

    ``scale`` avoids duration ties across traces, which some unrelated
    aggregation steps break via nondeterministic object identity.
    """
    return {
        "data": [
            {
                "traceID": trace_id,
                "processes": {
                    "P1": {"serviceName": "S1", "tags": []},
                    "P2": {"serviceName": "S2", "tags": []},
                },
                "spans": [
                    {
                        "traceID": trace_id,
                        "spanID": "R",
                        "operationName": "O1",
                        "references": [],
                        "startTime": 0,
                        "duration": 1000 * scale,
                        "processID": "P1",
                        "tags": [],
                        "logs": [],
                        "warnings": None,
                    },
                    {
                        "traceID": trace_id,
                        "spanID": "A",
                        "operationName": "OA",
                        "references": [
                            {"refType": "CHILD_OF", "traceID": trace_id, "spanID": "R"},
                        ],
                        "startTime": 0,
                        "duration": 400 * scale,
                        "processID": "P2",
                        "tags": [],
                        "logs": [],
                        "warnings": None,
                    },
                    {
                        "traceID": trace_id,
                        "spanID": "B",
                        "operationName": "OB",
                        "references": [
                            {"refType": "CHILD_OF", "traceID": trace_id, "spanID": "R"},
                        ],
                        "startTime": 500 * scale,
                        "duration": 500 * scale,
                        "processID": "P2",
                        "tags": [],
                        "logs": [],
                        "warnings": None,
                    },
                ],
            },
        ],
    }


def _write_synthetic_traces(traces_dir: str, num_traces: int = 2) -> None:
    for i in range(num_traces):
        trace_id = f"T{i}"
        with open(os.path.join(traces_dir, f"{trace_id}.json"), "w") as f:
            json.dump(_synthetic_two_root_child_trace(trace_id, scale=(i + 1)), f)


class TestComputeSlackDragFlag(TestCase):
    """--computeSlackDrag is opt-in/default-off: verify slackDrag.csv only
    appears when requested, and that turning it on/off never changes any
    *other* output file.
    """

    def test_slack_drag_csv_only_written_when_flag_enabled(self):
        with tempfile.TemporaryDirectory() as tmpdir_off, tempfile.TemporaryDirectory() as tmpdir_on:
            _write_synthetic_traces(tmpdir_off)
            _write_synthetic_traces(tmpdir_on)

            self.assertEqual(0, _run_dir(tmpdir_off))
            self.assertEqual(0, _run_dir(tmpdir_on, extra_args=["--computeSlackDrag"]))

            self.assertFalse(os.path.exists(os.path.join(tmpdir_off, "slackDrag.csv")))
            self.assertTrue(os.path.exists(os.path.join(tmpdir_on, "slackDrag.csv")))

            with open(os.path.join(tmpdir_on, "slackDrag.csv")) as f:
                content = f.read()
            self.assertIn("callPath,spanCount,avgDrag,totalDrag,avgSlack,totalSlack", content)

    def test_all_other_output_files_byte_identical_regardless_of_flag(self):
        """Every output file other than slackDrag.csv must be byte-identical regardless of --computeSlackDrag."""
        with tempfile.TemporaryDirectory() as tmpdir_off, tempfile.TemporaryDirectory() as tmpdir_on:
            _write_synthetic_traces(tmpdir_off)
            _write_synthetic_traces(tmpdir_on)
            input_filenames = {"T0.json", "T1.json"}

            self.assertEqual(0, _run_dir(tmpdir_off))
            self.assertEqual(0, _run_dir(tmpdir_on, extra_args=["--computeSlackDrag"]))

            files_off = set(os.listdir(tmpdir_off)) - input_filenames
            files_on = set(os.listdir(tmpdir_on)) - input_filenames

            # The only allowed difference in the generated file *set* is the new CSV.
            self.assertNotIn("slackDrag.csv", files_off)
            self.assertIn("slackDrag.csv", files_on)
            self.assertEqual(files_on - {"slackDrag.csv"}, files_off)

            for filename in sorted(files_off):
                path_off = os.path.join(tmpdir_off, filename)
                path_on = os.path.join(tmpdir_on, filename)
                if os.path.isdir(path_off):
                    continue
                if filename.endswith(".svg"):
                    # flamegraph.pl (the external tool that renders these) picks
                    # each box's color at random on every invocation by design --
                    # pre-existing, unrelated to --computeSlackDrag. Just confirm
                    # both runs produced the file; content is not comparable.
                    continue
                with open(path_off, "rb") as f_off, open(path_on, "rb") as f_on:
                    content_off = f_off.read()
                    content_on = f_on.read()
                if filename.endswith(".html"):
                    # heatmap.py's pandas Styler embeds a random per-render CSS id
                    # (e.g. "T_4b118_row0_col0") -- pre-existing nondeterminism,
                    # unrelated to --computeSlackDrag. Normalize it away so this
                    # test isolates the one thing it's actually checking.
                    content_off = re.sub(rb"T_[0-9a-f]{5}", b"T_XXXXX", content_off)
                    content_on = re.sub(rb"T_[0-9a-f]{5}", b"T_XXXXX", content_on)
                if "vs" in filename and filename.endswith(".cct"):
                    # The multi-percentile-window diff .cct files (produced via
                    # difffolded.pl) can emit their per-call-path lines in a
                    # run-dependent order that's pre-existing, unrelated to
                    # --computeSlackDrag (it doesn't touch this code path at
                    # all). Compare as a line multiset instead of raw bytes.
                    self.assertEqual(
                        sorted(content_off.splitlines()),
                        sorted(content_on.splitlines()),
                        f"{filename} must contain the same lines regardless of --computeSlackDrag",
                    )
                    continue
                self.assertEqual(
                    content_off,
                    content_on,
                    f"{filename} must be byte-identical regardless of --computeSlackDrag",
                )
