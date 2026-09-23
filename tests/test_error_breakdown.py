# ruff: noqa: I001
"""Tests for crisp/error_breakdown.py and its light-mode output.

Goldens are regenerated with scripts/generate_goldens.py; the Go port checks
the same goldens (go/crisp/error_breakdown_test.go). See CONFORMANCE.md.
"""
import copy
import json
import shutil
import types
from pathlib import Path

import pytest

import crisp.common as common
import crisp.graph as graph
import crisp.utils.span_utils as span_utils
from crisp.conformance import derive_root_span
from crisp.error_breakdown import (
    ERROR_BREAKDOWN_FILE,
    MODE_ORIGINS,
    MODE_PROP_TO_ROOT,
    MODES,
    ROOT_ANALYSIS,
    ROOT_TRACE,
    ROOTS,
    ErrorBreakdownOptions,
    extract_rpc_status,
    is_status_error,
    merge_error_breakdowns,
)
from crisp.models import GraphNode
from crisp.process_trace import lightProcess, process
from crisp.shared.models import SpanKind

REPO_ROOT = Path(__file__).resolve().parent.parent
TEST_CASES = REPO_ROOT / "test_cases"
GOLDEN_DIR = TEST_CASES / "golden"
ERROR_BREAKDOWN_DIR = TEST_CASES / "error_breakdown"
ERROR_BREAKDOWN_GOLDEN_DIR = ERROR_BREAKDOWN_DIR / "golden"


def _conformance_fixtures() -> dict[str, Path]:
    fixtures = sorted(TEST_CASES.glob("*.json")) + sorted(TEST_CASES.glob("err_pattern*/*.json"))
    return {"_".join(f.relative_to(TEST_CASES).with_suffix("").parts): f for f in fixtures}


CONFORMANCE_FIXTURES = _conformance_fixtures()
ERROR_BREAKDOWN_FIXTURES = {f.stem: f for f in sorted(ERROR_BREAKDOWN_DIR.glob("*.json"))}


def _run_light(fixture: Path, out_dir: Path, mode: str, root: str, rootTrace: bool) -> bytes:
    trace_path = out_dir / fixture.name
    shutil.copyfile(fixture, trace_path)
    with open(fixture, encoding="utf-8") as f:
        service, operation = derive_root_span(json.load(f))
    c = common.Config(
        serviceName=service,
        operationName=operation,
        rootTrace=rootTrace,
        lightMode=True,
        errorBreakdown=mode,
        errorBreakdownRoot=root,
        file=types.SimpleNamespace(name=str(trace_path)),
    )
    c.jaegerTraceFiles = [str(trace_path)]
    assert lightProcess(c) == 0
    return (out_dir / ERROR_BREAKDOWN_FILE).read_bytes()


@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("name", sorted(CONFORMANCE_FIXTURES))
def test_conformance_fixture_matches_golden(name, mode, tmp_path):
    got = _run_light(CONFORMANCE_FIXTURES[name], tmp_path, mode, ROOT_TRACE, rootTrace=True)
    want = (GOLDEN_DIR / name / f"error-breakdown-{mode}.json").read_bytes()
    assert got == want, f"{name}: error breakdown ({mode}) diverged; regenerate with scripts/generate_goldens.py"


@pytest.mark.parametrize("root", ROOTS)
@pytest.mark.parametrize("mode", MODES)
@pytest.mark.parametrize("name", sorted(ERROR_BREAKDOWN_FIXTURES))
def test_error_breakdown_fixture_matches_golden(name, mode, root, tmp_path):
    got = _run_light(ERROR_BREAKDOWN_FIXTURES[name], tmp_path, mode, root, rootTrace=False)
    want = (ERROR_BREAKDOWN_GOLDEN_DIR / name / f"{mode}-{root}.json").read_bytes()
    assert got == want, f"{name}: {mode}-{root} diverged; regenerate with scripts/generate_goldens.py"


def test_error_breakdown_fixtures_have_goldens():
    goldens = {d.name for d in ERROR_BREAKDOWN_GOLDEN_DIR.iterdir() if d.is_dir()}
    assert goldens == set(ERROR_BREAKDOWN_FIXTURES)


def _leaves(fixture: str, mode: str, root: str = ROOT_TRACE) -> list[str]:
    """Operation names of the last node of every error path."""
    with open(ERROR_BREAKDOWN_DIR / f"{fixture}.json", encoding="utf-8") as f:
        data = json.load(f)
    service, operation = derive_root_span(data)
    g = graph.Graph(data, service, operation, "t.json", rootTrace=False,
                    errorBreakdown=ErrorBreakdownOptions(mode, root))
    doc = merge_error_breakdowns([("t", g.errorBreakdown)], ErrorBreakdownOptions(mode, root), 3)
    return sorted(p["nodes"][-1]["operation"] for p in doc["paths"])


@pytest.mark.parametrize(
    "fixture, mode, want",
    [
        # The computePropToRootGraph reference tree (see test_compute_prop_to_root_graph).
        ("prop_to_root", MODE_PROP_TO_ROOT, ["B1", "B5", "B6", "B7", "B8"]),
        ("prop_to_root", MODE_ORIGINS, ["B1", "B3", "B4", "B5", "B6", "B7", "B8"]),
        # A successful RPC between a failed root and a deeper error.
        ("success_rpc_between", MODE_PROP_TO_ROOT, ["root"]),
        ("success_rpc_between", MODE_ORIGINS, ["B"]),
        # The request succeeded despite a downstream failure.
        ("root_succeeded", MODE_PROP_TO_ROOT, []),
        ("root_succeeded", MODE_ORIGINS, ["B"]),
        # Sanitization would drop "late"; the breakdown runs before it.
        ("sanitized_child", MODE_ORIGINS, ["late"]),
    ],
)
def test_counting_modes(fixture, mode, want):
    assert _leaves(fixture, mode) == want


def test_prop_to_root_matches_legacy_walker():
    """propToRoot from the analysis root reports the same paths as
    Graph.computePropToRootGraph (which feeds the --errorAnalysis flame graph)."""
    with open(ERROR_BREAKDOWN_DIR / "prop_to_root.json", encoding="utf-8") as f:
        data = json.load(f)
    options = ErrorBreakdownOptions(MODE_PROP_TO_ROOT, ROOT_ANALYSIS)
    g = graph.Graph(data, "testService", "root", "t.json", rootTrace=True, errorBreakdown=options)
    legacy = sorted(p.split("->")[-1].split("] ", 1)[1] for p in g.computePropToRootGraph())
    doc = merge_error_breakdowns([("t", g.errorBreakdown)], options, 3)
    assert legacy == sorted(p["nodes"][-1]["operation"] for p in doc["paths"])


def test_trace_root_skips_ignored_ops_and_proxies(monkeypatch):
    with open(ERROR_BREAKDOWN_DIR / "multi_root.json", encoding="utf-8") as f:
        data = json.load(f)
    assert _leaves("multi_root", MODE_ORIGINS) == ["first"]
    monkeypatch.setattr(span_utils, "IGNORED_ROOT_OPS", ["first"])
    assert _leaves("multi_root", MODE_ORIGINS) == ["fetch"]
    monkeypatch.setattr(span_utils, "IGNORED_ROOT_OPS", [])
    monkeypatch.setattr(span_utils, "PROXY_ONLY_OPS", ["first"])
    g = graph.Graph(data, "two", "second", "t.json", rootTrace=False, filterProxy=True,
                    errorBreakdown=ErrorBreakdownOptions(MODE_ORIGINS, ROOT_TRACE))
    assert [k.rsplit(";", 1)[-1] for k in g.errorBreakdown] == ["[two]fetch:http:500"]


def test_trace_root_without_spans_is_empty():
    g = graph.Graph({"data": [{"traceID": "t", "spans": [], "processes": {}}]}, "s", "o", "t.json",
                    errorBreakdown=ErrorBreakdownOptions(MODE_ORIGINS, ROOT_TRACE))
    assert g.errorBreakdown == {}
    assert g.rootNode is None


def _tag(key, value, type_="string"):
    return {"key": key, "type": type_, "value": value}


@pytest.mark.parametrize(
    "tags, want",
    [
        ([], ("", None)),
        ([_tag("http.response.status_code", 503, "int64")], ("http", 503)),
        ([_tag("http.response.status_code", "404")], ("http", 404)),
        ([_tag("http.status_code", 502, "int64")], ("http", 502)),
        ([_tag("grpc.status_code", 14, "int64")], ("grpc", 14)),
        ([_tag("rpc.grpc.status_code", "+7")], ("grpc", 7)),
        ([_tag("grpc.status", "OK")], ("grpc", None)),
        # The last status tag wins.
        ([_tag("grpc.status_code", 2, "int64"), _tag("http.response.status_code", 500, "int64")], ("http", 500)),
        # YARPC codes take the protocol from rpc.transport.
        ([_tag("rpc.yarpc.status_code", 5, "int64"), _tag("rpc.transport", "grpc")], ("yarpc_grpc", 5)),
        ([_tag("rpc.transport", "tchannel"), _tag("rpc.yarpc.status_code", "3")], ("yarpc_tchannel", 3)),
        ([_tag("rpc.yarpc.status_code", 5, "int64")], ("", 5)),
        # A direct status tag takes precedence over a YARPC one.
        ([_tag("rpc.yarpc.status_code", 5, "int64"), _tag("grpc.status_code", 1, "int64")], ("grpc", 1)),
        ([_tag("rpc.transport", "http")], ("http", None)),
        ([_tag("rpc.transport", "quic")], ("", None)),
        ([_tag("rpc.transport", 1, "int64")], ("", None)),
        # Values that are not integers carry no code.
        ([_tag("http.response.status_code", 404.0, "float64")], ("http", None)),
        ([_tag("http.response.status_code", True, "bool")], ("http", None)),
        ([_tag("http.response.status_code", " 404")], ("http", None)),
        ([_tag("http.response.status_code", "")], ("http", None)),
        ([_tag("http.response.status_code", 1 << 63, "int64")], ("http", None)),
        ([_tag("http.response.status_code", 404, "string")], ("http", None)),
        # Keys match exactly.
        ([_tag("HTTP.response.status_code", 404, "int64")], ("", None)),
        # Deployment lists ship empty.
        ([_tag("component", "my-http")], ("", None)),
        ([_tag("as", "thrift")], ("", None)),
        ([_tag("my.tchannel.status", 1, "int64")], ("", None)),
        ([_tag("my.yarpc.status", 1, "int64")], ("", None)),
    ],
)
def test_extract_rpc_status(tags, want):
    assert extract_rpc_status(tags) == want


def test_extract_rpc_status_deployment_lists(monkeypatch):
    monkeypatch.setattr(span_utils, "HTTP_COMPONENTS", ["my-http"])
    monkeypatch.setattr(span_utils, "TCHANNEL_MARKER_TAGS", ["as"])
    assert extract_rpc_status([_tag("component", "my-http")]) == ("http", None)
    # A component only fills in a missing protocol.
    assert extract_rpc_status([_tag("component", "my-http"), _tag("rpc.transport", "grpc")]) == ("http", None)
    assert extract_rpc_status([_tag("grpc.status_code", 2, "int64"), _tag("component", "my-http")]) == ("grpc", 2)
    # A marker tag forces TChannel and keeps the code.
    assert extract_rpc_status([_tag("http.response.status_code", 500, "int64"), _tag("as", "thrift")]) == (
        "tchannel", 500)
    assert extract_rpc_status([_tag("as", "json"), _tag("rpc.yarpc.status_code", 5, "int64")]) == ("tchannel", None)


def test_extract_rpc_status_status_tag_lists(monkeypatch):
    monkeypatch.setattr(span_utils, "TCHANNEL_STATUS_TAGS", ["my.tchannel.status"])
    monkeypatch.setattr(span_utils, "YARPC_STATUS_TAGS", ["my.yarpc.status"])
    assert extract_rpc_status([_tag("my.tchannel.status", 0, "int64")]) == ("tchannel", 0)
    assert extract_rpc_status([_tag("rpc.transport", "tchannel"), _tag("my.yarpc.status", "3")]) == (
        "yarpc_tchannel", 3)
    # Configured keys follow the same last-status-wins rule as standard ones.
    assert extract_rpc_status([_tag("my.tchannel.status", 1, "int64"), _tag("grpc.status_code", 2, "int64")]) == (
        "grpc", 2)


@pytest.mark.parametrize(
    "protocol, code, want",
    [
        ("http", None, False),
        ("http", 399, False),
        ("http", 400, True),
        ("grpc", 0, False),
        ("grpc", 14, True),
        ("tchannel", 1, True),
        ("yarpc_http", 400, True),
        ("", 0, False),
        ("", 3, True),
    ],
)
def test_is_status_error(protocol, code, want):
    assert is_status_error(protocol, code) == want


def test_merge_error_breakdowns():
    options = ErrorBreakdownOptions(MODE_ORIGINS, ROOT_TRACE)
    node = ("s", "op", "grpc", 2, "a1")
    per_trace = [
        ("t1", {"k": [2, [node]]}),
        ("t2", {"k": [1, [node]], "j": [1, [("s", "j", None, None, None)]]}),
        ("t1", {"k": [1, [node]]}),  # duplicate exemplar
        ("t3", {"k": [1, [("s", "op", "grpc", 2, "a3")]]}),
        ("t4", {"k": [1, [("s", "op", "grpc", 2, "a4")]]}),
    ]
    doc = merge_error_breakdowns(per_trace, options, 3)
    assert doc["traces"] == 5
    assert [p["key"] for p in doc["paths"]] == ["j", "k"]
    j, k = doc["paths"]
    assert j["nodes"][0]["exemplars"] == []
    assert k["count"] == 6
    assert k["nodes"][0]["exemplars"] == [
        {"spanID": "a1", "traceID": "t1"},
        {"spanID": "a1", "traceID": "t2"},
        {"spanID": "a3", "traceID": "t3"},
    ]
    assert k["nodes"][0]["protocol"] == "grpc"
    assert k["nodes"][0]["statusCode"] == 2


def test_repeated_key_in_one_trace_counts_without_exemplars():
    """Two sibling spans with the same path key: count 2, first span's exemplar."""
    with open(ERROR_BREAKDOWN_DIR / "prop_to_root.json", encoding="utf-8") as f:
        data = json.load(f)
    spans = data["data"][0]["spans"]
    b6 = next(s for s in spans if s["operationName"] == "B6")
    b6["operationName"] = "B5"
    options = ErrorBreakdownOptions(MODE_ORIGINS, ROOT_TRACE)
    g = graph.Graph(data, "testService", "root", "t.json", errorBreakdown=options)
    doc = merge_error_breakdowns([("t", g.errorBreakdown)], options, 3)
    path = next(p for p in doc["paths"] if p["key"].endswith("[testService]B5"))
    assert path["count"] == 2
    b5 = next(s for s in spans if s["operationName"] == "B5")
    assert path["nodes"][-1]["exemplars"] == [{"spanID": b5["spanID"], "traceID": "t"}]


def test_error_breakdown_options_validation():
    with pytest.raises(ValueError):
        ErrorBreakdownOptions("everything")
    with pytest.raises(ValueError):
        ErrorBreakdownOptions(MODE_ORIGINS, "somewhere")


def test_process_error_analysis_populates_prop_to_root_cct(tmp_path):
    """--errorAnalysis feeds computePropToRootGraph into the metrics (the
    errorsPropToRoot flame graph); without it the CCT stays empty."""
    trace_path = tmp_path / "t.json"
    shutil.copyfile(ERROR_BREAKDOWN_DIR / "prop_to_root.json", trace_path)
    c = common.Config(serviceName="testService", operationName="root", rootTrace=True)
    assert process(str(trace_path), c).propToRootErrCCT == {}
    c.errorAnalysis = True
    cct = process(str(trace_path), c).propToRootErrCCT
    assert sorted(p.split("->")[-1] for p in cct) == [
        "[otherService] B8", "[testService] B1", "[testService] B5", "[testService] B6", "[testService] B7",
    ]


def test_light_mode_without_error_breakdown_writes_nothing(tmp_path):
    trace_path = tmp_path / "t.json"
    shutil.copyfile(ERROR_BREAKDOWN_DIR / "prop_to_root.json", trace_path)
    c = common.Config(serviceName="testService", operationName="root", rootTrace=True, lightMode=True,
                      file=types.SimpleNamespace(name=str(trace_path)))
    c.jaegerTraceFiles = [str(trace_path)]
    assert lightProcess(c) == 0
    assert not (tmp_path / ERROR_BREAKDOWN_FILE).exists()


def test_compute_prop_to_root_graph():
    """Graph.computePropToRootGraph on a hand-built tree."""
    g = graph.Graph([], "testService", "testOperation", "nofile.txt", skipInitializationForTest=True)
    g.processName = {1: "testService", 2: "otherService"}
    g.rootNode = GraphNode(sid=1, startTime=0, duration=1000, parentSpanId=None, opName="root",
                           processID=1, spanKind=SpanKind.SERVER, peerService="testService", returnError=True)

    def node(sid, parent, opName, processID, spanKind, returnError, startTime, duration):
        n = GraphNode(sid=sid, startTime=startTime, duration=duration, parentSpanId=parent.sid, opName=opName,
                      processID=processID, spanKind=spanKind, peerService=g.processName[processID],
                      returnError=returnError)
        n.parent = parent
        parent.addChild(n)
        return n

    S, U = SpanKind.SERVER, SpanKind.UNKNOWN
    A1 = node(2, g.rootNode, "A1", 1, S, True, 0, 100)
    A2 = node(3, g.rootNode, "A2", 1, S, False, 100, 100)
    A3 = node(4, g.rootNode, "A3", 1, S, False, 200, 100)
    A4 = node(5, g.rootNode, "A4", 1, S, False, 300, 100)
    A5 = node(6, g.rootNode, "A5", 1, S, True, 400, 100)
    A6 = node(16, g.rootNode, "A6", 1, U, False, 500, 100)
    A7 = node(18, g.rootNode, "A7", 2, U, False, 600, 100)
    B1 = node(7, A1, "B1", 1, S, True, 10, 50)
    B2 = node(8, A2, "B2", 1, S, False, 110, 50)
    B3 = node(9, A3, "B3", 1, S, True, 210, 50)
    B4 = node(10, A4, "B4", 1, S, True, 310, 50)
    B5 = node(11, A5, "B5", 1, S, True, 410, 50)
    B6 = node(12, A5, "B6", 1, S, True, 420, 50)
    B7 = node(17, A6, "B7", 1, U, True, 510, 50)
    B8 = node(19, A7, "B8", 2, U, True, 610, 50)
    C4 = node(13, B4, "C4", 1, S, False, 330, 10)
    C5 = node(14, B5, "C5", 1, S, False, 430, 10)
    C6 = node(15, B6, "C6", 1, S, False, 440, 10)

    errMap = g.computePropToRootGraph()
    assert errMap == {g.getCallPath(n): 1 for n in (B1, B5, B6, B7, B8)}
    for n in (B2, B3, B4, C4, C5, C6):
        assert g.getCallPath(n) not in errMap


def test_graph_without_option_has_no_breakdown():
    with open(ERROR_BREAKDOWN_DIR / "prop_to_root.json", encoding="utf-8") as f:
        data = json.load(f)
    g = graph.Graph(copy.deepcopy(data), "testService", "root", "t.json")
    assert g.errorBreakdown is None
    assert g.rootNode is not None
