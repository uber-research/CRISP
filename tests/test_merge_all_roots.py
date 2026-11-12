"""--mergeAllRoots: which spans are analysis roots, and how a trace with several
roots is processed (see Graph.analysisRoots and CONFORMANCE.md).

Mirrors go/crisp/merge_all_roots_test.go on the same traces.
"""
import json

import pytest

import crisp.common as common
from crisp.error_breakdown import MODE_ORIGINS, ROOT_ANALYSIS, ROOT_TRACE
from crisp.graph import Graph
from crisp.process_trace import lightProcess, process, processRoots

LIGHT_CCT = "light-flame-graph-P100.cct"


def span(spanId, op, pid, parent=None, start=1000, dur=100, error=False):
    refs = [{"traceID": "T1", "spanID": parent, "refType": "CHILD_OF"}] if parent else []
    s = {
        "traceID": "T1", "spanID": spanId, "operationName": op, "references": refs,
        "startTime": start, "duration": dur, "processID": pid,
    }
    if error:
        s["tags"] = [{"key": "error", "type": "bool", "value": True}]
    return s


def trace(*spans):
    services = {"pA": "svcA", "pB": "svcB", "pX": "svcX"}
    pids = {s["processID"] for s in spans}
    return {"data": [{
        "traceID": "T1",
        "spans": list(spans),
        "processes": {pid: {"serviceName": services[pid]} for pid in sorted(pids)},
    }]}


# A trace root svcX/opX with two svcA/opA children (A is the first match, S its
# sibling), a second trace root B, and C, the head of an orphaned fragment.
# A, B and C are analysis roots; S has a parent and is not.
MULTI_ROOT = [
    span("X", "opX", "pX", start=0, dur=500),
    span("A", "opA", "pA", parent="X", start=10, dur=100),
    span("S", "opA", "pA", parent="X", start=120, dur=100),
    span("B", "opA", "pA", start=600, dur=200),
    span("C", "opA", "pA", parent="missing", start=900, dur=300),
]


def config(**overrides):
    return common.Config(serviceName="svcA", operationName="opA", rootTrace=False, lightMode=True, **overrides)


def roots(tmp_path, spans, **overrides):
    path = tmp_path / "T1.json"
    path.write_text(json.dumps(trace(*spans)))
    return processRoots(str(path), config(**overrides))


def root_ids(metrics):
    return [m.rootSpanID for m in metrics]


# --- which spans are analysis roots -------------------------------------------

def test_every_parentless_match_is_a_root(tmp_path):
    metrics = roots(tmp_path, MULTI_ROOT)
    assert root_ids(metrics) == ["A", "B", "C"]
    assert [m.CPMetrics.profile["[svcA] opA"].inc for m in metrics] == [100, 200, 300]


def test_no_merge_all_roots_keeps_the_first_match(tmp_path):
    assert root_ids(roots(tmp_path, MULTI_ROOT, mergeAllRoots=False)) == ["A"]


def test_root_trace_pins_a_single_root(tmp_path):
    spans = [span("A", "opA", "pA", dur=100)]
    path = tmp_path / "T1.json"
    path.write_text(json.dumps(trace(*spans)))
    c = common.Config(serviceName="svcA", operationName="opA", rootTrace=True, lightMode=True)
    assert root_ids(processRoots(str(path), c)) == ["A"]


def test_matches_with_a_parent_are_not_roots(tmp_path):
    """Sibling and nested matches count only through the critical path of the root above them."""
    spans = [
        span("X", "opX", "pX", start=0, dur=1000),
        span("A", "opA", "pA", parent="X", start=10, dur=400),
        span("N", "opA", "pA", parent="A", start=20, dur=100),
        span("S", "opA", "pA", parent="X", start=500, dur=100),
    ]
    metrics = roots(tmp_path, spans)
    assert root_ids(metrics) == ["A"]
    profile = metrics[0].CPMetrics.profile
    assert profile["[svcA] opA"].freq == 1
    assert profile["[svcA] opA->[svcA] opA"].freq == 1


def test_match_dropped_by_sanitization_is_not_a_root(tmp_path):
    """N starts after its parent A ended, so sanitization drops it and clears its parent."""
    spans = [
        span("X", "opX", "pX", start=0, dur=1000),
        span("A", "opA", "pA", parent="X", start=0, dur=100),
        span("N", "opA", "pA", parent="A", start=200, dur=50),
    ]
    assert root_ids(roots(tmp_path, spans)) == ["A"]


def test_span_root_id_pins_a_single_root():
    g = Graph(trace(*MULTI_ROOT), "", "", "T1.json", rootSpanId="B")
    assert [n.sid for n in g.analysisRoots(True)] == ["B"]


# --- every root is prepared the same way --------------------------------------

def test_every_root_is_sanitized(tmp_path):
    """L starts after root B ended. It must not reach B's critical path."""
    spans = [
        span("A", "opA", "pA", start=0, dur=100),
        span("B", "opA", "pA", start=1000, dur=100),
        span("L", "late", "pB", parent="B", start=2000, dur=5000),
    ]
    metrics = roots(tmp_path, spans)
    assert root_ids(metrics) == ["A", "B"]
    for m in metrics:
        assert list(m.CPMetrics.profile) == ["[svcA] opA"]
        assert m.CPMetrics.profile["[svcA] opA"].excl == 100


def test_merged_roots_equal_runs_pinned_to_each_root(tmp_path):
    """Each merged root gives the critical path of a run pinned to that span."""
    for m in roots(tmp_path, MULTI_ROOT):
        pinned = Graph(trace(*MULTI_ROOT), "", "", "T1.json", rootSpanId=m.rootSpanID)
        assert set(m.CPMetrics.profile) == {pinned.getCallPath(n) for n in pinned.findCriticalPath()}
        assert m.CPMetrics.profile["[svcA] opA"].inc == pinned.rootNode.duration


# --- duration window ----------------------------------------------------------

def test_first_root_is_the_first_match_in_the_duration_window(tmp_path):
    spans = [
        span("X", "opX", "pX", start=0, dur=9000),
        span("A", "opA", "pA", parent="X", start=10, dur=10),
        span("S", "opA", "pA", parent="X", start=100, dur=200),
    ]
    assert root_ids(roots(tmp_path, spans, minDuration=50)) == ["S"]
    assert root_ids(roots(tmp_path, spans)) == ["A"]
    assert roots(tmp_path, spans, minDuration=500) == []


def test_duration_window_applies_to_each_root(tmp_path):
    assert root_ids(roots(tmp_path, MULTI_ROOT, minDuration=150)) == ["B", "C"]
    assert root_ids(roots(tmp_path, MULTI_ROOT, maxDuration=250)) == ["A", "B"]


# --- outputs ------------------------------------------------------------------

def run_light(tmp_path, spans, **overrides):
    traces = tmp_path / "traces"
    traces.mkdir()
    path = traces / "T1.json"
    path.write_text(json.dumps(trace(*spans)))
    out = tmp_path / "out"
    out.mkdir()
    c = config(**overrides)
    c.jaegerTraceFiles = [str(path)]
    c.getOutputDir = lambda: str(out)
    lightProcess(c)
    return out


def test_light_cct_averages_over_roots(tmp_path):
    out = run_light(tmp_path, MULTI_ROOT)
    # Three roots of 100, 200 and 300us: mean exclusive time 200, frequency 3.
    assert (out / LIGHT_CCT).read_text() == "[svcA] opA 200 <<3>>\n"


def test_light_cct_without_merge_all_roots(tmp_path):
    out = run_light(tmp_path, MULTI_ROOT, mergeAllRoots=False)
    assert (out / LIGHT_CCT).read_text() == "[svcA] opA 100 <<1>>\n"


ERRORS = [
    span("A", "opA", "pA", start=0, dur=100, error=True),
    span("B", "opA", "pA", start=1000, dur=100, error=True),
]


@pytest.mark.parametrize("root, wantCount", [
    pytest.param(ROOT_ANALYSIS, 2, id="analysis root: summed over the roots"),
    pytest.param(ROOT_TRACE, 1, id="trace root: once per trace"),
])
def test_error_breakdown_is_one_per_trace(tmp_path, root, wantCount):
    out = run_light(tmp_path, ERRORS, errorBreakdown=MODE_ORIGINS, errorBreakdownRoot=root)
    doc = json.loads((out / "error-breakdown.json").read_text())
    assert doc["traces"] == 1
    assert sum(p["count"] for p in doc["paths"]) == wantCount


def test_drag_report_covers_each_root_subtree_once(tmp_path):
    spans = [
        span("A", "opA", "pA", start=0, dur=100),
        span("A1", "opB", "pB", parent="A", start=10, dur=50),
        span("B", "opA", "pA", start=1000, dur=100),
    ]
    metrics = roots(tmp_path, spans)
    counts = [{path: agg.span_count for path, agg in m.slackDragPerCallPath.items()} for m in metrics]
    assert counts == [{"[svcA] opA": 1, "[svcA] opA->[svcB] opB": 1}, {"[svcA] opA": 1}]


# --- outside the light pipeline -----------------------------------------------

def test_heavy_pipeline_keeps_one_root_per_trace(tmp_path):
    path = tmp_path / "T1.json"
    path.write_text(json.dumps(trace(*MULTI_ROOT)))
    c = common.Config(serviceName="svcA", operationName="opA", rootTrace=False)
    assert root_ids(processRoots(str(path), c)) == ["A"]
    assert process(str(path), c).rootSpanID == "A"
