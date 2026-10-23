"""Light-mode cohort filters: structural orphan detection, incomplete analysis
subtrees, --rejectSubtreeIncomplete, and --minDuration/--maxDuration.

Mirrors go/crisp/light_cohort_filters_test.go on the same traces.
"""
import json
import types
from pathlib import Path

import pytest

import crisp.common as common
import crisp.utils.span_utils as span_utils
from crisp.graph import Graph
from crisp.process_trace import lightProcess, process

LIGHT_CCT = "light-flame-graph-P100.cct"


def span(spanId, op, pid, parent=None, start=1000, dur=100, refType="CHILD_OF"):
    refs = [{"traceID": "T1", "spanID": parent, "refType": refType}] if parent else []
    return {
        "traceID": "T1", "spanID": spanId, "operationName": op, "references": refs,
        "startTime": start, "duration": dur, "processID": pid,
    }


def trace(*spans):
    services = {"pA": "svcA", "pB": "svcB", "pC": "svcC", "pX": "svcX", "pZ": "svcZ", "pP": "svcProxy"}
    pids = {s["processID"] for s in spans}
    return {"data": [{
        "traceID": "T1",
        "spans": list(spans),
        "processes": {pid: {"serviceName": services[pid]} for pid in sorted(pids)},
    }]}


# svcA/opA (the analysis root, 1000us) -> svcB/opB -> svcC/opC.
CHAIN = [
    span("1", "opA", "pA", start=1000, dur=1000),
    span("2", "opB", "pB", parent="1", start=1100, dur=600),
    span("3", "opC", "pC", parent="2", start=1200, dur=300),
]


# The same chain under a trace root svcX/opX.
UNDER_X = [
    span("0", "opX", "pX", start=900, dur=1200),
    span("1", "opA", "pA", parent="0", start=1000, dur=1000),
    *CHAIN[1:],
]


def orphan(spanId, pid):
    """A span whose CHILD_OF parent is not in the trace."""
    return span(spanId, "lost", pid, parent="missing-" + spanId, start=1300, dur=50)


@pytest.fixture
def proxy():
    """Registers svcProxy/opProxy as a proxy for the duration of a test."""
    saved = list(span_utils.PROXY_SERVICE_OP_PAIRS)
    span_utils.PROXY_SERVICE_OP_PAIRS.append(("svcProxy", "opProxy"))
    yield
    span_utils.PROXY_SERVICE_OP_PAIRS[:] = saved


def run_process(tmp_path, data, filterProxy=False, **config):
    path = tmp_path / "T1.json"
    path.write_text(json.dumps(data))
    c = common.Config(serviceName="svcA", operationName="opA", rootTrace=False,
                      filterProxy=filterProxy, **config)
    return process(str(path), c)


# --- Graph.orphanSpanIds -----------------------------------------------------

@pytest.mark.parametrize("spans, filterProxy, wantIds, wantServices", [
    pytest.param(CHAIN, False, set(), set(), id="complete"),
    pytest.param(CHAIN + [orphan("9", "pZ")], False, {"9"}, {"svcZ"}, id="orphan"),
    pytest.param(
        CHAIN + [span("9", "async", "pB", parent="missing", refType="FOLLOWS_FROM")],
        False, set(), set(), id="dangling FOLLOWS_FROM is not an orphan"),
    # Short-wiring a proxy with no parent makes its child a root, not an orphan.
    pytest.param(
        [span("1", "opProxy", "pP", dur=1000), span("2", "opA", "pA", parent="1", start=1010, dur=900)],
        True, set(), set(), id="child of a short-wired root proxy"),
    # When the proxy's own parent was dropped, the proxy is the orphan.
    pytest.param(
        [span("1", "opProxy", "pP", parent="dropped", dur=1000), span("2", "opA", "pA", parent="1", start=1010, dur=900)],
        True, {"1"}, {"svcProxy"}, id="proxy with a dropped parent"),
])
def test_orphan_spans(proxy, spans, filterProxy, wantIds, wantServices):
    g = Graph(trace(*spans), "svcA", "opA", "T1.json", rootTrace=False, filterProxy=filterProxy)
    assert g.orphanSpanIds == wantIds
    assert g.get_orphan_services() == wantServices


# --- process(): isIncomplete / isSubtreeIncomplete ---------------------------

@pytest.mark.parametrize("spans, wantIncomplete, wantSubtreeIncomplete", [
    pytest.param(CHAIN, False, False, id="complete"),
    pytest.param(CHAIN + [orphan("9", "pB")], True, True, id="orphan in a child's service"),
    pytest.param(CHAIN + [orphan("9", "pC")], True, True, id="orphan in a deep descendant's service"),
    pytest.param(CHAIN + [orphan("9", "pA")], True, True, id="orphan in the analysis root's service"),
    pytest.param(CHAIN + [orphan("9", "pZ")], True, False, id="orphan in an unrelated service"),
    pytest.param(UNDER_X + [orphan("9", "pX")], True, False, id="orphan service only above the analysis root"),
    # Only the orphan's own service counts; its children have their parent.
    pytest.param(
        CHAIN + [orphan("9", "pZ"), span("10", "opB", "pB", parent="9", start=1310, dur=20)],
        True, False, id="children of an orphan are not orphans"),
])
def test_incompleteness_flags(tmp_path, spans, wantIncomplete, wantSubtreeIncomplete):
    metrics = run_process(tmp_path, trace(*spans))
    assert metrics is not None
    assert metrics.isIncomplete == wantIncomplete
    assert metrics.isSubtreeIncomplete == wantSubtreeIncomplete


def test_incompleteness_ignores_span_warnings(tmp_path):
    """Detection is structural: a warning alone is not an orphan, and an orphan needs no warning."""
    warned = dict(CHAIN[1], warnings=["invalid parent span IDs=1; skipping clock skew adjustment"])
    metrics = run_process(tmp_path, trace(CHAIN[0], warned, CHAIN[2]))
    assert not metrics.isIncomplete


def test_short_wired_root_proxy_is_complete(tmp_path, proxy):
    spans = [span("1", "opProxy", "pP", dur=1000), span("2", "opA", "pA", parent="1", start=1010, dur=900),
             span("3", "opB", "pB", parent="2", start=1020, dur=500)]
    metrics = run_process(tmp_path, trace(*spans), filterProxy=True)
    assert not metrics.isIncomplete
    assert not metrics.isSubtreeIncomplete


# --- process(): --minDuration / --maxDuration ------------------------------

# The trace root svcX/opX is 200us; the analysis root svcA/opA is 100us.
NESTED = trace(
    span("0", "opX", "pX", start=1000, dur=200),
    span("1", "opA", "pA", parent="0", start=1010, dur=100),
    span("2", "opB", "pB", parent="1", start=1020, dur=50),
)


@pytest.mark.parametrize("bounds, kept", [
    pytest.param({}, True, id="no bounds"),
    pytest.param({"minDuration": 100}, True, id="min equal to duration"),
    pytest.param({"minDuration": 101}, False, id="min above duration"),
    pytest.param({"maxDuration": 100}, True, id="max equal to duration"),
    pytest.param({"maxDuration": 99}, False, id="max below duration"),
    pytest.param({"maxDuration": 0}, False, id="max zero is a bound"),
    pytest.param({"minDuration": 150}, False, id="min applies to the analysis root, not the trace root"),
    pytest.param({"maxDuration": 150}, True, id="max applies to the analysis root, not the trace root"),
])
def test_duration_bounds(tmp_path, bounds, kept):
    metrics = run_process(tmp_path, NESTED, lightMode=True, **bounds)
    assert (metrics is not None) == kept


def test_duration_bounds_are_light_mode_only(tmp_path):
    assert run_process(tmp_path, NESTED, lightMode=False, minDuration=101) is not None


def test_duration_bounds_apply_to_conformance_alone(tmp_path):
    """--conformance without --lightMode runs the light-mode pipeline, so the bounds apply."""
    assert run_process(tmp_path, NESTED, lightMode=False, conformance=True, minDuration=101) is None


# --- lightProcess(): --rejectSubtreeIncomplete ------------------------------

def run_light(dirPath: Path, files: dict, **config) -> bytes:
    dirPath.mkdir()
    paths = []
    for name, data in files.items():
        p = dirPath / name
        p.write_text(json.dumps(data))
        paths.append(str(p))
    c = common.Config(serviceName="svcA", operationName="opA", rootTrace=False, lightMode=True,
                      file=types.SimpleNamespace(name=paths[0]), **config)
    c.jaegerTraceFiles = paths
    assert lightProcess(c) == 0
    return (dirPath / LIGHT_CCT).read_bytes()


def test_reject_subtree_incomplete(tmp_path):
    complete = trace(*CHAIN)
    upstream = trace(*CHAIN, orphan("9", "pZ"))  # incomplete, but not under the analysis root
    downstream = trace(*[dict(s, duration=s["duration"] * 3) for s in CHAIN], orphan("9", "pB"))
    files = {"a.json": complete, "b.json": upstream, "c.json": downstream}

    rejected = run_light(tmp_path / "rejected", files, rejectSubtreeIncomplete=True)
    expected = run_light(tmp_path / "expected", {"a.json": complete, "b.json": upstream})
    kept = run_light(tmp_path / "kept", files)

    assert rejected == expected, "the rejected trace must not reach the outputs"
    assert kept != expected, "without the flag the incomplete trace is analyzed"


def test_reject_subtree_incomplete_everything_rejected(tmp_path):
    out = run_light(tmp_path / "all", {"c.json": trace(*CHAIN, orphan("9", "pC"))}, rejectSubtreeIncomplete=True)
    assert out == run_light(tmp_path / "none", {"x.json": trace(span("1", "opX", "pX"))})
