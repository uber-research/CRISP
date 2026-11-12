"""Trace fragments: pruneToFragment and the light pipeline's --fragment
(see crisp/fragment.py and CONFORMANCE.md, "Trace fragments").

Mirrors go/crisp/fragment_test.go on the same traces.
"""
import copy
import json
from pathlib import Path

import pytest

import crisp.common as common
import crisp.utils.span_utils as span_utils
from crisp.fragment import pruneToFragment
from crisp.process_trace import checkFragmentConfig, lightProcess, processRoots

FIXTURES = Path(__file__).resolve().parent.parent / "test_cases"


def span(spanId, op, pid, *parents, start=1000, dur=100, refType="CHILD_OF"):
    refs = [{"traceID": "T1", "spanID": parent, "refType": refType} for parent in parents]
    return {
        "traceID": "T1", "spanID": spanId, "operationName": op, "references": refs,
        "startTime": start, "duration": dur, "processID": pid,
    }


def trace(*spans):
    services = {"pA": "svcA", "pB": "svcB", "pX": "svcX", "pZ": "svcZ", "pP": "svcProxy"}
    return {"data": [{
        "traceID": "T1",
        "spans": list(spans),
        "processes": {pid: {"serviceName": name} for pid, name in services.items()},
    }]}


# X is the trace root. Its child A matches svcA/opA and calls B; its child U
# and U's child V are unrelated. M is a second trace root that matches. O is
# an orphan: its parent is not in the trace. W is a child of the orphan.
TRACE = [
    span("X", "opX", "pX", start=0, dur=1000),
    span("U", "opU", "pZ", "X", start=10, dur=100),
    span("V", "opV", "pZ", "U", start=20, dur=50),
    span("A", "opA", "pA", "X", start=200, dur=500),
    span("B", "opB", "pB", "A", start=250, dur=300),
    span("M", "opA", "pA", start=2000, dur=400),
    span("O", "opO", "pZ", "missing", start=3000, dur=100),
    span("W", "opW", "pZ", "O", start=3010, dur=50),
]


def fragment_spans(spans, service="svcA", operation="opA"):
    fragment = pruneToFragment(trace(*spans), service, operation)
    return {s["spanID"]: [r["spanID"] for r in s["references"]] for s in fragment["data"][0]["spans"]}


# --- pruneToFragment ------------------------------------------------------------

def test_fragment_keeps_matches_subtrees_ancestors_and_orphans():
    # U, V and W are gone. Every kept span keeps its parent reference.
    assert fragment_spans(TRACE) == {"X": [], "A": ["X"], "B": ["A"], "M": [], "O": ["missing"]}


def test_fragment_keeps_span_order_and_processes():
    data = trace(*TRACE)
    fragment = pruneToFragment(data, "svcA", "opA")
    assert [s["spanID"] for s in fragment["data"][0]["spans"]] == ["X", "A", "B", "M", "O"]
    assert fragment["data"][0]["processes"] == data["data"][0]["processes"]


def test_fragment_does_not_modify_the_trace():
    data = trace(*TRACE)
    before = copy.deepcopy(data)
    pruneToFragment(data, "svcA", "opA")
    assert data == before


def test_fragment_without_a_match_keeps_only_orphans():
    assert fragment_spans(TRACE, operation="none") == {"O": ["missing"]}


def test_fragment_references_to_removed_spans_are_dropped():
    spans = [
        span("X", "opX", "pX", start=0, dur=1000),
        span("A", "opA", "pA", "X", start=200, dur=500),
        span("U", "opU", "pZ", "X", start=10, dur=100),
        # K's parent is A. Its earlier reference to the removed U goes away.
        span("K", "opK", "pB", "U", "A", start=250, dur=100),
        # F follows from the removed U, from A, and from a span of another trace.
        span("F", "opF", "pB", "A", start=260, dur=10),
    ]
    spans[4]["references"] += [
        {"traceID": "T1", "spanID": "U", "refType": "FOLLOWS_FROM"},
        {"traceID": "T1", "spanID": "A", "refType": "FOLLOWS_FROM"},
        {"traceID": "T2", "spanID": "elsewhere", "refType": "FOLLOWS_FROM"},
    ]
    assert fragment_spans(spans) == {"X": [], "A": ["X"], "K": ["A"], "F": ["A", "A", "elsewhere"]}


def test_fragment_keeps_a_test_trace_marker(monkeypatch, tmp_path):
    """Graph reads the last CHILD_OF reference as the parent. When that parent
    is removed, an earlier reference must not become the parent."""
    monkeypatch.setattr(span_utils, "TEST_TRACE_OP_PREFIXES", ["test-"])
    spans = [
        span("X", "opX", "pX", start=0, dur=1000),
        span("A", "opA", "pA", "X", start=200, dur=500),
        span("U", "opU", "pZ", "X", start=10, dur=100),
        # T marks the trace as a test trace. Its parent is U, which is removed.
        span("T", "test-probe", "pZ", "A", "U", start=600, dur=90),
        span("T2", "test-probe", "pZ", "U", start=30, dur=50),
    ]
    assert fragment_spans(spans) == {"X": [], "A": ["X"], "T": []}
    # As a child of A, T would be on A's critical path.
    assert_fragment_outputs(tmp_path, spans)
    assert_fragment_outputs(tmp_path / "ignored", spans, ignoreTestTraces=True)


def test_fragment_ancestor_walk_ends_on_a_cycle():
    spans = [
        span("P", "opP", "pX", "Q", start=0, dur=1000),
        span("Q", "opQ", "pX", "P", start=0, dur=1000),
        span("A", "opA", "pA", "P", start=200, dur=500),
        span("U", "opU", "pZ", start=10, dur=100),
    ]
    assert fragment_spans(spans) == {"P": ["Q"], "Q": ["P"], "A": ["P"]}


# --- --fragment in the light pipeline -------------------------------------------

def config(**overrides):
    args = dict(serviceName="svcA", operationName="opA", rootTrace=False, lightMode=True, fragment=True)
    args.update(overrides)
    return common.Config(**args)


@pytest.mark.parametrize("overrides, message", [
    (dict(lightMode=False), "--lightMode"),
    (dict(rootTrace=True), "--rootTrace"),
    (dict(errorBreakdown="origins"), "--errorBreakdownRoot trace"),
    (dict(errorBreakdown="propToRoot", errorBreakdownRoot="trace"), "--errorBreakdownRoot trace"),
])
def test_fragment_rejects_options_that_need_the_whole_trace(overrides, message):
    with pytest.raises(ValueError, match=message):
        checkFragmentConfig(config(**overrides))
    c = config(**overrides)
    c.jaegerTraceFiles = []
    if c.lightMode:
        with pytest.raises(ValueError, match=message):
            lightProcess(c)


@pytest.mark.parametrize("overrides", [
    dict(),
    dict(conformance=True, lightMode=False),
    dict(errorBreakdown="origins", errorBreakdownRoot="analysis"),
    dict(fragment=False, rootTrace=True, errorBreakdown="origins"),
])
def test_fragment_accepts_the_other_options(overrides):
    checkFragmentConfig(config(**overrides))


def drag_paths(tmp_path, spans, **overrides):
    path = tmp_path / "T1.json"
    path.write_text(json.dumps(trace(*spans)))
    metrics = processRoots(str(path), config(**overrides))
    return [sorted(m.slackDragPerCallPath) for m in metrics]


def test_fragment_drag_report_covers_only_the_analysis_subtree(tmp_path):
    single = TRACE[:5]  # one analysis root, A
    subtree = ["[svcA] opA", "[svcA] opA->[svcB] opB"]
    assert drag_paths(tmp_path, single) == [subtree]
    # Without --fragment a single root reports every span of the trace.
    whole = drag_paths(tmp_path, single, fragment=False)
    assert len(whole) == 1 and set(subtree) < set(whole[0]) and len(whole[0]) == 5


# --- a fragment gives the outputs of its trace ----------------------------------

OPTION_SETS = {
    "default": dict(),
    "no merge": dict(mergeAllRoots=False),
    "cohort filters": dict(minDuration=20, maxDuration=5_000_000, rejectSubtreeIncomplete=True),
    "error origins": dict(errorBreakdown="origins", errorBreakdownRoot="analysis"),
    "error propToRoot": dict(errorBreakdown="propToRoot", errorBreakdownRoot="analysis", filterProxy=True),
    "ignore test traces": dict(ignoreTestTraces=True),
}


def fixture_files():
    return sorted(
        p for pattern in ("*.json", "err_pattern*/*.json", "error_breakdown/*.json") for p in FIXTURES.glob(pattern)
    )


def targets(data):
    """Every (service, operation) of the trace, in span order."""
    names = {pid: p["serviceName"] for item in data["data"] for pid, p in item["processes"].items()}
    seen = []
    for item in data["data"]:
        for s in item["spans"]:
            target = (names.get(s["processID"]), s["operationName"])
            if target not in seen:
                seen.append(target)
    return seen


def light_outputs(directory, name, data, service, operation, **overrides):
    """Run the light pipeline on one trace and return its output files."""
    traces = directory / "traces"
    out = directory / "out"
    traces.mkdir(parents=True)
    out.mkdir()
    path = traces / name
    path.write_text(json.dumps(data))
    c = common.Config(
        serviceName=service, operationName=operation, rootTrace=False, lightMode=True, conformance=True, **overrides
    )
    c.jaegerTraceFiles = [str(path)]
    c.getOutputDir = lambda: str(out)
    lightProcess(c)
    return {p.name: p.read_bytes() for p in sorted(out.iterdir())}


def assert_fragment_outputs(directory, spans, **options):
    """The light outputs for the fragment of a trace are those for the trace."""
    data = trace(*spans)
    fragment = pruneToFragment(data, "svcA", "opA")
    full = light_outputs(directory / "full", "T1.json", data, "svcA", "opA", fragment=True, **options)
    pruned = light_outputs(directory / "pruned", "T1.json", fragment, "svcA", "opA", fragment=True, **options)
    assert pruned == full
    return full


def test_fragment_keeps_the_orphans_that_make_a_subtree_incomplete(tmp_path):
    # The orphan Q is in svcB, which also appears under the root A: A's
    # subtree is incomplete. The other root, M, is complete.
    spans = TRACE + [span("Q", "opQ", "pB", "gone", start=4000, dur=10)]
    rejected = assert_fragment_outputs(tmp_path / "reject", spans, rejectSubtreeIncomplete=True)
    assert rejected["light-flame-graph-P100.cct"] == b"[svcA] opA 400 <<1>>\n"
    kept = assert_fragment_outputs(tmp_path / "keep", spans)
    assert kept["light-flame-graph-P100.cct"] != rejected["light-flame-graph-P100.cct"]


# With --filterProxy the children of a proxy span attach to the proxy's parent.
# X calls A through the proxy P1, next to the unrelated U. A calls B through
# the proxy P2. M matches under the proxy P3, whose parent is not in the trace.
PROXIED = [
    span("X", "opX", "pX", start=0, dur=2000),
    span("P1", "opProxy", "pP", "X", start=10, dur=900),
    span("U", "opU", "pZ", "P1", start=20, dur=100),
    span("A", "opA", "pA", "P1", start=200, dur=500),
    span("P2", "opProxy", "pP", "A", start=250, dur=300),
    span("B", "opB", "pB", "P2", start=260, dur=200),
    span("P3", "opProxy", "pP", "missing", start=3000, dur=500),
    span("M", "opA", "pA", "P3", start=3010, dur=400),
]


@pytest.fixture
def proxy():
    """Registers svcProxy/opProxy as a proxy for the duration of a test."""
    saved = list(span_utils.PROXY_SERVICE_OP_PAIRS)
    span_utils.PROXY_SERVICE_OP_PAIRS.append(("svcProxy", "opProxy"))
    yield
    span_utils.PROXY_SERVICE_OP_PAIRS[:] = saved


@pytest.mark.parametrize("options", [
    dict(filterProxy=True),
    dict(filterProxy=True, rejectSubtreeIncomplete=True),
    dict(filterProxy=True, mergeAllRoots=False),
    dict(),
], ids=str)
def test_fragment_gives_the_outputs_of_its_trace_through_proxies(tmp_path, proxy, options):
    data = trace(*PROXIED)
    fragment = pruneToFragment(data, "svcA", "opA")
    assert [s["spanID"] for s in fragment["data"][0]["spans"]] == ["X", "P1", "A", "P2", "B", "P3", "M"]
    full = light_outputs(tmp_path / "full", "T1.json", data, "svcA", "opA", fragment=True, **options)
    pruned = light_outputs(tmp_path / "pruned", "T1.json", fragment, "svcA", "opA", fragment=True, **options)
    assert pruned == full
    assert full["light-flame-graph-P100.cct"]


@pytest.mark.parametrize("fixture", fixture_files(), ids=lambda p: str(p.relative_to(FIXTURES)))
def test_fragment_gives_the_outputs_of_its_trace(tmp_path, fixture):
    data = json.loads(fixture.read_text())
    for t, (service, operation) in enumerate(targets(data)):
        fragment = pruneToFragment(data, service, operation)
        for o, (label, options) in enumerate(OPTION_SETS.items()):
            where = "%s/%s, %s" % (service, operation, label)
            run = tmp_path / ("%d-%d" % (t, o))
            full = light_outputs(run / "full", fixture.name, data, service, operation, fragment=True, **options)
            pruned = light_outputs(run / "pruned", fixture.name, fragment, service, operation, fragment=True, **options)
            assert pruned == full, where
            # --fragment only changes the drag report.
            plain = light_outputs(run / "plain", fixture.name, data, service, operation, **options)
            plain.pop("slackDrag.csv", None)
            full.pop("slackDrag.csv", None)
            assert plain == full, where
