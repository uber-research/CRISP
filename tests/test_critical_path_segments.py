import json
from pathlib import Path

import pytest

from crisp.conformance import derive_root_span
from crisp.critical_path_segments import (
    CriticalPathSpan,
    canonical_segments_json,
    critical_path_segments,
    main,
)
from crisp.graph import Graph, GraphNode
from crisp.shared.models import SpanKind

REPO_ROOT = Path(__file__).resolve().parent.parent
TEST_CASES = REPO_ROOT / "test_cases"
FIXTURES = sorted(TEST_CASES.glob("*.json")) + sorted(TEST_CASES.glob("err_pattern*/*.json"))


def _span(span_id, op, start, duration, parent_id=None):
    return {
        "traceID": "T",
        "spanID": span_id,
        "operationName": op,
        "startTime": start,
        "duration": duration,
        "processID": "P1",
        "warnings": None,
        "references": ([] if parent_id is None else [{"refType": "CHILD_OF", "traceID": "T", "spanID": parent_id}]),
    }


def _build_graph(spans, root_op=None):
    data = {"data": [{"processes": {"P1": {"serviceName": "S1", "tags": []}}, "traceID": "T", "spans": spans}]}
    if root_op is None:
        return Graph(data, "S1", spans[0]["operationName"], "", True)
    return Graph(data, "S1", root_op, "", False)


def _build_manual_graph(node_specs):
    """node_specs: (sid, startTime, duration, parentSid); the first is the root.

    Skips sanitization, so children may extend outside their parent the way
    unsanitized client/server spans do.
    """
    g = Graph([], "S1", "root", "nofile.txt", skipInitializationForTest=True)
    g.processName = {"P1": "S1"}
    nodes = {}
    for sid, start, duration, parent in node_specs:
        node = GraphNode(sid=sid, startTime=start, duration=duration, parentSpanId=None, opName=sid,
                         processID="P1", spanKind=SpanKind.UNKNOWN, peerService="S1", returnError=False)
        nodes[sid] = node
        g.nodeHT[sid] = node
        if parent is not None:
            node.setParent(nodes[parent])
            nodes[parent].addChild(node)
    g.rootNode = nodes[node_specs[0][0]]
    return g


def _segments(spans):
    return {s.span_id: list(s.segments) for s in spans}


def _exclusive(spans):
    return {s.span_id: s.exclusive for s in spans}


def test_long_span_partly_on_critical_path():
    # R [0,1000] -> A [0,1000] -> B [100,300], C [600,700]: only A's own time
    # outside B and C is A's.
    g = _build_graph([
        _span("R", "root", 0, 1000),
        _span("A", "a", 0, 1000, "R"),
        _span("B", "b", 100, 200, "A"),
        _span("C", "c", 600, 100, "A"),
    ])
    spans = g.criticalPathSegments()
    assert [s.span_id for s in spans] == ["R", "A", "C", "B"]
    assert _segments(spans) == {"R": [], "A": [(0, 100), (300, 600), (700, 1000)], "B": [(100, 300)], "C": [(600, 700)]}
    assert _exclusive(spans) == {"R": 0, "A": 700, "B": 200, "C": 100}
    assert spans[0].parent_span_id is None
    assert [s.parent_span_id for s in spans[1:]] == ["R", "A", "A"]
    assert (spans[1].start_time, spans[1].end_time, spans[1].service, spans[1].operation) == (0, 1000, "S1", "a")


def test_back_to_back_children_tile_the_parent():
    g = _build_graph([
        _span("R", "root", 0, 300),
        _span("A", "a", 0, 100, "R"),
        _span("B", "b", 100, 100, "R"),
        _span("C", "c", 200, 100, "R"),
    ])
    spans = g.criticalPathSegments()
    assert _segments(spans) == {"R": [], "A": [(0, 100)], "B": [(100, 200)], "C": [(200, 300)]}
    assert all(sum(e - s for s, e in sp.segments) == sp.exclusive for sp in spans)


def test_leaf_root_is_one_segment():
    spans = _build_graph([_span("R", "root", 5, 10)]).criticalPathSegments()
    assert spans == [CriticalPathSpan("R", None, "S1", "root", 5, 15, 10, ((5, 15),))]


def test_overlapping_siblings_within_allowance_share_the_overlap():
    # A [100,505] and B [500,800] overlap by 5us, under 1% of R's duration, so
    # both are on the critical path; [500,505] is in both, and R's exclusive
    # (1000 - 405 - 300) is 5us less than its segments.
    g = _build_graph([
        _span("R", "root", 0, 1000),
        _span("A", "a", 100, 405, "R"),
        _span("B", "b", 500, 300, "R"),
    ])
    spans = g.criticalPathSegments()
    assert _segments(spans) == {"R": [(0, 100), (800, 1000)], "A": [(100, 505)], "B": [(500, 800)]}
    assert _exclusive(spans)["R"] == 295


def test_child_ending_after_parent_is_clipped_to_parent():
    # S [400,700] runs past its parent A [0,500], as an unsanitized
    # client/server pair can.
    g = _build_manual_graph([("R", 0, 1000, None), ("A", 0, 500, "R"), ("S", 400, 300, "A")])
    spans = g.criticalPathSegments()
    assert _segments(spans) == {"R": [(500, 1000)], "A": [(0, 400)], "S": [(400, 500)]}
    assert _exclusive(spans) == {"R": 500, "A": 200, "S": 300}


def test_child_starting_before_parent_is_clipped_to_parent():
    g = _build_manual_graph([("R", 100, 900, None), ("A", 0, 300, "R")])
    assert _segments(g.criticalPathSegments()) == {"R": [(300, 1000)], "A": [(100, 300)]}


def test_child_entirely_outside_parent_has_no_segments():
    g = _build_manual_graph([("R", 0, 100, None), ("A", 200, 100, "R"), ("B", 250, 10, "A")])
    assert _segments(g.criticalPathSegments()) == {"R": [(0, 100)], "A": [], "B": []}


def test_sanitized_client_server_spans_and_zero_duration_child():
    # test_cases/26.json: server S runs past its client A (504us under a 500us
    # client, so sanitization shortens S's duration but keeps its endTime),
    # S's child X is clipped with S, server T starts before its client B, and
    # the zero-duration Z splits R's time into adjacent windows.
    spans = _fixture_graph(TEST_CASES / "26.json").criticalPathSegments()
    assert [(s.span_id, s.start_time, s.end_time, s.exclusive, list(s.segments)) for s in spans] == [
        ("R", 0, 1000, 300, [(0, 100), (600, 700), (700, 750), (950, 1000)]),
        ("B", 750, 950, 0, [(945, 950)]),
        ("T", 745, 945, 200, [(750, 945)]),
        ("Z", 700, 700, 0, []),
        ("A", 100, 600, 0, [(100, 105)]),
        ("S", 105, 609, 441, [(105, 550)]),
        ("X", 550, 609, 59, [(550, 600)]),
    ]


def test_analysis_root_below_trace_root():
    g = _build_graph([
        _span("R", "root", 0, 1000),
        _span("A", "a", 100, 500, "R"),
        _span("B", "b", 200, 100, "A"),
    ], root_op="a")
    spans = g.criticalPathSegments()
    assert [(s.span_id, s.parent_span_id) for s in spans] == [("A", None), ("B", "A")]
    assert _segments(spans) == {"A": [(100, 200), (300, 600)], "B": [(200, 300)]}


def test_explicit_cp_with_nested_siblings():
    # findCriticalPath never yields nested siblings, but a caller-supplied cp
    # can: B [200,300] lies inside its sibling A [100,900].
    g = _build_manual_graph([("R", 0, 1000, None), ("A", 100, 800, "R"), ("B", 200, 100, "R")])
    cp = [g.nodeHT["R"], g.nodeHT["A"], g.nodeHT["B"]]
    assert _segments(g.criticalPathSegments(cp)) == {"R": [(0, 100), (900, 1000)], "A": [(100, 900)], "B": [(200, 300)]}


def test_empty_critical_path():
    assert critical_path_segments(None, []) == []


def test_explicit_cp_matches_default():
    g = _build_graph([_span("R", "root", 0, 100), _span("A", "a", 10, 20, "R")])
    assert g.criticalPathSegments(g.findCriticalPath()) == g.criticalPathSegments()


def test_canonical_json_format():
    spans = [
        CriticalPathSpan("R", None, "S1", "root", 0, 10, 4, ((0, 2), (8, 10))),
        CriticalPathSpan("A", "R", "S1", "a", 2, 8, 6, ((2, 8),)),
    ]
    assert canonical_segments_json(spans) == (
        '{\n  "spans": [\n'
        '    {\n      "endTime": 10,\n      "exclusive": 4,\n      "operation": "root",\n'
        '      "parentSpanID": null,\n      "segments": [\n        [\n          0,\n          2\n        ],\n'
        '        [\n          8,\n          10\n        ]\n      ],\n      "service": "S1",\n'
        '      "spanID": "R",\n      "startTime": 0\n    },\n'
        '    {\n      "endTime": 8,\n      "exclusive": 6,\n      "operation": "a",\n'
        '      "parentSpanID": "R",\n      "segments": [\n        [\n          2,\n          8\n        ]\n'
        '      ],\n      "service": "S1",\n      "spanID": "A",\n      "startTime": 2\n    }\n'
        '  ]\n}\n'
    )
    assert canonical_segments_json([]) == '{\n  "spans": []\n}\n'


def _fixture_graph(fixture):
    with open(fixture, encoding="utf-8") as f:
        data = json.load(f)
    service, operation = derive_root_span(data)
    return Graph(data, service, operation, str(fixture), True)


@pytest.mark.parametrize("fixture", FIXTURES, ids=lambda p: str(p.relative_to(TEST_CASES)))
def test_fixture_segments_cover_root_exactly(fixture):
    g = _fixture_graph(fixture)
    cp = g.findCriticalPath()
    spans = g.criticalPathSegments(cp)
    assert [s.span_id for s in spans] == [n.sid for n in cp]

    root = spans[0]
    cursor, overlap = root.start_time, False
    for start, end in sorted(seg for s in spans for seg in s.segments):
        assert root.start_time <= start < end <= root.end_time
        assert start <= cursor, "gap in critical-path coverage"
        overlap = overlap or start < cursor
        cursor = max(cursor, end)
    assert cursor == root.end_time

    children: dict[str, list] = {s.span_id: [] for s in spans}
    for s in spans[1:]:
        children[s.parent_span_id].append((s.start_time, s.end_time))
    kids_overlap = {sid: any(a[1] > b[0] for a, b in zip(sorted(k), sorted(k)[1:])) for sid, k in children.items()}
    assert overlap <= any(kids_overlap.values())

    by_id = {s.span_id: s for s in spans}
    windows = {root.span_id: (root.start_time, root.end_time)}
    for s in spans[1:]:
        lo, hi = windows[s.parent_span_id]
        windows[s.span_id] = (min(max(s.start_time, lo), hi), max(min(s.end_time, hi), min(max(s.start_time, lo), hi)))
    for sid, (lo, hi) in windows.items():
        s = by_id[sid]
        whole = (lo, hi) == (s.start_time, s.end_time)
        kids_inside = all(s.start_time <= a and b <= s.end_time for a, b in children[sid])
        if whole and kids_inside and not kids_overlap[sid]:
            assert sum(e - b for b, e in s.segments) == s.exclusive, sid


def test_main_prints_canonical_json(capsys):
    fixture = TEST_CASES / "1.json"
    service, operation = derive_root_span(json.loads(fixture.read_text(encoding="utf-8")))
    assert main(["--file", str(fixture), "-s", service, "-a", operation, "--rootTrace"]) == 0
    assert capsys.readouterr().out == canonical_segments_json(_fixture_graph(fixture).criticalPathSegments())


def test_main_fails_without_root(capsys):
    assert main(["--file", str(TEST_CASES / "1.json"), "-s", "nope", "-a", "nope", "--rootTrace"]) == 1
    assert "no analysis root" in capsys.readouterr().err
