# ruff: noqa: I001
import json
import os

import pytest

from crisp.graph import Graph, GraphNode
from crisp.shared.models import SpanKind
from crisp.slack_drag import Drag, calculate_drag, _exclusive_cp_time


def _span(span_id, op, pid, start, duration, parent_id=None):
    return {
        "traceID": "T",
        "spanID": span_id,
        "operationName": op,
        "startTime": start,
        "duration": duration,
        "processID": pid,
        "warnings": None,
        "references": ([] if parent_id is None else [{"refType": "CHILD_OF", "traceID": "T", "spanID": parent_id}]),
    }


def _build_graph(spans, processes, root_service, root_op):
    jsonData = {
        "data": [
            {
                "processes": {pid: {"serviceName": svc, "tags": []} for pid, svc in processes.items()},
                "traceID": "T",
                "spans": spans,
            },
        ],
    }
    return Graph(jsonData, root_service, root_op, "", "")


def _build_manual_graph(node_specs, root_sid):
    """node_specs: list of (sid, opName, pid, svc, startTime, duration, parentSid)."""
    g = Graph([], "root_svc", "root_op", "nofile.txt", skipInitializationForTest=True)
    nodes = {}
    for sid, opName, pid, svc, startTime, duration, _ in node_specs:
        node = GraphNode(
            sid=sid,
            startTime=startTime,
            duration=duration,
            parentSpanId=None,
            opName=opName,
            processID=pid,
            spanKind=SpanKind.UNKNOWN,
            peerService=svc,
            returnError=False,
        )
        nodes[sid] = node
        g.nodeHT[sid] = node
        g.processName[pid] = svc

    for sid, _, _, _, _, _, parentSid in node_specs:
        if parentSid is not None:
            nodes[sid].setParent(nodes[parentSid])
            nodes[parentSid].addChild(nodes[sid])

    g.rootNode = nodes[root_sid]
    return g


# R ([S1] OR) -> A ([S2] OA)
#             -> B ([S2] OB)
#             -> C ([S2] OC)
# R runs 0-1000. A runs 0-200; B runs 195-400 (overlaps A's end by exactly 5, well
# within the default 1% overlap-allowance tolerance of R's duration); C runs 500-1000
# (no overlap with B). All three end up on the critical path, back-to-back, in
# reverse-chronological (by endTime) order: cp = [R, C, B, A].
#
# THE REGRESSION SCENARIO: for B (the *second* chained sibling appended to the flat
# cp list, right after C's own one-node "subtree"), cp[index_of(B) - 1] is C -- a
# completely unrelated leaf, NOT B's real parent. A naive drag implementation that
# used `cp[i - 1]` to find B's "parent" would look at C.children (empty, since C is
# a leaf), hit the "no true siblings" fallback, and report B's drag as B's full
# duration (205) -- silently missing the real overlap-based reduction that comes
# from B's TRUE parent (R) and TRUE siblings (A, B, C). The correct answer, using
# node.parent/node.parent.children, is 200 (205 minus the 5-unit overlap with A).
def sequential_siblings_with_slight_overlap_graph():
    spans = [
        _span("R", "OR", "S1", 0, 1000),
        _span("A", "OA", "S2", 0, 200, parent_id="R"),
        _span("B", "OB", "S2", 195, 205, parent_id="R"),
        _span("C", "OC", "S2", 500, 500, parent_id="R"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2"}, "S1", "OR")


# R ([S1] OR) -> X ([S2] OX) -> Y ([S3] OY)
# R runs 0-300; X runs 0-200 (R's only child); Y runs 0-100 (X's only child).
# A pure linear chain: no node ever has more than one child, so no sibling ever
# competes with (caps) another.
def linear_chain_graph():
    spans = [
        _span("R", "OR", "S1", 0, 300),
        _span("X", "OX", "S2", 0, 200, parent_id="R"),
        _span("Y", "OY", "S3", 0, 100, parent_id="X"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2", "S3": "S3"}, "S1", "OR")


# R ([S1] OR) -> A ([S2] OA)
#             -> B ([S2] OB)
# R runs 0-1000. A runs 0-500 (duration 500). B runs 495-1000 (duration 505),
# overlapping A's tail by exactly 5 (within the default 1% overlap-allowance
# tolerance of R's 1000 duration). Both end up on the critical path: cp = [R, B, A].
# B's true next-sibling (by descending endTime) is A, so B's inclusive drag is
# exactly B's own duration minus the 5-unit overlap: 505 - 5 = 500.
def mild_overlap_two_siblings_graph():
    spans = [
        _span("R", "OR", "S1", 0, 1000),
        _span("A", "OA", "S2", 0, 500, parent_id="R"),
        _span("B", "OB", "S2", 495, 505, parent_id="R"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2"}, "S1", "OR")


# R ([S1] OR) -> P ([S2] OP) -> P1 ([S3] OP1)
#             -> Q ([S2] OQ)
# R runs 0-1000. P runs 0-300, with its own child P1 running 10-260. Q runs
# 295-595, overlapping P's tail by 5 (within tolerance). Q ends *later* than P, so
# Q is the "last-running child" appended right after R in the flat cp list:
# cp = [R, Q, P, P1]. P is the *earliest*-ending of its two true siblings {P, Q},
# so P's drag hits the "no later sibling to cap it" fallback in BOTH inclusive and
# exclusive mode -- but the two modes still diverge, because P's own *duration*
# (own_metric in inclusive mode) differs from P's own *exclusive* critical-path time
# (own_metric in exclusive mode: P.duration minus P1's duration, since P1 is P's own
# critical-path-continuing child): 300 (inclusive) vs. 50 (exclusive).
def earliest_ending_sibling_with_own_child_graph():
    spans = [
        _span("R", "OR", "S1", 0, 1000),
        _span("P", "OP", "S2", 0, 300, parent_id="R"),
        _span("P1", "OP1", "S3", 10, 250, parent_id="P"),
        _span("Q", "OQ", "S2", 295, 300, parent_id="R"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2", "S3": "S3"}, "S1", "OR")


# R ([S1] OR) -> M ([S2] OM) -> M1 ([S3] OM1)
#             -> L ([S2] OL)
# R runs 0-1000. M runs 200-600, with its own child M1 running 210-590. L runs
# 55-205, overlapping M's *start* by 5 (within tolerance). L ends before M, so
# cp = [R, M, M1, L] -- M is NOT the earliest-ending sibling here (L is), so
# (unlike earliest_ending_sibling_with_own_child_graph above) M's drag genuinely
# goes through the sibling-gap FORMULA, not the fallback. This exercises the
# exclusive-mode "recompute the critical-path-continuing child from node.children,
# don't trust cp[i + 1]" rule directly: exclusive drag for M must net out the
# overlap contributed by M1 (M's real critical continuation) against the gap to L
# (M's real next sibling), giving 15 -- sharply less than the naive per-duration
# 400 - 380 = 20 you'd get from the child-agnostic fallback, and much less than
# the inclusive value of 395.
def exclusive_formula_branch_graph():
    spans = [
        _span("R", "OR", "S1", 0, 1000),
        _span("M", "OM", "S2", 200, 400, parent_id="R"),
        _span("M1", "OM1", "S3", 210, 380, parent_id="M"),
        _span("L", "OL", "S2", 55, 150, parent_id="R"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2", "S3": "S3"}, "S1", "OR")


# R ([S1] OR) -- a leaf root with no children.
def root_only_graph():
    spans = [_span("R", "OR", "S1", 0, 100)]
    return _build_graph(spans, {"S1": "S1"}, "S1", "OR")


# R ([S1] OR) -> A ([S2] OA)
#             -> B ([S2] OB)
# R runs 0-500. A runs 0-100 (ends earliest of the two siblings). B runs 200-450
# (ends latest, no overlap with A). cp = [R, B, A]. A, as the earliest-ending true
# sibling, has no later sibling to cap it, so its drag is its full duration (100)
# in both modes.
def earliest_ending_sibling_graph():
    spans = [
        _span("R", "OR", "S1", 0, 500),
        _span("A", "OA", "S2", 0, 100, parent_id="R"),
        _span("B", "OB", "S2", 200, 450, parent_id="R"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2"}, "S1", "OR")


# R ([S1] OR) -> A ([S2] OA)
# R runs 0-200; A runs 10-190 (R's only child -- no true siblings at all).
def only_child_graph():
    spans = [
        _span("R", "OR", "S1", 0, 200),
        _span("A", "OA", "S2", 10, 180, parent_id="R"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2"}, "S1", "OR")


# R ([S1] OR) -> A ([S2] OA)  [on the critical path]
#             -> B ([S2] OB)  [off the critical path]
# R runs 0-500. A runs 0-400 (dominant, ends up on the critical path alone).
# B runs 0-50 (ends and starts well before A in a way that fails happensBefore,
# so it never joins the critical path) -- but B still counts as one of A's TRUE
# siblings (GraphNode.children reflects real tree structure, not cp membership),
# so it still caps A's drag: cp = [R, A].
def branch_with_noncritical_sibling_graph():
    spans = [
        _span("R", "OR", "S1", 0, 500),
        _span("A", "OA", "S2", 0, 400, parent_id="R"),
        _span("B", "OB", "S2", 0, 50, parent_id="R"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2"}, "S1", "OR")


def _load_test_case(filename):
    test_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(test_dir, "..", "test_cases", filename)
    with open(path) as f:
        data = json.load(f)
    return Graph(data, "S1", "O1", path)


# --- The critical regression test: node.parent, never cp[i - 1]. ---


def test_naive_index_adjacency_parent_would_misreport_chained_sibling_drag():
    g = sequential_siblings_with_slight_overlap_graph()
    cp = g.findCriticalPath()

    # Confirm, rather than assume, that all three siblings end up on the
    # critical path, back-to-back, in the order the module docstring describes.
    assert [n.sid for n in cp] == ["R", "C", "B", "A"]

    b_node = next(n for n in cp if n.sid == "B")
    c_node = next(n for n in cp if n.sid == "C")
    # B's list predecessor is C -- but C is NOT B's real parent (R is).
    assert cp[cp.index(b_node) - 1] is c_node
    assert b_node.parent.sid == "R"
    assert c_node is not b_node.parent

    drag = calculate_drag(g, cp)

    # The naive (buggy) computation: fake_parent = cp[i - 1] = C, a leaf with no
    # children of its own, so `len(fake_parent.children) < 2` is trivially true
    # and it would fall back to B's full, uncapped duration.
    naive_b_drag = b_node.duration
    assert naive_b_drag == 205

    # The correct computation, using B's real parent (R) and real siblings
    # {A, B, C}, finds that B's true next sibling (by descending endTime) is A,
    # and correctly discounts B's 5-unit overlap with A.
    assert drag.drag_per_span["B"] == 200
    assert drag.drag_per_span["B"] != naive_b_drag


def test_regression_fixture_other_siblings_unaffected_by_the_same_bug():
    # C is the *first* child appended to the flat list (right after R), so a
    # naive cp[i - 1] lookup happens to also land on R for C -- no divergence
    # there. A is the earliest-ending sibling, so it gets full duration under
    # both the naive and correct rules. Only B (the chained, non-primary
    # sibling) is actually affected by the bug -- confirm the others still hold.
    g = sequential_siblings_with_slight_overlap_graph()
    cp = g.findCriticalPath()
    drag = calculate_drag(g, cp)

    assert drag.drag_per_span["R"] == 1000
    assert drag.drag_per_span["C"] == 500
    assert drag.drag_per_span["A"] == 200


# --- Cross-validation against Graph.accumeCPMetrics. ---


def test_exclusive_time_matches_accume_cp_metrics_on_linear_chain():
    g = linear_chain_graph()
    cp = g.findCriticalPath()

    _cpp, sid_time_exclusive = g.accumeCPMetrics(cp, "test-trace", g.rootNode)
    assert _exclusive_cp_time(cp) == sid_time_exclusive


def test_exclusive_time_matches_accume_cp_metrics_on_branching_graph():
    g = sequential_siblings_with_slight_overlap_graph()
    cp = g.findCriticalPath()

    _cpp, sid_time_exclusive = g.accumeCPMetrics(cp, "test-trace", g.rootNode)
    assert _exclusive_cp_time(cp) == sid_time_exclusive


def test_exclusive_time_matches_accume_cp_metrics_including_sanitized_negative():
    # A.duration (500) + B.duration (505) exceed R.duration (1000) by 5 (the
    # overlap), so R's raw exclusive time would be -5 -- both accumeCPMetrics and
    # _exclusive_cp_time must independently sanitize this to 0, and still agree.
    g = mild_overlap_two_siblings_graph()
    cp = g.findCriticalPath()

    _cpp, sid_time_exclusive = g.accumeCPMetrics(cp, "test-trace", g.rootNode)
    assert sid_time_exclusive["R"] == 0
    assert _exclusive_cp_time(cp) == sid_time_exclusive


# --- Linear chain (no branching anywhere). ---


def test_linear_chain_inclusive_drag_equals_each_nodes_own_full_duration():
    g = linear_chain_graph()
    cp = g.findCriticalPath()
    drag = calculate_drag(g, cp)

    assert drag.drag_per_span == {"R": 300, "X": 200, "Y": 100}


def test_linear_chain_exclusive_drag_equals_exclusive_cp_time_not_own_duration():
    # NOTE: exclusive drag is NOT equal to inclusive drag here for R and X, even
    # though there is no sibling competition anywhere on this chain. That's
    # because the exclusive/inclusive distinction is driven purely by whether a
    # node has its own critical-path-continuing child (netting out that child's
    # contribution) -- it is orthogonal to sibling competition. Only the leaf
    # (Y, with no children of its own) has inclusive == exclusive. This is
    # cross-validated against Graph.accumeCPMetrics above.
    g = linear_chain_graph()
    cp = g.findCriticalPath()
    drag = calculate_drag(g, cp, exclusive=True)

    assert drag.drag_per_span == {"R": 100, "X": 100, "Y": 100}
    y_node = next(n for n in cp if n.sid == "Y")
    assert drag.drag_per_span["Y"] == y_node.duration


# --- Hand-computable gap-based inclusive drag. ---


def test_branching_siblings_exact_gap_hand_computed_inclusive_drag():
    g = mild_overlap_two_siblings_graph()
    cp = g.findCriticalPath()
    assert [n.sid for n in cp] == ["R", "B", "A"]

    drag = calculate_drag(g, cp)

    b_node = next(n for n in cp if n.sid == "B")
    a_node = next(n for n in cp if n.sid == "A")
    overlap = a_node.endTime - b_node.startTime
    assert overlap == 5
    assert drag.drag_per_span["B"] == b_node.duration - overlap == 500
    # A, the earliest-ending sibling, is uncapped.
    assert drag.drag_per_span["A"] == a_node.duration == 500


# --- Inclusive vs. exclusive divergence. ---


def test_inclusive_vs_exclusive_diverge_via_fallback_own_metric():
    g = earliest_ending_sibling_with_own_child_graph()
    cp = g.findCriticalPath()
    assert [n.sid for n in cp] == ["R", "Q", "P", "P1"]

    inclusive = calculate_drag(g, cp)
    exclusive = calculate_drag(g, cp, exclusive=True)

    p_node = next(n for n in cp if n.sid == "P")
    p1_node = next(n for n in cp if n.sid == "P1")
    assert inclusive.drag_per_span["P"] == p_node.duration == 300
    assert exclusive.drag_per_span["P"] == p_node.duration - p1_node.duration == 50


def test_inclusive_vs_exclusive_diverge_via_formula_branch_child_recompute():
    # This is the exclusive-mode analog of the main parent-lookup regression test:
    # it specifically exercises the "recompute node's own critical-path-continuing
    # child from node.children, don't trust cp[i + 1]" rule, on a node (M) that
    # goes through the sibling-gap FORMULA branch (not the simpler fallback).
    g = exclusive_formula_branch_graph()
    cp = g.findCriticalPath()
    assert [n.sid for n in cp] == ["R", "M", "M1", "L"]

    inclusive = calculate_drag(g, cp)
    exclusive = calculate_drag(g, cp, exclusive=True)

    assert inclusive.drag_per_span["M"] == 395
    assert exclusive.drag_per_span["M"] == 15
    assert exclusive.drag_per_span["M"] < inclusive.drag_per_span["M"]


# --- Root-only graph. ---


def test_root_only_graph_gets_full_duration_in_both_modes():
    g = root_only_graph()
    cp = g.findCriticalPath()
    assert [n.sid for n in cp] == ["R"]

    inclusive = calculate_drag(g, cp)
    exclusive = calculate_drag(g, cp, exclusive=True)

    assert inclusive.drag_per_span == {"R": 100}
    assert exclusive.drag_per_span == {"R": 100}
    assert inclusive.total_drag == exclusive.total_drag == 100


# --- Earliest-ending sibling (no later sibling to cap it). ---


def test_earliest_ending_sibling_gets_full_duration():
    g = earliest_ending_sibling_graph()
    cp = g.findCriticalPath()
    assert [n.sid for n in cp] == ["R", "B", "A"]

    drag = calculate_drag(g, cp)
    a_node = next(n for n in cp if n.sid == "A")
    assert drag.drag_per_span["A"] == a_node.duration == 100


# --- Only child (no true siblings at all). ---


def test_only_child_gets_full_duration_in_both_modes():
    g = only_child_graph()
    cp = g.findCriticalPath()
    assert [n.sid for n in cp] == ["R", "A"]

    a_node = next(n for n in cp if n.sid == "A")
    assert len(a_node.parent.children) == 1

    inclusive = calculate_drag(g, cp)
    exclusive = calculate_drag(g, cp, exclusive=True)
    assert inclusive.drag_per_span["A"] == exclusive.drag_per_span["A"] == a_node.duration == 180


# --- Non-critical-path node contract. ---


def test_non_critical_path_node_is_omitted_from_drag_per_span():
    g = branch_with_noncritical_sibling_graph()
    cp = g.findCriticalPath()
    assert [n.sid for n in cp] == ["R", "A"]

    drag = calculate_drag(g, cp)

    # B never appears as a key -- see Drag.drag_per_span's documented contract.
    assert "B" not in drag.drag_per_span
    assert drag.drag_per_span.get("B", 0.0) == 0.0

    # B still counts as one of A's true siblings (real tree structure, not cp
    # membership), so it still caps A's drag below A's full 400 duration.
    a_node = next(n for n in cp if n.sid == "A")
    assert drag.drag_per_span["A"] == 350
    assert drag.drag_per_span["A"] < a_node.duration


# --- total_drag correctness. ---


def test_total_drag_equals_sum_of_drag_per_span_values():
    g = exclusive_formula_branch_graph()
    cp = g.findCriticalPath()

    for exclusive in (False, True):
        drag = calculate_drag(g, cp, exclusive=exclusive)
        assert drag.total_drag == sum(drag.drag_per_span.values())


def test_total_drag_equals_root_duration_on_linear_chain_in_exclusive_mode():
    # Exclusive time is a telescoping sum over the critical path (every non-root
    # node's duration is added once to itself and subtracted once from its
    # parent), so it always sums to the root's duration -- as long as no
    # sibling-gap capping ever reduces a node below its own exclusive share.
    # On a pure, unbranched chain, no capping ever happens (every node is an
    # "only child"), so this holds exactly.
    g = linear_chain_graph()
    cp = g.findCriticalPath()
    drag = calculate_drag(g, cp, exclusive=True)

    assert drag.total_drag == g.rootNode.duration == 300


# --- Realistic fixtures from test_cases/*.json (same ones test_graph.py uses). ---


def test_realistic_fixtures_total_drag_bounded_and_entries_present():
    for filename in ("1.json", "5.json"):
        g = _load_test_case(filename)
        assert g.rootNode is not None
        cp = g.findCriticalPath()

        # Exclusive total_drag can never exceed the root's total duration: it's a
        # telescoping partition of the root's own time, only ever reduced (never
        # inflated) by sibling-gap capping. (Inclusive total_drag has no such
        # bound -- it can double-count nested contributions -- so this invariant
        # is specifically about exclusive mode.)
        drag = calculate_drag(g, cp, exclusive=True)
        assert drag.total_drag <= g.rootNode.duration

        for node in cp:
            assert node.sid in drag.drag_per_span
            assert drag.drag_per_span[node.sid] >= 0


# --- Graph.calculateDrag() hook. ---


def test_graph_calculate_drag_hook_matches_direct_module_call():
    g = exclusive_formula_branch_graph()
    cp = g.findCriticalPath()

    for exclusive in (False, True):
        via_hook = g.calculateDrag(cp, exclusive=exclusive)
        via_module = calculate_drag(g, cp, exclusive=exclusive)
        assert via_hook.drag_per_span == via_module.drag_per_span
        assert via_hook.total_drag == via_module.total_drag


def test_graph_calculate_drag_hook_defaults_cp_to_find_critical_path():
    g = exclusive_formula_branch_graph()

    via_default = g.calculateDrag()
    via_explicit = calculate_drag(g, g.findCriticalPath())

    assert via_default.drag_per_span == via_explicit.drag_per_span
    assert via_default.total_drag == via_explicit.total_drag
    # Sanity: this isn't just two empty dicts matching each other.
    assert via_default.drag_per_span


# --- General API contract sanity. ---


def test_exclusive_defaults_to_false():
    g = sequential_siblings_with_slight_overlap_graph()
    cp = g.findCriticalPath()

    default_call = calculate_drag(g, cp)
    explicit_inclusive = calculate_drag(g, cp, exclusive=False)
    assert default_call.drag_per_span == explicit_inclusive.drag_per_span


def test_calculate_drag_is_pure_and_deterministic():
    g = sequential_siblings_with_slight_overlap_graph()
    cp = g.findCriticalPath()
    durations_before = {n.sid: n.duration for n in cp}

    first = calculate_drag(g, cp)
    second = calculate_drag(g, cp)

    assert first.drag_per_span == second.drag_per_span
    assert first.total_drag == second.total_drag
    assert {n.sid: n.duration for n in cp} == durations_before


def test_drag_dataclass_is_frozen():
    drag = Drag(drag_per_span={"A": 1.0}, total_drag=1.0)
    with pytest.raises(AttributeError):
        drag.total_drag = 2.0
