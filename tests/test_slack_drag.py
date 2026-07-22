# ruff: noqa: I001
import json
import os

import pytest

from crisp.graph import Graph, GraphNode
from crisp.shared.models import SpanKind
from crisp.dependency_graph import DependencyGraph
from crisp.slack_drag import (
    Drag,
    Slack,
    PerMethodSlackDrag,
    calculate_drag,
    calculate_slack,
    aggregate_drag_slack_by_callpath,
    merge_per_method_slack_drag,
    _exclusive_cp_time,
)


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


# Same shape/op/service names as linear_chain_graph() above (so every node shares
# the exact same Graph.getCallPath identity across the two graphs), but with
# different durations throughout -- used to build two distinct "traces" that
# genuinely share call paths, for testing merge_per_method_slack_drag's
# cross-trace summation without hand-guessing drag/slack numbers.
def linear_chain_graph_variant():
    spans = [
        _span("R", "OR", "S1", 0, 600),
        _span("X", "OX", "S2", 0, 400, parent_id="R"),
        _span("Y", "OY", "S3", 0, 200, parent_id="X"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2", "S3": "S3"}, "S1", "OR")


# R ([S1] OR) -> A ([S2] OX)
#             -> B ([S3] OX)
# Same operation name ("OX") on both children, but different services (S2 vs
# S3) -- Graph.getCallPath includes the service in its canonical name, so
# these must NOT be merged into one call path by aggregate_drag_slack_by_callpath.
def same_opname_different_service_siblings_graph():
    spans = [
        _span("R", "OR", "S1", 0, 500),
        _span("A", "OX", "S2", 0, 100, parent_id="R"),
        _span("B", "OX", "S3", 200, 300, parent_id="R"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2", "S3": "S3"}, "S1", "OR")


# --- Fixtures for calculate_slack. ---


# Same shape as sequential_siblings_with_slight_overlap_graph() (R -> A, B, C
# chaining onto cp = [R, C, B, A]) plus a 4th sibling D that stays off the
# critical path. D has a real happens_before dependency on A (A ends at 200,
# well before D starts at 250), and its critical_end_time ceiling should come
# from B's real endTime (400) via R.children -- not from whatever cp[i - 1]
# happens to be when B is visited (see
# test_naive_index_adjacency_would_misreport_chained_sibling_dependent_slack).
def sequential_siblings_with_dependent_off_cp_sibling_graph():
    spans = [
        _span("R", "OR", "S1", 0, 1000),
        _span("A", "OA", "S2", 0, 200, parent_id="R"),
        _span("B", "OB", "S2", 195, 205, parent_id="R"),
        _span("C", "OC", "S2", 500, 500, parent_id="R"),
        _span("D", "OD", "S2", 250, 130, parent_id="R"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2"}, "S1", "OR")


# R -> N (off cp, runs 400-520, overlaps TAIL's start by 2% -- past tolerance --
# so never joins cp) and R -> TAIL (on cp, runs 500-1000, ends exactly when R
# does). N is the earliest-starting sibling with no happens_before constraint, so
# its earliest possible start is its own actual start (400) -- no banked slack.
# Its critical_end_time is inherited from TAIL via R.children, and TAIL ends
# exactly at R's endTime (1000), making this a tight boundary for the cross-check
# in test_extending_leaf_by_exactly_its_slack_reaches_root_endtime_boundary.
def leaf_slack_capped_by_root_endtime_via_sibling_graph():
    spans = [
        _span("R", "OR", "S1", 0, 1000),
        _span("N", "ON", "S2", 400, 120, parent_id="R"),
        _span("TAIL", "OT", "S2", 500, 500, parent_id="R"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2"}, "S1", "OR")


# R -> P -> N, and R -> C. P runs 0-1000 (P.endTime == R.endTime exactly) with
# its only child N running 10-800. C runs 900-1000, tied with P on endTime but
# wins the tie-break (inserted after P; computeCriticalPath's stable sort favors
# it), so P stays off cp: cp = [R, C]. N, as P's only child, has no competing
# sibling, so its earliest possible start is trivially its own actual start (10).
# Since N's parent (P) isn't a cp node, N's critical_end_time falls to the
# "inherit my parent's own endTime" fallback -- and P's endTime happens to equal
# R's (1000), giving a tight boundary bounded purely by "parent's endTime, no
# competing sibling" (contrast with the sibling-gap-bounded fixture above).
def leaf_slack_capped_by_root_endtime_via_nested_only_child_graph():
    spans = [
        _span("R", "OR", "S1", 0, 1000),
        _span("P", "OP", "S2", 0, 1000, parent_id="R"),
        _span("N", "ON", "S3", 10, 790, parent_id="P"),
        _span("C", "OC", "S2", 900, 100, parent_id="R"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2", "S3": "S3"}, "S1", "OR")


# R -> A (runs 0-100), R -> B (runs 300-750, depends on A), R -> D (runs
# 700-1000, on cp as R's last-running child). B overlaps D's tail by 5% (past
# tolerance), so B never joins cp. A's real happens_before predecessor
# relationship with B (DependencyGraph checks all sibling pairs, not just
# cp-adjacent ones) makes B's earliest possible start A's endTime (100), not the
# naive "earliest of any sibling's start" fallback (which would be A's own
# start, 0).
def happens_before_chain_constrains_sibling_slack_graph():
    spans = [
        _span("R", "OR", "S1", 0, 1000),
        _span("A", "OA", "S2", 0, 100, parent_id="R"),
        _span("B", "OB", "S3", 300, 450, parent_id="R"),
        _span("D", "OD", "S4", 700, 300, parent_id="R"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2", "S3": "S3", "S4": "S4"}, "S1", "OR")


# P -> B1, B2 (two instances of the same call-path "Ob") and P -> C (on cp,
# spans all of P, 0-1000). B1 runs 0-100; B2 runs 200-300 (off cp). Since B1 (an
# earlier instance of "Ob") finishes before B2 starts, DependencyGraph records a
# self-referential happens_before edge for "Ob" (see dependency_graph.py).
def fanout_same_callpath_off_cp_graph():
    spans = [
        _span("P", "OP", "S1", 0, 1000),
        _span("B1", "Ob", "S2", 0, 100, parent_id="P"),
        _span("B2", "Ob", "S2", 200, 100, parent_id="P"),
        _span("C", "Oc", "S3", 0, 1000, parent_id="P"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2", "S3": "S3"}, "S1", "OP")


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


# ============================================================================
# calculate_slack tests.
# ============================================================================


# --- Every critical-path node has zero slack. ---


def test_every_critical_path_node_has_zero_slack_on_a_linear_chain():
    g = linear_chain_graph()
    cp = g.findCriticalPath()
    slack = calculate_slack(g, cp)

    for node in cp:
        assert slack.slack_per_span[node.sid] == 0


def test_every_critical_path_node_has_zero_slack_on_a_branching_graph():
    g = sequential_siblings_with_slight_overlap_graph()
    cp = g.findCriticalPath()
    assert [n.sid for n in cp] == ["R", "C", "B", "A"]

    slack = calculate_slack(g, cp)

    for node in cp:
        assert slack.slack_per_span[node.sid] == 0


# --- The rigorous "slack is a tight boundary" cross-check. ---


def test_extending_leaf_by_exactly_its_slack_reaches_but_never_exceeds_root_endtime_via_sibling_gap():
    g = leaf_slack_capped_by_root_endtime_via_sibling_graph()
    cp = g.findCriticalPath()
    assert [n.sid for n in cp] == ["R", "TAIL"]

    slack = calculate_slack(g, cp)
    n_node = g.nodeHT["N"]
    assert n_node.children == {}
    assert slack.slack_per_span["N"] > 0
    root_end_time = g.rootNode.endTime

    n_node.duration += slack.slack_per_span["N"]
    n_node.endTime = n_node.startTime + n_node.duration
    assert n_node.endTime <= root_end_time


def test_extending_leaf_by_one_more_than_its_slack_exceeds_root_endtime_via_sibling_gap():
    g = leaf_slack_capped_by_root_endtime_via_sibling_graph()
    cp = g.findCriticalPath()
    slack = calculate_slack(g, cp)
    n_node = g.nodeHT["N"]
    root_end_time = g.rootNode.endTime

    n_node.duration += slack.slack_per_span["N"] + 1
    n_node.endTime = n_node.startTime + n_node.duration
    assert n_node.endTime > root_end_time


def test_extending_leaf_by_exactly_its_slack_reaches_but_never_exceeds_root_endtime_via_nested_only_child():
    g = leaf_slack_capped_by_root_endtime_via_nested_only_child_graph()
    cp = g.findCriticalPath()
    assert [n.sid for n in cp] == ["R", "C"]

    slack = calculate_slack(g, cp)
    n_node = g.nodeHT["N"]
    assert n_node.children == {}
    assert len(n_node.parent.children) == 1  # no competing sibling at N's own level.
    assert slack.slack_per_span["N"] > 0
    root_end_time = g.rootNode.endTime

    n_node.duration += slack.slack_per_span["N"]
    n_node.endTime = n_node.startTime + n_node.duration
    assert n_node.endTime <= root_end_time


def test_extending_leaf_by_one_more_than_its_slack_exceeds_root_endtime_via_nested_only_child():
    g = leaf_slack_capped_by_root_endtime_via_nested_only_child_graph()
    cp = g.findCriticalPath()
    slack = calculate_slack(g, cp)
    n_node = g.nodeHT["N"]
    root_end_time = g.rootNode.endTime

    n_node.duration += slack.slack_per_span["N"] + 1
    n_node.endTime = n_node.startTime + n_node.duration
    assert n_node.endTime > root_end_time


# --- happens_before chain constrains a sibling's slack. ---


def test_happens_before_chain_constrains_sibling_slack_tighter_than_naive_sibling_gap():
    g = happens_before_chain_constrains_sibling_slack_graph()
    cp = g.findCriticalPath()

    # B overlaps D's start by 5% (past tolerance), so it never joins cp -- it's
    # the one node whose slack we're testing.
    assert "B" not in [n.sid for n in cp]

    dep = DependencyGraph(graph=g)
    b_name = g.getCallPath(g.nodeHT["B"])
    a_name = g.getCallPath(g.nodeHT["A"])
    assert dep.deps[b_name].happens_before == {a_name}

    slack = calculate_slack(g, cp, dep)

    a_node = g.nodeHT["A"]
    # B's earliest possible start is A's real endTime (100), not the naive
    # "earliest of any sibling's start" fallback (which would use A's start, 0).
    assert slack.slack_per_span["B"] == 450

    naive_slack_ignoring_happens_before = slack.slack_per_span["B"] + (a_node.endTime - a_node.startTime)
    assert naive_slack_ignoring_happens_before == 550
    assert slack.slack_per_span["B"] != naive_slack_ignoring_happens_before


# --- Same-call-path fanout: self-referential happens_before. ---


def test_fanout_same_callpath_second_instance_uses_real_predecessor_span_not_bare_name_match():
    g = fanout_same_callpath_off_cp_graph()
    cp = g.findCriticalPath()
    assert [n.sid for n in cp] == ["P", "C"]

    dep = DependencyGraph(graph=g)
    ob_name = g.getCallPath(g.nodeHT["B1"])
    assert g.getCallPath(g.nodeHT["B2"]) == ob_name
    # Self-referential edge: an earlier instance of the same call-path ("Ob")
    # is recorded as happening before it.
    assert dep.deps[ob_name].happens_before == {ob_name}

    slack = calculate_slack(g, cp, dep)

    # B1 is the earlier instance: nothing precedes it, so it falls back to the
    # "earliest sibling start" rule -- it never spuriously matches itself, since
    # the search breaks at B1's own span id before comparing against itself.
    assert slack.slack_per_span["B1"] == 900

    # B2's earliest possible start correctly uses B1's real endTime (100), not
    # a bare name-membership match against its own call-path name.
    assert slack.slack_per_span["B2"] == 800
    assert slack.slack_per_span["B2"] != slack.slack_per_span["B1"]


# --- Regression: node.parent/node.children, never cp list-adjacency. ---


def test_naive_index_adjacency_would_misreport_chained_sibling_dependent_slack():
    g = sequential_siblings_with_dependent_off_cp_sibling_graph()
    cp = g.findCriticalPath()

    # Same chained-sibling shape as the drag regression fixture, plus a 4th
    # off-cp sibling D with a real happens_before dependency on A.
    assert [n.sid for n in cp] == ["R", "C", "B", "A"]
    d_node = g.nodeHT["D"]
    assert d_node.sid not in [n.sid for n in cp]

    dep = DependencyGraph(graph=g)
    d_name = g.getCallPath(d_node)
    a_name = g.getCallPath(g.nodeHT["A"])
    assert dep.deps[d_name].happens_before == {a_name}

    slack = calculate_slack(g, cp, dep)

    # The naive (buggy) computation: a cp[i - 1]-based "parent" for B would be
    # the leaf C, so D never gets updated via B's inheritance step, leaving
    # D's critical_end_time at R's (coincidentally correct for C, but not B): 1000.
    naive_critical_end_time_for_d = g.rootNode.endTime
    naive_latest_start = naive_critical_end_time_for_d - d_node.duration
    a_node = g.nodeHT["A"]
    earliest_start_for_d = a_node.endTime  # unaffected by the cp-adjacency bug.
    naive_slack = naive_latest_start - earliest_start_for_d
    assert naive_slack == 670

    # The correct computation, using D's real parent (R) and real siblings,
    # finds B's real endTime (400) -- not C's (1000) -- as D's tightest cap.
    assert slack.slack_per_span["D"] == 70
    assert slack.slack_per_span["D"] != naive_slack


# --- Default dependency_graph=None builds one internally. ---


def test_default_dependency_graph_matches_explicit_prebuilt_one():
    g = sequential_siblings_with_slight_overlap_graph()
    cp = g.findCriticalPath()

    via_default = calculate_slack(g, cp)
    via_explicit = calculate_slack(g, cp, dependency_graph=DependencyGraph(graph=g))

    assert via_default.slack_per_span == via_explicit.slack_per_span
    assert via_default.total_slack == via_explicit.total_slack


# --- Accepts a multi-trace aggregate DependencyGraph. ---


def test_accepts_multi_trace_aggregate_dependency_graph_without_erroring():
    g = sequential_siblings_with_slight_overlap_graph()
    cp = g.findCriticalPath()
    aggregate = DependencyGraph(graphs=[g, g])

    slack = calculate_slack(g, cp, dependency_graph=aggregate)

    assert set(slack.slack_per_span.keys()) == set(g.nodeHT.keys())
    for node in cp:
        assert slack.slack_per_span[node.sid] == 0
    assert slack.total_slack == sum(slack.slack_per_span.values())


# --- Root node slack. ---


def test_root_node_always_has_zero_slack():
    for fixture in (
        linear_chain_graph,
        sequential_siblings_with_slight_overlap_graph,
        branch_with_noncritical_sibling_graph,
    ):
        g = fixture()
        cp = g.findCriticalPath()
        slack = calculate_slack(g, cp)
        assert slack.slack_per_span[g.rootNode.sid] == 0


# --- total_slack correctness. ---


def test_total_slack_equals_sum_of_slack_per_span_values():
    g = happens_before_chain_constrains_sibling_slack_graph()
    cp = g.findCriticalPath()
    slack = calculate_slack(g, cp)

    assert slack.total_slack == sum(slack.slack_per_span.values())


# --- slack_per_span covers every node (contrast with Drag's key-omission). ---


def test_non_critical_path_node_is_present_in_slack_per_span_unlike_drag():
    g = branch_with_noncritical_sibling_graph()
    cp = g.findCriticalPath()
    assert [n.sid for n in cp] == ["R", "A"]

    slack = calculate_slack(g, cp)

    # Unlike Drag.drag_per_span (which omits "B" entirely -- see
    # test_non_critical_path_node_is_omitted_from_drag_per_span above), "B" IS
    # a key here, with a real, non-negative value.
    assert "B" in slack.slack_per_span
    assert slack.slack_per_span["B"] >= 0
    assert slack.slack_per_span["B"] == 350

    # Every node in nodeHT gets a slack_per_span entry, not just cp members.
    assert set(slack.slack_per_span.keys()) == set(g.nodeHT.keys())


# --- Graph.calculateSlack() hook. ---


def test_graph_calculate_slack_hook_matches_direct_module_call():
    g = happens_before_chain_constrains_sibling_slack_graph()
    cp = g.findCriticalPath()
    dep = DependencyGraph(graph=g)

    via_hook = g.calculateSlack(cp, dependency_graph=dep)
    via_module = calculate_slack(g, cp, dep)

    assert via_hook.slack_per_span == via_module.slack_per_span
    assert via_hook.total_slack == via_module.total_slack


def test_graph_calculate_slack_hook_defaults_cp_and_dependency_graph():
    g = happens_before_chain_constrains_sibling_slack_graph()

    via_default = g.calculateSlack()
    via_explicit = calculate_slack(g, g.findCriticalPath())

    assert via_default.slack_per_span == via_explicit.slack_per_span
    assert via_default.total_slack == via_explicit.total_slack
    # Sanity: this isn't just two empty dicts matching each other.
    assert via_default.slack_per_span


# --- Realistic fixtures from test_cases/*.json. ---


def test_realistic_fixtures_every_node_has_nonnegative_slack_and_cp_nodes_are_zero():
    for filename in ("1.json", "5.json"):
        g = _load_test_case(filename)
        assert g.rootNode is not None
        cp = g.findCriticalPath()

        slack = calculate_slack(g, cp)

        assert set(slack.slack_per_span.keys()) == set(g.nodeHT.keys())
        for sid, value in slack.slack_per_span.items():
            assert value >= 0, f"expected non-negative slack for {sid}, got {value}"

        for node in cp:
            assert slack.slack_per_span[node.sid] == 0


# --- General API contract sanity. ---


def test_calculate_slack_is_pure_and_deterministic():
    g = sequential_siblings_with_slight_overlap_graph()
    cp = g.findCriticalPath()
    durations_before = {sid: node.duration for sid, node in g.nodeHT.items()}

    first = calculate_slack(g, cp)
    second = calculate_slack(g, cp)

    assert first.slack_per_span == second.slack_per_span
    assert first.total_slack == second.total_slack
    assert {sid: node.duration for sid, node in g.nodeHT.items()} == durations_before


def test_slack_dataclass_is_frozen():
    slack = Slack(slack_per_span={"A": 1.0}, total_slack=1.0)
    with pytest.raises(AttributeError):
        slack.total_slack = 2.0


# --- aggregate_drag_slack_by_callpath: single-trace per-method aggregation. ---


def test_aggregate_by_callpath_distinct_paths_matches_per_span_values_exactly():
    # No call path repeats in this graph, so every aggregate entry should be a
    # trivial (span_count=1) passthrough of the underlying per-span values.
    g = linear_chain_graph()
    cp = g.findCriticalPath()
    drag = calculate_drag(g, cp)
    slack = calculate_slack(g, cp)

    agg = aggregate_drag_slack_by_callpath(g, drag, slack)

    assert len(agg) == len(g.nodeHT) == 3
    for node in g.nodeHT.values():
        call_path = g.getCallPath(node)
        entry = agg[call_path]
        assert entry.call_path == call_path
        assert entry.span_count == 1
        assert entry.total_drag == drag.drag_per_span.get(node.sid, 0.0)
        assert entry.avg_drag == entry.total_drag
        assert entry.total_slack == slack.slack_per_span[node.sid]
        assert entry.avg_slack == entry.total_slack


def test_aggregate_by_callpath_groups_same_callpath_and_averages():
    g = fanout_same_callpath_off_cp_graph()
    cp = g.findCriticalPath()
    drag = calculate_drag(g, cp)
    slack = calculate_slack(g, cp)

    b1, b2 = g.nodeHT["B1"], g.nodeHT["B2"]
    ob_call_path = g.getCallPath(b1)
    # Sanity: confirm the two "Ob" instances really do share one call path
    # (same op name/service, same parent) before relying on that below.
    assert ob_call_path == g.getCallPath(b2)

    agg = aggregate_drag_slack_by_callpath(g, drag, slack)
    entry = agg[ob_call_path]

    expected_total_drag = drag.drag_per_span.get("B1", 0.0) + drag.drag_per_span.get("B2", 0.0)
    expected_total_slack = slack.slack_per_span["B1"] + slack.slack_per_span["B2"]

    assert entry.span_count == 2
    assert entry.total_drag == expected_total_drag
    assert entry.avg_drag == expected_total_drag / 2
    assert entry.total_slack == expected_total_slack
    assert entry.avg_slack == expected_total_slack / 2

    # Other call paths in the same graph (P, C) are unaffected -- span_count 1.
    p_call_path = g.getCallPath(g.nodeHT["P"])
    assert agg[p_call_path].span_count == 1


def test_aggregate_by_callpath_distinguishes_same_opname_different_service():
    g = same_opname_different_service_siblings_graph()
    cp = g.findCriticalPath()
    drag = calculate_drag(g, cp)
    slack = calculate_slack(g, cp)

    a_path = g.getCallPath(g.nodeHT["A"])
    b_path = g.getCallPath(g.nodeHT["B"])
    assert a_path != b_path

    agg = aggregate_drag_slack_by_callpath(g, drag, slack)

    assert agg[a_path].span_count == 1
    assert agg[b_path].span_count == 1
    assert len(agg) == 3  # R, A (S2::OX), B (S3::OX) all stay distinct


@pytest.mark.parametrize(
    "graph_fn",
    [
        linear_chain_graph,
        sequential_siblings_with_slight_overlap_graph,
        fanout_same_callpath_off_cp_graph,
        branch_with_noncritical_sibling_graph,
        happens_before_chain_constrains_sibling_slack_graph,
        earliest_ending_sibling_with_own_child_graph,
        exclusive_formula_branch_graph,
        only_child_graph,
        root_only_graph,
    ],
)
def test_aggregate_by_callpath_totals_equal_underlying_drag_and_slack_totals(graph_fn):
    # Regrouping by call path must never lose or double-count a span's drag/slack:
    # summing back across every aggregate entry must reproduce the original totals.
    g = graph_fn()
    cp = g.findCriticalPath()
    drag = calculate_drag(g, cp)
    slack = calculate_slack(g, cp)

    agg = aggregate_drag_slack_by_callpath(g, drag, slack)

    assert sum(entry.total_drag for entry in agg.values()) == pytest.approx(drag.total_drag)
    assert sum(entry.total_slack for entry in agg.values()) == pytest.approx(slack.total_slack)
    assert sum(entry.span_count for entry in agg.values()) == len(g.nodeHT)


def test_aggregate_by_callpath_reuses_given_drag_not_recomputed():
    # exclusive_formula_branch_graph's own docstring documents that M's inclusive
    # vs exclusive drag differ sharply (395 vs 15). aggregate_drag_slack_by_callpath
    # must reflect whichever Drag it was handed -- never recompute its own.
    g = exclusive_formula_branch_graph()
    cp = g.findCriticalPath()
    inclusive_drag = calculate_drag(g, cp, exclusive=False)
    exclusive_drag = calculate_drag(g, cp, exclusive=True)
    slack = calculate_slack(g, cp)

    m_path = g.getCallPath(g.nodeHT["M"])
    agg_inclusive = aggregate_drag_slack_by_callpath(g, inclusive_drag, slack)
    agg_exclusive = aggregate_drag_slack_by_callpath(g, exclusive_drag, slack)

    assert agg_inclusive[m_path].total_drag == inclusive_drag.drag_per_span["M"]
    assert agg_exclusive[m_path].total_drag == exclusive_drag.drag_per_span["M"]
    assert agg_inclusive[m_path].total_drag != agg_exclusive[m_path].total_drag


@pytest.mark.parametrize("filename", ["1.json", "5.json"])
def test_aggregate_by_callpath_realistic_fixtures_preserve_totals(filename):
    g = _load_test_case(filename)
    cp = g.findCriticalPath()
    drag = calculate_drag(g, cp)
    slack = calculate_slack(g, cp)

    agg = aggregate_drag_slack_by_callpath(g, drag, slack)

    assert sum(entry.total_drag for entry in agg.values()) == pytest.approx(drag.total_drag)
    assert sum(entry.total_slack for entry in agg.values()) == pytest.approx(slack.total_slack)
    assert sum(entry.span_count for entry in agg.values()) == len(g.nodeHT)
    # Every call path present should have at least one span backing it.
    assert all(entry.span_count > 0 for entry in agg.values())


def test_per_method_slack_drag_dataclass_is_frozen():
    entry = PerMethodSlackDrag(
        call_path="[S1] OR",
        span_count=1,
        total_drag=1.0,
        avg_drag=1.0,
        total_slack=0.0,
        avg_slack=0.0,
    )
    with pytest.raises(AttributeError):
        entry.span_count = 2


# --- merge_per_method_slack_drag: cross-trace aggregation. ---


def test_merge_sums_weighted_not_average_of_per_trace_averages():
    # trace1 saw this call path 3 times (avg 10); trace2 saw it once (avg 100).
    # A naive average-of-averages would report (10 + 100) / 2 == 55. The correct,
    # weighted answer -- summing span_count/total_drag first -- is 130 / 4 == 32.5.
    trace1 = {
        "cp": PerMethodSlackDrag(call_path="cp", span_count=3, total_drag=30.0, avg_drag=10.0, total_slack=0.0, avg_slack=0.0),
    }
    trace2 = {
        "cp": PerMethodSlackDrag(call_path="cp", span_count=1, total_drag=100.0, avg_drag=100.0, total_slack=0.0, avg_slack=0.0),
    }

    merged = merge_per_method_slack_drag([trace1, trace2])

    assert merged["cp"].span_count == 4
    assert merged["cp"].total_drag == 130.0
    assert merged["cp"].avg_drag == pytest.approx(32.5)
    assert merged["cp"].avg_drag != pytest.approx(55.0)


def test_merge_unions_call_paths_present_in_only_some_traces():
    trace1 = {
        "a": PerMethodSlackDrag("a", 1, 10.0, 10.0, 0.0, 0.0),
        "b": PerMethodSlackDrag("b", 2, 20.0, 10.0, 4.0, 2.0),
    }
    trace2 = {
        "b": PerMethodSlackDrag("b", 1, 5.0, 5.0, 1.0, 1.0),
        "c": PerMethodSlackDrag("c", 1, 7.0, 7.0, 0.0, 0.0),
    }

    merged = merge_per_method_slack_drag([trace1, trace2])

    assert set(merged.keys()) == {"a", "b", "c"}
    assert merged["a"].span_count == 1
    assert merged["a"].total_drag == 10.0
    assert merged["b"].span_count == 3
    assert merged["b"].total_drag == 25.0
    assert merged["b"].total_slack == 5.0
    assert merged["b"].avg_drag == pytest.approx(25.0 / 3)
    assert merged["c"].span_count == 1
    assert merged["c"].total_drag == 7.0


def test_merge_empty_iterable_returns_empty_dict():
    assert merge_per_method_slack_drag([]) == {}
    assert merge_per_method_slack_drag(iter([])) == {}


def test_merge_single_trace_is_identity():
    g = fanout_same_callpath_off_cp_graph()
    cp = g.findCriticalPath()
    drag = calculate_drag(g, cp)
    slack = calculate_slack(g, cp)
    agg = aggregate_drag_slack_by_callpath(g, drag, slack)

    merged = merge_per_method_slack_drag([agg])

    assert merged == agg


def test_merge_many_identical_traces_scales_totals_but_preserves_average():
    g = fanout_same_callpath_off_cp_graph()
    cp = g.findCriticalPath()
    drag = calculate_drag(g, cp)
    slack = calculate_slack(g, cp)
    agg = aggregate_drag_slack_by_callpath(g, drag, slack)

    num_copies = 5
    merged = merge_per_method_slack_drag([agg] * num_copies)

    for call_path, single_trace_entry in agg.items():
        merged_entry = merged[call_path]
        assert merged_entry.span_count == single_trace_entry.span_count * num_copies
        assert merged_entry.total_drag == pytest.approx(single_trace_entry.total_drag * num_copies)
        assert merged_entry.total_slack == pytest.approx(single_trace_entry.total_slack * num_copies)
        # Identical traces -> the merged average must equal each trace's own average.
        assert merged_entry.avg_drag == pytest.approx(single_trace_entry.avg_drag)
        assert merged_entry.avg_slack == pytest.approx(single_trace_entry.avg_slack)


def test_aggregate_then_merge_end_to_end_across_two_different_traces():
    # Two traces sharing every call path (same op/service names, see
    # linear_chain_graph_variant's docstring) but with different durations --
    # exercises the full aggregate-per-trace-then-merge-across-traces pipeline
    # that process_trace.py wires together, cross-checked against the real
    # calculate_drag/calculate_slack outputs rather than hand-predicted numbers.
    g1 = linear_chain_graph()
    g2 = linear_chain_graph_variant()

    agg1 = aggregate_drag_slack_by_callpath(g1, calculate_drag(g1, g1.findCriticalPath()), calculate_slack(g1, g1.findCriticalPath()))
    agg2 = aggregate_drag_slack_by_callpath(g2, calculate_drag(g2, g2.findCriticalPath()), calculate_slack(g2, g2.findCriticalPath()))

    merged = merge_per_method_slack_drag([agg1, agg2])

    assert set(merged.keys()) == set(agg1.keys()) == set(agg2.keys())
    for call_path in merged:
        expected_span_count = agg1[call_path].span_count + agg2[call_path].span_count
        expected_total_drag = agg1[call_path].total_drag + agg2[call_path].total_drag
        expected_total_slack = agg1[call_path].total_slack + agg2[call_path].total_slack

        assert merged[call_path].span_count == expected_span_count
        assert merged[call_path].total_drag == pytest.approx(expected_total_drag)
        assert merged[call_path].total_slack == pytest.approx(expected_total_slack)
        assert merged[call_path].avg_drag == pytest.approx(expected_total_drag / expected_span_count)
        assert merged[call_path].avg_slack == pytest.approx(expected_total_slack / expected_span_count)
