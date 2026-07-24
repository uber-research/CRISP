# ruff: noqa: I001
"""Cross-validates Retimer.retime_method against Graph.computeProjectedCPMetrics.

Per the porting plan's "Coexistence" section: on a graph with no parallel
siblings (a pure single-child chain), the two algorithms' semantics
genuinely coincide, so their latency-change answers must agree. On a graph
with parallel/overlapping siblings, they may legitimately diverge -- the
whole reason the Retimer exists is to model sibling reordering that
computeProjectedCPMetrics cannot.

Sign convention (verified empirically, not assumed -- see PR description):
``retime_method`` returns ``original_root_duration - retimed_root_duration``,
i.e. positive means the root got *faster*. ``computeProjectedCPMetrics``'s
``projected_latency`` (its second return value) carries the *same sign as
the injected ``deltaMicroSec``* -- negative for a shrink, positive for a
growth -- because it comes from ``Graph.computeTimeChange``, which reports
how much the root's end time itself moved (negative == moved earlier ==
faster). The two are therefore negatives of each other whenever the
algorithms agree: ``retime_method(...) == -projected_latency``.
"""

import pytest

from crisp.graph import Graph
from crisp.dependency_graph import DependencyGraph
from crisp.retimer import Retimer


def _span(span_id, op, pid, start, duration, parent_id=None, kind="server"):
    return {
        "traceID": "T",
        "spanID": span_id,
        "operationName": op,
        "startTime": start,
        "duration": duration,
        "processID": pid,
        "warnings": None,
        "references": ([] if parent_id is None else [{"refType": "CHILD_OF", "traceID": "T", "spanID": parent_id}]),
        # Both computeProjectedCPMetrics and retime_method only ever match
        # SERVER spans (see graph.py's _findMatchingNodes and retimer.py's
        # _matches_method) -- every span below needs this tag to be a
        # candidate "method" at all.
        "tags": [{"key": "span.kind", "type": "string", "value": kind}],
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


# R ([S1] OR) -> A ([S2] OA) -> B ([S3] OB)
# R runs 0-1000; A (the target method) runs 100-600 and has one child, B,
# running 200-400. A single-child chain top to bottom -- no node ever has a
# sibling, so there is nothing for the DependencyGraph to reorder.
def linear_chain_graph():
    spans = [
        _span("R", "OR", "S1", 0, 1000),
        _span("A", "OA", "S2", 100, 500, parent_id="R"),
        _span("B", "OB", "S3", 200, 200, parent_id="A"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2", "S3": "S3"}, "S1", "OR")


# R ([S1] OR) -> A ([S2] OA) -> B ([S3] OB) -> C ([S2] OA)
# Same shape as linear_chain_graph, but the target method (S2/OA) occurs
# twice along the single chain: once at A (100-600, has a child) and again
# at C (250-300, a leaf nested three levels down inside B). Still a pure
# chain -- no siblings anywhere -- so the two instances don't interact via
# any sibling relationship; they only interact via the shared ancestor
# chain itself (retiming C cascades up through B into A, same as retiming
# A directly would).
def chain_with_repeated_method_graph():
    spans = [
        _span("R", "OR", "S1", 0, 1000),
        _span("A", "OA", "S2", 100, 500, parent_id="R"),
        _span("B", "OB", "S3", 200, 200, parent_id="A"),
        _span("C", "OA", "S2", 250, 50, parent_id="B"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2", "S3": "S3"}, "S1", "OR")


# R ([S1] OR) -> A ([S2] OA) -- 0-400, the target method
#             -> B ([S2] OB) -- 50-450, overlaps A; neither happens-before
#                                the other (too much overlap for either
#                                direction), so A and B are genuine parallel
#                                siblings.
#             -> C ([S3] OC) -- 700-900, starts well after both A and B end,
#                                so C happens-before-depends on BOTH of them
#                                individually (a real, direct edge from A to
#                                C, not mediated by B).
def overlapping_siblings_then_series_graph():
    spans = [
        _span("R", "OR", "S1", 0, 1000),
        _span("A", "OA", "S2", 0, 400, parent_id="R"),
        _span("B", "OB", "S2", 50, 400, parent_id="R"),
        _span("C", "OC", "S3", 700, 200, parent_id="R"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2", "S3": "S3"}, "S1", "OR")


# --- Chain-only agreement: fixed_difference. ---


@pytest.mark.parametrize(
    ("target_service", "target_operation", "fixed_difference"),
    [
        ("S2", "OA", -50),  # shrink a non-leaf method; floor = its child B's endTime
        ("S2", "OA", 80),  # grow the same, non-leaf method
        ("S3", "OB", -500),  # shrink a leaf far past its own duration -- floor-clamped to 0
    ],
    ids=["shrink_non_leaf", "grow_non_leaf", "shrink_leaf_floor_clamped"],
)
def test_chain_fixed_difference_matches_projected_latency_sign_flipped(target_service, target_operation, fixed_difference):
    g1 = linear_chain_graph()
    retime_result = Retimer(DependencyGraph(graph=g1)).retime_method(g1, target_service, target_operation, fixed_difference=fixed_difference)

    g2 = linear_chain_graph()
    _, projected_latency = g2.computeProjectedCPMetrics(fixed_difference, target_service, target_operation)

    assert retime_result == pytest.approx(-projected_latency)


# --- Chain-only agreement: percent_difference (translated to an equivalent fixed delta). ---


def test_chain_percent_difference_matches_projected_latency_using_equivalent_fixed_delta():
    g1 = linear_chain_graph()
    original_duration = g1.nodeHT["A"].duration
    retime_result = Retimer(DependencyGraph(graph=g1)).retime_method(g1, "S2", "OA", percent_difference=-20)

    # computeProjectedCPMetrics only takes a fixed deltaMicroSec, so translate
    # the percent difference into the equivalent fixed amount ourselves.
    equivalent_fixed_delta = original_duration * -20 / 100.0
    g2 = linear_chain_graph()
    _, projected_latency = g2.computeProjectedCPMetrics(equivalent_fixed_delta, "S2", "OA")

    assert retime_result == pytest.approx(-projected_latency)


# --- Chain-only agreement: the same method retimed at more than one instance. ---


def test_multiple_instances_of_same_method_are_all_retimed_and_agree_with_projected_latency():
    g1 = chain_with_repeated_method_graph()
    a_before_end = g1.nodeHT["A"].endTime
    c_before_end = g1.nodeHT["C"].endTime

    retime_result = Retimer(DependencyGraph(graph=g1)).retime_method(g1, "S2", "OA", fixed_difference=-30)

    # Both instances (A and C) must be retimed directly -- not just the first
    # one found. A shrinks by more than 30 because C is nested inside A along
    # the chain: C's own -30 cascades up through B into A on top of A's own
    # direct -30 -- an expected compounding of a chain, not a bug.
    assert g1.nodeHT["C"].endTime == c_before_end - 30
    assert g1.nodeHT["A"].endTime == a_before_end - 60

    g2 = chain_with_repeated_method_graph()
    _, projected_latency = g2.computeProjectedCPMetrics(-30, "S2", "OA")

    assert retime_result == pytest.approx(-projected_latency)


# --- Parallel/overlapping siblings: intentional divergence. ---


def test_parallel_overlapping_siblings_cause_intentional_divergence_from_projected_latency():
    g1 = overlapping_siblings_then_series_graph()
    retime_result = Retimer(DependencyGraph(graph=g1)).retime_method(g1, "S2", "OA", fixed_difference=-100)

    g2 = overlapping_siblings_then_series_graph()
    _, projected_latency = g2.computeProjectedCPMetrics(-100, "S2", "OA")

    # Divergence, by design (see the plan's "Coexistence" section). A has a
    # genuine, direct happens-before edge to C (A ends at 400, strictly
    # before C starts at 700), so retime_method's sibling cascade shifts C --
    # and therefore the root -- earlier by the same 100 units, regardless of
    # A's overlapping-but-unrelated sibling B.
    #
    # computeProjectedCPMetrics's projected_latency comes from
    # Graph.computeTimeChange/ComputeAllSeriesTimeChange, which groups
    # overlapping children into one "parallel block" (A and B together,
    # since neither strictly happens-before the other) bounded by whichever
    # member ends last -- here, B, which this delta never touches. That
    # block-level view has no way to express "only A, not the whole block,
    # feeds forward to C", so it reports no latency change at all.
    assert retime_result == 100
    assert projected_latency == 0
    assert retime_result != pytest.approx(-projected_latency)


# --- retime_method's own contract, independent of cross-validation. ---


def test_retime_method_raises_when_both_difference_kinds_are_set():
    g = linear_chain_graph()
    with pytest.raises(ValueError, match="Only one of"):
        Retimer(DependencyGraph(graph=g)).retime_method(g, "S2", "OA", percent_difference=10, fixed_difference=10)


def test_retime_method_raises_when_neither_difference_kind_is_set():
    g = linear_chain_graph()
    with pytest.raises(ValueError, match="One of"):
        Retimer(DependencyGraph(graph=g)).retime_method(g, "S2", "OA")


def test_retime_method_is_a_no_op_when_no_node_matches():
    g = linear_chain_graph()
    before = Retimer.snapshot(g)

    retime_result = Retimer(DependencyGraph(graph=g)).retime_method(g, "NoSuchService", "NoSuchOp", fixed_difference=-50)

    assert retime_result == 0
    assert Retimer.snapshot(g) == before


def test_percent_difference_result_is_independent_of_nodeHT_iteration_order():
    # A (S2/OA) and C (S2/OA) are the same method, nested along one chain
    # (A is C's grandparent). Processing C before A would otherwise shrink
    # A's `duration` via cascade before A's own percent adjustment is
    # computed off of it -- so build the identical graph with spans listed
    # in the opposite order (flips g.nodeHT's insertion/iteration order)
    # and confirm the two runs still agree.
    g_forward = chain_with_repeated_method_graph()
    forward_result = Retimer(DependencyGraph(graph=g_forward)).retime_method(g_forward, "S2", "OA", percent_difference=-20)

    g_reversed = _build_graph(
        list(
            reversed(
                [
                    _span("R", "OR", "S1", 0, 1000),
                    _span("A", "OA", "S2", 100, 500, parent_id="R"),
                    _span("B", "OB", "S3", 200, 200, parent_id="A"),
                    _span("C", "OA", "S2", 250, 50, parent_id="B"),
                ]
            )
        ),
        {"S1": "S1", "S2": "S2", "S3": "S3"},
        "S1",
        "OR",
    )
    reversed_result = Retimer(DependencyGraph(graph=g_reversed)).retime_method(g_reversed, "S2", "OA", percent_difference=-20)

    assert forward_result == pytest.approx(reversed_result)
    assert (g_forward.nodeHT["A"].endTime, g_forward.nodeHT["C"].endTime) == pytest.approx(
        (g_reversed.nodeHT["A"].endTime, g_reversed.nodeHT["C"].endTime)
    )


# --- rootNode scoping: a Graph can hold more than one independent root subtree. ---


# Two disjoint root spans (each with no parent) inside one Graph/nodeHT --
# mirrors mergeAllRoots' getMatchingRootsFromGraph, which finds every
# parentless, service/operation-matching node in graph.nodeHT.values().
def two_independent_roots_graph():
    spans = [
        _span("R1", "OR", "S1", 0, 1000),
        _span("A1", "OA", "S2", 100, 500, parent_id="R1"),
        _span("R2", "OR", "S1", 2000, 1000),
        _span("A2", "OA", "S2", 2100, 500, parent_id="R2"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2"}, "S1", "OR")


def test_retime_method_scoped_to_an_explicit_root_only_touches_that_roots_subtree():
    g = two_independent_roots_graph()
    # g.rootNode is whichever of R1/R2 the constructor happened to pick first;
    # scope this call to the *other* one to prove the choice isn't hardcoded.
    scoped_root, scoped_a = ("R2", "A2") if g.rootNode.sid == "R1" else ("R1", "A1")
    other_root, other_a = ("R1", "A1") if scoped_root == "R2" else ("R2", "A2")
    scoped_a_before_end = g.nodeHT[scoped_a].endTime
    other_a_before_end = g.nodeHT[other_a].endTime
    other_root_before_end = g.nodeHT[other_root].endTime

    retime_result = Retimer(DependencyGraph(graph=g)).retime_method(
        g,
        "S2",
        "OA",
        fixed_difference=-50,
        rootNode=g.nodeHT[scoped_root],
    )

    assert retime_result == pytest.approx(50)
    assert g.nodeHT[scoped_a].endTime == scoped_a_before_end - 50
    # The other, unrelated root subtree is left completely untouched.
    assert g.nodeHT[other_a].endTime == other_a_before_end
    assert g.nodeHT[other_root].endTime == other_root_before_end


def test_retime_method_defaults_to_g_root_node_when_rootNode_not_passed():
    g = two_independent_roots_graph()
    root_a = "A1" if g.rootNode.sid == "R1" else "A2"
    other_a = "A2" if root_a == "A1" else "A1"
    other_a_before = g.nodeHT[other_a].endTime

    retime_result = Retimer(DependencyGraph(graph=g)).retime_method(g, "S2", "OA", fixed_difference=-50)

    assert retime_result == pytest.approx(50)
    assert g.nodeHT[other_a].endTime == other_a_before  # the non-default root is untouched
