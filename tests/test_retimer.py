# ruff: noqa: I001
import json
import os

import pytest

from crisp.graph import Graph
from crisp.dependency_graph import DependencyGraph
from crisp.retimer import Retimer


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


def _load_test_case(filename):
    test_dir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.join(test_dir, "..", "test_cases", filename)
    with open(path) as f:
        data = json.load(f)
    return Graph(data, "S1", "O1", path)


# R ([S1] OR) -> X ([S2] OX)
# R runs 0-300; X runs 50-250 (R's only child -- no true sibling to compete with).
def only_child_graph():
    spans = [
        _span("R", "OR", "S1", 0, 300),
        _span("X", "OX", "S2", 50, 200, parent_id="R"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2"}, "S1", "OR")


# R ([S1] OR) -> A ([S2] OA)
#             -> B ([S2] OB)
# R runs 0-1000. A runs 0-200; B runs 300-600, well after A ends -- a genuine
# happens-before edge (A -> B) is inferred by the real DependencyGraph.
def two_siblings_dependent_graph():
    spans = [
        _span("R", "OR", "S1", 0, 1000),
        _span("A", "OA", "S2", 0, 200, parent_id="R"),
        _span("B", "OB", "S2", 300, 300, parent_id="R"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2"}, "S1", "OR")


# R ([S1] OR) -> A ([S2] OA)
#             -> B ([S2] OB)
#             -> C ([S2] OC)
# R runs 0-1000. A runs 0-100; B runs 200-500; C runs 700-900. Each earlier
# sibling ends well before the next starts, so DependencyGraph infers a full
# chain of happens-before edges: A -> B, A -> C, B -> C.
def three_siblings_chain_graph():
    spans = [
        _span("R", "OR", "S1", 0, 1000),
        _span("A", "OA", "S2", 0, 100, parent_id="R"),
        _span("B", "OB", "S2", 200, 300, parent_id="R"),
        _span("C", "OC", "S2", 700, 200, parent_id="R"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2"}, "S1", "OR")


# R ([S1] OR) -> A ([S2] OA)  -- a long-running sibling spanning most of R.
#             -> B ([S2] OB)  -- runs entirely inside A's window; no happens-before
#                                relationship is inferred either way (A starts
#                                before B and ends after B, so neither "ends
#                                before the other starts").
def spanning_sibling_and_nested_sibling_graph():
    spans = [
        _span("R", "OR", "S1", 0, 1000),
        _span("A", "OA", "S2", 0, 900, parent_id="R"),
        _span("B", "OB", "S2", 100, 100, parent_id="R"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2"}, "S1", "OR")


# P ([S1] OP) -> B1 ([S2] Ob)  -- runs 0-100
#             -> B2 ([S2] Ob)  -- runs 200-300, a second, later instance of the
#                                 SAME call path ("Ob")
#             -> C ([S3] Oc)   -- runs 0-1000, spans all of P
# Since B1 (an earlier instance of "Ob") finishes before B2 starts, DependencyGraph
# records a self-referential happens_before edge for "Ob" (see dependency_graph.py).
def fanout_same_callpath_graph():
    spans = [
        _span("P", "OP", "S1", 0, 1000),
        _span("B1", "Ob", "S2", 0, 100, parent_id="P"),
        _span("B2", "Ob", "S2", 200, 100, parent_id="P"),
        _span("C", "Oc", "S3", 0, 1000, parent_id="P"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2", "S3": "S3"}, "S1", "OP")


# GG ([S1] OGG) -> P ([S2] OP) -- GG's only child, 150-950
#                    -> A ([S3] OA) -- 150-250
#                    -> B ([S3] OB) -- 300-500, happens-before A
# Three levels deep: retiming A can cascade through P up to GG.
def three_level_graph():
    spans = [
        _span("GG", "OGG", "S1", 100, 900),
        _span("P", "OP", "S2", 150, 800, parent_id="GG"),
        _span("A", "OA", "S3", 150, 100, parent_id="P"),
        _span("B", "OB", "S3", 300, 200, parent_id="P"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2", "S3": "S3"}, "S1", "OGG")


# R ([S1] OR) -- a leaf root with no children and no parent.
def root_only_graph():
    spans = [_span("R", "OR", "S1", 0, 100)]
    return _build_graph(spans, {"S1": "S1"}, "S1", "OR")


# --- No-op and validation. ---


def test_no_op_when_new_times_equal_current_makes_no_changes_and_does_not_set_retimed():
    g = two_siblings_dependent_graph()
    dep = DependencyGraph(graph=g)
    before = Retimer.snapshot(g)

    Retimer(dep).retime_node(g, "A", g.nodeHT["A"].startTime, g.nodeHT["A"].endTime)

    assert Retimer.snapshot(g) == before
    assert g.retimed is False


def test_new_start_greater_than_new_end_raises_value_error():
    g = two_siblings_dependent_graph()
    dep = DependencyGraph(graph=g)

    with pytest.raises(ValueError, match="greater than new end time"):
        Retimer(dep).retime_node(g, "A", 300, 100)


# --- Leaf retime with zero siblings (only child): always the extremum. ---


def test_only_child_retime_later_propagates_full_delay_to_parent():
    g = only_child_graph()
    dep = DependencyGraph(graph=g)

    Retimer(dep).retime_node(g, "X", 50, 300)  # delayed by 50

    assert (g.nodeHT["X"].startTime, g.nodeHT["X"].endTime) == (50, 300)
    # X is R's only child, so it's always the extremum -- R's end must extend
    # by the same 50, preserving the original 50-unit aggregation gap (300-250).
    assert (g.nodeHT["R"].startTime, g.nodeHT["R"].endTime) == (0, 350)


def test_only_child_retime_earlier_propagates_full_rush_to_parent():
    g = only_child_graph()
    dep = DependencyGraph(graph=g)

    Retimer(dep).retime_node(g, "X", 50, 200)  # rushed earlier by 50

    assert (g.nodeHT["X"].startTime, g.nodeHT["X"].endTime) == (50, 200)
    assert (g.nodeHT["R"].startTime, g.nodeHT["R"].endTime) == (0, 250)


# --- Root/no-parent retime. ---


def test_root_node_retime_mutates_self_and_returns_cleanly():
    g = root_only_graph()
    dep = DependencyGraph(graph=g)

    Retimer(dep).retime_node(g, "R", 0, 150)

    assert (g.nodeHT["R"].startTime, g.nodeHT["R"].endTime, g.nodeHT["R"].duration) == (0, 150, 150)
    assert g.retimed is True


# --- Sibling shift is gated on a genuine happens-before relationship. ---


@pytest.mark.parametrize(
    ("new_end", "expected_delay"),
    [(250, 50), (150, -50)],
    ids=["delayed_later", "rushed_earlier"],
)
def test_dependent_sibling_shifts_by_exactly_the_delay(new_end, expected_delay):
    g = two_siblings_dependent_graph()
    dep = DependencyGraph(graph=g)
    a_name = g.getCallPath(g.nodeHT["A"])
    b_name = g.getCallPath(g.nodeHT["B"])
    assert dep.deps[b_name].happens_before == {a_name}

    b_before = (g.nodeHT["B"].startTime, g.nodeHT["B"].endTime)
    Retimer(dep).retime_node(g, "A", 0, new_end)
    b_after = (g.nodeHT["B"].startTime, g.nodeHT["B"].endTime)

    assert b_after == (b_before[0] + expected_delay, b_before[1] + expected_delay)


def test_delay_cascades_through_a_chain_of_dependent_siblings():
    g = three_siblings_chain_graph()
    dep = DependencyGraph(graph=g)

    Retimer(dep).retime_node(g, "B", 200, 600)  # B delayed by 100 (was 200-500)

    assert (g.nodeHT["A"].startTime, g.nodeHT["A"].endTime) == (0, 100)  # unaffected: nothing precedes A
    assert (g.nodeHT["B"].startTime, g.nodeHT["B"].endTime) == (200, 600)
    # C happens-before-depends on B, so it cascades by the same 100-unit delay.
    assert (g.nodeHT["C"].startTime, g.nodeHT["C"].endTime) == (800, 1000)
    # C becomes R's new latest-ending child, extending R's end by the same delay.
    assert (g.nodeHT["R"].startTime, g.nodeHT["R"].endTime) == (0, 1100)


def test_sibling_with_no_happens_before_relationship_never_shifts():
    g = spanning_sibling_and_nested_sibling_graph()
    dep = DependencyGraph(graph=g)
    a_name = g.getCallPath(g.nodeHT["A"])
    b_name = g.getCallPath(g.nodeHT["B"])
    assert a_name not in dep.deps[b_name].happens_before
    assert b_name not in dep.deps[a_name].happens_before

    Retimer(dep).retime_node(g, "B", 150, 250)  # delayed by 50

    # A has no happens-before relationship to B in either direction, so it must
    # not move even though B's new end (250) is still well within A's span.
    assert (g.nodeHT["A"].startTime, g.nodeHT["A"].endTime) == (0, 900)


def test_manually_cleared_happens_before_edge_prevents_sibling_shift():
    # Simulates a multi-trace aggregate where a counter-example dropped the edge
    # (see dependency_graph.py's aggregation rule) -- retime_node must respect
    # whatever DependencyGraph.deps says, not re-derive the relationship itself.
    g = two_siblings_dependent_graph()
    dep = DependencyGraph(graph=g)
    b_name = g.getCallPath(g.nodeHT["B"])
    assert dep.deps[b_name].happens_before  # sanity: the edge exists before clearing
    dep.deps[b_name].happens_before.clear()

    Retimer(dep).retime_node(g, "A", 0, 250)

    assert (g.nodeHT["B"].startTime, g.nodeHT["B"].endTime) == (300, 600)


# --- Parent propagation: extremum vs. non-extremum children. ---


def test_child_becoming_new_latest_ending_sibling_extends_parent_end():
    g = spanning_sibling_and_nested_sibling_graph()
    dep = DependencyGraph(graph=g)

    Retimer(dep).retime_node(g, "B", 850, 950)  # B now ends later than A (900)

    assert (g.nodeHT["A"].startTime, g.nodeHT["A"].endTime) == (0, 900)  # untouched
    # R's end extends by the same 100-unit gap it originally had past A (1000-900).
    assert (g.nodeHT["R"].startTime, g.nodeHT["R"].endTime) == (0, 1050)


def test_child_not_becoming_new_extremum_leaves_parent_untouched():
    g = spanning_sibling_and_nested_sibling_graph()
    dep = DependencyGraph(graph=g)

    Retimer(dep).retime_node(g, "B", 150, 250)  # still far short of A's endTime (900)

    assert (g.nodeHT["A"].startTime, g.nodeHT["A"].endTime) == (0, 900)
    assert (g.nodeHT["R"].startTime, g.nodeHT["R"].endTime) == (0, 1000)


# --- freeze_starts. ---


def test_freeze_starts_true_keeps_parent_start_fixed_when_child_moves_earlier():
    g = two_siblings_dependent_graph()
    dep = DependencyGraph(graph=g)

    Retimer(dep).retime_node(g, "A", -50, 150, freeze_starts=True)

    assert g.nodeHT["A"].startTime == -50
    assert g.nodeHT["R"].startTime == 0  # frozen, despite A now starting earlier


def test_freeze_starts_false_moves_parent_start_when_child_becomes_new_earliest():
    g = two_siblings_dependent_graph()
    dep = DependencyGraph(graph=g)

    Retimer(dep).retime_node(g, "A", -50, 150, freeze_starts=False)

    assert g.nodeHT["A"].startTime == -50
    assert g.nodeHT["R"].startTime == -50  # follows A's new, earlier start


def test_freeze_starts_only_applies_to_immediate_parent_not_grandparent():
    g_frozen = three_level_graph()
    Retimer(DependencyGraph(graph=g_frozen)).retime_node(g_frozen, "A", 50, 150, freeze_starts=True)

    g_unfrozen = three_level_graph()
    Retimer(DependencyGraph(graph=g_unfrozen)).retime_node(g_unfrozen, "A", 50, 150, freeze_starts=False)

    # The immediate parent (P) differs between the two runs...
    assert g_frozen.nodeHT["P"].startTime == 150
    assert g_unfrozen.nodeHT["P"].startTime == 50
    assert g_frozen.nodeHT["P"].startTime != g_unfrozen.nodeHT["P"].startTime

    # ...but propagation beyond the immediate parent always uses freeze_starts=True,
    # regardless of what the top-level caller passed -- so GG's start is identical.
    assert g_frozen.nodeHT["GG"].startTime == g_unfrozen.nodeHT["GG"].startTime == 100


# --- Multi-level propagation. ---


def test_multilevel_propagation_cascades_through_parent_and_grandparent():
    g = three_level_graph()
    dep = DependencyGraph(graph=g)

    Retimer(dep).retime_node(g, "A", 150, 250 + 40)  # A delayed by 40

    assert g.nodeHT["A"].endTime == 290
    # B happens-before-depends on A, so it cascades by the same 40-unit delay.
    assert (g.nodeHT["B"].startTime, g.nodeHT["B"].endTime) == (340, 540)
    # B becomes P's new latest-ending child, so P's end extends by 40 too
    # (preserving P's original 450-unit gap past B: 950-500).
    assert g.nodeHT["P"].endTime == 990
    # And P is GG's only child, so GG's end extends by the same 40 in turn.
    assert g.nodeHT["GG"].endTime == 1040


# --- Same-call-path fanout: the sid-based self-guard. ---


def test_fanout_retiming_later_instance_does_not_affect_earlier_instance():
    g = fanout_same_callpath_graph()
    dep = DependencyGraph(graph=g)
    ob_name = g.getCallPath(g.nodeHT["B1"])
    assert dep.deps[ob_name].happens_before == {ob_name}  # self-referential, by design

    # Jump B2 forward (not just stretch it) so a naive, un-guarded check (that
    # failed to exclude the node's own span id before the happens_before lookup)
    # would spuriously re-match B2 against its own self-referential edge.
    Retimer(dep).retime_node(g, "B2", 500, 600)

    assert (g.nodeHT["B1"].startTime, g.nodeHT["B1"].endTime) == (0, 100)
    assert (g.nodeHT["B2"].startTime, g.nodeHT["B2"].endTime) == (500, 600)


def test_fanout_retiming_earlier_instance_cascades_delay_to_later_instance():
    g = fanout_same_callpath_graph()
    dep = DependencyGraph(graph=g)

    Retimer(dep).retime_node(g, "B1", 0, 150)  # B1 delayed by 50

    assert (g.nodeHT["B1"].startTime, g.nodeHT["B1"].endTime) == (0, 150)
    # B2, the later instance of the same call path, cascades by the same delay,
    # via the legitimate self-referential happens_before edge -- not a self-match.
    assert (g.nodeHT["B2"].startTime, g.nodeHT["B2"].endTime) == (250, 350)


# --- Round trip: retime forward, then back, restores the graph exactly. ---


def test_round_trip_retime_forward_then_back_restores_every_node_exactly():
    g = two_siblings_dependent_graph()
    dep = DependencyGraph(graph=g)
    before = Retimer.snapshot(g)
    a = g.nodeHT["A"]
    original_start, original_end = a.startTime, a.endTime

    Retimer(dep).retime_node(g, "A", original_start, original_end + 75)
    assert Retimer.snapshot(g) != before  # sanity: the forward retime really changed something

    Retimer(dep).retime_node(g, "A", original_start, original_end)

    assert Retimer.snapshot(g) == before


def test_round_trip_multilevel_restores_every_node_exactly():
    g = three_level_graph()
    dep = DependencyGraph(graph=g)
    before = Retimer.snapshot(g)
    a = g.nodeHT["A"]
    original_start, original_end = a.startTime, a.endTime

    Retimer(dep).retime_node(g, "A", original_start - 20, original_end - 20)
    assert Retimer.snapshot(g) != before

    Retimer(dep).retime_node(g, "A", original_start, original_end)

    assert Retimer.snapshot(g) == before


# --- snapshot()/restore(): explicit alternative to a second retime_node call. ---


def test_restore_undoes_a_multilevel_cascade_without_a_second_retime_call():
    g = three_level_graph()
    dep = DependencyGraph(graph=g)
    before = Retimer.snapshot(g)
    a = g.nodeHT["A"]

    Retimer(dep).retime_node(g, "A", a.startTime - 20, a.endTime - 20)
    assert Retimer.snapshot(g) != before  # sanity: the cascade really touched the graph
    assert g.retimed is True

    Retimer.restore(g, before)

    assert Retimer.snapshot(g) == before
    assert g.retimed is False


def test_restore_is_exact_even_when_the_touched_node_set_is_unknown_in_advance():
    # Unlike computeProjectedCPMetrics, callers can't predict which nodes
    # retime_node will touch ahead of time (sibling shifts + cascading parent
    # propagation), so restore() must work from a snapshot of the whole graph.
    g = two_siblings_dependent_graph()
    dep = DependencyGraph(graph=g)
    before = Retimer.snapshot(g)

    b = g.nodeHT["B"]
    Retimer(dep).retime_node(g, "B", b.startTime, b.endTime + 40)

    Retimer.restore(g, before)

    assert Retimer.snapshot(g) == before


# --- Graceful degradation with an unrelated/mismatched DependencyGraph. ---


def test_missing_dependency_graph_entries_does_not_crash_and_skips_unknown_shifts():
    unrelated_graph = _build_graph([_span("Z", "OZ", "SX", 0, 50)], {"SX": "SX"}, "SX", "OZ")
    dep = DependencyGraph(graph=unrelated_graph)  # knows nothing about g below

    g = two_siblings_dependent_graph()

    Retimer(dep).retime_node(g, "A", 0, 250)  # must not KeyError

    # No happens_before info is available for B's call path, so it can't be
    # known to depend on A -- it must not shift.
    assert (g.nodeHT["B"].startTime, g.nodeHT["B"].endTime) == (300, 600)


# --- Graph.retimeNodeWithDependencyGraph hook. ---


def test_graph_hook_matches_direct_retimer_usage():
    g_hook = two_siblings_dependent_graph()
    g_direct = two_siblings_dependent_graph()
    dep = DependencyGraph(graph=g_direct)

    g_hook.retimeNodeWithDependencyGraph("A", 0, 260)
    Retimer(dep).retime_node(g_direct, "A", 0, 260)

    assert Retimer.snapshot(g_hook) == Retimer.snapshot(g_direct)


def test_graph_hook_builds_dependency_graph_internally_when_none_passed():
    g = two_siblings_dependent_graph()

    g.retimeNodeWithDependencyGraph("A", 0, 260)

    # The internally-built DependencyGraph must still infer the real
    # happens-before edge, so B cascades exactly as it would with an explicit one.
    assert (g.nodeHT["B"].startTime, g.nodeHT["B"].endTime) == (360, 660)


def test_graph_hook_accepts_an_explicit_prebuilt_dependency_graph():
    g = two_siblings_dependent_graph()
    dep = DependencyGraph(graph=g)

    g.retimeNodeWithDependencyGraph("A", 0, 260, dependency_graph=dep)

    assert (g.nodeHT["B"].startTime, g.nodeHT["B"].endTime) == (360, 660)


# --- Graph.retimed flag. ---


def test_retimed_flag_starts_false_and_flips_true_after_a_real_mutation():
    g = two_siblings_dependent_graph()
    assert g.retimed is False

    Retimer(DependencyGraph(graph=g)).retime_node(g, "A", 0, 260)

    assert g.retimed is True


# --- One Retimer/DependencyGraph reused across multiple, unrelated Graphs. ---


def test_retimer_instance_reused_across_multiple_graphs():
    g1 = only_child_graph()
    g2 = only_child_graph()
    dep = DependencyGraph(graph=g1)
    retimer = Retimer(dep)

    retimer.retime_node(g1, "X", 50, 300)
    retimer.retime_node(g2, "X", 50, 200)

    assert (g1.nodeHT["R"].startTime, g1.nodeHT["R"].endTime) == (0, 350)
    assert (g2.nodeHT["R"].startTime, g2.nodeHT["R"].endTime) == (0, 250)


# --- Realistic fixtures from test_cases/*.json. ---


@pytest.mark.parametrize("filename", ["3.json", "5.json"])
def test_realistic_fixture_leaf_retime_does_not_crash_and_updates_root(filename):
    g = _load_test_case(filename)
    assert g.rootNode is not None

    leaf = next(n for n in g.nodeHT.values() if not n.children and n.parent is not None)
    root_end_before = g.rootNode.endTime

    dep = DependencyGraph(graph=g)
    Retimer(dep).retime_node(g, leaf.sid, leaf.startTime, leaf.endTime + 10)

    assert leaf.endTime == leaf.startTime + leaf.duration
    assert g.rootNode.endTime >= root_end_before
    assert g.retimed is True
