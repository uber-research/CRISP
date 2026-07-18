# ruff: noqa: I001
import pytest

from crisp.graph import Graph, GraphNode
from crisp.shared.models import SpanKind
from crisp.dependency_graph import (
    DependencyGraph,
    DependencyGraphNode,
)
from crisp.configuration import (
    ConfigBuilder,
    reset_config,
    set_config,
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


# Async (fire-and-forget) children -- i.e. a child whose endTime is genuinely after its
# parent's endTime -- get truncated by Graph's own sanitizeOverflowingChildren() unless
# they're recognized as a lone client/server RPC pair. To construct such timing
# deterministically (including alongside ordinary siblings, where that RPC exemption
# does not apply), build the Graph directly via GraphNode, bypassing Graph's JSON
# parsing/sanitization entirely -- the same pattern used in test_graph_timesaved.py.
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


# A ([S1] O1) -- a leaf root with no children.
# A runs 0-100.
def leaf_only_graph():
    spans = [_span("A", "O1", "S1", 0, 100)]
    return _build_graph(spans, {"S1": "S1"}, "S1", "O1")


# A ([S1] O1) -> B ([S2] O2)
# A runs 0-100, B runs 10-90 (single child, no siblings).
def single_child_graph():
    spans = [
        _span("A", "O1", "S1", 0, 100),
        _span("B", "O2", "S2", 10, 80, parent_id="A"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2"}, "S1", "O1")


# A ([S1] O1) -> B ([S2] O2)
#             -> C ([S2] O3)
#             -> D ([S2] O4)
# A runs 0-300; B runs 0-80; C runs 100-180; D runs 200-280.
# Strictly serial: B finishes well before C starts, C finishes well before D starts.
def three_series_siblings_graph():
    spans = [
        _span("A", "O1", "S1", 0, 300),
        _span("B", "O2", "S2", 0, 80, parent_id="A"),
        _span("C", "O3", "S2", 100, 80, parent_id="A"),
        _span("D", "O4", "S2", 200, 80, parent_id="A"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2"}, "S1", "O1")


# A ([S1] O1) -> B ([S2] O2)
#             -> C ([S2] O3)
#             -> D ([S2] O4)
# A runs 0-500; B runs 0-50; C runs 60-300; D runs 100-400.
# B truly finishes before both C and D start. C and D overlap heavily (30-300 vs 100-400
# overlap region is far beyond the overlap-allowance tolerance), so D must NOT inherit a
# happens_before edge from C even though C ends before D.
def gap_violated_by_overlap_graph():
    spans = [
        _span("A", "O1", "S1", 0, 500),
        _span("B", "O2", "S2", 0, 50, parent_id="A"),
        _span("C", "O3", "S2", 60, 240, parent_id="A"),
        _span("D", "O4", "S2", 100, 300, parent_id="A"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2"}, "S1", "O1")


# A ([S1] O1) -> B ([S2] O2)
#             -> C ([S2] O3)
# A runs 0-200; B runs 0-150; C runs 10-160. B and C run concurrently with no clear
# ordering (overlap far exceeds the overlap-allowance tolerance in both directions).
def parallel_siblings_graph():
    spans = [
        _span("A", "O1", "S1", 0, 200),
        _span("B", "O2", "S2", 0, 150, parent_id="A"),
        _span("C", "O3", "S2", 10, 150, parent_id="A"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2"}, "S1", "O1")


# A ([S1] O1) -> B ([S2] O2)
#             -> C ([S2] O3)
# A runs 0-1000; B runs 0-500; C runs 495-1000.
# B and C overlap by 5, which is within the default 1% overlap-allowance tolerance of
# A's duration (1000). Adapted from test_graph.py's mild_overlap_graph().
def mild_overlap_graph():
    spans = [
        _span("A", "O1", "S1", 0, 1000),
        _span("B", "O2", "S2", 0, 500, parent_id="A"),
        _span("C", "O3", "S2", 495, 505, parent_id="A"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2"}, "S1", "O1")


# A ([S1] O1) -> B ([S2] O2)
# A runs 0-100; B runs 50-150 (B ends after A -- an async / fire-and-forget child).
def async_child_graph():
    return _build_manual_graph(
        [
            ("A", "O1", "S1", "S1", 0, 100, None),
            ("B", "O2", "S2", "S2", 50, 100, "A"),
        ],
        root_sid="A",
    )


# A ([S1] O1) -> B ([S2] O2) [async]
#             -> C ([S2] O3) [normal]
# A runs 0-100; B runs 50-150 (async, ends after A); C runs 0-40 (finishes before A ends
# AND before B starts). B being async should not prevent it from inheriting a
# happens_before edge from C: async-ness and sibling ordering are orthogonal.
def async_child_with_earlier_sibling_graph():
    return _build_manual_graph(
        [
            ("A", "O1", "S1", "S1", 0, 100, None),
            ("B", "O2", "S2", "S2", 50, 100, "A"),
            ("C", "O3", "S2", "S2", 0, 40, "A"),
        ],
        root_sid="A",
    )


# A ([S1] O1) -> B ([S2] O2)
# A runs 0-100; B runs 50-100 (B's endTime exactly equals A's endTime).
def child_ends_exactly_with_parent_graph():
    spans = [
        _span("A", "O1", "S1", 0, 100),
        _span("B", "O2", "S2", 50, 50, parent_id="A"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2"}, "S1", "O1")


# A ([S1] O1) -> B1 ([S2] Ob) -> C1 ([S3] Oc)
#             -> B2 ([S2] Ob) -> C2 ([S3] Oc)
# A runs 0-1000. B1 runs 0-400 with child C1 10-390. B2 runs 500-900 with child C2 520-880.
# A fans out to the same op ("Ob") twice; both instances share ONE DependencyGraphNode,
# same for the grandchildren ("Oc").
def fanout_same_callpath_graph():
    spans = [
        _span("A", "O1", "S1", 0, 1000),
        _span("B1", "Ob", "S2", 0, 400, parent_id="A"),
        _span("C1", "Oc", "S3", 10, 380, parent_id="B1"),
        _span("B2", "Ob", "S2", 500, 400, parent_id="A"),
        _span("C2", "Oc", "S3", 520, 360, parent_id="B2"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2", "S3": "S3"}, "S1", "O1")


# A ([S1] O1) -> B ([S2] Ob) -> Leaf ([S3] OShared)
#             -> D ([S4] Od) -> Leaf2 ([S3] OShared)
# Two different parents both eventually call the same op+service ("OShared"), but at two
# different call-tree positions -- they must resolve to two DISTINCT DependencyGraphNodes,
# proving identity is call-path-specific and not just op-name-specific.
def same_op_different_parents_graph():
    spans = [
        _span("A", "O1", "S1", 0, 1000),
        _span("B", "Ob", "S2", 0, 500, parent_id="A"),
        _span("Leaf", "OShared", "S3", 10, 90, parent_id="B"),
        _span("D", "Od", "S4", 500, 500, parent_id="A"),
        _span("Leaf2", "OShared", "S3", 510, 90, parent_id="D"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2", "S3": "S3", "S4": "S4"}, "S1", "O1")


# A ([S1] O1) -> B ([S2] O2) -> C ([S3] O3)
#             -> B ([S2] O2) -> D ([S4] O4)
# A runs 0-100; B runs 10-70; C runs 25-45; D runs 30-50.
# Adapted from test_graph.py's sampleGraph(): three levels deep, with C and D overlapping
# enough (well beyond the overlap-allowance tolerance of B's 60-length duration) that
# neither should get a happens_before edge from the other.
def multilevel_nesting_graph():
    spans = [
        _span("A", "O1", "S1", 0, 100),
        _span("B", "O2", "S2", 10, 60, parent_id="A"),
        _span("C", "O3", "S3", 25, 20, parent_id="B"),
        _span("D", "O4", "S4", 30, 20, parent_id="B"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2", "S3": "S3", "S4": "S4"}, "S1", "O1")


# A ([S1] O1) -> B ([S2] O2) [ends exactly with A]
#             -> C ([S2] O3) [normal, finishes early]
#             -> D ([S2] O4) [async, finishes after A]
# A runs 0-100; B runs 0-100; C runs 0-30; D runs 40-150.
def mixed_children_graph():
    return _build_manual_graph(
        [
            ("A", "O1", "S1", "S1", 0, 100, None),
            ("B", "O2", "S2", "S2", 0, 100, "A"),
            ("C", "O3", "S2", "S2", 0, 30, "A"),
            ("D", "O4", "S2", "S2", 40, 110, "A"),
        ],
        root_sid="A",
    )


# A ([S1] O1) -- root span; X ([S2] OX) -- an orphan span whose declared parent span ID
# ("MISSING") is not present anywhere in the trace, simulating Jaeger sampling loss.
def orphan_node_graph():
    spans = [
        _span("A", "O1", "S1", 0, 100),
        _span("X", "OX", "S2", 0, 10, parent_id="MISSING"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2"}, "S1", "O1")


# --- Fixtures used only by the multi-trace aggregation tests below. ---


# A ([S1] O1) -> C ([S2] O3)
#             -> D ([S2] O4)
# A runs 0-300; C runs 0-80; D runs 200-280 -- same shape as
# three_series_siblings_graph() but with the "B" call-path entirely absent, to
# simulate a trace that never observed that call-path at all.
def two_series_siblings_no_b_graph():
    spans = [
        _span("A", "O1", "S1", 0, 300),
        _span("C", "O3", "S2", 0, 80, parent_id="A"),
        _span("D", "O4", "S2", 200, 80, parent_id="A"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2"}, "S1", "O1")


# A ([S1] O1) -> B ([S2] O2)
#             -> D ([S2] O4)
# A runs 0-300; B runs 0-80; D runs 200-280. B strictly finishes before D
# starts, so this trace confirms "B happens-before D".
def two_series_b_then_d_graph():
    spans = [
        _span("A", "O1", "S1", 0, 300),
        _span("B", "O2", "S2", 0, 80, parent_id="A"),
        _span("D", "O4", "S2", 200, 80, parent_id="A"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2"}, "S1", "O1")


# A ([S1] O1) -> D ([S2] O4)
#             -> B ([S2] O2)
# Same two call-paths as two_series_b_then_d_graph(), but with the order
# reversed (D runs 0-80, B runs 200-280) -- this trace contradicts
# "B happens-before D".
def two_series_d_then_b_graph():
    spans = [
        _span("A", "O1", "S1", 0, 300),
        _span("D", "O4", "S2", 0, 80, parent_id="A"),
        _span("B", "O2", "S2", 200, 80, parent_id="A"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2"}, "S1", "O1")


# A ([S1] O1) -> B ([S2] O2), a single child whose timing is caller-supplied,
# used to build several traces with distinct parent_start_delay/parent_end_delay
# values for the delay-concatenation aggregation test.
def single_child_with_delay_graph(child_start, child_duration):
    spans = [
        _span("A", "O1", "S1", 0, 100),
        _span("B", "O2", "S2", child_start, child_duration, parent_id="A"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2"}, "S1", "O1")


# A ([S1] O1) -> B1 ([S2] Ob), a single occurrence of fanout_same_callpath_graph()'s
# fanout op (no second sibling at the same call-path). Used to contradict that
# graph's self-referential happens_before edge for "[S1] O1->[S2] Ob".
def single_ob_child_graph():
    spans = [
        _span("A", "O1", "S1", 0, 1000),
        _span("B1", "Ob", "S2", 0, 400, parent_id="A"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2"}, "S1", "O1")


# A ([S1] O1) -> Z ([S2] O9), a single child at a different op than
# single_child_graph()'s B, at the same parent call-path. Used to confirm that
# distinct call-paths observed in different traces both end up in the aggregate.
def single_child_graph_alt_op():
    spans = [
        _span("A", "O1", "S1", 0, 100),
        _span("Z", "O9", "S2", 10, 80, parent_id="A"),
    ]
    return _build_graph(spans, {"S1": "S1", "S2": "S2"}, "S1", "O1")


@pytest.fixture
def restore_config():
    yield
    reset_config()


def test_leaf_node_has_no_parent_delay_or_dependents():
    g = leaf_only_graph()
    deps = DependencyGraph(g).deps

    node = deps["[S1] O1"]
    assert node.parent_start_delay == []
    assert node.parent_end_delay == []
    assert node.child_dependents == set()
    assert node.async_children == set()
    assert node.happens_before == set()


def test_single_child_creates_child_dependent_without_happens_before():
    g = single_child_graph()
    deps = DependencyGraph(g).deps

    name_a, name_b = "[S1] O1", "[S1] O1->[S2] O2"
    assert deps[name_a].child_dependents == {name_b}
    assert deps[name_a].parent_start_delay == [10]
    assert deps[name_a].parent_end_delay == [10]
    assert deps[name_b].happens_before == set()
    assert deps[name_b].async_children == set()
    assert deps[name_b].child_dependents == set()


def test_three_series_siblings_happens_before_chain_and_direction():
    g = three_series_siblings_graph()
    deps = DependencyGraph(g).deps

    name_b = "[S1] O1->[S2] O2"
    name_c = "[S1] O1->[S2] O3"
    name_d = "[S1] O1->[S2] O4"

    assert deps[name_b].happens_before == set()
    assert deps[name_c].happens_before == {name_b}
    assert deps[name_d].happens_before == {name_b, name_c}
    # direction must not be reversed
    assert name_d not in deps[name_b].happens_before
    assert name_d not in deps[name_c].happens_before
    assert name_c not in deps[name_b].happens_before

    assert deps["[S1] O1"].child_dependents == {name_b, name_c, name_d}


def test_two_siblings_strict_series_edge_not_reversed():
    # Reuses the first two (of three) series siblings to isolate a pure 2-sibling
    # series relationship, independent of the third sibling D.
    g = three_series_siblings_graph()
    deps = DependencyGraph(g).deps
    name_b, name_c = "[S1] O1->[S2] O2", "[S1] O1->[S2] O3"

    assert deps[name_c].happens_before == {name_b}
    assert deps[name_b].happens_before == set()


def test_gap_violated_by_overlap_limits_happens_before():
    g = gap_violated_by_overlap_graph()
    deps = DependencyGraph(g).deps

    name_b = "[S1] O1->[S2] O2"
    name_c = "[S1] O1->[S2] O3"
    name_d = "[S1] O1->[S2] O4"

    # D truly starts after B ends, so B happens-before D.
    assert deps[name_d].happens_before == {name_b}
    # C overlaps D far beyond tolerance, so C must NOT be recorded as happening before D.
    assert name_c not in deps[name_d].happens_before
    assert deps[name_c].happens_before == {name_b}


def test_parallel_overlapping_siblings_have_no_happens_before():
    g = parallel_siblings_graph()
    deps = DependencyGraph(g).deps

    name_b, name_c = "[S1] O1->[S2] O2", "[S1] O1->[S2] O3"
    assert deps[name_b].happens_before == set()
    assert deps[name_c].happens_before == set()
    # both still finish before the parent, so both are child_dependents of A.
    assert deps["[S1] O1"].child_dependents == {name_b, name_c}


def test_mild_overlap_within_tolerance_yields_happens_before():
    g = mild_overlap_graph()
    deps = DependencyGraph(g).deps

    name_b, name_c = "[S1] O1->[S2] O2", "[S1] O1->[S2] O3"
    assert deps[name_c].happens_before == {name_b}


def test_overlap_exceeding_shrunk_tolerance_yields_no_happens_before(restore_config):  # noqa: ARG001
    set_config(ConfigBuilder().overlap_allowance(0.001).build())

    g = mild_overlap_graph()
    deps = DependencyGraph(g).deps

    name_c = "[S1] O1->[S2] O3"
    assert deps[name_c].happens_before == set()


def test_async_child_recorded_and_excluded_from_child_dependents():
    g = async_child_graph()
    deps = DependencyGraph(g).deps

    name_a, name_b = "[S1] O1", "[S1] O1->[S2] O2"
    assert deps[name_a].async_children == {name_b}
    assert name_b not in deps[name_a].child_dependents
    assert deps[name_a].parent_start_delay == [50]
    assert deps[name_a].parent_end_delay == [-50]


def test_async_child_can_still_receive_happens_before_from_earlier_sibling():
    g = async_child_with_earlier_sibling_graph()
    deps = DependencyGraph(g).deps

    name_a = "[S1] O1"
    name_b = "[S1] O1->[S2] O2"
    name_c = "[S1] O1->[S2] O3"

    assert name_b in deps[name_a].async_children
    assert name_c in deps[name_a].child_dependents
    assert name_b not in deps[name_a].child_dependents
    # B is async relative to its parent, but still happens after C w.r.t. sibling timing.
    assert deps[name_b].happens_before == {name_c}


def test_child_end_time_equal_parent_end_time_is_neither_async_nor_dependent():
    g = child_ends_exactly_with_parent_graph()
    deps = DependencyGraph(g).deps

    name_a, name_b = "[S1] O1", "[S1] O1->[S2] O2"
    assert name_b not in deps[name_a].async_children
    assert name_b not in deps[name_a].child_dependents


def test_fanout_same_callpath_shares_one_node_accumulates_delays_and_self_edge():
    g = fanout_same_callpath_graph()
    deps = DependencyGraph(g).deps

    name_a = "[S1] O1"
    name_b = "[S1] O1->[S2] Ob"
    name_c = "[S1] O1->[S2] Ob->[S3] Oc"

    # exactly one shared entry per call-path, despite two occurrences of each.
    assert len(deps) == 3
    assert deps[name_a].child_dependents == {name_b}
    assert sorted(deps[name_b].parent_start_delay) == [10, 20]
    assert sorted(deps[name_b].parent_end_delay) == [10, 20]

    # B1 (ends 400) finishes well before B2 starts (500), and both share name_b --
    # this intentionally registers a self-referential happens_before edge: it means
    # "an earlier instance of this call-path finished before this instance started"
    # (e.g. sequential retries), not literal self-dependency -- see the NOTE on
    # DependencyGraphNode.happens_before. A future retimer relies on this to
    # correctly cascade a delay from one same-call-path invocation to the next; it
    # must always pair this name-based lookup with a genuine per-span identity check
    # (never a bare name check).
    assert deps[name_b].happens_before == {name_b}
    # C1/C2 (name_c) are never compared as siblings -- they're children of B1/B2
    # respectively (different parents), not siblings of each other -- so no edge.
    assert deps[name_c].happens_before == set()


def test_same_op_different_parents_yields_distinct_dependency_nodes():
    g = same_op_different_parents_graph()
    deps = DependencyGraph(g).deps

    name_leaf1 = "[S1] O1->[S2] Ob->[S3] OShared"
    name_leaf2 = "[S1] O1->[S4] Od->[S3] OShared"

    assert name_leaf1 != name_leaf2
    assert name_leaf1 in deps
    assert name_leaf2 in deps
    assert len(deps) == 5


def test_multilevel_nesting_matches_hand_computed_delays():
    g = multilevel_nesting_graph()
    deps = DependencyGraph(g).deps

    name_a = "[S1] O1"
    name_b = "[S1] O1->[S2] O2"
    name_c = "[S1] O1->[S2] O2->[S3] O3"
    name_d = "[S1] O1->[S2] O2->[S4] O4"

    assert deps[name_a].child_dependents == {name_b}
    assert deps[name_a].parent_start_delay == [10]
    assert deps[name_a].parent_end_delay == [30]

    assert deps[name_b].child_dependents == {name_c, name_d}
    assert deps[name_b].parent_start_delay == [15]
    assert deps[name_b].parent_end_delay == [20]

    # leaves have no children, so no delay/dependent bookkeeping of their own.
    assert deps[name_c].parent_start_delay == []
    assert deps[name_d].parent_start_delay == []


def test_multilevel_nesting_overlap_blocks_happens_before_between_children():
    g = multilevel_nesting_graph()
    deps = DependencyGraph(g).deps

    name_c = "[S1] O1->[S2] O2->[S3] O3"
    name_d = "[S1] O1->[S2] O2->[S4] O4"

    assert deps[name_c].happens_before == set()
    assert deps[name_d].happens_before == set()


def test_mixed_children_all_classified_correctly():
    g = mixed_children_graph()
    deps = DependencyGraph(g).deps

    name_a = "[S1] O1"
    name_b = "[S1] O1->[S2] O2"
    name_c = "[S1] O1->[S2] O3"
    name_d = "[S1] O1->[S2] O4"

    assert deps[name_a].async_children == {name_d}
    assert deps[name_a].child_dependents == {name_c}
    assert name_b not in deps[name_a].child_dependents
    assert name_b not in deps[name_a].async_children


def test_root_node_is_never_a_dependency_target():
    g = multilevel_nesting_graph()
    deps = DependencyGraph(g).deps

    root_name = "[S1] O1"
    assert deps[root_name].happens_before == set()
    for name, node in deps.items():
        if name == root_name:
            continue
        assert root_name not in node.happens_before
        assert root_name not in node.child_dependents
        assert root_name not in node.async_children


def test_orphaned_node_does_not_crash_and_gets_own_entry():
    g = orphan_node_graph()
    orphan = next(n for n in g.nodeHT.values() if n.sid == "X")
    assert orphan.parent is None
    assert orphan is not g.rootNode

    deps = DependencyGraph(g).deps

    assert "[S1] O1" in deps
    assert "[S2] OX" in deps
    assert deps["[S2] OX"].parent_start_delay == []
    assert deps["[S2] OX"].child_dependents == set()


def test_get_dependencies_classmethod_matches_instance_deps():
    g = single_child_graph()
    via_instance = DependencyGraph(g).deps
    via_classmethod = DependencyGraph.get_dependencies(g)

    assert via_instance.keys() == via_classmethod.keys()
    for name in via_instance:
        assert via_instance[name].happens_before == via_classmethod[name].happens_before
        assert via_instance[name].child_dependents == via_classmethod[name].child_dependents
        assert via_instance[name].async_children == via_classmethod[name].async_children


def test_deps_keys_use_full_getcallpath_strings():
    g = single_child_graph()
    deps = DependencyGraph(g).deps

    b_node = next(n for n in g.nodeHT.values() if n.sid == "B")
    assert g.getCallPath(b_node) in deps
    assert g.getCallPath(b_node) == "[S1] O1->[S2] O2"


def test_dependency_graph_node_repr_contains_name():
    node = DependencyGraphNode("[S1] O1")
    assert "[S1] O1" in repr(node)


# --- Multi-trace aggregation (get_aggregate_dependencies / DependencyGraph(graphs=...)). ---


def test_aggregate_happens_before_survives_when_two_traces_confirm_same_edge():
    g1 = three_series_siblings_graph()
    g2 = three_series_siblings_graph()
    agg = DependencyGraph.get_aggregate_dependencies([g1, g2])

    name_b = "[S1] O1->[S2] O2"
    name_c = "[S1] O1->[S2] O3"
    name_d = "[S1] O1->[S2] O4"

    assert agg[name_c].happens_before == {name_b}
    assert agg[name_d].happens_before == {name_b, name_c}


def test_aggregate_happens_before_dropped_by_genuine_counter_example():
    # g1 confirms "B happens-before C"; g2 has both B and C present but runs
    # them concurrently, contradicting that ordering.
    g1 = three_series_siblings_graph()
    g2 = parallel_siblings_graph()
    agg = DependencyGraph.get_aggregate_dependencies([g1, g2])

    name_c = "[S1] O1->[S2] O3"
    assert agg[name_c].happens_before == set()


def test_aggregate_happens_before_survives_when_later_trace_omits_candidate_entirely():
    # g2 never observed "B" at all (different call pattern), so it cannot
    # contradict D's confirmed happens_before relationship with B from g1.
    g1 = three_series_siblings_graph()
    g2 = two_series_siblings_no_b_graph()
    agg = DependencyGraph.get_aggregate_dependencies([g1, g2])

    name_b = "[S1] O1->[S2] O2"
    name_c = "[S1] O1->[S2] O3"
    name_d = "[S1] O1->[S2] O4"

    assert agg[name_d].happens_before == {name_b, name_c}


def test_aggregate_happens_before_rejection_is_permanent_across_three_traces():
    # trace 1 confirms "B happens-before D"; trace 2 contradicts it (D runs
    # first); trace 3 re-observes the exact same confirming pattern as trace 1
    # -- the edge must remain dropped, never resurrected.
    g1 = two_series_b_then_d_graph()
    g2 = two_series_d_then_b_graph()
    g3 = two_series_b_then_d_graph()
    agg = DependencyGraph.get_aggregate_dependencies([g1, g2, g3])

    name_d = "[S1] O1->[S2] O4"
    assert agg[name_d].happens_before == set()


def test_aggregate_first_trace_non_confirmation_does_not_block_later_confirmation():
    # g1 sees B and C concurrently (no order between them); since this is the
    # *first* sighting of C, there is no prior confirmed evidence yet to
    # contradict, so a later trace can still add a brand-new confirmed edge.
    g1 = parallel_siblings_graph()
    g2 = three_series_siblings_graph()
    agg = DependencyGraph.get_aggregate_dependencies([g1, g2])

    name_b = "[S1] O1->[S2] O2"
    name_c = "[S1] O1->[S2] O3"
    assert agg[name_c].happens_before == {name_b}


def test_aggregate_self_referential_happens_before_survives_confirmation():
    # Fan-out self-edges (see test_fanout_same_callpath_shares_one_node_...)
    # must follow the exact same aggregation rules as any other candidate name.
    g1 = fanout_same_callpath_graph()
    g2 = fanout_same_callpath_graph()
    agg = DependencyGraph.get_aggregate_dependencies([g1, g2])

    name_b = "[S1] O1->[S2] Ob"
    assert agg[name_b].happens_before == {name_b}


def test_aggregate_self_referential_happens_before_dropped_by_counter_example():
    # g2 has only a single occurrence of the fanout call-path, so it observes
    # "Ob" without confirming Ob happens-before itself -- a genuine
    # counter-example against the self-referential edge confirmed by g1.
    g1 = fanout_same_callpath_graph()
    g2 = single_ob_child_graph()
    agg = DependencyGraph.get_aggregate_dependencies([g1, g2])

    name_b = "[S1] O1->[S2] Ob"
    assert agg[name_b].happens_before == set()


def test_aggregate_async_children_union_evicts_from_child_dependents():
    # B is a plain (sync) child_dependent in g1, but async in g2 -- the
    # aggregate must mark it async and evict it from child_dependents, even
    # though an earlier trace had it there as a plain dependent.
    g1 = single_child_graph()
    g2 = async_child_graph()
    agg = DependencyGraph.get_aggregate_dependencies([g1, g2])

    name_a, name_b = "[S1] O1", "[S1] O1->[S2] O2"
    assert agg[name_a].async_children == {name_b}
    assert name_b not in agg[name_a].child_dependents


def test_aggregate_child_dependents_union_across_distinct_traces():
    g1 = single_child_graph()
    g2 = single_child_graph_alt_op()
    agg = DependencyGraph.get_aggregate_dependencies([g1, g2])

    name_a = "[S1] O1"
    name_b = "[S1] O1->[S2] O2"
    name_z = "[S1] O1->[S2] O9"

    assert name_b in agg
    assert name_z in agg
    assert agg[name_a].child_dependents == {name_b, name_z}


def test_aggregate_delay_lists_concatenate_across_traces():
    g1 = single_child_with_delay_graph(10, 80)
    g2 = single_child_with_delay_graph(20, 70)
    g3 = single_child_with_delay_graph(5, 90)
    agg = DependencyGraph.get_aggregate_dependencies([g1, g2, g3])

    name_a = "[S1] O1"
    assert sorted(agg[name_a].parent_start_delay) == [5, 10, 20]
    assert sorted(agg[name_a].parent_end_delay) == [5, 10, 10]


def test_aggregate_new_node_first_seen_in_later_trace_is_not_aliased():
    # "[S2] O2" only exists starting from g2; when first observed, its
    # aggregate DependencyGraphNode must be a fresh object, not aliased to the
    # per-trace one -- mutating the aggregate must not leak into a separately
    # computed per-trace result.
    g1 = leaf_only_graph()
    g2 = single_child_graph()
    agg = DependencyGraph.get_aggregate_dependencies([g1, g2])

    name_b = "[S1] O1->[S2] O2"
    per_trace_deps = DependencyGraph(g2).deps

    assert name_b in agg
    assert agg[name_b] is not per_trace_deps[name_b]

    agg[name_b].happens_before.add("BOGUS")
    agg[name_b].child_dependents.add("BOGUS")
    agg[name_b].parent_start_delay.append(999)

    assert "BOGUS" not in per_trace_deps[name_b].happens_before
    assert "BOGUS" not in per_trace_deps[name_b].child_dependents
    assert 999 not in per_trace_deps[name_b].parent_start_delay


def test_aggregate_single_graph_list_matches_single_trace_mode():
    g = three_series_siblings_graph()
    via_single = DependencyGraph(g).deps
    via_aggregate = DependencyGraph(graphs=[g]).deps

    assert via_single.keys() == via_aggregate.keys()
    for name in via_single:
        assert via_single[name].happens_before == via_aggregate[name].happens_before
        assert via_single[name].child_dependents == via_aggregate[name].child_dependents
        assert via_single[name].async_children == via_aggregate[name].async_children
        assert via_single[name].parent_start_delay == via_aggregate[name].parent_start_delay
        assert via_single[name].parent_end_delay == via_aggregate[name].parent_end_delay


def test_aggregate_constructor_requires_exactly_one_of_graph_or_graphs():
    g = leaf_only_graph()

    with pytest.raises(ValueError):
        DependencyGraph()

    with pytest.raises(ValueError):
        DependencyGraph(graph=g, graphs=[g])

    # existing positional single-graph call style must keep working unchanged.
    assert DependencyGraph(g).deps.keys() == DependencyGraph.get_dependencies(g).keys()
