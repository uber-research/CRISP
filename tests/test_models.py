"""Tests for crisp.models — the heavy graph types and GetRemainingTags.

The lighter metric containers live in crisp.shared.models and are
covered by tests/shared/test_models.py. This file focuses on PBlock,
GraphNode, and GetRemainingTags. Internal critical_path has no
dedicated test_models.py for these types, so per the LAYERS.md
"zero coverage" rule the tests below are fresh.
"""

from unittest import TestCase

from crisp.models import GetRemainingTags, GraphNode, PBlock
from crisp.shared.constants import TAG_NAME, TAG_SEARCH_DEPTH, TAG_VALUE
from crisp.shared.models import SpanKind


def _makeNode(sid="s1", startTime=0, duration=10, opName="op"):
    """Build a real GraphNode for use as a PBlock member.

    Tests use real GraphNode instances rather than lightweight stand-ins
    so the GraphNode contract (`startTime`, `endTime`, ordering)
    actually exercises the production class.
    """
    return GraphNode(
        sid=sid,
        startTime=startTime,
        duration=duration,
        parentSpanId=None,
        opName=opName,
        processID="pid",
        spanKind=SpanKind.SERVER,
        peerService="peer",
        returnError=False,
    )


class TestPBlockConstructor(TestCase):
    def test_initial_state_matches_node(self):
        node = _makeNode(startTime=5, duration=20)
        parent = object()
        pb = PBlock(parent, node)
        self.assertEqual(pb.startTime, 5)
        self.assertEqual(pb.endTime, 25)
        self.assertIs(pb.parent, parent)
        self.assertIs(pb.firstNode, node)
        self.assertEqual(pb.spanSet, {node})
        self.assertEqual(pb.overlap, 0.0)

    def test_explicit_overlap_is_stored(self):
        node = _makeNode()
        pb = PBlock(object(), node, overlap=3.5)
        self.assertEqual(pb.overlap, 3.5)
        self.assertEqual(pb.GetOverlap(), 3.5)


class TestPBlockAdd(TestCase):
    def test_add_earlier_node_updates_first_node_and_start(self):
        # The Pblock is anchored on the latest-endTime node; adding an
        # earlier node should pull startTime / firstNode back.
        last = _makeNode(sid="late", startTime=10, duration=20)  # endTime 30
        pb = PBlock(object(), last)
        early = _makeNode(sid="early", startTime=2, duration=8)  # endTime 10
        pb.Add(early)
        self.assertIs(pb.firstNode, early)
        self.assertEqual(pb.startTime, 2)
        # endTime is anchored on the original last node — unchanged.
        self.assertEqual(pb.endTime, 30)
        self.assertEqual(pb.spanSet, {last, early})

    def test_add_later_starting_node_does_not_shift_first_node(self):
        # If the new node starts after firstNode.startTime, the anchor
        # is preserved.
        last = _makeNode(sid="late", startTime=0, duration=30)
        pb = PBlock(object(), last)
        mid = _makeNode(sid="mid", startTime=5, duration=20)  # endTime 25
        pb.Add(mid)
        self.assertIs(pb.firstNode, last)
        self.assertEqual(pb.startTime, 0)
        self.assertIn(mid, pb.spanSet)

    def test_add_violates_invariant_raises(self):
        # Add asserts node.endTime <= self.endTime; an endTime past the
        # anchor should fire the AssertionError.
        anchor = _makeNode(startTime=0, duration=10)  # endTime 10
        pb = PBlock(object(), anchor)
        intruder = _makeNode(sid="x", startTime=0, duration=20)  # endTime 20
        with self.assertRaises(AssertionError):
            pb.Add(intruder)


class TestPBlockHappensAfter(TestCase):
    def test_delegates_to_graph_happens_before(self):
        # HappensAfter forwards (parent, parent.children, childBefore,
        # firstNode) to graph.happensBefore. A stub graph captures the
        # call args and returns a recognizable sentinel so we can verify
        # the delegation.
        anchor = _makeNode()
        parent = _makeNode(sid="parent")
        pb = PBlock(parent, anchor)
        childBefore = _makeNode(sid="before")

        calls = []

        class GraphStub:
            def happensBefore(self, parent, children, before, first):
                calls.append((parent, children, before, first))
                return "sentinel"

        result = pb.HappensAfter(GraphStub(), childBefore)
        self.assertEqual(result, "sentinel")
        self.assertEqual(len(calls), 1)
        p, ch, b, f = calls[0]
        self.assertIs(p, parent)
        self.assertIs(ch, parent.children)
        self.assertIs(b, childBefore)
        self.assertIs(f, anchor)


class TestPBlockRepr(TestCase):
    def test_repr_contains_key_fields(self):
        node = _makeNode(startTime=1, duration=4)
        pb = PBlock(object(), node, overlap=0.25)
        rep = repr(pb)
        self.assertIn("startTime=1", rep)
        self.assertIn("endTime=5", rep)
        self.assertIn("overlap=0.25", rep)
        self.assertIn("spanSet=", rep)


class TestGraphNodeConstructor(TestCase):
    def test_constructor_stores_all_fields(self):
        n = GraphNode(
            sid="s1",
            startTime=10,
            duration=5,
            parentSpanId="parent",
            opName="op",
            processID="proc",
            spanKind=SpanKind.CLIENT,
            peerService="peer",
            returnError=True,
        )
        self.assertEqual(n.sid, "s1")
        self.assertEqual(n.startTime, 10)
        self.assertEqual(n.duration, 5)
        self.assertEqual(n.endTime, 15)
        # The original* mirrors record the construction-time values for
        # later restoration after edits.
        self.assertEqual(n.originalStartTime, 10)
        self.assertEqual(n.originalDuration, 5)
        self.assertEqual(n.parentSpanId, "parent")
        self.assertEqual(n.opName, "op")
        self.assertEqual(n.pid, "proc")  # renamed from processID
        self.assertEqual(n.peerService, "peer")
        self.assertIs(n.spanKind, SpanKind.CLIENT)
        self.assertTrue(n.returnError)

    def test_constructor_initializes_graph_bookkeeping(self):
        n = _makeNode()
        self.assertIsNone(n.parent)
        self.assertEqual(n.children, {})
        self.assertEqual(n.timeSavedOnCPPessimistic, 0)
        self.assertEqual(n.timeSavedOnCPOptimistic, 0)
        self.assertEqual(n.timeSavedOnCPAllSeries, 0)
        self.assertEqual(n.timeChangeOnCPAllSeries, 0)


class TestGraphNodeSetParent(TestCase):
    def test_setParent_updates_parent_and_parentSpanId(self):
        child = _makeNode(sid="child")
        parent = _makeNode(sid="parent")
        child.setParent(parent)
        self.assertIs(child.parent, parent)
        self.assertEqual(child.parentSpanId, "parent")


class TestGraphNodeAddChild(TestCase):
    def test_addChild_inserts_into_children_dict(self):
        parent = _makeNode(sid="parent")
        c1 = _makeNode(sid="c1")
        c2 = _makeNode(sid="c2")
        parent.addChild(c1)
        parent.addChild(c2)
        self.assertEqual(set(parent.children.keys()), {c1, c2})
        self.assertTrue(parent.children[c1])
        self.assertTrue(parent.children[c2])

    def test_addChild_dedupes_same_child(self):
        # children is a dict keyed by child; adding twice should not
        # produce two entries.
        parent = _makeNode(sid="parent")
        c = _makeNode(sid="c")
        parent.addChild(c)
        parent.addChild(c)
        self.assertEqual(len(parent.children), 1)


class TestGraphNodeLt(TestCase):
    def test_ordering_by_endTime(self):
        a = _makeNode(sid="a", startTime=0, duration=5)   # endTime 5
        b = _makeNode(sid="b", startTime=0, duration=10)  # endTime 10
        self.assertTrue(a < b)
        self.assertFalse(b < a)

    def test_tiebreak_by_startTime_when_endTime_equal(self):
        # Equal endTimes -> earlier startTime sorts first.
        a = _makeNode(sid="a", startTime=2, duration=8)  # endTime 10
        b = _makeNode(sid="b", startTime=5, duration=5)  # endTime 10
        self.assertTrue(a < b)
        self.assertFalse(b < a)

    def test_tiebreak_by_sid_when_times_equal(self):
        a = _makeNode(sid="a", startTime=0, duration=10)
        b = _makeNode(sid="b", startTime=0, duration=10)
        self.assertTrue(a < b)
        self.assertFalse(b < a)

    def test_tiebreak_by_opName_when_sid_equal(self):
        a = _makeNode(sid="same", startTime=0, duration=10, opName="A")
        b = _makeNode(sid="same", startTime=0, duration=10, opName="B")
        self.assertTrue(a < b)
        self.assertFalse(b < a)

    def test_equal_nodes_are_not_less_than_each_other(self):
        # All ordering keys equal -> __lt__ returns False both ways.
        a = _makeNode(sid="x", startTime=0, duration=10, opName="op")
        b = _makeNode(sid="x", startTime=0, duration=10, opName="op")
        self.assertFalse(a < b)
        self.assertFalse(b < a)


class TestGraphNodeRepr(TestCase):
    def test_repr_contains_key_fields(self):
        n = _makeNode(sid="s1", startTime=10, duration=5, opName="op")
        rep = repr(n)
        self.assertIn("SpanID=s1", rep)
        self.assertIn("startTime=10", rep)
        self.assertIn("duration=5", rep)
        self.assertIn("opName=op", rep)


# GetRemainingTags inspects each tag dict via the promoted
# TAG_SEARCH_DEPTH key. We construct tags directly from the shared
# constants (rather than hard-coding "search_depth") so a future rename
# would cascade and fail loudly here.
def _tag(name, value, depth):
    return {TAG_NAME: name, TAG_VALUE: value, TAG_SEARCH_DEPTH: depth}


class TestGetRemainingTags(TestCase):
    def test_empty_all_tags_returns_empty(self):
        self.assertEqual(GetRemainingTags(foundTags=[], allTags=[], curSearchDepth=0), [])

    def test_all_tags_found_returns_empty(self):
        t1 = _tag("k1", "v1", 1)
        t2 = _tag("k2", "v2", 1)
        # foundTags contains the same dicts -> nothing remaining.
        self.assertEqual(
            GetRemainingTags(foundTags=[t1, t2], allTags=[t1, t2], curSearchDepth=1),
            [],
        )

    def test_partial_overlap_returns_unfound(self):
        found = _tag("k1", "v1", 1)
        missing = _tag("k2", "v2", 1)
        self.assertEqual(
            GetRemainingTags(foundTags=[found], allTags=[found, missing], curSearchDepth=1),
            [missing],
        )

    def test_depth_filter_skips_shallower_tags(self):
        # A tag whose search_depth is below curSearchDepth is filtered
        # out via the `curSearchDepth > m[TAG_SEARCH_DEPTH]` branch even
        # though it hasn't been found yet.
        shallow = _tag("shallow", "v", 1)
        deep = _tag("deep", "v", 5)
        result = GetRemainingTags(foundTags=[], allTags=[shallow, deep], curSearchDepth=3)
        self.assertEqual(result, [deep])

    def test_depth_equal_keeps_tag(self):
        # Branch is strict `>`, so a tag at exactly curSearchDepth is
        # retained.
        tag = _tag("k", "v", 2)
        self.assertEqual(
            GetRemainingTags(foundTags=[], allTags=[tag], curSearchDepth=2),
            [tag],
        )

    def test_preserves_order_of_allTags(self):
        a = _tag("a", "v", 1)
        b = _tag("b", "v", 1)
        c = _tag("c", "v", 1)
        self.assertEqual(
            GetRemainingTags(foundTags=[], allTags=[a, b, c], curSearchDepth=1),
            [a, b, c],
        )
