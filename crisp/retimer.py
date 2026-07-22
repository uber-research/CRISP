"""Single-node retime simulation for a critical path call tree.

This module ports the "retimer" concept from Google's `calligator` project
(Apache-2.0, see https://github.com/google/calligator/blob/main/retimer.py,
class ``Retimer``, method ``retime_node``) to this codebase's
:class:`~crisp.graph.Graph` and :class:`~crisp.graph.GraphNode`.

Given a node's new start/end times, :meth:`Retimer.retime_node` mutates that
node in place, shifts any true sibling that the :class:`DependencyGraph`
says genuinely happens after it (by real timestamp, not just call-path
identity), and -- if the node became its parent's new earliest-starting or
latest-ending child -- recurses to retime the parent too. This is strictly
more accurate than :meth:`Graph.computeProjectedCPMetrics`'s ancestor-only
cascade, which has no notion of siblings at all. The two are intentionally
kept separate -- ``retime_node`` is a library-only, opt-in entry point that
is never called from ``computeProjectedCPMetrics``/``--deltaMicroSec``, and
the two may legitimately disagree on graphs with parallel siblings.

Unlike every other mutating operation in this codebase (e.g.
``Graph.computeProjectedCPMetrics``), ``retime_node`` has no built-in
restore -- it permanently mutates ``g`` to simulate a "what-if" retimed
state. Callers that need the original graph back afterward should take a
:meth:`Retimer.snapshot` before retiming and pass it to
:meth:`Retimer.restore` when done (cheaper than deep-copying the whole
``Graph``, since retiming only ever touches node timing fields), or call
``retime_node`` a second time with the original ``(startTime, endTime)``
(see the round-trip tests in ``tests/test_retimer.py``).

Node identity is :meth:`Graph.getCallPath`, matching ``dependency_graph.py``
and ``slack_drag.py``. As documented on ``DependencyGraphNode.happens_before``,
that set can legitimately contain a node's own call-path name (a parent
fanning out to the same call-path more than once in series). Every
name-based ``happens_before``/``child_dependents`` lookup below is paired
with a genuine per-span check (excluding the node's own span id, or
comparing real timestamps) -- never a bare name-membership test -- so that
self-referential entries cascade delays correctly without ever mutating a
node based on its own happens-before edge to itself.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from crisp.dependency_graph import DependencyGraph
    from crisp.graph import Graph, GraphNode


def _original_end_time(node: GraphNode) -> float:
    """A node's end time as originally constructed, never mutated by retiming."""
    return node.originalStartTime + node.originalDuration


class Retimer:
    """Retimes nodes in one or more Graphs using a single DependencyGraph.

    A ``Retimer`` holds one :class:`DependencyGraph` and can be reused to
    retime any number of nodes, across any number of ``Graph`` instances
    (mirrors calligator's own ``Retimer`` docstring).
    """

    def __init__(self, dependency_graph: DependencyGraph):
        self.dependency_graph = dependency_graph

    @staticmethod
    def snapshot(g: Graph) -> dict[str, tuple[float, float, float]]:
        """Capture every node's (startTime, endTime, duration) for a later :meth:`restore`."""
        return {sid: (n.startTime, n.endTime, n.duration) for sid, n in g.nodeHT.items()}

    @staticmethod
    def restore(g: Graph, snapshot: dict[str, tuple[float, float, float]]) -> None:
        """Write back a :meth:`snapshot` taken before any ``retime_node`` calls on ``g``."""
        for sid, (start, end, duration) in snapshot.items():
            node = g.nodeHT[sid]
            node.startTime, node.endTime, node.duration = start, end, duration
        g.retimed = False

    def retime_node(
        self,
        g: Graph,
        node_span_id: str,
        new_start: float,
        new_end: float,
        freeze_starts: bool = True,
    ) -> None:
        """Retime one node, cascading the effect to siblings and ancestors.

        Assumptions (carried over from calligator): the new start/end do not
        conflict with the node's own children; the graph only has
        happens-before relationships between spans; the new times don't
        violate any such relationship; and a parent span takes constant time
        to dispatch/aggregate its children, so it only needs retiming when
        the changed node becomes its new earliest-starting or
        latest-(non-async-)ending child.

        Args:
            g: the Graph to mutate.
            node_span_id: span id of the node to retime.
            new_start: the node's new start time.
            new_end: the node's new end time.
            freeze_starts: if True, this node's own parent's startTime is
                left untouched even if propagation would otherwise move it
                (useful when retiming several nodes in parallel against the
                same parent). Only applies to the immediate parent -- any
                further propagation to a grandparent (and beyond) always
                recurses with the default (True), matching calligator.

        Raises:
            ValueError: if ``new_start > new_end``.
        """
        deps = self.dependency_graph.deps
        node = g.nodeHT[node_span_id]

        if new_start == node.startTime and new_end == node.endTime:
            return  # nothing to change

        if new_start > new_end:
            raise ValueError(
                f"New start time {new_start} is greater than new end time {new_end} for node {node.pid}.{node.opName}.",
            )

        g.retimed = True

        delay = new_end - node.endTime  # positive = ends later, negative = ends sooner
        parent = node.parent

        # Snapshot of the parent's own dispatch/aggregation overhead, taken before
        # this node's mutation below, to preserve if the parent needs retiming.
        parent_start_delay = 0
        parent_end_delay = 0
        if parent is not None:
            by_start = sorted(parent.children, key=lambda c: c.startTime)
            parent_start_delay = by_start[0].startTime - parent.startTime if by_start else 0

            by_end = sorted(parent.children, key=lambda c: c.endTime)
            non_async_children = [c for c in by_end if c.endTime <= parent.endTime]
            parent_end_delay = parent.endTime - non_async_children[-1].endTime if non_async_children else 0

        original_node_end = node.endTime  # compared against siblings below
        node.startTime = new_start
        node.endTime = new_end
        node.duration = new_end - new_start

        if parent is None:
            return  # no parent to propagate to

        # Shift true siblings with a genuine happens-before relationship to this
        # node. `c.sid != node_span_id` (below) excludes the node itself BEFORE any
        # name-based happens_before lookup -- required so a same-call-path fan-out
        # (see module docstring) never treats the node as its own predecessor.
        node_name = g.getCallPath(node)
        siblings = [c for c in parent.children if c.sid != node_span_id]
        for sibling in siblings:
            sibling_dep = deps.get(g.getCallPath(sibling))
            if sibling_dep is not None and node_name in sibling_dep.happens_before and sibling.startTime > original_node_end:
                sibling.startTime += delay
                sibling.endTime += delay
            # else: no happens-before relationship -- unaffected by this delay/rush.

        # Find the earliest-starting and latest-(non-async-)ending true sibling,
        # including this node itself post-mutation.
        siblings.append(node)
        first_sibling_after_parent_start = node
        last_sibling_before_parent_end = node

        parent_dep = deps.get(g.getCallPath(parent))
        child_dependents = parent_dep.child_dependents if parent_dep is not None else set()
        dependent_siblings = [c for c in siblings if g.getCallPath(c) in child_dependents and _original_end_time(c) <= _original_end_time(parent)]
        if dependent_siblings:
            last_sibling_before_parent_end = max(dependent_siblings, key=lambda c: c.endTime)

        for sibling in siblings:
            if parent.startTime <= sibling.startTime < first_sibling_after_parent_start.startTime:
                first_sibling_after_parent_start = sibling
            if (
                sibling.endTime <= parent.endTime
                and sibling.endTime > last_sibling_before_parent_end.endTime
                and _original_end_time(sibling) <= _original_end_time(parent)
            ):
                last_sibling_before_parent_end = sibling

        if freeze_starts:
            new_parent_start = parent.startTime
        elif parent_start_delay > 0:
            new_parent_start = first_sibling_after_parent_start.startTime - parent_start_delay
        else:
            new_parent_start = min(parent.startTime, first_sibling_after_parent_start.startTime)

        if _original_end_time(last_sibling_before_parent_end) > _original_end_time(parent):
            new_parent_end = parent.endTime
        else:
            new_parent_end = parent_end_delay + last_sibling_before_parent_end.endTime

        if new_parent_start > new_parent_end:
            return

        # Matches calligator: propagation beyond the immediate parent always uses
        # the default freeze_starts (True), regardless of what the caller passed.
        self.retime_node(g, parent.sid, new_parent_start, new_parent_end)
