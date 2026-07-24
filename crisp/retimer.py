"""Single-node and method-level retime simulation for a critical path call tree.

This module ports the "retimer" concept from Google's `calligator` project
(Apache-2.0, see https://github.com/google/calligator/blob/main/retimer.py,
class ``Retimer``, methods ``retime_node``/``retime_method``) to this
codebase's :class:`~crisp.graph.Graph` and :class:`~crisp.graph.GraphNode`.

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

:meth:`Retimer.retime_method` applies a percent/fixed delta to every
instance of one method (repeated calls to ``retime_node``) and reports the
net effect on the target root's duration. Deliberate deviations from
calligator's literal source, required for a fair comparison against
:meth:`Graph.computeProjectedCPMetrics` in
``tests/test_retimer_cross_validation.py``:

* Method identity is ``(targetService, targetOperation)`` on ``SERVER``
  spans -- the same predicate ``computeProjectedCPMetrics``/
  ``_findMatchingNodes`` already use -- not calligator's ``pid + '.' +
  opName`` (``pid`` is only a trace-local Jaeger process alias, not a
  stable cross-trace method identity).
* Calligator's fixed-difference clamp branch (``if node.endTime +
  fixed_difference < node.startTime: adjustment_amount = node.duration``)
  inverts the intended shrink floor -- on a large shrink it *doubles* the
  node instead of stopping the shrink at some floor -- and is not ported.
  ``retime_method`` instead floors the new end time the same way
  ``computeProjectedCPMetrics`` does: down to 0 duration for a leaf, or
  down to the furthest-reaching child's end time otherwise, since
  ``retime_node`` requires a new end time that doesn't conflict with the
  node's own children. Same precedent as ``slack_drag.py``'s "IMPORTANT --
  correctness note" for departing from a literal calligator port.
* Calligator's method scans its whole ``graph.nodeHT``. This codebase's
  ``Graph`` can hold more than one independent root subtree at once
  (``mergeAllRoots`` / ``--span-input``, see ``process_trace.py``), so
  ``retime_method`` instead takes an optional ``rootNode`` (default
  ``g.rootNode``, mirroring ``computeProjectedCPMetrics``) and scopes both
  node-matching and the returned delta to that root's own subtree.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from crisp.shared.models import SpanKind

if TYPE_CHECKING:
    from crisp.dependency_graph import DependencyGraph
    from crisp.graph import Graph, GraphNode


def _original_end_time(node: GraphNode) -> float:
    """A node's end time as originally constructed, never mutated by retiming."""
    return node.originalStartTime + node.originalDuration


def _matches_method(g: Graph, node: GraphNode, target_service: str, target_operation: str) -> bool:
    """Whether node is a SERVER span for (target_service, target_operation), matching computeProjectedCPMetrics."""
    return node.spanKind == SpanKind.SERVER and g.processName.get(node.pid, "") == target_service and node.opName == target_operation


def _find_matching_nodes(g: Graph, root: GraphNode, target_service: str, target_operation: str) -> list[GraphNode]:
    """Matching nodes reachable from root only -- a Graph can hold more than one root
    subtree (mergeAllRoots / --span-input), so this must not scan all of g.nodeHT."""
    matches = []
    stack = [root]
    while stack:
        node = stack.pop()
        if _matches_method(g, node, target_service, target_operation):
            matches.append(node)
        stack.extend(node.children)
    return matches


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

    def retime_method(
        self,
        g: Graph,
        target_service: str,
        target_operation: str,
        percent_difference: float | None = None,
        fixed_difference: float | None = None,
        rootNode: GraphNode | None = None,
    ) -> float:
        """Retimes every instance of one method, via repeated :meth:`retime_node` calls.

        A "method" is every ``SERVER`` span matching ``target_service`` and
        ``target_operation`` -- the same predicate
        ``Graph.computeProjectedCPMetrics`` uses (see module docstring for
        why this departs from calligator's ``pid + '.' + opName``).

        Args:
            g: the Graph to mutate.
            target_service: service name of the method to retime.
            target_operation: operation name of the method to retime.
            percent_difference: grow (positive) or shrink (negative) each
                matching node's duration by this percent. Mutually
                exclusive with fixed_difference.
            fixed_difference: grow (positive) or shrink (negative) each
                matching node's duration by this fixed amount. Mutually
                exclusive with percent_difference.
            rootNode: optional root to scope matching and the returned delta
                to (default ``g.rootNode``) -- see module docstring for why.
                Candidate roots from ``--span-input`` can be nested inside
                each other, and unlike ``computeProjectedCPMetrics``, this
                method doesn't auto-restore its mutations. Callers looping
                over several roots must :meth:`snapshot`/:meth:`restore`
                between calls, or an overlapping subtree gets double-applied.

        Returns:
            Original root duration minus the retimed root duration -- i.e.
            positive means the root got faster overall.

        Raises:
            ValueError: if neither or both of percent_difference/fixed_difference are set.
        """
        if percent_difference is not None and fixed_difference is not None:
            raise ValueError("Only one of percent_difference or fixed_difference should be set.")
        if percent_difference is None and fixed_difference is None:
            raise ValueError("One of percent_difference or fixed_difference must be set.")

        root = rootNode if rootNode is not None else g.rootNode
        original_duration = root.duration

        matching_nodes = _find_matching_nodes(g, root, target_service, target_operation)
        for node in matching_nodes:
            if percent_difference is not None:
                # originalDuration (never mutated by retiming), not duration -- otherwise
                # a same-method ancestor/descendant pair processed earlier in this loop
                # would already have changed `duration` via cascade, making the percent
                # basis (and thus the result) depend on g.nodeHT iteration order.
                adjustment_amount = node.originalDuration * percent_difference / 100.0
            else:
                adjustment_amount = fixed_difference

            new_end = node.endTime + adjustment_amount
            # Deviation from calligator (see module docstring): floor the new
            # end time the same way computeProjectedCPMetrics does, instead
            # of porting calligator's inverted clamp branch. new_end must
            # never drop below what still contains the node's own children --
            # retime_node assumes a non-conflicting new_start/new_end.
            floor_end = max(c.endTime for c in node.children) if node.children else node.startTime
            new_end = max(new_end, floor_end)

            self.retime_node(g, node.sid, node.startTime, new_end)

        return original_duration - root.duration
