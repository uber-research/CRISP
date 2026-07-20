"""Drag calculation for a critical path call tree.

This module ports the "drag" concept from Google's `calligator` project
(Apache-2.0, see
https://github.com/google/calligator/blob/main/critical_path.py,
class ``CriticalPath``, methods ``calculate_drag``, ``_get_exclusive_cp_time``)
to this codebase's :class:`~crisp.graph.Graph` and :meth:`Graph.computeCriticalPath`.

Drag measures, for a node on the critical path, how much its duration could
shrink before it stops being the limiting factor for its parent's/ancestors'
timing. It is capped at the node's own duration (or exclusive duration, in
exclusive mode): a node can never have "negative existence", so drag can
never exceed how much time the node itself contributes.

IMPORTANT -- correctness note (why this isn't a straight copy of calligator):
calligator's ``calculate_drag`` assumes that for a node at index ``i`` in the
flat critical-path list (``self.cp``, as returned by ``graph.findCriticalPath()``),
``self.cp[i - 1]`` is that node's parent, and (in the exclusive-drag branch)
``self.cp[i + 1]`` is that node's own child continuing onto the critical path.
Both assumptions are unsafe here: :meth:`Graph.computeCriticalPath` can chain
multiple siblings of the *same* parent back-to-back in the flat list whenever
several children of one parent are all sequential/non-overlapping (a common,
not rare, shape -- e.g. a parent making three sequential leaf calls A, then B,
then C). For the second and later chained siblings (e.g. B), ``cp[i - 1]`` is
NOT B's parent -- it's some unrelated node left over from flattening the
previous sibling's (A's) own subtree.

The fix applied throughout this module: never use list-adjacency (``cp[i-1]``)
to find a node's parent or its critical-continuing child. Always use
``node.parent`` (always correct) and, for the child term, recompute
``sorted(node.children, key=lambda x: x.endTime)[::-1][0]`` directly (this is
always the node's own critical continuation when it has children, per how
:meth:`Graph.computeCriticalPath` picks the highest-``endTime`` child to
recurse into -- regardless of whether the node arrived in the flat list as
its parent's primary or secondary/chained pick). When a node has no
children, there is no child term to net out at all.

See also :meth:`Graph.accumeCPMetrics`, this codebase's own existing
"exclusive critical-path time per node" primitive, which already computes an
equivalent quantity to calligator's ``_get_exclusive_cp_time`` (and already
correctly uses ``n.parent``, never list adjacency).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from crisp.utils.dict_utils import accumulateInDict

if TYPE_CHECKING:
    from crisp.graph import Graph, GraphNode


@dataclass(frozen=True)
class Drag:
    """Drag for every node on a critical path.

    Attributes:
        drag_per_span: mapping from span id to that node's drag. Only nodes
            on the critical path (the ``cp`` passed to :func:`calculate_drag`)
            ever get an entry -- nodes not on the critical path conceptually
            have zero drag, and are simply omitted as keys (matching
            calligator's own behavior of only ever writing ``drag[node.sid]``
            for ``node in cp``). Callers that need a value for every node
            should use ``drag_per_span.get(sid, 0.0)``.
        total_drag: ``sum(drag_per_span.values())``.
    """

    drag_per_span: dict[str, float]
    total_drag: float


def _exclusive_cp_time(cp: list[GraphNode]) -> dict[str, float]:
    """Compute each critical-path node's exclusive critical-path time.

    This is the same quantity, computed the same way, as the
    ``sidTimeExclusive`` half of ``Graph.accumeCPMetrics``'s return value
    (and equivalent to calligator's ``_get_exclusive_cp_time``): start with
    each node's own duration, then subtract each node's duration from its
    real parent's (``node.parent``, never list-adjacency) total, so that a
    node's exclusive time nets out the portion already attributed to its
    critical-path children.

    Args:
        cp: the critical path, as returned by ``Graph.findCriticalPath()``.

    Returns:
        A mapping from span id (for every node in ``cp``) to its exclusive
        critical-path time. Mirrors ``Graph.sanitizeExclusiveTime`` by
        zeroing out any negative entries (clock-skew artifacts from the
        overlap-tolerant portions of ``Graph.happensBefore`` can otherwise
        occasionally produce a small negative value).
    """
    exclusive_time: dict[str, float] = {}
    for node in reversed(cp):
        accumulateInDict(exclusive_time, node.sid, node.duration)
        if node.parent is not None:
            accumulateInDict(exclusive_time, node.parent.sid, -node.duration)

    for sid, value in exclusive_time.items():
        if value < 0:
            exclusive_time[sid] = 0.0

    return exclusive_time


def calculate_drag(graph: Graph, cp: list[GraphNode], exclusive: bool = False) -> Drag:  # noqa: ARG001
    """Calculate drag for every node on the critical path ``cp``.

    For a node on the critical path, drag is how much its duration could
    shrink before it stops being the limiting factor for its parent's/
    ancestors' timing, capped at the node's own (inclusive or exclusive)
    duration:

    * The root node (no parent), and any node that is a single child (its
      parent's only child -- no true sibling to compete with), gets the
      full, uncapped duration -- there's no sibling that could ever become
      the new bottleneck in its place.
    * Order a node's true siblings (``node.parent.children``) from
      latest-ending to earliest-ending -- the same order
      ``Graph.computeCriticalPath`` itself uses to pick which child is
      critical. The earliest-ending sibling in that order has no sibling
      after it, so it also gets the full, uncapped duration.
    * Every other sibling's drag is capped by the gap between its own
      ``endTime`` and the ``endTime`` of the *next* sibling in that same
      latest-to-earliest order: how far this node's end time could move
      backward before that next sibling becomes the new latest-ending (and
      thus the new bottleneck). In exclusive mode, this gap is further
      reduced to remove the portion already attributed to this node's own
      critical-path-continuing child (if it has one), so that stretch of
      time isn't double-counted as both this node's drag and its child's.

    Nodes not on the critical path conceptually have zero drag; see
    ``Drag.drag_per_span`` for the exact (key-omission) contract.

    Args:
        graph: the trace's Graph that ``cp`` was computed from. Not used by
            the computation itself (every quantity needed is reachable from
            ``GraphNode.parent``/``GraphNode.children`` on ``cp``'s own
            nodes), but accepted for symmetry with ``Graph.calculateDrag``
            and to leave room for future graph-level context (e.g. a
            configurable overlap allowance) without an API break.
        cp: the critical path, as returned by ``Graph.findCriticalPath()``.
        exclusive: if True, compute exclusive drag (discount the portion of
            drag already attributable to a node's own critical-path child);
            if False (default), compute inclusive drag.

    Returns:
        A Drag with one entry per node in ``cp``.
    """
    exclusive_cp_time = _exclusive_cp_time(cp)

    drag_per_span: dict[str, float] = {}
    for node in cp:
        own_metric = exclusive_cp_time[node.sid] if exclusive else node.duration

        parent = node.parent
        if parent is None:
            drag_per_span[node.sid] = own_metric
            continue

        true_siblings = list(parent.children)
        if len(true_siblings) == 1:
            # node is a single child: it's always a member of parent.children,
            # so this is the only way there can be no true sibling to cap it.
            drag_per_span[node.sid] = own_metric
            continue

        # Reverse-sort by endTime, mirroring Graph.computeCriticalPath's sortedChildren
        # (using the identical [::-1]-after-ascending-sort idiom, not `reverse=True`,
        # so that endTime ties break the same way as the critical-path algorithm itself).
        sorted_siblings = sorted(true_siblings, key=lambda s: s.endTime)[::-1]
        node_idx = next((idx for idx, sibling in enumerate(sorted_siblings) if sibling is node), None)

        if node_idx is None or node_idx == len(sorted_siblings) - 1:
            # Not found among the true siblings (defensive; shouldn't happen for a
            # well-formed tree) or the earliest-ending sibling: no later sibling to cap it.
            drag_per_span[node.sid] = own_metric
            continue

        next_sibling = sorted_siblings[node_idx + 1]

        if not exclusive or not node.children:
            drag_val = node.endTime - next_sibling.endTime
        else:
            # node's own critical-path continuation: computeCriticalPath always
            # recurses into the highest-endTime child, regardless of whether node
            # arrived on cp as its parent's primary or a chained sibling pick -- so
            # this is safe to recompute directly, unlike relying on cp[i + 1].
            own_cp_child = sorted(node.children, key=lambda c: c.endTime)[::-1][0]
            drag_val = node.endTime - max(own_cp_child.endTime, next_sibling.endTime)
            if own_cp_child.startTime > next_sibling.endTime:
                drag_val += own_cp_child.startTime - next_sibling.endTime

        # Drag can never exceed how much time the node itself contributes, nor
        # be negative (a defensive clamp: an async/overrunning critical-path
        # child, though not expected in the fixtures this module is tested
        # against, could otherwise push the exclusive-branch formula below 0).
        drag_per_span[node.sid] = max(0.0, min(own_metric, drag_val))

    total_drag = sum(drag_per_span.values())
    return Drag(drag_per_span, total_drag)
