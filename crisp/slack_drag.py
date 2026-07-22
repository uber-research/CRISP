"""Drag and slack calculation for a critical path call tree.

This module ports the "drag" and "slack" concepts from Google's `calligator`
project (Apache-2.0, see
https://github.com/google/calligator/blob/main/critical_path.py,
class ``CriticalPath``, methods ``calculate_drag``, ``_get_exclusive_cp_time``,
``calculate_slack``, ``_compute_earliest_start_times``,
``_compute_latest_start_times``) to this codebase's
:class:`~crisp.graph.Graph` and :meth:`Graph.computeCriticalPath`.

Drag measures, for a node on the critical path, how much its duration could
shrink before it stops being the limiting factor for its parent's/ancestors'
timing. It is capped at the node's own duration (or exclusive duration, in
exclusive mode): a node can never have "negative existence", so drag can
never exceed how much time the node itself contributes.

Slack measures, for any node, how much scheduling room it has: the gap
between the earliest it could have started (without violating any inferred
happens-before ordering among its siblings) and the latest it could have
started while still finishing by its critical-path boundary. Critical-path
nodes always have zero slack -- they're already the bottleneck.

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

``calculate_slack`` never had this hazard: it always resolves parent/sibling
via ``node.parent``/``parent.children`` directly, never via ``cp`` position
(see
``test_naive_index_adjacency_would_misreport_chained_sibling_dependent_slack``).

See also :meth:`Graph.accumeCPMetrics`, this codebase's own existing
"exclusive critical-path time per node" primitive, which already computes an
equivalent quantity to calligator's ``_get_exclusive_cp_time`` (and already
correctly uses ``n.parent``, never list adjacency).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from crisp.dependency_graph import DependencyGraph
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


@dataclass(frozen=True)
class Slack:
    """Slack for every node in a graph.

    Contrast with :class:`Drag`: ``drag_per_span`` only has entries for
    critical-path nodes; ``slack_per_span`` covers *every* node in the graph,
    with an explicit ``0.0`` for critical-path nodes (matches calligator's
    own ``calculate_slack``). Callers can safely index
    ``slack_per_span[sid]`` directly for any span id -- no ``.get(sid, 0.0)``
    fallback needed (unlike ``Drag.drag_per_span``).

    Attributes:
        slack_per_span: mapping from span id to that node's slack -- the gap
            between the earliest it could have started (without violating
            any inferred happens-before ordering among its siblings) and the
            latest it could have started while still finishing by its
            critical-path boundary. Critical-path nodes (and thus the root)
            are exactly ``0.0``.
        total_slack: ``sum(slack_per_span.values())``.
    """

    slack_per_span: dict[str, float]
    total_slack: float


def _critical_end_times(graph: Graph, cp: list[GraphNode]) -> dict[str, float]:
    """Compute each node's "critical end time": the endTime ceiling it must respect.

    For a critical-path node, this is its own endTime. For a true sibling of
    a critical-path node that ends earlier, this is that critical-path
    node's endTime (the sibling could run later, up to that boundary,
    without becoming the new bottleneck). Every other node falls back to its
    immediate parent's endTime.

    Args:
        graph: the trace's Graph that ``cp`` was computed from.
        cp: the critical path, as returned by ``Graph.findCriticalPath()``.

    Returns:
        A mapping from span id (for every node in ``graph.nodeHT``) to its
        critical end time.
    """
    critical_end_times: dict[str, float] = {}
    for node in cp:
        critical_end_times[node.sid] = node.endTime

        parent = node.parent
        if parent is not None:
            for sibling in parent.children:
                if sibling.endTime < node.endTime:
                    critical_end_times[sibling.sid] = node.endTime

    for sid, node in graph.nodeHT.items():
        if sid not in critical_end_times:
            parent = node.parent
            critical_end_times[sid] = parent.endTime if parent is not None else node.endTime

    return critical_end_times


def _compute_earliest_start_time(
    node: GraphNode,
    earliest_start_times: dict[str, float],
    dependency_graph: DependencyGraph,
    graph: Graph,
) -> None:
    """Compute the earliest ``node`` could start without violating happens-before.

    Without any inferred happens-before constraint, a node's true siblings
    are assumed schedulable in any order, so the earliest it could have
    started is the earliest start time among all of them. If ``node`` does
    have a happens-before constraint (from ``dependency_graph``), its
    earliest start is instead the ``endTime`` of the *last* true sibling
    that both (a) shares a call-path name in ``node``'s happens-before set,
    and (b) genuinely finished (by real timestamp comparison, never by
    name-membership alone) before ``node`` started.

    ``happens_before`` can legitimately contain ``node``'s own call-path name
    (a parent fanning out to the same call-path more than once in series --
    see ``dependency_graph.py``). This is handled correctly: siblings are
    only considered up to (not including) ``node`` itself in start-time
    order, so an earlier same-call-path instance can still match while
    ``node`` never spuriously matches itself.

    Args:
        node: the node to compute an earliest start time for.
        earliest_start_times: mapping from span id to earliest start time,
            updated in place with ``node``'s entry.
        dependency_graph: dependency info keyed by call-path name (see
            ``DependencyGraph.deps``), used to look up ``node``'s
            happens-before set.
        graph: the trace's Graph that ``node`` belongs to (needed for
            ``Graph.getCallPath``, the same key function ``dependency_graph``
            itself is keyed by).
    """
    parent = node.parent
    if parent is None:
        earliest_start_times[node.sid] = node.startTime
        return

    siblings = sorted(parent.children, key=lambda s: s.startTime)
    if node not in siblings:
        return

    first_sibling_start_time = siblings[0].startTime

    dep_node = dependency_graph.deps.get(graph.getCallPath(node))
    happens_before = dep_node.happens_before if dep_node is not None else set()

    if not happens_before:
        earliest_start_times[node.sid] = first_sibling_start_time
        return

    previous_happens_before_node = None
    for sibling in siblings:
        if sibling.sid == node.sid:
            break
        if graph.getCallPath(sibling) in happens_before and sibling.endTime < node.startTime:
            previous_happens_before_node = sibling

    earliest_start_times[node.sid] = previous_happens_before_node.endTime if previous_happens_before_node is not None else first_sibling_start_time


def _compute_latest_start_time(
    node: GraphNode,
    latest_start_times: dict[str, float],
    critical_end_times: dict[str, float],
) -> None:
    """Compute the latest ``node`` could start while still meeting its critical end time.

    This is the scenario where ``node`` finishes right at the boundary that
    ``critical_end_times`` assigns it: starting any later would push its own
    endTime past that boundary.

    Args:
        node: the node to compute a latest start time for.
        latest_start_times: mapping from span id to latest start time,
            updated in place with ``node``'s entry.
        critical_end_times: each node's endTime ceiling, from
            ``_critical_end_times``.
    """
    if node.sid not in latest_start_times:
        latest_start_times[node.sid] = critical_end_times[node.sid] - node.duration


def calculate_slack(
    graph: Graph,
    cp: list[GraphNode],
    dependency_graph: DependencyGraph | None = None,
) -> Slack:
    """Calculate slack for every node in ``graph``.

    Slack is the gap between the earliest a node could have started (without
    violating any inferred happens-before ordering among its true siblings)
    and the latest it could have started while still finishing by its
    critical-path boundary -- how much scheduling room it has before it'd
    become (part of) the new bottleneck. Nodes on ``cp`` have zero slack by
    construction; see ``Slack.slack_per_span`` for the full key contract.

    Args:
        graph: the trace's Graph that ``cp`` was computed from.
        cp: the critical path, as returned by ``Graph.findCriticalPath()``.
        dependency_graph: optional pre-built dependency info (see
            ``dependency_graph.py``). If None, one is built internally via
            ``DependencyGraph(graph=graph)``. A multi-trace aggregate (built
            via ``DependencyGraph(graphs=[...])``) is also accepted as a
            drop-in -- this function only ever reads
            ``dependency_graph.deps[name].happens_before``.

    Returns:
        A Slack with one entry per node in ``graph.nodeHT``.
    """
    if dependency_graph is None:
        dependency_graph = DependencyGraph(graph=graph)

    critical_end_times = _critical_end_times(graph, cp)

    earliest_start_times: dict[str, float] = {}
    latest_start_times: dict[str, float] = {}
    for node in graph.nodeHT.values():
        _compute_earliest_start_time(node, earliest_start_times, dependency_graph, graph)
        _compute_latest_start_time(node, latest_start_times, critical_end_times)

    cp_nodes = set(cp)
    slack_per_span: dict[str, float] = {}
    for node in graph.nodeHT.values():
        if node not in cp_nodes and node.sid in latest_start_times and node.sid in earliest_start_times:
            slack_per_span[node.sid] = latest_start_times[node.sid] - earliest_start_times[node.sid]
        else:
            slack_per_span[node.sid] = 0.0

    total_slack = sum(slack_per_span.values())
    return Slack(slack_per_span, total_slack)
