"""Single-trace and multi-trace dependency inference for critical path call trees.

This module infers timing dependencies between sibling and parent/child
spans of a trace's :class:`~crisp.graph.Graph`.

Nodes are keyed by :meth:`Graph.getCallPath`, the established call-path
identity used throughout this codebase (see ``CallPathProfile``,
``metrics/aggregators.py``'s ``MergeCallPathProfilesWithExemplars``,
flamegraphs, and CCTs) to mean "the same logical position in the call
tree", including across many different traces of the same endpoint.
Sibling ordering is inferred with :meth:`Graph.happensBefore`, the same
clock-skew-tolerant primitive the critical-path algorithm itself uses
(see ``Graph.computeCriticalPath``).

Two modes are supported:

* :meth:`DependencyGraph.get_dependencies` accumulates dependency
  information for exactly one :class:`Graph` (single trace).
* :meth:`DependencyGraph.get_aggregate_dependencies` folds the single-trace
  results of many :class:`Graph` instances (e.g. many traces of the same
  endpoint) into one aggregate dependency dict, per call-path name.

Aggregation of ``async_children``, ``child_dependents``, and the delay
lists is a straightforward union/concatenation across traces. Aggregation
of ``happens_before`` is subtler: it follows a "one counter-example
permanently drops the edge" rule. A candidate call-path name is only
recorded as happening before a node once it has been observed doing so
*and never subsequently contradicted*. If some later trace has evidence
that the same node and the same candidate call-path both occurred but the
candidate did NOT happen before the node, the edge is dropped forever --
even if a still-later trace goes on to re-observe the original confirming
ordering. This intentionally biases towards precision (few false-positive
edges) over recall. A trace that simply never observed a given call-path
at all cannot contradict prior evidence about it (absence is not a
counter-example).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from crisp.graph import Graph


class DependencyGraphNode:
    """Dependency info for one logical position in the call tree (keyed by Graph.getCallPath).

    Attributes:
        name: the call-path identity (Graph.getCallPath output) for this node.
        happens_before: set of call-path names of sibling nodes that must finish
            before this node can start (inferred from observed sibling ordering).
            NOTE: this can include the node's own name when a parent fans out to
            the same call-path more than once in series (e.g. sequential retries
            or a loop) -- that means "an earlier instance of this call-path was
            observed finishing before this instance started", not literal
            self-dependency. Consumers must pair any lookup with a genuine
            per-span identity/timing check, never a bare name-membership test.
        child_dependents: set of call-path names of this node's children whose
            completion this node (as a parent) waits on before it can end
            (i.e. non-async children that finish before the parent ends).
        async_children: set of call-path names of children that are observed to
            end AFTER their parent ends (fire-and-forget / async children).
        parent_start_delay: list of observed (first-child-start - parent-start)
            gaps, one entry per occurrence of this node acting as a parent.
        parent_end_delay: list of observed (parent-end - last-child-end) gaps,
            one entry per occurrence of this node acting as a parent.
    """

    def __init__(self, name: str):
        self.name = name
        self.happens_before: set[str] = set()
        self.child_dependents: set[str] = set()
        self.async_children: set[str] = set()
        self.parent_start_delay: list[float] = []
        self.parent_end_delay: list[float] = []

    def __repr__(self) -> str:
        return (
            f"DependencyGraphNode(name={self.name!r}, "
            f"happens_before={self.happens_before}, "
            f"child_dependents={self.child_dependents}, "
            f"async_children={self.async_children}, "
            f"parent_start_delay={self.parent_start_delay}, "
            f"parent_end_delay={self.parent_end_delay})"
        )


class DependencyGraph:
    """Infers sibling/parent-child timing dependencies for one or many traces' Graphs."""

    def __init__(self, graph: Graph | None = None, graphs: list[Graph] | None = None):
        if (graph is None) == (graphs is None):
            raise ValueError("DependencyGraph requires exactly one of `graph` or `graphs`.")

        if graph is not None:
            self.deps: dict[str, DependencyGraphNode] = self.get_dependencies(graph)
        else:
            self.deps = self.get_aggregate_dependencies(graphs)

    @classmethod
    def _get_or_create(cls, deps: dict[str, DependencyGraphNode], name: str) -> DependencyGraphNode:
        if name not in deps:
            deps[name] = DependencyGraphNode(name)
        return deps[name]

    @classmethod
    def get_dependencies(cls, graph: Graph) -> dict[str, DependencyGraphNode]:
        """Infer sibling/parent-child dependencies for every node in a single trace.

        Args:
            graph: the trace's Graph to analyze.

        Returns:
            A dict mapping call-path name (Graph.getCallPath) to the
            DependencyGraphNode accumulating that call-path's dependency info.
        """
        deps: dict[str, DependencyGraphNode] = {}

        for node in graph.nodeHT.values():
            node_name = graph.getCallPath(node)
            cls._get_or_create(deps, node_name)

            if node.parent is not None and node.endTime > node.parent.endTime:
                parent_name = graph.getCallPath(node.parent)
                parent_dep = cls._get_or_create(deps, parent_name)
                parent_dep.async_children.add(node_name)
                parent_dep.child_dependents.discard(node_name)

            if not node.children:
                continue

            # Reverse-sort by endTime, mirroring Graph.computeCriticalPath's sortedChildren.
            sortedChildren = sorted(node.children, key=lambda x: x.endTime)[::-1]

            node_dep = cls._get_or_create(deps, node_name)
            node_dep.parent_start_delay.append(
                min(c.startTime for c in sortedChildren) - node.startTime,
            )
            node_dep.parent_end_delay.append(node.endTime - sortedChildren[0].endTime)

            for i, child in enumerate(sortedChildren):
                child_name = graph.getCallPath(child)
                child_dep = cls._get_or_create(deps, child_name)

                if child.endTime < node.endTime and child_name not in node_dep.async_children:
                    node_dep.child_dependents.add(child_name)

                for candidate in sortedChildren[i + 1 :]:
                    if graph.happensBefore(node, sortedChildren, candidate, child):
                        # NOTE: if candidate and child share a call-path (e.g. a parent
                        # fanning out to the same op twice in series), this intentionally
                        # adds child_name to its own happens_before set. That records a
                        # real, useful fact -- "an earlier instance of this call-path was
                        # observed finishing before this instance started" (e.g. a loop
                        # issuing sequential calls to the same op) -- it does NOT mean
                        # "this exact span depends on itself". Consumers MUST pair any
                        # name-based happens_before lookup with a genuine per-span identity
                        # check (e.g. excluding the node's own span id) before acting on it,
                        # never treat name-membership alone as meaningful. See
                        # test_fanout_same_callpath_has_self_referential_happens_before.
                        child_dep.happens_before.add(graph.getCallPath(candidate))

        return deps

    @classmethod
    def get_aggregate_dependencies(cls, graphs: list[Graph]) -> dict[str, DependencyGraphNode]:
        """Fold single-trace dependencies from many traces into one aggregate dict.

        Traces are processed in order, maintaining a running aggregate. See the
        module docstring for the "one counter-example permanently drops the
        edge" rule applied to ``happens_before``; ``async_children`` is a plain
        union; ``child_dependents`` is a union that evicts any name observed as
        async (in this trace or an earlier one); and the delay lists are
        concatenated across all traces, in order.

        Args:
            graphs: the traces' Graphs to aggregate, in the order to process them.

        Returns:
            A dict mapping call-path name (Graph.getCallPath) to a fresh
            aggregate DependencyGraphNode (never aliased to any per-trace node).
        """
        aggregate: dict[str, DependencyGraphNode] = {}
        # Every call-path name ever proposed as a happens_before candidate for a
        # given node_name, whether currently confirmed or previously rejected.
        ever_proposed: dict[str, set[str]] = {}

        for graph in graphs:
            dependencies = cls.get_dependencies(graph)

            for node_name, dependency in dependencies.items():
                if node_name not in aggregate:
                    agg_node = DependencyGraphNode(node_name)
                    agg_node.happens_before = set(dependency.happens_before)
                    ever_proposed[node_name] = set(dependency.happens_before)
                    aggregate[node_name] = agg_node
                else:
                    agg_node = aggregate[node_name]
                    seen = ever_proposed[node_name]
                    new_happens_before: set[str] = set()

                    for candidate in dependency.happens_before:
                        if candidate in agg_node.happens_before:
                            new_happens_before.add(candidate)
                        elif candidate not in seen:
                            new_happens_before.add(candidate)
                            seen.add(candidate)
                        # else: previously proposed but not currently confirmed --
                        # a prior trace already contradicted it, so it must not
                        # be resurrected even if this trace's evidence matches.

                    for candidate in agg_node.happens_before:
                        if candidate in dependency.happens_before:
                            continue  # already handled above
                        if candidate not in dependencies:
                            # This trace never observed the candidate call-path at
                            # all, so it can't contradict prior evidence about it.
                            new_happens_before.add(candidate)
                        # else: candidate was observed in this trace but not
                        # confirmed as happens-before here -- a genuine
                        # counter-example, so drop it.

                    agg_node.happens_before = new_happens_before

                # async_children: plain union. child_dependents: union, but a name
                # must be evicted if it's async in this trace OR any earlier one.
                async_before_this_trace = set(agg_node.async_children)
                for name in dependency.child_dependents:
                    if name not in async_before_this_trace:
                        agg_node.child_dependents.add(name)

                newly_async = dependency.async_children - agg_node.async_children
                agg_node.async_children |= dependency.async_children
                agg_node.child_dependents -= newly_async

                agg_node.parent_start_delay.extend(dependency.parent_start_delay)
                agg_node.parent_end_delay.extend(dependency.parent_end_delay)

        return aggregate
