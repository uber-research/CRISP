"""Single-trace dependency inference for critical path call trees.

This module infers timing dependencies between sibling and parent/child
spans of a single trace's :class:`~crisp.graph.Graph`.

Nodes are keyed by :meth:`Graph.getCallPath`, the established call-path
identity used throughout this codebase (see ``CallPathProfile``,
``metrics/aggregators.py``'s ``MergeCallPathProfilesWithExemplars``,
flamegraphs, and CCTs) to mean "the same logical position in the call
tree", including across many different traces of the same endpoint.
Sibling ordering is inferred with :meth:`Graph.happensBefore`, the same
clock-skew-tolerant primitive the critical-path algorithm itself uses
(see ``Graph.computeCriticalPath``).

Cross-trace aggregation is out of scope for this module; it accumulates
dependency information for exactly one :class:`Graph`.
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
    """Infers sibling/parent-child timing dependencies for a single trace's Graph."""

    def __init__(self, graph: Graph):
        self.deps: dict[str, DependencyGraphNode] = self.get_dependencies(graph)

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
