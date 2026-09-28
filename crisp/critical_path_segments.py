"""Per-span time windows on one trace's critical path.

The light-mode pipeline reduces each critical-path span to one exclusive
time and then merges traces, so *when* a span was on the critical path is
lost. This module keeps it for a single trace: for every span on the path,
the windows of its time, within its parent's, that no critical-path child
covers.

Times are integer microseconds: each node's startTime and endTime after
timeline sanitization. For a span S on the critical path, its effective
window is [startTime, endTime] clipped to its parent's effective window (the
root's is its own interval). S's critical-path children are taken in
startTime order; a cursor starts at S's window start, each child emits
[cursor, child.startTime] and advances the cursor to max(cursor,
child.endTime), and a final [cursor, window end] is emitted. Every window is
clipped to S's effective window, and empty windows are dropped.

Consequences:

* Every segment lies within the root span, and the segments of all spans
  together cover it exactly.
* Segments of different spans are disjoint unless two critical-path siblings
  overlap (the clock-skew allowance in ``Graph.happensBefore``); the overlap
  is then covered in both siblings' subtrees.
* ``exclusive`` is ``Graph.accumeCPMetrics``'s per-span value: duration
  minus critical-path children's durations, clamped at zero. It equals the
  sum of the span's segments when the span's effective window is its whole
  interval and its critical-path children lie within that interval without
  overlapping each other. Otherwise they can differ: client/server spans are
  never trimmed to their parent by sanitization, and a server's duration may
  be shortened to its client's without moving its endTime.

See CONFORMANCE.md for the golden file format.
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from crisp.graph import Graph, GraphNode

CP_SEGMENTS_FILE = "cp-segments.json"


@dataclass(frozen=True)
class CriticalPathSpan:
    """One span on a trace's critical path; times are integer microseconds."""

    span_id: str
    parent_span_id: Optional[str]  # None for the analysis root
    service: str
    operation: str
    start_time: int
    end_time: int
    exclusive: int
    segments: tuple[tuple[int, int], ...]


def critical_path_segments(graph: Graph, cp: list[GraphNode]) -> list[CriticalPathSpan]:
    """Return a CriticalPathSpan for each node of cp, in cp order.

    cp is a critical path as returned by ``Graph.findCriticalPath()``: its
    first node is the analysis root, every later node's parent appears
    earlier in cp, and no node repeats.
    """
    if not cp:
        return []
    root = cp[0]
    _, exclusive = graph.accumeCPMetrics(cp, "", root)

    cp_children: dict[str, list] = {node.sid: [] for node in cp}
    for node in cp[1:]:
        cp_children[node.parent.sid].append(node)

    windows = {root.sid: (root.startTime, root.endTime)}
    spans = []
    for node in cp:
        lo, hi = windows[node.sid]
        segments = []
        cursor = lo
        for child in sorted(cp_children[node.sid], key=lambda c: c.startTime):
            child_lo = min(max(child.startTime, lo), hi)
            windows[child.sid] = (child_lo, max(min(child.endTime, hi), child_lo))
            _append_clipped(segments, cursor, child.startTime, lo, hi)
            cursor = max(cursor, child.endTime)
        _append_clipped(segments, cursor, hi, lo, hi)

        spans.append(CriticalPathSpan(
            span_id=node.sid,
            parent_span_id=node.parent.sid if node is not root else None,
            service=graph.processName[node.pid],
            operation=node.opName,
            start_time=node.startTime,
            end_time=node.endTime,
            exclusive=exclusive[node.sid],
            segments=tuple(segments),
        ))
    return spans


def _append_clipped(segments: list, start: int, end: int, lo: int, hi: int) -> None:
    start, end = max(start, lo), min(end, hi)
    if end > start:
        segments.append((start, end))


def canonical_segments_json(spans: list[CriticalPathSpan]) -> str:
    """Render spans as canonical JSON (sorted keys, 2-space indent)."""
    doc = {
        "spans": [
            {
                "spanID": s.span_id,
                "parentSpanID": s.parent_span_id,
                "service": s.service,
                "operation": s.operation,
                "startTime": s.start_time,
                "endTime": s.end_time,
                "exclusive": s.exclusive,
                "segments": [list(seg) for seg in s.segments],
            }
            for s in spans
        ],
    }
    return json.dumps(doc, sort_keys=True, indent=2, ensure_ascii=False) + "\n"


def main(argv: Optional[list[str]] = None) -> int:
    """Print one trace's canonical critical-path segments JSON to stdout."""
    from crisp.graph import Graph

    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--file", required=True, help="Jaeger JSON trace file")
    parser.add_argument("-s", "--serviceName", required=True)
    parser.add_argument("-a", "--operationName", required=True)
    parser.add_argument("--rootTrace", action="store_true", help="the root span must be serviceName/operationName")
    args = parser.parse_args(argv)

    with open(args.file, encoding="utf-8") as f:
        data = json.load(f)
    graph = Graph(data, args.serviceName, args.operationName, args.file, args.rootTrace)
    if graph.rootNode is None:
        print(f"error: no analysis root in {args.file}", file=sys.stderr)
        return 1
    sys.stdout.buffer.write(canonical_segments_json(graph.criticalPathSegments()).encode("utf-8"))
    return 0


if __name__ == "__main__":
    sys.exit(main())
