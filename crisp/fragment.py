"""Trace fragments: a trace reduced to what the light pipeline needs to
analyze one service and operation.

Traces can be very large, and the light pipeline only analyzes the subtrees of
the spans that match ``-s``/``-a``. ``pruneToFragment`` removes the rest of a
trace, and ``--fragment`` tells the light pipeline that its input may be such
a fragment. The contract, in CONFORMANCE.md ("Trace fragments"): with
``--fragment``, a fragment gives the same outputs as the trace it was made
from.

Usage::

    python -m crisp.fragment --file trace.json -s SERVICE -a OPERATION -o fragment.json
"""
import argparse
import json
from typing import Any, Dict, List, Optional, Set

from crisp.constants import (
    CHILD_OF,
    OPERATION_NAME,
    PROCESS_ID,
    PROCESSES,
    REF_TYPE,
    REFERENCES,
    SPAN_ID,
    SPANS,
)
from crisp.utils.span_utils import isTestTraceByOpName


def _lastChildOf(span: Dict[str, Any]) -> Optional[str]:
    """The parent span ID as Graph.parseNode reads it: the last CHILD_OF reference."""
    parent = None
    for ref in span.get(REFERENCES) or []:
        if ref.get(REF_TYPE) == CHILD_OF:
            parent = ref.get(SPAN_ID)
    return parent


def _fragmentReferences(span: Dict[str, Any], keep: Set[str], present: Set[str]) -> List[Dict[str, Any]]:
    """The references of a kept span.

    A reference stays when its target is kept, or was never in the trace (the
    span is then an orphan in the fragment too). Of the CHILD_OF references
    only the last one can stay: it is the parent, and an earlier one must not
    become the parent when the last one is removed.
    """
    refs = span.get(REFERENCES) or []
    lastChildOf = max((i for i, ref in enumerate(refs) if ref.get(REF_TYPE) == CHILD_OF), default=-1)
    out = []
    for i, ref in enumerate(refs):
        if ref.get(REF_TYPE) == CHILD_OF and i != lastChildOf:
            continue
        target = ref.get(SPAN_ID)
        if target in keep or target not in present:
            out.append(ref)
    return out


def pruneToFragment(data: Dict[str, Any], serviceName: str, operationName: str) -> Dict[str, Any]:
    """Return the fragment of a Jaeger JSON trace for one service and operation.

    The fragment keeps, in their original order:

    - every span that matches the service and operation, with its descendants;
    - the ancestors of every match, so that a match that has a parent keeps
      it and does not become an analysis root;
    - every orphan span (its CHILD_OF parent is not in the trace), so that
      incomplete subtrees are still detected;
    - the first span whose operation marks a test trace, and all processes,
      so that a test trace is still one.

    ``data`` is not modified. The fragment shares the kept spans' nested
    objects (tags, logs) with it.
    """
    spans = [span for item in data.get("data") or [] for span in item.get(SPANS) or []]
    processName = {}
    for item in data.get("data") or []:
        for pid, process in (item.get(PROCESSES) or {}).items():
            processName[pid] = process.get("serviceName")

    present = {span[SPAN_ID] for span in spans}
    parent: Dict[str, Optional[str]] = {}
    children: Dict[str, List[str]] = {}
    for span in spans:
        parentId = _lastChildOf(span)
        # For a repeated span ID the last span wins, as in the Graph.
        parent[span[SPAN_ID]] = parentId
        if parentId is not None:
            children.setdefault(parentId, []).append(span[SPAN_ID])

    matches = [
        span[SPAN_ID]
        for span in spans
        if processName.get(span.get(PROCESS_ID)) == serviceName and span.get(OPERATION_NAME) == operationName
    ]

    keep: Set[str] = set()
    stack = list(matches)
    while stack:
        spanId = stack.pop()
        if spanId in keep:
            continue
        keep.add(spanId)
        stack.extend(children.get(spanId, ()))
    # The walk up stops at a span that is already kept, which also ends it on
    # a CHILD_OF cycle.
    for spanId in matches:
        ancestor = parent.get(spanId)
        while ancestor is not None and ancestor in present and ancestor not in keep:
            keep.add(ancestor)
            ancestor = parent.get(ancestor)
    for span in spans:
        parentId = _lastChildOf(span)
        if parentId is not None and parentId not in present:
            keep.add(span[SPAN_ID])
    for span in spans:
        if isTestTraceByOpName(span.get(OPERATION_NAME)):
            keep.add(span[SPAN_ID])
            break

    fragment = dict(data)
    fragment["data"] = []
    for item in data.get("data") or []:
        newItem = dict(item)
        newItem[SPANS] = [
            {**span, REFERENCES: _fragmentReferences(span, keep, present)}
            for span in item.get(SPANS) or []
            if span[SPAN_ID] in keep
        ]
        fragment["data"].append(newItem)
    return fragment


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Write the fragment of a Jaeger JSON trace for one service and operation. "
        "Analyze it with crisp-trace --lightMode --fragment."
    )
    parser.add_argument("--file", required=True, help="Jaeger JSON trace file")
    parser.add_argument("-s", "--serviceName", required=True, help="service name")
    parser.add_argument("-a", "--operationName", required=True, help="operation name")
    parser.add_argument("-o", "--output", required=True, help="fragment file to write")
    args = parser.parse_args()

    with open(args.file, "r") as f:
        data = json.load(f)
    with open(args.output, "w") as f:
        json.dump(pruneToFragment(data, args.serviceName, args.operationName), f)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
