"""Error-path breakdown: which call paths end in an error, keyed by RPC status.

For every analyzed trace, walk the span tree from a root and record each
"error path": the chain of spans from the root down to an erroring span none
of whose (visited) children error. Paths are keyed by
``[service]operation[:protocol][:statusCode]`` elements (protocol and status
only for RPC spans) joined by ``;``, counted per trace, and merged across
traces with up to ``maxExemplars`` (traceID, spanID) exemplars per path node.

Two counting modes:

* ``origins``: every erroring span is followed into all of its children, and
  non-erroring spans are traversed too, so every error origin is reported.
* ``propToRoot``: only errors that propagate to the root are reported. A
  non-erroring RPC (client/server) span stops the walk; a non-erroring
  user-defined span only descends into children of its own service. This is
  the walk of ``Graph.computePropToRootGraph``.

Two root modes:

* ``trace``: the trace's own root -- the first span (document order) with no
  ``CHILD_OF`` reference that is neither a proxy node nor in
  ``span_utils.IGNORED_ROOT_OPS``. With no such span, spans whose parent is
  missing hang off a virtual ``[INCOMPLETE_TRACE]virtual_root`` node.
* ``analysis``: the root selected by Graph construction for the requested
  service/operation (``rootTrace`` semantics).

A span errors here if ``parseForErrorReturn`` flagged it (``returnError``,
including error-propagation nodes) OR its RPC status code signals failure
(see ``extract_rpc_status``). The walk uses the parent/child links as built
from span references, before timeline sanitization and op exclusion. See
CONFORMANCE.md for the output format.
"""
import json
import os
import re
from typing import Any, Iterable, Optional

from crisp.shared.models import SpanKind
from crisp.utils import span_utils

ERROR_BREAKDOWN_FILE = "error-breakdown.json"

MODE_ORIGINS = "origins"
MODE_PROP_TO_ROOT = "propToRoot"
MODES = (MODE_ORIGINS, MODE_PROP_TO_ROOT)

ROOT_TRACE = "trace"
ROOT_ANALYSIS = "analysis"
ROOTS = (ROOT_TRACE, ROOT_ANALYSIS)

VIRTUAL_ROOT_SERVICE = "INCOMPLETE_TRACE"
VIRTUAL_ROOT_OPERATION = "virtual_root"

PROTOCOL_HTTP = "http"
PROTOCOL_GRPC = "grpc"
PROTOCOL_TCHANNEL = "tchannel"
PROTOCOL_YARPC_HTTP = "yarpc_http"
PROTOCOL_YARPC_GRPC = "yarpc_grpc"
PROTOCOL_YARPC_TCHANNEL = "yarpc_tchannel"

_GRPC_STATUS_KEYS = ("grpc.status", "grpc.status_code", "rpc.grpc.status_code")
_HTTP_STATUS_KEYS = ("http.response.status_code", "http.status_code")
_YARPC_STATUS_KEY = "rpc.yarpc.status_code"
_TRANSPORT_KEY = "rpc.transport"
_COMPONENT_KEY = "component"

# transport -> (plain protocol, YARPC protocol)
_TRANSPORT_PROTOCOLS = {
    "tchannel": (PROTOCOL_TCHANNEL, PROTOCOL_YARPC_TCHANNEL),
    "http": (PROTOCOL_HTTP, PROTOCOL_YARPC_HTTP),
    "grpc": (PROTOCOL_GRPC, PROTOCOL_YARPC_GRPC),
}

# Go's strconv.Atoi syntax: optional sign, ASCII digits, nothing else.
_ATOI_RE = re.compile(r"[+-]?[0-9]+")
_INT64_MIN = -(1 << 63)
_INT64_MAX = (1 << 63) - 1


class ErrorBreakdownOptions:
    """Selects the counting mode and root mode of the error breakdown."""

    def __init__(self, mode: str, root: str = ROOT_TRACE):
        if mode not in MODES:
            raise ValueError(f"unknown error breakdown mode {mode!r}; want one of {MODES}")
        if root not in ROOTS:
            raise ValueError(f"unknown error breakdown root {root!r}; want one of {ROOTS}")
        self.mode = mode
        self.root = root


class _VirtualRoot:
    """Stand-in root for traces without a root span; parents all orphans."""

    sid = None
    spanKind = SpanKind.UNKNOWN
    returnError = False
    rpcProtocol = ""
    rpcStatusCode = None

    def __init__(self, orphans):
        self.children = {o: True for o in orphans}


def _in_int64(n: int) -> bool:
    return _INT64_MIN <= n <= _INT64_MAX


def _parse_code(tag: dict) -> Optional[int]:
    """A status code from a numeric string value or an int64-typed value."""
    value = tag.get("value")
    if isinstance(value, str) and value != "" and _ATOI_RE.fullmatch(value):
        n = int(value)
        if _in_int64(n):
            return n
    if tag.get("type") == "int64" and isinstance(value, int) and not isinstance(value, bool):
        if _in_int64(value):
            return value
    return None


def _protocol_from_transport(transport: str, yarpc: bool) -> str:
    pair = _TRANSPORT_PROTOCOLS.get(transport)
    if pair is None:
        return ""
    return pair[1] if yarpc else pair[0]


def extract_rpc_status(tags) -> tuple[str, Optional[int]]:
    """Derive (protocol, statusCode) from a span's tags.

    Keys match exactly. A later status tag overrides an earlier one. The
    protocol is "" when unknown; the status code is None when absent or not
    an integer (so a gRPC status name such as ``"UNAVAILABLE"`` sets only the
    protocol). Keys in span_utils.TCHANNEL_STATUS_TAGS and
    span_utils.YARPC_STATUS_TAGS extend the standard status keys. Tags in
    span_utils.TCHANNEL_MARKER_TAGS force the TChannel protocol; a
    ``component`` value in span_utils.HTTP_COMPONENTS implies HTTP when
    nothing else set the protocol.
    """
    protocol = ""
    code: Optional[int] = None
    yarpc_code: Optional[int] = None
    transport = ""
    component_http = False
    for tag in tags:
        key = tag.get("key")
        if key in _GRPC_STATUS_KEYS:
            protocol, code = PROTOCOL_GRPC, _parse_code(tag)
        elif key in _HTTP_STATUS_KEYS:
            protocol, code = PROTOCOL_HTTP, _parse_code(tag)
        elif key == _YARPC_STATUS_KEY:
            yarpc_code = _parse_code(tag)
        elif key == _TRANSPORT_KEY:
            value = tag.get("value")
            transport = value if isinstance(value, str) else ""
        elif key == _COMPONENT_KEY:
            component_http = tag.get("value") in span_utils.HTTP_COMPONENTS
        elif key in span_utils.TCHANNEL_STATUS_TAGS:
            protocol, code = PROTOCOL_TCHANNEL, _parse_code(tag)
        elif key in span_utils.YARPC_STATUS_TAGS:
            yarpc_code = _parse_code(tag)
        elif key in span_utils.TCHANNEL_MARKER_TAGS:
            protocol = PROTOCOL_TCHANNEL

    if protocol == "":
        if yarpc_code is not None:
            protocol, code = _protocol_from_transport(transport, True), yarpc_code
        elif component_http:
            protocol = PROTOCOL_HTTP
        elif transport != "":
            protocol = _protocol_from_transport(transport, False)
    return protocol, code


def is_status_error(protocol: str, code: Optional[int]) -> bool:
    """HTTP errors are codes >= 400; for every other protocol, any non-zero code."""
    if code is None:
        return False
    if protocol == PROTOCOL_HTTP:
        return code >= 400
    return code != 0


def select_trace_root(graph, potentialRoots):
    """The trace's root span, a virtual root over orphans, or None."""
    orphans = []
    for node in potentialRoots:
        if node.parentSpanId is None:
            if node.opName in span_utils.IGNORED_ROOT_OPS or node.sid in graph.proxyNodes:
                continue
            return node
        orphans.append(node)
    if orphans:
        return _VirtualRoot(orphans)
    return None


def compute_trace_breakdown(graph, root, mode: str) -> dict:
    """Per-trace error paths: key -> [count, nodes].

    nodes is a list of (service, operation, protocol, statusCode, spanID)
    tuples along the path; protocol/statusCode are None for non-RPC spans
    and the virtual root's spanID is None. A key seen twice in one trace
    only increments its count.
    """
    paths: dict = {}
    if root is None:
        return paths

    def service(node):
        if isinstance(node, _VirtualRoot):
            return VIRTUAL_ROOT_SERVICE
        return graph.processName[node.pid]

    def operation(node):
        if isinstance(node, _VirtualRoot):
            return VIRTUAL_ROOT_OPERATION
        return node.opName

    def is_rpc(node):
        return node.spanKind == SpanKind.SERVER or node.spanKind == SpanKind.CLIENT

    def errors(node):
        return node.returnError or is_status_error(node.rpcProtocol, node.rpcStatusCode)

    def element(node):
        svc, op = service(node), operation(node)
        label = f"[{svc}]{op}"
        protocol = code = None
        if is_rpc(node):
            if node.rpcProtocol:
                protocol = node.rpcProtocol
                label += ":" + protocol
            if node.rpcStatusCode is not None:
                code = node.rpcStatusCode
                label += ":" + str(code)
        return label, (svc, op, protocol, code, node.sid)

    def record(path):
        key = ";".join(label for label, _ in path)
        entry = paths.get(key)
        if entry is None:
            paths[key] = [1, [info for _, info in path]]
        else:
            entry[0] += 1

    def visit_erroring(node, path):
        # Visit every child (no short-circuit): each may record its own paths.
        if sum([visit(c, path) for c in node.children]) == 0:
            record(path)
        return 1

    def visit(node, path):
        path = path + [element(node)]
        if isinstance(node, _VirtualRoot):
            return 1 if sum([visit(c, path) for c in node.children]) else 0
        if errors(node):
            return visit_erroring(node, path)
        if mode == MODE_ORIGINS:
            return 1 if sum([visit(c, path) for c in node.children]) else 0
        if is_rpc(node):
            return 0
        own = service(node)
        return 1 if sum([visit(c, path) for c in node.children if service(c) == own]) else 0

    visit(root, [])
    return paths


def merge_error_breakdowns(
    per_trace: Iterable[tuple[str, dict]],
    options: ErrorBreakdownOptions,
    max_exemplars: int,
) -> dict[str, Any]:
    """Merge (traceID, per-trace breakdown) pairs into the output document."""
    merged: dict = {}
    num_traces = 0
    for trace_id, paths in per_trace:
        num_traces += 1
        for key, (count, nodes) in paths.items():
            entry = merged.get(key)
            if entry is None:
                entry = {
                    "count": 0,
                    "key": key,
                    "nodes": [
                        {
                            "exemplars": [],
                            "operation": op,
                            "protocol": protocol,
                            "service": svc,
                            "statusCode": code,
                        }
                        for svc, op, protocol, code, _ in nodes
                    ],
                }
                merged[key] = entry
            entry["count"] += count
            for out_node, (_, _, _, _, span_id) in zip(entry["nodes"], nodes):
                if span_id is None:
                    continue
                exemplar = {"spanID": span_id, "traceID": trace_id}
                exemplars = out_node["exemplars"]
                if len(exemplars) < max_exemplars and exemplar not in exemplars:
                    exemplars.append(exemplar)
    return {
        "mode": options.mode,
        "paths": [merged[k] for k in sorted(merged)],
        "root": options.root,
        "traces": num_traces,
    }


def canonical_error_breakdown_json(doc: dict[str, Any]) -> str:
    """Sorted keys, 2-space indent, UTF-8 verbatim (same as conformance.json)."""
    return json.dumps(doc, sort_keys=True, indent=2, ensure_ascii=False) + "\n"


def write_error_breakdown(output_dir: str, doc: dict[str, Any]) -> str:
    """Write error-breakdown.json into output_dir and return its path."""
    path = os.path.join(output_dir, ERROR_BREAKDOWN_FILE)
    with open(path, "w", encoding="utf-8") as f:
        f.write(canonical_error_breakdown_json(doc))
    return path
