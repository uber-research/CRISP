import logging
import re
from typing import Any, Optional

from crisp.proto import analyzer_pb2
from crisp.shared.models import CallPathProfile

logger = logging.getLogger(__name__)

TIMING_PATTERN = re.compile(r'(\d+)\s*<<(\d+)>>$')


def parse_call_path_part(part: str) -> dict[str, str]:
    """Parse a single call path part into service and operation."""
    if not (part.startswith('[') and ']' in part):
        return {}
    service_end = part.find(']')
    service = part[1:service_end]
    operation = part[service_end + 1:].strip()
    return {'service': service, 'operation_name': operation}


def parse_cct_line(line: str) -> dict[str, Any]:
    """Parse a single line from the CCT file."""
    line = line.strip()
    if not line:
        return {}
    parts = line.split(';')
    if not parts:
        return {}

    last_part = parts[-1]
    timing_match = TIMING_PATTERN.search(last_part)
    if not timing_match:
        return {}

    duration = int(timing_match.group(1))
    frequency = int(timing_match.group(2))
    last_part = last_part[: timing_match.start()].strip()

    call_path: list[dict[str, str]] = []
    for part in parts[:-1]:
        p = parse_call_path_part(part)
        if p:
            call_path.append(p)
    p_last = parse_call_path_part(last_part)
    if p_last:
        call_path.append(p_last)

    if not call_path:
        return {}

    return {'call_path': call_path, 'duration': duration, 'frequency': frequency}


def parse_cct_file(cct_path: str) -> list[dict[str, Any]]:
    """Parse a CCT file and return call chain summaries."""
    summaries: list[dict[str, Any]] = []
    try:
        with open(cct_path) as f:
            for line in f:
                summary = parse_cct_line(line)
                if summary:
                    summaries.append(summary)
    except Exception as e:
        logger.error("Error parsing CCT file %s: %s", cct_path, e)
    return summaries


def _escape_dot_label(s: str) -> str:
    """Escape characters that are special inside DOT double-quoted strings."""
    return s.replace("\\", "\\\\").replace('"', '\\"')


def _make_node_label(label: str, excl_time: int, incl_time: int, freq: int) -> str:
    """Build a multi-line DOT node label with timing info."""
    parts = [_escape_dot_label(label)]
    if incl_time > 0:
        parts.append(f"incl: {incl_time}\u00b5s")
    if excl_time > 0:
        parts.append(f"excl: {excl_time}\u00b5s")
    if freq > 0:
        parts.append(f"freq: {freq}")
    return "\\n".join(parts)


def cct_to_dot(summaries: list[dict[str, Any]]) -> str:
    """Convert parsed CCT summaries into a Graphviz DOT digraph."""
    if not summaries:
        return "digraph CCT {\n}\n"

    path_to_id: dict[tuple[str, ...], int] = {}
    node_label: dict[int, str] = {}
    node_excl: dict[int, int] = {}
    node_freq: dict[int, int] = {}
    children: dict[int, list[int]] = {}
    edge_set: set[tuple[int, int]] = set()
    next_id = 0

    def _get_or_create(path_key: tuple[str, ...], label: str) -> int:
        nonlocal next_id
        if path_key in path_to_id:
            return path_to_id[path_key]
        nid = next_id
        next_id += 1
        path_to_id[path_key] = nid
        node_label[nid] = label
        node_excl[nid] = 0
        node_freq[nid] = 0
        children[nid] = []
        return nid

    for summary in summaries:
        call_path = summary['call_path']
        duration = summary['duration']
        frequency = summary['frequency']

        prev_id = None
        for i, part in enumerate(call_path):
            path_key = tuple(
                f"[{p['service']}]{p['operation_name']}" for p in call_path[:i + 1]
            )
            label = f"[{part['service']}] {part['operation_name']}"
            nid = _get_or_create(path_key, label)

            if i == len(call_path) - 1:
                node_excl[nid] += duration
                node_freq[nid] += frequency

            if prev_id is not None and (prev_id, nid) not in edge_set:
                children[prev_id].append(nid)
                edge_set.add((prev_id, nid))
            prev_id = nid

    # Compute inclusive times bottom-up via post-order traversal
    node_incl: dict[int, int] = {}

    def _compute_inclusive(nid: int) -> int:
        total = node_excl[nid]
        for cid in children[nid]:
            total += _compute_inclusive(cid)
        node_incl[nid] = total
        return total

    roots = {nid for nid in node_label if not any(nid in ch for ch in children.values())}
    for root in roots:
        _compute_inclusive(root)

    lines = [
        "digraph CCT {",
        '    rankdir=TB;',
        '    node [shape=box, style=filled, fillcolor=lightyellow, '
        'fontname="Helvetica", fontsize=10];',
        '    edge [fontname="Helvetica", fontsize=8];',
        "",
    ]

    for nid in sorted(node_label):
        label = _make_node_label(
            node_label[nid], node_excl[nid], node_incl.get(nid, 0), node_freq[nid],
        )
        lines.append(f'    n{nid} [label="{label}"];')

    lines.append("")

    for parent_id, child_id in sorted(edge_set):
        lines.append(f"    n{parent_id} -> n{child_id};")

    lines.append("}")
    return "\n".join(lines) + "\n"


def _build_exemplar_lookup(
    merged_cpp: Optional[CallPathProfile],
    max_exemplars: int,
) -> dict[str, list[tuple[str, str]]]:
    """Build a mapping from ``->``-separated call path key to its exemplar list.

    Returns an empty dict when ``merged_cpp`` is None or ``max_exemplars`` is 0.
    """
    if merged_cpp is None or max_exemplars <= 0:
        return {}
    lookup: dict[str, list[tuple[str, str]]] = {}
    for call_path_str, metric_vals in merged_cpp.profile.items():
        if metric_vals.exemplars:
            lookup[call_path_str] = metric_vals.exemplars[:max_exemplars]
    return lookup


def _cct_key_to_profile_key(call_path: list[dict[str, str]]) -> str:
    """Convert a parsed CCT call_path list to the ``->``-separated profile key.

    For example ``[{'service': 'svcA', 'operation_name': 'opA'}]`` becomes
    ``"[svcA] opA"``.
    """
    return "->".join(
        f"[{p['service']}] {p['operation_name']}" for p in call_path
    )


def create_protobuf_response_with_exemplars(
    call_chain_summaries: list[dict[str, Any]],
    merged_cpp: Optional[CallPathProfile] = None,
    max_exemplars: int = 3,
) -> analyzer_pb2.AnalyzeResponse:
    """Create a protobuf AnalyzeResponse from CCT summaries with exemplars.

    Duration and frequency values are taken from the CCT summaries (which
    contain the correctly averaged values), while exemplar (trace_id, span_id)
    pairs are grafted from the merged CallPathProfile onto the leaf CallPath
    of each entry.

    Args:
        call_chain_summaries: Parsed CCT summaries from ``parse_cct_file``.
        merged_cpp: Optional merged call-path profile carrying exemplar IDs.
        max_exemplars: Maximum exemplar pairs to attach per leaf call path.

    Returns:
        A populated ``AnalyzeResponse`` protobuf message.
    """
    exemplar_lookup = _build_exemplar_lookup(merged_cpp, max_exemplars)

    response = analyzer_pb2.AnalyzeResponse()
    for summary in call_chain_summaries:
        entry = response.report_window_1.add()
        call_path = summary['call_path']
        for i, path in enumerate(call_path):
            cp = entry.call_path.add()
            cp.service = path['service']
            cp.operation_name = path['operation_name']

            is_leaf = i == len(call_path) - 1
            if is_leaf and exemplar_lookup:
                profile_key = _cct_key_to_profile_key(call_path)
                for trace_id, span_id in exemplar_lookup.get(profile_key, []):
                    ex = cp.exemplars.add()
                    ex.trace_id = str(trace_id)
                    ex.span_id = str(span_id)

        entry.base.duration.FromMicroseconds(summary['duration'])
        entry.base.frequency = summary['frequency']

    if not response.IsInitialized():
        logger.error("Failed to initialize protobuf message")
        return analyzer_pb2.AnalyzeResponse()
    return response
