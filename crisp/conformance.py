"""Deterministic conformance outputs for cross-implementation testing.

Writes conformance.cct (byte-sorted folded-stack CCT; compare byte-wise) and
conformance.json (canonical JSON of the AnalyzeResponse built from the sorted
CCT; compare parsed). See CONFORMANCE.md for the contract.
"""
import json
import os
from typing import Any, Optional

from google.protobuf import json_format

from crisp.cct_utils import create_protobuf_response_with_exemplars, parse_cct_line
from crisp.shared.models import CallPathProfile

CONFORMANCE_CCT_FILE = "conformance.cct"
CONFORMANCE_JSON_FILE = "conformance.json"


def canonical_cct(flame_graph_str: str) -> str:
    """Sort folded-stack lines by code point (identical to UTF-8 byte order)."""
    lines = [line for line in flame_graph_str.split("\n") if line]
    lines.sort()
    if not lines:
        return ""
    return "\n".join(lines) + "\n"


def canonical_response_json(response) -> str:
    """Render an AnalyzeResponse as canonical JSON (sorted keys, 2-space indent)."""
    d = json_format.MessageToDict(response, preserving_proto_field_name=True)
    return json.dumps(d, sort_keys=True, indent=2, ensure_ascii=False) + "\n"


def build_conformance_response(
    canonical_cct_str: str,
    merged_cpp: Optional[CallPathProfile],
    max_exemplars: int,
):
    """Build an AnalyzeResponse whose entries follow the sorted CCT line order."""
    summaries = []
    for line in canonical_cct_str.split("\n"):
        summary = parse_cct_line(line)
        if summary:
            summaries.append(summary)
    return create_protobuf_response_with_exemplars(summaries, merged_cpp, max_exemplars)


def write_conformance_outputs(
    output_dir: str,
    flame_graph_str: str,
    merged_cpp: Optional[CallPathProfile],
    max_exemplars: int = 3,
) -> tuple[str, str]:
    """Write conformance.cct and conformance.json; return their paths."""
    cct_str = canonical_cct(flame_graph_str)
    cct_path = os.path.join(output_dir, CONFORMANCE_CCT_FILE)
    with open(cct_path, "w", encoding="utf-8") as f:
        f.write(cct_str)

    response = build_conformance_response(cct_str, merged_cpp, max_exemplars)
    json_path = os.path.join(output_dir, CONFORMANCE_JSON_FILE)
    with open(json_path, "w", encoding="utf-8") as f:
        f.write(canonical_response_json(response))
    return cct_path, json_path


def derive_root_span(trace: dict[str, Any]) -> tuple[str, str]:
    """Return (serviceName, operationName) of the span with no in-trace CHILD_OF
    parent; ties broken by earliest startTime, then spanID."""
    data = trace["data"][0]
    spans = data["spans"]
    processes = data["processes"]
    span_ids = {s["spanID"] for s in spans}

    candidates = []
    for s in spans:
        refs = s.get("references") or []
        has_parent = any(
            r.get("refType") == "CHILD_OF" and r.get("spanID") in span_ids
            for r in refs
        )
        if not has_parent:
            candidates.append(s)
    if not candidates:
        raise ValueError("no root span found in trace")

    root = min(candidates, key=lambda s: (s["startTime"], s["spanID"]))
    service = processes[root["processID"]]["serviceName"]
    return service, root["operationName"]
