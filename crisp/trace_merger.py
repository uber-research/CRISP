"""
Trace Merger for Split Jaeger Traces

PROBLEM:
--------
Large distributed traces can be split by Jaeger into multiple trace files due to size limits, particularly for large traces.
When traces are split, the child trace is created as a separate trace with its own traceID,
and the connection to the parent trace is maintained via a FOLLOWS_FROM reference
(it's incorrect and should be CHILD_OF, Jaeger side did this for UI hacks).

Example:
- Parent trace: aabbccdd11223344.json (810 spans)
  - Contains span aabbccdd00000001: "[action] appLaunch"
    - Child span aabbccdd00000002: "TraceSplitPoint" (marks split point)

- Child trace: 1122334455667788.json (50,000 spans)
  - Root span 1122334455667788: "[subaction] appLaunch"
    - Has FOLLOWS_FROM reference to parent trace aabbccdd11223344, span aabbccdd00000002

GRAPH.PY LIMITATION:
--------------------
The graph.py parseNode() method (lines 651-656) only processes CHILD_OF references:

    for parent in span[REFERENCES]:
        if parent[REF_TYPE] == CHILD_OF:
            parentSpanId = parent[SPAN_ID]

This means:
1. FOLLOWS_FROM references are ignored
2. Cross-trace references (different traceIDs) are not handled
3. Split traces cannot be analyzed as a unified call graph

SOLUTION:
---------
This module merges split traces by:
1. Finding FOLLOWS_FROM references from child to parent trace
2. Combining all spans from both traces
3. Updating all child span traceIDs to match parent
4. Converting FOLLOWS_FROM to CHILD_OF for the cross-trace link
5. Merging process dictionaries
6. Preserving all span properties and relationships

After merging, graph.py can process the combined trace as a single unified trace.

RECOMMENDED USAGE:
------------------
Use create_merged_graph() which creates a Graph object directly (no intermediate files):

    from crisp.trace_merger import create_merged_graph

    # Single child
    graph = create_merged_graph(
        parent_trace_path='parent.json',
        child_trace_paths='child.json',
        serviceName='my-service',
        operationName='my-operation'
    )

    # Multiple children (optimized - parent loaded only once)
    graph = create_merged_graph(
        parent_trace_path='parent.json',
        child_trace_paths=['child1.json', 'child2.json', 'child3.json'],
        serviceName='my-service',
        operationName='my-operation'
    )

    # Use immediately
    critical_path = graph.findCriticalPath()
    metrics = graph.computeTimeSaved()

ALTERNATIVE USAGE:
------------------
For cases where you need the merged data structure (not Graph):

    from crisp.trace_merger import (
        load_and_merge_traces,
        merge_trace_data,
        merge_multiple_child_traces
    )

    # Load and merge from files
    merged_data = load_and_merge_traces('parent.json', 'child.json')
    # Or multiple children
    merged_data = load_and_merge_traces('parent.json', ['child1.json', 'child2.json'])

    # Or merge already-loaded data
    merged_data = merge_trace_data(parent_data, child_data)
    # Or multiple children (optimized)
    merged_data = merge_multiple_child_traces(parent_data, [child1_data, child2_data])

MULTI-CHILD OPTIMIZATION:
--------------------------
When merging one parent with multiple children, use the list form for better performance:

    # ❌ Inefficient (loads parent N times):
    merged = merge_trace_data(parent, child1)
    merged = merge_trace_data(merged, child2)  # Re-processes parent!
    merged = merge_trace_data(merged, child3)  # Re-processes parent + child1!

    # ✅ Efficient (loads parent once):
    graph = create_merged_graph('parent.json', ['child1.json', 'child2.json', 'child3.json'], ...)

DETECT SPLIT TRACES:
--------------------
Check if a trace has external references (indicating it's part of a split):

    from crisp.trace_merger import identify_split_traces
    import json

    with open('trace.json') as f:
        data = json.load(f)

    external_refs = identify_split_traces(data)
    if external_refs:
        print(f"Found {len(external_refs)} external trace references")
        for ref in external_refs:
            print(f"  Span {ref['span_id']} -> Trace {ref['referenced_trace_id']}")

VALIDATION:
-----------
The merger validates:
- Child trace has FOLLOWS_FROM reference to parent trace
- Referenced parent span exists
- Span ID collisions (warns if found)
- Process ID collisions (renumbers child processes to avoid conflicts)

PROCESS MERGING:
----------------
When merging processes, the merger handles collisions intelligently:
- If process IDs collide but definitions are identical: reuses existing ID
- If process IDs collide with different definitions: renumbers child process and updates all child spans
- Example: Parent p1=service-a, Child p1=service-b → Child renumbered to p231, all child spans updated

This ensures all service names are preserved correctly in the merged trace.

DESIGN DECISION:
----------------
We chose to merge traces BEFORE creating Graph objects rather than modifying graph.py
to handle split traces directly because:
1. Simpler implementation - no changes to core graph.py logic
2. Better separation of concerns - merging is pre-processing
3. Easier to test and validate
4. No intermediate JSON files needed - works in-memory
5. One-time cost - Graph creation happens once, merge overhead acceptable
"""

import json
import logging
import copy
from typing import Optional, Union

from crisp.graph import Graph

logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)


class TraceMergeError(Exception):
    """Exception raised when trace merge fails validation."""


def find_cross_trace_link(child_trace_data: dict, parent_trace_id: str) -> Optional[dict]:
    """
    Find the FOLLOWS_FROM reference from child trace to parent trace.

    Args:
        child_trace_data: Child trace JSON data
        parent_trace_id: Expected parent trace ID

    Returns:
        Dict with cross-trace link info, or None if not found:
        {
            'child_span_id': str,
            'parent_span_id': str,
            'ref_type': str,
            'child_operation': str
        }
    """
    child_trace = child_trace_data['data'][0]

    candidates = [
        {
            'child_span_id': span['spanID'],
            'parent_span_id': ref['spanID'],
            'ref_type': ref['refType'],
            'child_operation': span['operationName'],
        }
        for span in child_trace['spans']
        for ref in span.get('references', [])
        if ref.get('traceID') == parent_trace_id and ref.get('refType') == 'FOLLOWS_FROM'
    ]

    return candidates[0] if candidates else None


def validate_merge_preconditions(parent_trace_data: dict, child_trace_data: dict, link: dict) -> None:
    """
    Validate that merge can proceed safely.

    Args:
        parent_trace_data: Parent trace JSON data
        child_trace_data: Child trace JSON data
        link: Cross-trace link info from find_cross_trace_link()

    Raises:
        TraceMergeError: If validation fails
    """
    parent_trace = parent_trace_data['data'][0]
    child_trace = child_trace_data['data'][0]

    # Check referenced parent span exists
    parent_span_ids = {span['spanID'] for span in parent_trace['spans']}
    if link['parent_span_id'] not in parent_span_ids:
        raise TraceMergeError(
            f"Parent span {link['parent_span_id']} referenced by child trace "
            f"not found in parent trace {parent_trace['traceID']}"
        )

    # Check for span ID collisions (warn only)
    child_span_ids = {span['spanID'] for span in child_trace['spans']}
    collisions = parent_span_ids & child_span_ids
    if collisions:
        logging.warning(
            f"Found {len(collisions)} span ID collisions between parent and child traces: {collisions}"
        )


def merge_trace_data(parent_trace_data: dict, child_trace_data: dict) -> dict:
    """
    Merge a child trace into a parent trace.

    Args:
        parent_trace_data: Parent trace JSON data
        child_trace_data: Child trace JSON data

    Returns:
        Merged trace data with all spans combined

    Raises:
        TraceMergeError: If merge validation fails
    """
    # Deep copy to avoid modifying originals
    merged_data = copy.deepcopy(parent_trace_data)
    parent_trace = merged_data['data'][0]
    parent_trace_id = parent_trace['traceID']
    child_trace = child_trace_data['data'][0]
    child_trace_id = child_trace['traceID']

    # Find cross-trace link
    link = find_cross_trace_link(child_trace_data, parent_trace_id)
    if not link:
        raise TraceMergeError(
            f"No FOLLOWS_FROM reference found from child trace {child_trace_id} "
            f"to parent trace {parent_trace_id}"
        )

    # Validate merge preconditions
    validate_merge_preconditions(parent_trace_data, child_trace_data, link)

    logging.info(
        f"Merging child trace {child_trace_id} ({len(child_trace['spans'])} spans) "
        f"into parent trace {parent_trace_id} ({len(parent_trace['spans'])} spans)"
    )

    # Handle process ID collisions by renumbering child processes
    max_parent_process_id = max([int(pid[1:]) for pid in parent_trace['processes'].keys()])
    process_id_mapping = {}

    for process_id, process_data in child_trace.get('processes', {}).items():
        if process_id in parent_trace['processes']:
            # Check if they're actually the same (avoid unnecessary renumbering)
            if parent_trace['processes'][process_id] == process_data:
                # Identical definition, no need to renumber
                process_id_mapping[process_id] = process_id
            else:
                # Collision with different data - assign new ID
                new_id = f'p{max_parent_process_id + 1}'
                max_parent_process_id += 1
                process_id_mapping[process_id] = new_id
                parent_trace['processes'][new_id] = process_data
                logging.info(
                    f"Process ID collision: {process_id} "
                    f"(parent: {parent_trace['processes'][process_id]['serviceName']}, "
                    f"child: {process_data['serviceName']}) - renumbered to {new_id}"
                )
        else:
            # No collision, use original ID
            process_id_mapping[process_id] = process_id
            parent_trace['processes'][process_id] = process_data

    # Copy and update child spans
    for span in child_trace['spans']:
        # Create a copy of the span
        merged_span = copy.deepcopy(span)

        # Update traceID for all child spans
        merged_span['traceID'] = parent_trace_id

        # Update processID if it was remapped
        if merged_span['processID'] in process_id_mapping:
            merged_span['processID'] = process_id_mapping[merged_span['processID']]

        # Update references to use parent trace ID
        for ref in merged_span.get('references', []):
            if ref['traceID'] == child_trace_id:
                ref['traceID'] = parent_trace_id

            # Convert FOLLOWS_FROM to CHILD_OF for the cross-trace link
            if (merged_span['spanID'] == link['child_span_id'] and
                ref['spanID'] == link['parent_span_id'] and
                ref['refType'] == 'FOLLOWS_FROM'):
                ref['refType'] = 'CHILD_OF'
                logging.info(
                    f"Converted FOLLOWS_FROM to CHILD_OF for cross-trace link: "
                    f"{link['child_span_id']} -> {link['parent_span_id']}"
                )

        parent_trace['spans'].append(merged_span)

    logging.info(
        f"Merge complete: {len(parent_trace['spans'])} total spans, "
        f"{len(parent_trace['processes'])} processes"
    )

    return merged_data


def merge_multiple_child_traces(parent_trace_data: dict, child_trace_data_list: list[dict]) -> dict:
    """
    Merge multiple child traces into a parent trace (optimized).

    This is more efficient than calling merge_trace_data() sequentially because
    it only processes the parent trace once.

    Args:
        parent_trace_data: Parent trace JSON data
        child_trace_data_list: List of child trace JSON data

    Returns:
        Merged trace data with all child traces combined

    Raises:
        TraceMergeError: If any merge validation fails
    """
    if not child_trace_data_list:
        return copy.deepcopy(parent_trace_data)

    # Start with parent trace
    merged_data = copy.deepcopy(parent_trace_data)

    # Merge each child trace
    for i, child_trace_data in enumerate(child_trace_data_list):
        logging.info(f"Merging child trace {i+1}/{len(child_trace_data_list)}")

        parent_trace = merged_data['data'][0]
        parent_trace_id = parent_trace['traceID']
        child_trace = child_trace_data['data'][0]
        child_trace_id = child_trace['traceID']

        # Find cross-trace link
        link = find_cross_trace_link(child_trace_data, parent_trace_id)
        if not link:
            raise TraceMergeError(
                f"No FOLLOWS_FROM reference found from child trace {child_trace_id} "
                f"to parent trace {parent_trace_id}"
            )

        # Validate merge preconditions
        validate_merge_preconditions(merged_data, child_trace_data, link)

        logging.info(
            f"Merging child trace {child_trace_id} ({len(child_trace['spans'])} spans) "
            f"into parent trace {parent_trace_id} (currently {len(parent_trace['spans'])} spans)"
        )

        # Handle process ID collisions by renumbering child processes
        max_parent_process_id = max([int(pid[1:]) for pid in parent_trace['processes'].keys()])
        process_id_mapping = {}

        for process_id, process_data in child_trace.get('processes', {}).items():
            if process_id in parent_trace['processes']:
                # Check if they're actually the same (avoid unnecessary renumbering)
                if parent_trace['processes'][process_id] == process_data:
                    # Identical definition, no need to renumber
                    process_id_mapping[process_id] = process_id
                else:
                    # Collision with different data - assign new ID
                    new_id = f'p{max_parent_process_id + 1}'
                    max_parent_process_id += 1
                    process_id_mapping[process_id] = new_id
                    parent_trace['processes'][new_id] = process_data
                    logging.info(
                        f"Process ID collision: {process_id} "
                        f"(parent: {parent_trace['processes'][process_id]['serviceName']}, "
                        f"child: {process_data['serviceName']}) - renumbered to {new_id}"
                    )
            else:
                # No collision, use original ID
                process_id_mapping[process_id] = process_id
                parent_trace['processes'][process_id] = process_data

        # Copy and update child spans
        for span in child_trace['spans']:
            merged_span = copy.deepcopy(span)
            merged_span['traceID'] = parent_trace_id

            # Update processID if it was remapped
            if merged_span['processID'] in process_id_mapping:
                merged_span['processID'] = process_id_mapping[merged_span['processID']]

            # Update references
            for ref in merged_span.get('references', []):
                if ref['traceID'] == child_trace_id:
                    ref['traceID'] = parent_trace_id

                # Convert FOLLOWS_FROM to CHILD_OF for cross-trace link
                if (merged_span['spanID'] == link['child_span_id'] and
                    ref['spanID'] == link['parent_span_id'] and
                    ref['refType'] == 'FOLLOWS_FROM'):
                    ref['refType'] = 'CHILD_OF'
                    logging.info(
                        f"Converted FOLLOWS_FROM to CHILD_OF for cross-trace link: "
                        f"{link['child_span_id']} -> {link['parent_span_id']}"
                    )

            parent_trace['spans'].append(merged_span)

    final_span_count = len(merged_data['data'][0]['spans'])
    final_process_count = len(merged_data['data'][0]['processes'])
    logging.info(
        f"Multi-child merge complete: {final_span_count} total spans, "
        f"{final_process_count} processes"
    )

    return merged_data


def load_and_merge_traces(
    parent_trace_path: str,
    child_trace_paths: Union[str, list[str]]
) -> dict:
    """
    Load and merge trace files.

    Args:
        parent_trace_path: Path to parent trace JSON file
        child_trace_paths: Path to child trace JSON file, or list of paths for multiple children

    Returns:
        Merged trace data

    Raises:
        FileNotFoundError: If any trace file not found
        TraceMergeError: If merge validation fails
    """
    # Load parent trace
    with open(parent_trace_path) as f:
        parent_data = json.load(f)

    # Handle single child or multiple children
    if isinstance(child_trace_paths, str):
        # Single child
        with open(child_trace_paths) as f:
            child_data = json.load(f)
        return merge_trace_data(parent_data, child_data)
    else:
        # Multiple children
        child_data_list = []
        for child_path in child_trace_paths:
            with open(child_path) as f:
                child_data_list.append(json.load(f))
        return merge_multiple_child_traces(parent_data, child_data_list)


def create_merged_graph(
    parent_trace_path: str,
    child_trace_paths: Union[str, list[str]],
    serviceName: str,
    operationName: str,
    rootTrace: bool = True,
    filterProxy: bool = True,
    tags: Optional[list] = None,
    exclusionSet: Optional[dict] = None,
) -> Graph:
    """
    Create a Graph object from merged parent and child traces (RECOMMENDED).

    This is the main API for working with split traces. It merges the traces
    in-memory and creates a Graph object directly, without writing intermediate files.

    Args:
        parent_trace_path: Path to parent trace JSON file
        child_trace_paths: Path to child trace JSON file, or list of paths for multiple children
        serviceName: Service name for root node selection
        operationName: Operation name for root node selection
        rootTrace: If True, use first span as root; if False, search for matching node
        filterProxy: Whether to filter proxy spans
        tags: List of tags for filtering
        exclusionSet: Set of operations to exclude

    Returns:
        Graph object with merged trace data

    Raises:
        FileNotFoundError: If any trace file not found
        TraceMergeError: If merge validation fails

    Example:
        # Single child
        graph = create_merged_graph(
            'parent.json',
            'child.json',
            serviceName='my-service',
            operationName='my-operation'
        )

        # Multiple children (optimized)
        graph = create_merged_graph(
            'parent.json',
            ['child1.json', 'child2.json', 'child3.json'],
            serviceName='my-service',
            operationName='my-operation'
        )
    """
    # Load and merge traces
    merged_data = load_and_merge_traces(parent_trace_path, child_trace_paths)

    # Create Graph from merged data
    graph = Graph(
        data=merged_data,
        serviceName=serviceName,
        operationName=operationName,
        filename='merged_trace.json',  # Dummy filename for Graph
        rootTrace=rootTrace,
        filterProxy=filterProxy,
        tags=tags or [],
        exclusionSet=exclusionSet or {},
    )

    return graph


def identify_split_traces(trace_data: dict) -> list[dict]:
    """
    Identify if a trace has external references (indicating it's part of a split).

    Args:
        trace_data: Trace JSON data

    Returns:
        List of external trace references, each with:
        {
            'span_id': str,
            'referenced_trace_id': str,
            'referenced_span_id': str,
            'ref_type': str
        }
    """
    trace = trace_data['data'][0]
    trace_id = trace['traceID']
    external_refs = []

    for span in trace['spans']:
        for ref in span.get('references', []):
            ref_trace_id = ref.get('traceID')
            if ref_trace_id and ref_trace_id != trace_id:
                external_refs.append({
                    'span_id': span['spanID'],
                    'referenced_trace_id': ref_trace_id,
                    'referenced_span_id': ref['spanID'],
                    'ref_type': ref.get('refType', 'UNKNOWN'),
                })

    return external_refs
