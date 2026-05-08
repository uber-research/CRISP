"""Aggregation and merging functions for critical path metrics.

This module contains functions for merging and aggregating call path profiles,
call chains, and metric values across multiple traces.
"""

from __future__ import annotations

import copy
import heapq

from crisp.shared.models import CallPathProfile


def mergeCallChains(callMap, totalCallMap):
    """Merge call chains from callMap into totalCallMap.

    Collects all call chains per operation name from callMap and adds them
    to the corresponding sets in totalCallMap.

    Args:
        callMap: Dictionary mapping operation names to sets of call chains
        totalCallMap: Dictionary to accumulate all call chains (modified in place)
    """
    # Collect all call chains per opName
    for opName in callMap:
        if opName not in totalCallMap:
            totalCallMap[opName] = set()
        for name in callMap[opName]:
            totalCallMap[opName].add(name)


def mergeCallpathTime(callMap, callPathMap, field, totalBreakdownTime):
    """Merge call path times from callPathMap into totalBreakdownTime.

    Collects all call paths and their corresponding time values for the specified
    field attribute.

    Args:
        callMap: Dictionary mapping operation names to lists of call paths
        callPathMap: Dictionary mapping call paths to metric objects
        field: String name of the metric field to extract (e.g., 'excl', 'inc')
        totalBreakdownTime: Dictionary to accumulate time values (modified in place)
    """
    # Collect all call paths and thier corresponding time
    for opName, paths in callMap.items():
        if opName not in totalBreakdownTime:
            totalBreakdownTime[opName] = {}
        for p in paths:
            if p not in totalBreakdownTime[opName]:
                totalBreakdownTime[opName][p] = []
            v = getattr(callPathMap[p], field)
            totalBreakdownTime[opName][p].append(v)


def mergeExampleID(traceID, localExampleMap, exampleMap):
    """Merge example trace IDs, maintaining the worst case example per call path.

    For each operation in localExampleMap, either adds it to exampleMap or updates
    the existing entry if the new example has a higher value.

    Args:
        traceID: ID of the current trace
        localExampleMap: Dictionary mapping operation names to (path, value) tuples
        exampleMap: Dictionary to accumulate examples (modified in place)
                   Maps operation names to (traceID, path, value) tuples
    """
    # Maintain the worst case example per call path.
    for opName in localExampleMap:
        if opName not in exampleMap:
            exampleMap[opName] = (
                traceID,
                localExampleMap[opName][0],
                localExampleMap[opName][1],
            )
        elif localExampleMap[opName][1] > exampleMap[opName][2]:
            exampleMap[opName] = (
                traceID,
                localExampleMap[opName][0],
                localExampleMap[opName][1],
            )


def MergeMetricValsWithTrace(a, _, b, bTrace):
    """Merge metric values from b into a, tracking trace IDs for examples.

    Adds the inclusive, exclusive, and frequency values from b to a.
    Updates trace IDs when b has higher example values.

    Args:
        a: MetricVals object to accumulate into (modified in place)
        _: Unused parameter (kept for backward compatibility)
        b: MetricVals object to merge from
        bTrace: Trace ID associated with metric b

    Returns:
        The modified metric a
    """
    a.inc += b.inc
    a.excl += b.excl
    a.freq += b.freq
    if b.incExVal > a.incExVal:
        a.incEx = b.incEx
        a.incTrace = bTrace
        a.incExVal = b.incExVal
    if b.exclExVal > a.exclExVal:
        a.exclEx = b.exclEx
        a.exclTrace = bTrace
        a.exclExVal = b.exclExVal
    return a


def MergeCallPathProfilesWithExample(metrics):
    """Merge call path profiles from multiple metrics, tracking example traces.

    Delegates to MergeCallPathProfilesWithExemplars with exemplar collection
    disabled. Kept for backward compatibility.

    Args:
        metrics: List of Metrics objects containing CPMetrics.profile dictionaries

    Returns:
        A CallPathProfile object with merged profiles and accumulated count
    """
    return MergeCallPathProfilesWithExemplars(metrics, max_exemplars=0)


def MergeCallPathProfilesWithExemplars(metrics, max_exemplars=3):
    """Merge call path profiles, optionally collecting top exemplars.

    Aggregates all call path profiles from the input metrics into a single
    profile, maintaining example trace IDs for the worst-case inclusive and
    exclusive times for each call path.

    When max_exemplars > 0, also collects the top-N (trace_id, span_id)
    pairs per call path, ranked by exclusive time value, and stores them
    in MetricVals.exemplars.

    Args:
        metrics: List of Metrics objects containing CPMetrics.profile dictionaries
        max_exemplars: Maximum number of exemplars to retain per call path
                       (0 disables exemplar collection)

    Returns:
        A CallPathProfile with merged profiles and per-call-path exemplars
    """
    result = CallPathProfile({}, 0, None)
    exemplar_heaps: dict[str, list[tuple]] = {} if max_exemplars > 0 else None

    for m in metrics:
        for call_path, metric in m.CPMetrics.profile.items():
            if call_path in result.profile:
                MergeMetricValsWithTrace(
                    result.profile[call_path],
                    "NA",
                    metric,
                    m.traceID,
                )
            else:
                result.profile[call_path] = copy.copy(metric)
                result.profile[call_path].exclTrace = m.traceID
                result.profile[call_path].incTrace = m.traceID
                if exemplar_heaps is not None:
                    exemplar_heaps[call_path] = []

            if exemplar_heaps is not None:
                entry = (metric.exclExVal, str(m.traceID), str(metric.exclEx))
                heap = exemplar_heaps[call_path]
                if len(heap) < max_exemplars:
                    heapq.heappush(heap, entry)
                elif entry[0] > heap[0][0]:
                    heapq.heapreplace(heap, entry)

        result.count += m.CPMetrics.count

    if exemplar_heaps is not None:
        for call_path, heap in exemplar_heaps.items():
            sorted_exemplars = sorted(heap, key=lambda x: x[0], reverse=True)
            result.profile[call_path].exemplars = [
                (tid, sid) for _, tid, sid in sorted_exemplars
            ]

    return result
