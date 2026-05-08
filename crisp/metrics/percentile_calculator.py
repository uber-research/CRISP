"""Percentile calculation functions for critical path metrics.

This module contains functions for calculating percentiles and creating
DataFrames with percentile information for trace analysis.
"""

import logging

import pandas as pd

from crisp.shared.constants import TOTAL_TIME
from crisp.shared.models import LatencyData
from crisp.shared.utils import getLeafNodeFromCallPath


def processMetricChunk(chunk, field):
    """Process a chunk of metrics and create a DataFrame with operation times.

    Args:
        chunk: List of Metrics objects to process
        field: Field name to extract from metrics (e.g., 'inc', 'excl')

    Returns:
        DataFrame with traceID as index and operations as columns
    """
    data = []
    index = []  # To store traceID for each row
    for m in chunk:
        d = {TOTAL_TIME: m.latency}
        for k, v in m.CPMetrics.profile.items():
            leaf = getLeafNodeFromCallPath(k)
            if leaf in d:
                d[leaf] += getattr(v, field)
            else:
                d[leaf] = getattr(v, field)
        data.append(d)
        index.append(m.traceID)  # Append traceID to the index list

    # Create DataFrame from the data and set traceID as the index
    dfChunk = pd.DataFrame(data, index=index).fillna(0)
    return dfChunk


def insertInDF(metrics, field):
    """Create a DataFrame from metrics by processing in chunks.

    Args:
        metrics: List of Metrics objects
        field: Field name to extract from metrics (e.g., 'inc', 'excl')

    Returns:
        DataFrame with all metrics processed and concatenated
    """
    # Process metrics in chunks
    chunkSize = 10000
    chunks = [metrics[i : i + chunkSize] for i in range(0, len(metrics), chunkSize)]
    # Process each chunk and concatenate the results
    dfs = [processMetricChunk(chunk, field) for chunk in chunks]

    if len(dfs) > 0:
        return pd.concat(dfs)
    return pd.DataFrame()


def addPercentileColumns(df, percentiles):
    """Add percentile columns to a DataFrame.

    Takes a DataFrame with operations and traces, calculates percentiles
    based on total time, and adds percentile columns.

    Args:
        df: DataFrame with traceID as index and operations as columns
        percentiles: List of Percentile objects

    Returns:
        Transposed DataFrame with percentile columns added
    """
    # Here a data frame looks like this:
    # traceId       Op1     Op2     Op3 totalTime
    #   687216      99      1       30    130
    #   287382      89      2       20    111
    #   79827       90      3       40    133

    columnsToAdd = {}
    for p in percentiles:
        columnsToAdd[p.percentileWithMaxPrefix()] = []
        columnsToAdd[p.percentageWithAvgPrefix()] = []

    dfByTotalTime = df.sort_values(by=[TOTAL_TIME])
    numRows = len(df.index)
    for p in percentiles:
        # Denominator is the sum of all values till the given percentile.
        # For example, P50 of 1000 traces will add the total time of
        # all traces from 0th to 499th.
        endRow = int(numRows * p.percentile)
        denominator = dfByTotalTime[TOTAL_TIME][:endRow].sum()
        for i in dfByTotalTime:
            # Numerator is the sum of all values seen for the given operation sorted by the total time.
            # For example, P50 of 1000 traces will add the the ith operation present in the 0th-499th
            # traces sorted by the total time.
            numerator = dfByTotalTime[i][:endRow].sum()
            worstCase = dfByTotalTime[i][:endRow].max()
            p.pVal[i] = worstCase
            p.pPct[i] = (numerator / denominator) if denominator != 0 else 0
            columnsToAdd[p.percentileWithMaxPrefix()].append(p.pVal[i])
            columnsToAdd[p.percentageWithAvgPrefix()].append(p.pPct[i])

    df = df.transpose()

    # Here a data frame looks like this:
    #      687216 287382 79827
    # op1   ?      ?       ?
    # op2   ?      ?       ?
    # op3   ?      ?       ?

    for i, p in enumerate(percentiles):
        df.insert(
            i,
            p.percentileWithMaxPrefix(),
            columnsToAdd[p.percentileWithMaxPrefix()],
        )

    for i, p in enumerate(percentiles):
        df.insert(
            len(percentiles) + i,
            p.percentageWithAvgPrefix(),
            columnsToAdd[p.percentageWithAvgPrefix()],
        )

    # Here a data frame looks like this:
    #       p50 P95 P99  P50% P95% P99%  687216 287382 79827
    # op1    ?    ?   ?   ?    ?    ?     ?      ?       ?
    # op2    ?    ?   ?   ?    ?    ?     ?      ?       ?
    # op3    ?    ?   ?   ?    ?    ?     ?      ?       ?

    return df


def insertInclusivePercentileInfoDF(df, percentilesInclusive, inclusiveDF):
    """Insert inclusive percentile columns into a DataFrame.

    Args:
        df: DataFrame to insert columns into
        percentilesInclusive: List of Percentile objects for inclusive metrics
        inclusiveDF: DataFrame containing inclusive percentile data

    Returns:
        DataFrame with inclusive percentile columns inserted
    """
    # Insert percentileStr columns.
    for idx, p in enumerate(percentilesInclusive):
        df.insert(
            idx,
            p.percentileWithMaxPrefix(),
            inclusiveDF[p.percentileWithMaxPrefix()],
        )

    # Insert percentileWithPercentSign columns.
    for idx, p in enumerate(percentilesInclusive):
        df.insert(
            len(percentilesInclusive) + idx,
            p.percentageWithAvgPrefix(),
            inclusiveDF[p.percentageWithAvgPrefix()],
        )
    return df


def genLatencyPercentile(latencyHypo, percentiles, sortBy, tailLatency):
    """Generate latency percentile statistics.

    Args:
        latencyHypo: List of LatencyData objects
        percentiles: List of percentile values (e.g., [1, 5, 10, 50, 90, 95, 99, 100])
        sortBy: Function to use for sorting (e.g., lambda x: x.latency)
        tailLatency: If True, calculate tail latency (top percentile), otherwise head latency

    Returns:
        List of tuples (percentile, count, averaged_latency_data)
    """
    latencyPercentile = []
    latencySorted = sorted(latencyHypo, key=sortBy, reverse=False)
    for p in percentiles:
        limit = round(len(latencySorted) * p / 100)
        if limit == 0:
            logging.info("not enough samples for top " + str(p) + " % time saved")
            continue
        if tailLatency:
            start = len(latencySorted) - limit
            sortedList = latencySorted[start:]
        else:
            sortedList = latencySorted[:limit]
        assert len(sortedList) == limit
        latency = sum(sortedList, LatencyData("", 0, 0, 0, 0))
        latency.average(limit)
        latencyPercentile.append((p, limit, latency))

    return latencyPercentile
