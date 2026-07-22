"""CSV file generation functions for critical path analysis.

This module contains all functions that generate CSV output files from metrics data.
"""

import logging
import os

import pandas as pd

# Note: Imports that would create circular dependencies are avoided
# These will be imported at runtime as needed


# CSV generation functions moved from process_trace.py

def computeLatencyReduction(df):
    """Compute latency reduction percentage for a single scenario."""
    df["latency_reduction"] = df.apply(
        lambda row: (
            1
            - (
                row["no_err_latency"] / row["observed_latency"]
                if row["observed_latency"] != 0
                else 1
            )
        )
        * 100,
        axis=1,
    )
    df = df.round(2)
    return df


def computeMultipleLatencyReduction(df):
    """Compute latency reduction percentages for multiple scenarios."""
    df["latency_reduction"] = df.apply(
        lambda row: (
            1
            - (
                row["no_err_latency"] / row["observed_latency"]
                if row["observed_latency"] != 0
                else 1
            )
        )
        * 100,
        axis=1,
    )
    df["latency_reduction_optimistic"] = df.apply(
        lambda row: (
            1
            - (
                row["no_err_latency_optimistic"] / row["observed_latency"]
                if row["observed_latency"] != 0
                else 1
            )
        )
        * 100,
        axis=1,
    )
    df["latency_reduction_pessimistic"] = df.apply(
        lambda row: (
            1
            - (
                row["no_err_latency_pessimistic"] / row["observed_latency"]
                if row["observed_latency"] != 0
                else 1
            )
        )
        * 100,
        axis=1,
    )
    df = df.round(2)
    return df


def genEmptyCSVFile(filename, columns):
    """Generate an empty CSV file with the specified column headers."""
    df = pd.DataFrame([], columns=columns)
    df.to_csv(filename, index=False, compression=None)


def genSummaryCSVFile(
    pointLatencyPercentiles,
    headLatencyPercentile,
    tailLatencyPercentile,
    c,  # common.Config - avoiding circular import
):
    """Generate summary CSV file with latency percentiles."""
    # Note: This function needs access to os and logging at runtime

    summaryCSVFile = os.path.join(c.getOutputDir(), "summary.csv")
    logging.info(
        "Producing [%s]%s summary csv file %s",
        c.serviceName,
        c.operationName,
        summaryCSVFile,
    )

    allRows = (
        len(headLatencyPercentile)
        + len(tailLatencyPercentile)
        + len(pointLatencyPercentiles)
    )
    if allRows == 0:
        columns = [
            "percentile",
            "num_traces",
            "observed_latency",
            "no_err_latency",
            "latency_reduction",
            "no_err_latency_optimistic",
            "latency_reduction_optimistic",
            "no_err_latency_pessimistic",
            "latency_reduction_pessimistic",
        ]
        genEmptyCSVFile(summaryCSVFile, columns)
    else:
        columns = [
            "percentile",
            "num_traces",
            "observed_latency",
            "no_err_latency",
            "no_err_latency_optimistic",
            "no_err_latency_pessimistic",
        ]
        res = pd.DataFrame(columns=columns)
        for prefix, lat in [
            ("P", pointLatencyPercentiles),
            ("Head", headLatencyPercentile),
            ("Tail", tailLatencyPercentile),
        ]:
            if not lat:
                continue
            data = []
            for item in lat:
                if len(item) == 3:
                    percentile, count, latencyData = item
                    data.append((
                        prefix + str(percentile),
                        count,
                        latencyData.latency,
                        latencyData.hypoLatency,
                        latencyData.hypoLatencyOptimistic,
                        latencyData.hypoLatencyPessimistic,
                    ))
                else:
                    # Skip malformed items for now
                    continue
            df = pd.DataFrame(data, columns=columns)
            res = pd.concat([res, df], ignore_index=True)

        res = computeMultipleLatencyReduction(res)
        res.to_csv(summaryCSVFile, index=False, compression=None)

    return summaryCSVFile


def genHypoLatencyCSVFile(latencyPercentile, hypoLatencyPercentile, c):
    """Generate hypothetical latency CSV file."""
    # Note: This function needs access to os and logging at runtime

    hypoLatencyCSVFile = os.path.join(c.getOutputDir(), "hypoLatency.csv")
    logging.info(
        "Producing [%s]%s hypo latency csv file %s",
        c.serviceName,
        c.operationName,
        hypoLatencyCSVFile,
    )

    if len(hypoLatencyPercentile) == 0:
        columns = [
            "percentile",
            "num_traces",
            "observed_latency",
            "no_err_latency",
            "latency_reduction",
        ]
        genEmptyCSVFile(hypoLatencyCSVFile, columns)

    else:
        # extract the current latency from latencyPrecentile
        df1 = pd.DataFrame(
            latencyPercentile,
            columns=["percentile", "num_traces", "latency"],
        )
        df1["observed_latency"] = df1.apply(lambda row: row["latency"].latency, axis=1)
        df1 = df1.drop(columns=["latency"])

        # extract the hypo latency from hypoLatencyPrecentile
        df2 = pd.DataFrame(
            hypoLatencyPercentile,
            columns=["percentile", "num_traces", "latency"],
        )
        df2["no_err_latency"] = df2.apply(
            lambda row: row["latency"].hypoLatency,
            axis=1,
        )
        df2 = df2.drop(columns=["latency"])

        res = pd.merge(df1, df2, on=["percentile", "num_traces"])
        res = computeLatencyReduction(res)
        res.to_csv(hypoLatencyCSVFile, index=False, compression=None)

    return hypoLatencyCSVFile


def genCyclesCSVFile(metrics, c, filename=None):
    """Generate cycles CSV file."""
    # Note: This function needs access to os and logging at runtime

    outputDir = c.getOutputDir()
    cyclesCSVFile = os.path.join(outputDir, filename or "cycles.csv")

    cycles = [(metric.traceID, metric.cycles) for metric in metrics if metric.cycles]
    columns = ["traceID", "spanID", "callPath"]
    if cycles:
        logging.info(
            "Producing [%s]%s cycles csv file %s",
            c.serviceName,
            c.operationName,
            cyclesCSVFile,
        )
        rows = []
        for (traceIDWithPath, cycleMap) in cycles:
            traceID = traceIDWithPath.split("/")[-1]
            for spanID, callPathInList in cycleMap.items():
                callPath = ",".join(callPathInList)
                rows.append([traceID, spanID, callPath])
        df = pd.DataFrame(rows, columns=columns)
        df.to_csv(cyclesCSVFile, index=False)
    else:
        logging.info('No cycles found')
        return None

    return cyclesCSVFile


def genSlackDragCSVFile(perMethodSlackDrag, c, filename=None):
    """Generate per-call-path Drag/Slack CSV file, sorted by descending average drag."""
    outputDir = c.getOutputDir()
    slackDragCSVFile = os.path.join(outputDir, filename or "slackDrag.csv")
    columns = ["callPath", "spanCount", "avgDrag", "totalDrag", "avgSlack", "totalSlack"]

    if not perMethodSlackDrag:
        logging.info("No slack/drag data found")
        return None

    logging.info(
        "Producing [%s]%s slack/drag csv file %s",
        c.serviceName,
        c.operationName,
        slackDragCSVFile,
    )
    rows = [
        [
            agg.call_path,
            agg.span_count,
            agg.avg_drag,
            agg.total_drag,
            agg.avg_slack,
            agg.total_slack,
        ]
        for agg in perMethodSlackDrag.values()
    ]
    df = pd.DataFrame(rows, columns=columns)
    df = df.sort_values(by="avgDrag", ascending=False).reset_index(drop=True)
    df.to_csv(slackDragCSVFile, index=False)

    return slackDragCSVFile


def genCrossRegionCallsCSVFile(metrics, c, filename=None):
    """Generate cross-region calls CSV file."""
    # Note: This function needs access to os and logging at runtime

    outputDir = c.getOutputDir()
    crossRegionCallsCSVFile = os.path.join(outputDir, filename or "crossRegionCalls.csv")
    crossRegionCallsData = [(metric.traceID, metric.crossRegionCalls) for metric in metrics if metric.crossRegionCalls]
    columns = ["traceID", "parentSpanId", "childSpanId", "operationName",
               "parentRegion", "childRegion", "parentService", "childService",
               "parentDuration", "childDuration", "durationRatio", "callPath"]

    if crossRegionCallsData:
        logging.info(
            "Producing [%s]%s cross-region calls csv file %s",
            c.serviceName,
            c.operationName,
            crossRegionCallsCSVFile,
        )
        rows = []
        for (traceIDWithPath, crossRegionCallsMap) in crossRegionCallsData:
            traceID = traceIDWithPath.split("/")[-1]
            rows.extend([
                traceID,
                callData['parentSpanId'],
                callData['childSpanId'],
                callData['operationName'],
                callData['parentRegion'],
                callData['childRegion'],
                callData['parentService'],
                callData['childService'],
                callData['parentDuration'],
                callData['childDuration'],
                callData['durationRatio'],
                callData['callPath']
            ] for callData in crossRegionCallsMap.values())
        df = pd.DataFrame(rows, columns=columns)
        df.to_csv(crossRegionCallsCSVFile, index=False)
    else:
        logging.info('No cross-region calls found')
        return None
    return crossRegionCallsCSVFile
