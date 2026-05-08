"""DataFrame and HTML formatting utilities for critical path analysis."""

import logging

from crisp.shared.constants import (
    JAEGER_UI_URL,
    SORTABLE_COL_CLASS,
    TOTAL_TIME,
)


def makeClickable(url, name):
    """Create an HTML link with target="_blank"."""
    return f'<a href="{url}" rel="noopener noreferrer" target="_blank">{name}</a>'


def sortableColHeader(name):
    """Add sortable icon to column header."""
    return name + SORTABLE_COL_CLASS


def addHyperLinkToTrace(df, tracespanIDmap):
    """Make each trace column header navigatable to Jaeger UI."""
    hyperLinkHT = {}
    for k, v in tracespanIDmap.items():
        hyperLinkHT[k] = makeClickable(f"{JAEGER_UI_URL}{k}?uiFind={v}", "#")
    df.rename(columns=hyperLinkHT, inplace=True)
    return df


def renameSortableIcon(df, columns):
    """Use fas fa-sort script to make columns sortable."""
    sortableRenameHT = {}
    for col in columns:
        sortableRenameHT[col] = sortableColHeader(col)

    df.rename(columns=sortableRenameHT, inplace=True)
    return df


def insertOccurenceCol(df, jaegerTraceFiles, nonZeros):
    """Insert one column that counts the number of times the operation is seen on the critical path."""
    occurenceColHeader = f"occurence ({len(jaegerTraceFiles)})"
    # Initialize with 0 (not "") so the column gets numeric dtype; the loop
    # below assigns ints, and pandas >=2.x rejects int assignment into a
    # str-inferred column. End state is unchanged (every cell is overwritten).
    df.insert(0, occurenceColHeader, 0)
    for i in range(len(df)):
        df.at[df.index[i], f"occurence ({len(jaegerTraceFiles)})"] = int(nonZeros[i])
    return df, occurenceColHeader


def reindexDescending(df, prefixColumns, traceIDIndex):
    """Sort rows by the descending sum of values in traceIDIndex columns."""
    df_sorted_by_row = df.reindex(
        df[traceIDIndex].sum(axis=1).sort_values(ascending=False).index,
    )

    logging.info("reindexDescending df.loc start")
    # Sort traceIDIndex columns by descending values in the TOTAL_TIME row
    traceIDSorted = (
        df.loc[TOTAL_TIME, traceIDIndex]
        .sort_values(ascending=False)
        .index.tolist()
    )

    # Combine the sorted columns with prefixColumns and reindex the DataFrame
    return df_sorted_by_row.reindex(columns=prefixColumns + traceIDSorted)


def setCellFormating(df, percentiles, occurenceColHeader):
    """Set cell formatting precision for different column types."""
    precisionHT = {}
    for i in df.columns.values:
        # All columns except percentiles with % sign will be in scientific.
        precisionHT[i] = "{:.2e}"

    for p in percentiles:
        precisionHT[sortableColHeader(p.percentageWithAvgPrefix())] = "{:.2f}"

    # occurence column will be in decimal
    precisionHT[sortableColHeader(occurenceColHeader)] = "{:5d}"
    return precisionHT
