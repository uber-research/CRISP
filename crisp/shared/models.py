"""Shared data models used across critical path analysis modules.

This module contains core data structures that are shared between
multiple modules to avoid circular dependencies.
"""

from __future__ import annotations

import copy
import logging
from enum import Enum
from typing import Any

from crisp.shared.constants import (
    DEFAULT_MAX_DEPTH,
    DEFAULT_MIN_DEPTH,
    DEFAULT_PROPAGATED_ERRORS,
    DEFAULT_SELF_ERRORS,
    DEFAULT_STOPPED_ERRORS,
    PERCENTILE_50,
    PERCENTILE_90,
    PERCENTILE_95,
    PERCENTILE_99,
    SpanKindValues,
)
from crisp.utils.dict_utils import getCPSize


class MetricVals:
    """Container for metric values with inclusive and exclusive times.

    Tracks inclusive time, exclusive time, frequency, and example span IDs
    for the worst-case inclusive and exclusive measurements.
    """

    def __init__(self, inc, excl, freq, sid, exemplars: list[tuple[str, str]] | None = None):
        self.inc = inc
        self.excl = excl
        self.freq = freq
        self.incExVal, self.incEx = inc, sid
        self.exclExVal, self.exclEx = excl, sid
        self.exemplars: list[tuple[str, str]] = exemplars if exemplars is not None else []

    def __str__(self) -> str:
        return f"self.inc={self.inc}, self.excl={self.excl}, self.freq={self.freq}, self.incEx={self.incEx}, self.exclEx={self.exclEx}"

    # Adds the values of metrics ignoring the sid.
    def __add__(self, other):
        result = MetricVals(0, 0, 0, -1)
        result += self
        result += other
        return result

    # Adds the values of metrics ignoring the sid.
    def __iadd__(self, metric):
        self.inc += metric.inc
        self.excl += metric.excl
        self.freq += metric.freq
        if metric.incExVal > self.incExVal:
            self.incEx = metric.incEx
            self.incExVal = metric.incExVal
        if metric.exclExVal > self.exclExVal:
            self.exclEx = metric.exclEx
            self.exclExVal = metric.exclExVal
        return self

    # Divides each metric value by val. Ignores the sids.
    def __floordiv__(self, val):
        result = MetricVals(0, 0, 0, -1)
        result.inc = self.inc
        result.excl = self.excl
        result.freq = self.freq
        result //= val
        return result

    # Divides each metric value by val. Ignores the sids.
    def __ifloordiv__(self, val):
        self.inc //= val
        self.excl //= val
        self.freq //= val
        return self


class CallPathProfile:
    """Profile containing metrics for multiple call paths.

    Aggregates metric values across multiple call paths, tracking
    the total count of profiles merged and example trace IDs.
    """

    def __init__(self, kv: dict[str, Any], count: int, traceId):
        self.profile = kv
        self.count = count
        self.traceId = traceId

    def Get(self):
        return self.profile

    # Returns the profile metrics divided by the count. Ignores SID.
    def GetNormalized(self):
        if self.count == 0:
            raise Exception("GetNormalized called with zero count.")  # noqa: TRY002
        result = {}
        for k, v in self.profile.items():
            result[k] = v // self.count
        return result

    # Divides profile metrics by the count. Ignores SID.
    def Normalize(self):
        if self.count == 0:
            raise Exception("Normalize called with zero count.")  # noqa: TRY002
        for k in self.profile:
            self.profile[k] = self.profile[k] // self.count

    # Divides the specified field of the metrics in profiles by the count.
    def NormalizeField(self, field):
        if self.count == 0:
            raise Exception("NormalizeField called with zero count.")  # noqa: TRY002
        for k in self.profile:
            v = getattr(self.profile[k], field) // self.count
            setattr(self.profile[k], field, v)

    # Updates (via addition) or inserts a new callpath metric without changing the count.
    def Upsert(self, path, metric):
        if path in self.profile:
            self.profile[path] += metric
        else:
            self.profile[path] = copy.copy(metric)

    # Makes the field zero if it is negative.
    def Sanitize(self, field, debug_on):
        for k, v in self.profile.items():
            if hasattr(v, field):
                if getattr(v, field) < 0:
                    # NOTE: self.filename is not set by __init__; this debug
                    # branch mirrors the internal source verbatim and would
                    # AttributeError if reached with debug_on=True. Fixing
                    # is out of scope for this port; tracked for a follow-up.
                    debug_on and logging.debug(
                        f"In {self.filename} zero out time entry for key {k} field {field}",
                    )
                    setattr(self.profile[k], field, 0)

    # Merges two call path profiles returning the summation. The SIDs are not propagated.
    def __add__(self, other):
        result = CallPathProfile({}, 0, -1)
        result += self
        result += other
        return result

    # Merges two call path profiles returning the summation. The SIDs are not propagated.
    def __iadd__(self, other):
        for call_path, metric in other.profile.items():
            self.Upsert(call_path, metric)
        self.count += other.count
        return self

    def __repr__(self):
        return str(self.profile)


class LatencyData:
    """Data structure for storing latency measurements and hypothetical scenarios."""

    def __init__(
        self,
        traceID,
        latency,
        hypoLatency,
        hypoLatencyOptimistic,
        hypoLatencyPessimistic,
    ):
        self.traceID = traceID
        self.latency = latency
        self.hypoLatency = hypoLatency
        self.hypoLatencyOptimistic = hypoLatencyOptimistic
        self.hypoLatencyPessimistic = hypoLatencyPessimistic

    def __str__(self):
        return f"({self.traceID},{self.latency}.{self.hypoLatency})"

    def __add__(self, other):
        latency = self.latency + other.latency
        hypo = self.hypoLatency + other.hypoLatency
        opt = self.hypoLatencyOptimistic + other.hypoLatencyOptimistic
        pes = self.hypoLatencyPessimistic + other.hypoLatencyPessimistic
        return LatencyData("", latency, hypo, opt, pes)

    def __radd__(self, other):
        return self.__add__(other)

    def average(self, count):
        self.traceID = ""
        self.latency = self.latency / count
        self.hypoLatency = self.hypoLatency / count
        self.hypoLatencyOptimistic = self.hypoLatencyOptimistic / count
        self.hypoLatencyPessimistic = self.hypoLatencyPessimistic / count


class QuantizedMetrics:
    headers: tuple[str, ...] = (
        "items",
        "p0",
        "p100",
        "avg",
        "p50",
        "p90",
        "p95",
        "p99",
    )

    def __init__(self, histo: dict[int, int]):
        self.p100 = None
        self.p0 = None
        self.items = None
        self.avg = None
        self.p50 = None
        self.p90 = None
        self.p95 = None
        self.p99 = None
        self.isValid = False
        if len(histo) == 0:
            return

        all = []
        maxDepth = DEFAULT_MAX_DEPTH
        minDepth = DEFAULT_MIN_DEPTH
        sum = 0
        items = 0
        for k, v in histo.items():
            all.extend([k] * v)
            items += v
            sum += k * v
            minDepth = min(minDepth, k)
            maxDepth = max(maxDepth, k)

        avg = sum / items
        p50 = all[int(len(all) * PERCENTILE_50)]
        p90 = all[int(len(all) * PERCENTILE_90)]
        p95 = all[int(len(all) * PERCENTILE_95)]
        p99 = all[int(len(all) * PERCENTILE_99)]

        self.p100 = maxDepth
        self.p0 = minDepth
        self.items = items
        self.avg = avg
        self.p50 = p50
        self.p90 = p90
        self.p95 = p95
        self.p99 = p99
        self.isValid = True

    def getRow(self):
        if not self.isValid:
            return None
        return [getattr(self, x) for x in QuantizedMetrics.headers]


class ErrCountsData:
    def __init__(
        self,
        selfErrors=DEFAULT_SELF_ERRORS,
        propagatedErrors=DEFAULT_PROPAGATED_ERRORS,
        stoppedErrors=DEFAULT_STOPPED_ERRORS,
    ):
        self.selfErrors = selfErrors
        self.propagatedErrors = propagatedErrors
        self.stoppedErrors = stoppedErrors

    def __str__(self):
        return f"{self.selfErrors},{self.propagatedErrors},{self.stoppedErrors}"

    def __add__(self, other):
        return ErrCountsData(
            self.selfErrors + other.selfErrors,
            self.propagatedErrors + other.propagatedErrors,
            self.stoppedErrors + other.stoppedErrors,
        )

    def __radd__(self, other):
        return self.__add__(other)

    def toArray(self):
        return [self.selfErrors, self.propagatedErrors, self.stoppedErrors]


class SavingData:
    def __init__(self, timeSaved, latency, opCount):
        self.timeSaved = timeSaved
        self.latency = latency
        self.opCount = opCount

    def __add__(self, other):
        return SavingData(
            self.timeSaved + other.timeSaved,
            self.latency + other.latency,
            self.opCount + other.opCount,
        )

    def __radd__(self, other):
        return self.__add__(other)


class SpanKind(Enum):
    CLIENT = SpanKindValues.CLIENT
    SERVER = SpanKindValues.SERVER
    UNKNOWN = SpanKindValues.UNKNOWN


# The full error critical path metrics
class ErrorCPMetrics:
    """
    ErrorCPMetric represents the following measurements as dictionaries
    1. errCPCallpathTimeExclusive: the error call-path profile with exclusive callpath times
        computed based on the full error critical path
    2. errCPErrCounts: the error call-path profile with error counts computed based on the
        full error critical path
    3. savingPotential: a map [opName]: exclusive timeSaved spent in op
    4. numCPErrors: the number of errored out nodes on the critical path
    5. numRelatedToCPErrors: the number of errored out nodes not on the
        critical path but connected to the critical path via a chain of failed calls
    """

    def __init__(
        self,
        errCPCallpathTimeExclusive,
        errCPErrCounts,
        savingPotential,
        numCPErrors,
        numRelatedToCPErrors,
    ):
        self.errCPCallpathTimeExclusive = errCPCallpathTimeExclusive
        self.errCPErrCounts = errCPErrCounts
        self.errCPSize = getCPSize(errCPCallpathTimeExclusive)
        self.savingPotential = savingPotential
        self.numCPErrors = numCPErrors
        self.numRelatedToCPErrors = numRelatedToCPErrors


# whole program error metrics
class ErrorMetrics:
    """
    ErrorMetrics contains various metrics on errors for the whole program
    1. numAllErrors: the number of errored spans
    2. errCounts: a map from opCallpath to error counts
        ["opCallpath": <selfErrors, propagatedErrors, stoppedErrors>]
    3. errCallChainCounts: a map from opCallPath to error call-chain counts
        ["opCallpath": integer count of errored out callcahin]
    4. selfErrDepthList: a list of self error depth
    5. stoppedErrDepthList: a list of stopped error depth
    6. depthMap: a map from depth (key) to ErrCountsData
        [depth: <selfErrors, propagatedErrors, stoppedErrors>]
    7. propLengthMap: a map from propagation length to count of self errors
        [propLength: integer counts of self errors with that propLength]
    8. resiliencyMap: a map from canonicalOpName to ErrCountsData
        ["canonicalOpName": <selfErrors, propagatedErrors, stoppedErrors>]
        though we only update the propagated and stopped errors for this map
    9. maxErrDepthPropToRoot: the max depth of self error propagated to root
        this value is -1 if the root did not error
    10. propToRootHistoQuantized: selferrors propagated to root, captured as a QuantizedError
    11. notPropToRootHistoQuantized: selferrors not propagated to root, captured as a QuantizedError
    12. propToRooOnCPtHistoQuantized: selferrors propagated to root on critical path, captured as a QuantizedError
    13. notPropToRooOnCPtHistoQuantized: selferrors not propagated to root on critical path, captured as a QuantizedError
    14. supressHistoQuantized:  supressed errors, captured as a QuantizedError
    15. supressOnCPHistoQuantized: supressed errors on critical path, captured as a QuantizedError
    """

    def __init__(
        self,
        numAllErrors,
        errCounts,
        errCallChainCounts,
        selfErrDepthList,
        stoppedErrDepthList,
        errDepthMap,
        errPropLengthMap,
        resiliencyMap,
        maxErrDepthPropToRoot,
        propToRootHistoQuantized,
        notPropToRootHistoQuantized,
        propToRootOnCPHistoQuantized,
        notPropToRootOnCPHistoQuantized,
        supressHistoQuantized,
        supressOnCPHistoQuantized,
    ):
        self.numAllErrors = numAllErrors
        self.errCounts = errCounts
        self.errCallChainCounts = errCallChainCounts
        self.selfErrDepthList = selfErrDepthList
        self.stoppedErrDepthList = stoppedErrDepthList
        self.errDepthMap = errDepthMap
        self.errPropLengthMap = errPropLengthMap
        self.resiliencyMap = resiliencyMap
        self.maxErrDepthPropToRoot = maxErrDepthPropToRoot
        self.propToRootHistoQuantized = propToRootHistoQuantized
        self.notPropToRootHistoQuantized = notPropToRootHistoQuantized
        self.propToRootOnCPHistoQuantized = propToRootOnCPHistoQuantized
        self.notPropToRootOnCPHistoQuantized = notPropToRootOnCPHistoQuantized
        self.supressHistoQuantized = supressHistoQuantized
        self.supressOnCPHistoQuantized = supressOnCPHistoQuantized


class Metrics:
    def __init__(
        self,
        traceID,
        traceSz,
        CPMetrics,
        errCPMetrics,
        errMetrics,
        totalWork,
        timeSavedOnWork,
        latency,
        timeSavedOnCPPessimistic,
        timeSavedOnCPOptimistic,
        timeSavedOnCPAllSeries,
        rootSpanID,
        descendants,
        depth,
        numNodesOnCP,
        rootReturnError,
        propToRootErrCCT,
        isCtfTest,
        numProxyRoots,
        tags,
        cycles,
        crossRegionCalls,
        projectedCPMetrics=None,
        projectedLatency=None,
        isIncomplete=False,
        isSubtreeIncomplete=False,
    ):
        self.traceID = traceID
        self.CPMetrics = CPMetrics
        self.errCPMetrics = errCPMetrics
        self.errMetrics = errMetrics
        self.fileSz = traceSz
        self.propToRootErrCCT = propToRootErrCCT

        if CPMetrics:
            self.cpSize = getCPSize(CPMetrics.profile)

        self.totalWork = totalWork
        self.timeSavedOnWork = timeSavedOnWork
        self.latency = latency
        self.timeSavedOnCPPessimistic = timeSavedOnCPPessimistic
        self.timeSavedOnCPOptimistic = timeSavedOnCPOptimistic
        self.timeSavedOnCPAllSeries = timeSavedOnCPAllSeries
        self.rootSpanID = rootSpanID
        self.numNodes = descendants
        self.depth = depth
        self.numNodesOnCP = numNodesOnCP

        self.rootReturnError = rootReturnError
        self.isCtfTest = isCtfTest
        self.numProxyRoots = numProxyRoots
        self.tags = tags
        self.cycles = cycles
        self.crossRegionCalls = crossRegionCalls
        self.projectedCPMetrics = projectedCPMetrics
        self.projectedLatency = projectedLatency
        self.isIncomplete = isIncomplete
        self.isSubtreeIncomplete = isSubtreeIncomplete
