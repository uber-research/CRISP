"""Shared data models used across critical path analysis modules.

This module contains core data structures that are shared between
multiple modules to avoid circular dependencies.
"""

from __future__ import annotations

import copy
import logging
from typing import Any


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
