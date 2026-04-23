# ruff: noqa: I001
"""Tests for shared data models.

This module contains unit tests for MetricVals, CallPathProfile, and
LatencyData, plus the metrics data types relocated from internal
critical_path/models.py (QuantizedMetrics, ErrCountsData, SavingData,
SpanKind, ErrorCPMetrics, ErrorMetrics, Metrics).
"""

from unittest import TestCase

from crisp.shared.constants import SpanKindValues
from crisp.shared.models import (
    CallPathProfile,
    ErrCountsData,
    ErrorCPMetrics,
    ErrorMetrics,
    LatencyData,
    Metrics,
    MetricVals,
    QuantizedMetrics,
    SavingData,
    SpanKind,
)
from crisp.utils.dict_utils import getCPSize


class TestMetricVals(TestCase):
    def test_addition(self):
        metric1 = MetricVals(1, 2, 3, 100)
        metric2 = MetricVals(4, 5, 6, 200)
        result = metric1 + metric2
        self.assertEqual(result.inc, 5)
        self.assertEqual(result.excl, 7)
        self.assertEqual(result.freq, 9)

    def test_in_place_addition(self):
        metric = MetricVals(1, 2, 3, 100)
        metric += MetricVals(4, 5, 6, 200)
        self.assertEqual(metric.inc, 5)
        self.assertEqual(metric.excl, 7)
        self.assertEqual(metric.freq, 9)

    def test_floordiv(self):
        metric = MetricVals(10, 20, 30, 100)
        result = metric // 2
        self.assertEqual(result.inc, 5)
        self.assertEqual(result.excl, 10)
        self.assertEqual(result.freq, 15)

    def test_in_place_floordiv(self):
        metric = MetricVals(10, 20, 30, 100)
        metric //= 2
        self.assertEqual(metric.inc, 5)
        self.assertEqual(metric.excl, 10)
        self.assertEqual(metric.freq, 15)


class TestCallPathProfile(TestCase):
    def setUp(self):
        self.metric1 = MetricVals(1, 2, 3, 100)
        self.metric2 = MetricVals(4, 5, 6, 200)
        self.profile1 = CallPathProfile({"path1": self.metric1}, 2, 1)
        self.profile2 = CallPathProfile({"path2": self.metric2}, 3, 2)

    def test_get_normalized(self):
        result = self.profile1.GetNormalized()
        self.assertEqual(result["path1"].inc, 0)
        self.assertEqual(result["path1"].excl, 1)
        self.assertEqual(result["path1"].freq, 1)

    def test_normalize(self):
        self.profile1.Normalize()
        self.assertEqual(self.profile1.profile["path1"].inc, 0)
        self.assertEqual(self.profile1.profile["path1"].excl, 1)
        self.assertEqual(self.profile1.profile["path1"].freq, 1)

    def test_normalize_field(self):
        self.profile1.NormalizeField("inc")
        self.assertEqual(self.profile1.profile["path1"].inc, 0)

    def test_upsert_existing(self):
        self.profile1.Upsert("path1", MetricVals(1, 1, 1, 300))
        self.assertEqual(self.profile1.profile["path1"].inc, 2)
        self.assertEqual(self.profile1.profile["path1"].excl, 3)
        self.assertEqual(self.profile1.profile["path1"].freq, 4)

    def test_upsert_new(self):
        self.profile1.Upsert("path3", MetricVals(1, 1, 1, 300))
        self.assertEqual(self.profile1.profile["path3"].inc, 1)
        self.assertEqual(self.profile1.profile["path3"].excl, 1)
        self.assertEqual(self.profile1.profile["path3"].freq, 1)

    def test_add_profiles(self):
        result = self.profile1 + self.profile2
        self.assertIn("path1", result.profile)
        self.assertIn("path2", result.profile)
        self.assertEqual(result.count, 5)

    def test_in_place_add_profiles(self):
        self.profile1 += self.profile2
        self.assertIn("path1", self.profile1.profile)
        self.assertIn("path2", self.profile1.profile)
        self.assertEqual(self.profile1.count, 5)


class TestLatencyData(TestCase):
    def test_addition_and_average(self):
        """Test LatencyData addition using sum() and average() method."""
        d1 = LatencyData("1", 100, 10, 1, 2)
        d2 = LatencyData("2", 100, 20, 3, 4)
        d3 = LatencyData("3", 100, 30, 5, 6)

        process_traces = sum(
            [d1, d2, d3],
            start=LatencyData("", 0, 0, 0, 0),
        )

        self.assertEqual(process_traces.latency, 300)
        self.assertEqual(process_traces.hypoLatency, 60)
        self.assertEqual(process_traces.hypoLatencyOptimistic, 9)
        self.assertEqual(process_traces.hypoLatencyPessimistic, 12)

        process_traces.average(3)

        self.assertEqual(process_traces.latency, 100)
        self.assertEqual(process_traces.hypoLatency, 20)
        self.assertEqual(process_traces.hypoLatencyOptimistic, 3)
        self.assertEqual(process_traces.hypoLatencyPessimistic, 4)


class TestQuantizedMetrics(TestCase):
    def test_empty_histo(self):
        qm = QuantizedMetrics({})
        self.assertFalse(qm.isValid)
        for field in ("p0", "p100", "items", "avg", "p50", "p90", "p95", "p99"):
            self.assertIsNone(getattr(qm, field))
        self.assertIsNone(qm.getRow())

    def test_single_value_histo(self):
        qm = QuantizedMetrics({5: 1})
        self.assertTrue(qm.isValid)
        self.assertEqual(qm.items, 1)
        self.assertEqual(qm.p0, 5)
        self.assertEqual(qm.p100, 5)
        self.assertEqual(qm.avg, 5.0)
        self.assertEqual(qm.p50, 5)
        self.assertEqual(qm.p90, 5)
        self.assertEqual(qm.p95, 5)
        self.assertEqual(qm.p99, 5)

    def test_typical_histo(self):
        # histo keys 1..5 each with count 1 -> expanded list [1,2,3,4,5],
        # items=5, sum=15, avg=3. Percentiles use int(len*frac) indexing.
        # int(5*0.5)=2 -> all[2]=3; int(5*0.9)=4 -> all[4]=5; 0.95 & 0.99
        # also round to index 4 -> 5.
        qm = QuantizedMetrics({1: 1, 2: 1, 3: 1, 4: 1, 5: 1})
        self.assertTrue(qm.isValid)
        self.assertEqual(qm.items, 5)
        self.assertEqual(qm.avg, 3.0)
        self.assertEqual(qm.p0, 1)
        self.assertEqual(qm.p100, 5)
        self.assertEqual(qm.p50, 3)
        self.assertEqual(qm.p90, 5)
        self.assertEqual(qm.p95, 5)
        self.assertEqual(qm.p99, 5)

    def test_histo_with_repetition(self):
        # {10: 3} -> expanded list [10, 10, 10]. items=3, avg=10. All
        # percentile indices resolve to 10.
        qm = QuantizedMetrics({10: 3})
        self.assertTrue(qm.isValid)
        self.assertEqual(qm.items, 3)
        self.assertEqual(qm.avg, 10.0)
        self.assertEqual(qm.p0, 10)
        self.assertEqual(qm.p100, 10)
        self.assertEqual(qm.p50, 10)

    def test_get_row_valid(self):
        qm = QuantizedMetrics({5: 1})
        row = qm.getRow()
        self.assertEqual(
            row,
            [qm.items, qm.p0, qm.p100, qm.avg, qm.p50, qm.p90, qm.p95, qm.p99],
        )
        self.assertEqual(len(row), len(QuantizedMetrics.headers))


class TestErrCountsData(TestCase):
    def test_defaults(self):
        e = ErrCountsData()
        self.assertEqual(e.selfErrors, 0)
        self.assertEqual(e.propagatedErrors, 0)
        self.assertEqual(e.stoppedErrors, 0)

    def test_explicit_values(self):
        e = ErrCountsData(1, 2, 3)
        self.assertEqual(e.selfErrors, 1)
        self.assertEqual(e.propagatedErrors, 2)
        self.assertEqual(e.stoppedErrors, 3)

    def test_str(self):
        self.assertEqual(str(ErrCountsData(1, 2, 3)), "1,2,3")

    def test_add(self):
        a = ErrCountsData(1, 2, 3)
        b = ErrCountsData(4, 5, 6)
        c = a + b
        self.assertEqual((c.selfErrors, c.propagatedErrors, c.stoppedErrors), (5, 7, 9))
        # operands are not mutated
        self.assertEqual(a.selfErrors, 1)
        self.assertEqual(b.selfErrors, 4)

    def test_sum_with_explicit_start(self):
        # __radd__ delegates to __add__, which indexes .selfErrors on the
        # right-hand operand — so bare sum() (which seeds start=0, an
        # int) fails. Pass an ErrCountsData start explicitly, matching
        # the pattern used in TestLatencyData and in the internal tests.
        total = sum(
            [ErrCountsData(1, 1, 1), ErrCountsData(2, 2, 2)],
            start=ErrCountsData(),
        )
        self.assertEqual(
            (total.selfErrors, total.propagatedErrors, total.stoppedErrors),
            (3, 3, 3),
        )

    def test_to_array(self):
        self.assertEqual(ErrCountsData(1, 2, 3).toArray(), [1, 2, 3])


class TestSavingData(TestCase):
    def test_constructor_stores_fields(self):
        s = SavingData(10, 100, 2)
        self.assertEqual(s.timeSaved, 10)
        self.assertEqual(s.latency, 100)
        self.assertEqual(s.opCount, 2)

    def test_add(self):
        result = SavingData(1, 2, 3) + SavingData(10, 20, 30)
        self.assertEqual((result.timeSaved, result.latency, result.opCount), (11, 22, 33))

    def test_sum_with_explicit_start(self):
        # See TestErrCountsData.test_sum_with_explicit_start for why a
        # start value is required.
        total = sum(
            [SavingData(1, 1, 1), SavingData(2, 2, 2), SavingData(3, 3, 3)],
            start=SavingData(0, 0, 0),
        )
        self.assertEqual((total.timeSaved, total.latency, total.opCount), (6, 6, 6))


class TestSpanKind(TestCase):
    def test_values_match_numeric_constants(self):
        self.assertEqual(SpanKind.CLIENT.value, SpanKindValues.CLIENT)
        self.assertEqual(SpanKind.SERVER.value, SpanKindValues.SERVER)
        self.assertEqual(SpanKind.UNKNOWN.value, SpanKindValues.UNKNOWN)

    def test_three_distinct_members(self):
        self.assertEqual(len(set(SpanKind)), 3)

    def test_membership(self):
        self.assertIn(SpanKind.CLIENT, SpanKind)
        self.assertIn(SpanKind.SERVER, SpanKind)
        self.assertIn(SpanKind.UNKNOWN, SpanKind)


class TestErrorCPMetrics(TestCase):
    def test_constructor_stores_fields(self):
        errCPCallpathTime = {"a->b": 5, "c": 10}
        errCPErrCounts = {"a->b": ErrCountsData(1, 0, 0)}
        savingPotential = {"a": 3}
        m = ErrorCPMetrics(errCPCallpathTime, errCPErrCounts, savingPotential, 2, 1)
        self.assertIs(m.errCPCallpathTimeExclusive, errCPCallpathTime)
        self.assertIs(m.errCPErrCounts, errCPErrCounts)
        self.assertIs(m.savingPotential, savingPotential)
        self.assertEqual(m.numCPErrors, 2)
        self.assertEqual(m.numRelatedToCPErrors, 1)

    def test_err_cp_size_matches_get_cp_size(self):
        # errCPSize is derived at construction time via getCPSize; the
        # value should equal getCPSize applied to the same dict.
        errCPCallpathTime = {"a->b->c": 5, "d->e": 42}
        m = ErrorCPMetrics(errCPCallpathTime, {}, {}, 0, 0)
        self.assertEqual(m.errCPSize, getCPSize(errCPCallpathTime))

    def test_err_cp_size_empty(self):
        m = ErrorCPMetrics({}, {}, {}, 0, 0)
        self.assertEqual(m.errCPSize, 0)


class TestErrorMetrics(TestCase):
    def test_constructor_stores_all_fields(self):
        # Dense constructor, but every field needs to be preserved; we
        # sample-check each with a distinct sentinel value so a future
        # assignment swap inside __init__ is caught.
        m = ErrorMetrics(
            numAllErrors=7,
            errCounts={"a": ErrCountsData(1, 2, 3)},
            errCallChainCounts={"a": 4},
            selfErrDepthList=[1, 2],
            stoppedErrDepthList=[3],
            errDepthMap={1: ErrCountsData(1, 0, 0)},
            errPropLengthMap={2: 5},
            resiliencyMap={"op1": ErrCountsData(0, 1, 2)},
            maxErrDepthPropToRoot=-1,
            propToRootHistoQuantized=QuantizedMetrics({}),
            notPropToRootHistoQuantized=QuantizedMetrics({}),
            propToRootOnCPHistoQuantized=QuantizedMetrics({}),
            notPropToRootOnCPHistoQuantized=QuantizedMetrics({}),
            supressHistoQuantized=QuantizedMetrics({}),
            supressOnCPHistoQuantized=QuantizedMetrics({}),
        )
        self.assertEqual(m.numAllErrors, 7)
        self.assertEqual(m.errCounts["a"].selfErrors, 1)
        self.assertEqual(m.errCallChainCounts["a"], 4)
        self.assertEqual(m.selfErrDepthList, [1, 2])
        self.assertEqual(m.stoppedErrDepthList, [3])
        self.assertEqual(m.errDepthMap[1].selfErrors, 1)
        self.assertEqual(m.errPropLengthMap[2], 5)
        self.assertEqual(m.resiliencyMap["op1"].propagatedErrors, 1)
        self.assertEqual(m.maxErrDepthPropToRoot, -1)
        # The six *Quantized fields are all stored verbatim.
        for field in (
            "propToRootHistoQuantized",
            "notPropToRootHistoQuantized",
            "propToRootOnCPHistoQuantized",
            "notPropToRootOnCPHistoQuantized",
            "supressHistoQuantized",
            "supressOnCPHistoQuantized",
        ):
            self.assertIsInstance(getattr(m, field), QuantizedMetrics)


class TestMetrics(TestCase):
    def _build(self, CPMetrics):
        return Metrics(
            traceID="trace-1",
            traceSz=123,
            CPMetrics=CPMetrics,
            errCPMetrics=None,
            errMetrics=None,
            totalWork=1000,
            timeSavedOnWork=50,
            latency=500,
            timeSavedOnCPPessimistic=10,
            timeSavedOnCPOptimistic=20,
            timeSavedOnCPAllSeries=15,
            rootSpanID="root-1",
            descendants=8,
            depth=4,
            numNodesOnCP=3,
            rootReturnError=False,
            propToRootErrCCT={},
            isCtfTest=False,
            numProxyRoots=0,
            tags=[],
            cycles=0,
            crossRegionCalls=0,
        )

    def test_constructor_stores_all_fields(self):
        profile = CallPathProfile({"a->b": MetricVals(1, 2, 3, 100)}, 2, "trace-1")
        m = self._build(CPMetrics=profile)
        self.assertEqual(m.traceID, "trace-1")
        self.assertEqual(m.fileSz, 123)  # renamed from traceSz by __init__
        self.assertIs(m.CPMetrics, profile)
        self.assertEqual(m.totalWork, 1000)
        self.assertEqual(m.timeSavedOnWork, 50)
        self.assertEqual(m.latency, 500)
        self.assertEqual(m.timeSavedOnCPPessimistic, 10)
        self.assertEqual(m.timeSavedOnCPOptimistic, 20)
        self.assertEqual(m.timeSavedOnCPAllSeries, 15)
        self.assertEqual(m.rootSpanID, "root-1")
        self.assertEqual(m.numNodes, 8)  # renamed from descendants by __init__
        self.assertEqual(m.depth, 4)
        self.assertEqual(m.numNodesOnCP, 3)
        self.assertFalse(m.rootReturnError)
        self.assertFalse(m.isCtfTest)
        self.assertEqual(m.numProxyRoots, 0)
        self.assertEqual(m.tags, [])
        self.assertEqual(m.cycles, 0)
        self.assertEqual(m.crossRegionCalls, 0)
        # Defaults
        self.assertIsNone(m.projectedCPMetrics)
        self.assertIsNone(m.projectedLatency)
        self.assertFalse(m.isIncomplete)
        self.assertFalse(m.isSubtreeIncomplete)

    def test_cp_size_computed_when_cpmetrics_truthy(self):
        profile = CallPathProfile({"a->b->c": MetricVals(1, 2, 3, 100)}, 1, "trace-1")
        m = self._build(CPMetrics=profile)
        self.assertEqual(m.cpSize, getCPSize(profile.profile))

    def test_cp_size_skipped_when_cpmetrics_none(self):
        # The internal __init__ guards `if CPMetrics:` before computing
        # cpSize, so a falsy CPMetrics leaves the attribute unset. This
        # is existing behavior we preserve verbatim; callers that need a
        # uniform attribute should add their own default.
        m = self._build(CPMetrics=None)
        self.assertFalse(hasattr(m, "cpSize"))
