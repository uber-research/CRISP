import copy
import os
import tempfile
from unittest import TestCase, mock

import crisp.flamegraph as flamegraph
from crisp.shared.models import (
    MetricVals,
    CallPathProfile,
    ErrCountsData,
    ErrorCPMetrics,
    QuantizedMetrics,
    Metrics,
    ErrorMetrics,
)


class FlamegraphTestCase(TestCase):
    def test_getParentCallPath(self):
        assert flamegraph.getParentCallPath("a") == ""
        assert flamegraph.getParentCallPath("a->b") == "a"
        assert flamegraph.getParentCallPath("a->b->c") == "a->b"

    def sanitizeAggregatedMap(self):
        input1 = {
            "a->b": 0,
            "a->c": 0,
            "a": 0,
        }
        expect1 = {
            "a->b": 1,
            "a->c": 1,
            "a": 0,
        }
        input2 = {
            "a->b->c": 10,
            "a->b": 0,
            "a": 0,
        }
        expect2 = {
            "a->b->c": 10,
            "a->b": 0,
            "a": 0,
        }

        assert flamegraph.sanitizeAggregatedMap(input1) == expect1
        assert flamegraph.sanitizeAggregatedMap(input2) == expect2

    def test_aggregateTimeMapList(self):
        input = [
            (
                1,
                {
                    "a->b": 10,
                    "a->c": 30,
                },
            ),
            (
                2,
                {
                    "a->b->c": 20,
                },
            ),
            (
                3,
                {
                    "a->c": 30,
                    "a->b": 10,
                },
            ),
            (4, {}),
        ]  # this one must be ignored since it does not contribute to the CCT.
        expectNoAverage = {
            "a->b": 20,
            "a->c": 60,
            "a->b->c": 20,
        }
        expectAverage = {
            "a->b": 20 // 3,
            "a->c": 60 // 3,
            "a->b->c": 20 // 3,
        }

        assert expectNoAverage == flamegraph.aggregateTimeMapList(input, average=False)
        assert expectAverage == flamegraph.aggregateTimeMapList(input, average=True)

    def test_aggregateCCTs(self):
        input = [
            (
                1,
                {
                    "a->b": 10,
                    "a->c": 30,
                },
            ),
            (
                2,
                {
                    "a->b->c": 20,
                },
            ),
            (
                3,
                {
                    "a->c": 30,
                    "a->b": 10,
                },
            ),
        ]
        errCounts = [
            (
                1,
                {
                    "a": ErrCountsData(stoppedErrors=1),
                    "a->b": ErrCountsData(selfErrors=1),
                    "a->c": ErrCountsData(selfErrors=1),
                },
            ),
            (
                2,
                {
                    "a->b": ErrCountsData(stoppedErrors=1),
                    "a->b->c": ErrCountsData(selfErrors=1),
                },
            ),
            (
                3,
                {
                    "a": ErrCountsData(stoppedErrors=1),
                    "a->c": ErrCountsData(selfErrors=1),
                    "a->b": ErrCountsData(selfErrors=1),
                },
            ),
        ]
        expected = sorted(
            "\na 0\na;b 6\na;b;c 6\na;c 20".split(
                "\n",
            ),
        )
        result = sorted(flamegraph.aggregateCCTs(input, errCounts).split("\n"))
        assert result == expected

        input = [
            (
                1,
                {
                    "a;a->b;b": 10,
                    "a;a->c;c": 30,
                },
            ),
            (
                2,
                {
                    "a;a->c;c": 30,
                    "a;a->b;b": 10,
                },
            ),
        ]
        errCounts = [
            (
                1,
                {
                    "a;a": ErrCountsData(stoppedErrors=1),
                    "a;a->b;b": ErrCountsData(selfErrors=1),
                    "a;a->c;c": ErrCountsData(selfErrors=1),
                },
            ),
            (
                3,
                {
                    "a;a": ErrCountsData(stoppedErrors=1),
                    "a;a->c;c": ErrCountsData(selfErrors=1),
                    "a;a->b;b": ErrCountsData(selfErrors=1),
                },
            ),
        ]
        expected = sorted(
            "\na_a 0\na_a;b_b 10\na_a;c_c 30".split("\n"),
        )
        result = sorted(flamegraph.aggregateCCTs(input, errCounts).split("\n"))
        assert result == expected

        input = [
            (
                1,
                {
                    "a;a->b;b": 10,
                    "a;a->c;c": 30,
                },
            ),
            (
                2,
                {
                    "a;a->c;c": 30,
                    "a;a->b;b": 10,
                },
            ),
        ]
        errCounts = [
            (
                1,
                {
                    "a;a": ErrCountsData(stoppedErrors=1),
                    "a;a->b;b": ErrCountsData(selfErrors=1),
                    "a;a->c;c": ErrCountsData(selfErrors=1),
                },
            ),
            (
                3,
                {
                    "a;a": ErrCountsData(stoppedErrors=1),
                    "a;a->c;c": ErrCountsData(selfErrors=1),
                    "a;a->b;b": ErrCountsData(selfErrors=1),
                },
            ),
        ]
        expected = sorted(
            "\na_a 0\na_a;b_b 10\na_a;c_c 30".split("\n"),
        )
        result = sorted(flamegraph.aggregateCCTs(input, errCounts).split("\n"))
        assert result == expected

    def create_mock_metrics_for_flameGraph(self):
        cpp = CallPathProfile({}, 1, 0)
        cpp.Upsert("[S1] O1->[S2] O2", MetricVals(30, 30, 1, 0))
        cpp.Upsert("[S1] O1", MetricVals(100, 70, 1, 0))
        errCPCallpathTimeExc = {
            "errorCriticalPathSyntheticRoot->[S1] O1->[S2] O2": 30,
        }
        # error data not accurate but doesn't matter; mock info not used
        errCPMetrics = ErrorCPMetrics(errCPCallpathTimeExc, {}, {}, 0, 0)
        errMetrics = ErrorMetrics(
            0,
            {},
            {},
            [],
            [],
            {},
            {},
            {},
            -1,
            propToRootHistoQuantized=QuantizedMetrics({}),
            notPropToRootHistoQuantized=QuantizedMetrics({}),
            propToRootOnCPHistoQuantized=QuantizedMetrics({}),
            notPropToRootOnCPHistoQuantized=QuantizedMetrics({}),
            supressHistoQuantized=QuantizedMetrics({}),
            supressOnCPHistoQuantized=QuantizedMetrics({}),
        )
        metrics = Metrics(
            1,
            1,
            cpp,
            errCPMetrics,
            errMetrics,
            130,
            30,
            100,
            30,
            0,
            0,
            0,
            0,
            0,
            0,
            False,
            {},
            False,
            0,
            tags=[],
            cycles={},
            crossRegionCalls={},
        )

        return metrics

    def create_N_mock_metrics_for_flameGraph(self, n):
        metrics = []
        for i in range(1, n + 1):
            cpp = CallPathProfile({}, 1, i)
            cpp.Upsert("[S1] O1", MetricVals(i * 100, i * 70, i, 0))
            cpp.Upsert("[S1] O1->[S2] O2", MetricVals(i * 30, i * 30, i, 1))

            errCPCallpathTimeExc = {
                "errorCriticalPathSyntheticRoot->[S1] O1->[S2] O2": i * 30,
            }

            # error data not accurate but doesn't matter; mock info not used
            errCPMetrics = ErrorCPMetrics(errCPCallpathTimeExc, {}, {}, 0, 0)
            errMetrics = ErrorMetrics(
                0,
                {},
                {},
                [],
                [],
                {},
                {},
                {},
                -1,
                propToRootHistoQuantized=QuantizedMetrics({}),
                notPropToRootHistoQuantized=QuantizedMetrics({}),
                propToRootOnCPHistoQuantized=QuantizedMetrics({}),
                notPropToRootOnCPHistoQuantized=QuantizedMetrics({}),
                supressHistoQuantized=QuantizedMetrics({}),
                supressOnCPHistoQuantized=QuantizedMetrics({}),
            )
            metrics.append(
                Metrics(
                    1,
                    0,
                    cpp,
                    errCPMetrics,
                    errMetrics,
                    i * 130,
                    i * 30,
                    i * 100,
                    30,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    False,
                    {},
                    False,
                    0,
                    tags=[],
                    cycles={},
                    crossRegionCalls={},
                ),
            )

        return metrics

    @mock.patch("crisp.flamegraph.subprocess.check_call", return_value=0)
    def test_flameGraph(self, check_call_mock):  # noqa: ARG002
        m1 = self.create_mock_metrics_for_flameGraph()
        m2 = self.create_mock_metrics_for_flameGraph()
        m3 = self.create_mock_metrics_for_flameGraph()

        m = [m1, m2, m3]
        pcts = flamegraph.STANDARD_PERCENTILES
        with tempfile.TemporaryDirectory() as tmpdirname:
            flamePairs = [
                (f"P0-{x[1]}", os.path.join(tmpdirname, f"flame-graph-P{x[1]}.cct.svg"))
                for x in pcts
            ]

            diffFlames = []
            for id, p in enumerate(pcts):
                if id != 0:
                    diffFlames += [
                        os.path.join(
                            tmpdirname,
                            f"flame-graph-P0-{x[1]}vsP0-{p[1]}.cct.svg",
                        )
                        for x in pcts[:id]
                    ]

            [
                os.path.join(tmpdirname, f"flame-graph-P0-{x}vsP0-{x}.cct.svg")
                for x in pcts
            ]
            errFlamePairs = [
                (
                    f"P0-{x[1]}",
                    os.path.join(tmpdirname, f"err-flame-graph-P{x[1]}.cct.svg"),
                )
                for x in pcts
            ]

            errDiffFlames = []
            for id, p in enumerate(pcts):
                if id != 0:
                    errDiffFlames += [
                        os.path.join(
                            tmpdirname,
                            f"err-flame-graph-P0-{x[1]}vsP0-{p[1]}.cct.svg",
                        )
                        for x in pcts[:id]
                    ]
            result = flamegraph.flameGraph(m, tmpdirname, "S1", "O1", True)
            # the errored-API ones won't get generated as no metrics is marked as root errored
            expect = flamegraph.FlameGraphSet(
                flamePairs,
                diffFlames,
                errFlamePairs,
                errDiffFlames,
                [],
                [],
                [],
                [],
            )
            assert result == expect

    @mock.patch("crisp.flamegraph.subprocess.check_call", return_value=0)
    def test_flameGraphWithRange(self, check_call_mock):  # noqa: ARG002
        m = self.create_N_mock_metrics_for_flameGraph(100)
        stdPcts = flamegraph.STANDARD_PERCENTILES
        additionalPcts = flamegraph.ADDITIONAL_RANGES
        noDiffPairs = flamegraph.RANGES_SKIP_DIFF
        with tempfile.TemporaryDirectory() as tmpdirname:
            flamePairs = [
                (f"P0-{x[1]}", os.path.join(tmpdirname, f"flame-graph-P{x[1]}.cct.svg"))
                for x in stdPcts
            ]
            flamePairs += [
                (
                    f"P{x[0]}-{x[1]}",
                    os.path.join(tmpdirname, f"flame-graph-P{x[0]}-{x[1]}.cct.svg"),
                )
                for x in additionalPcts
            ]
            flamePairs += [
                (
                    f"P{x[0]}-{x[1]}",
                    os.path.join(tmpdirname, f"flame-graph-P{x[0]}-{x[1]}.cct.svg"),
                )
                for x in noDiffPairs
            ]

            diffFlames = []
            for pcts in [stdPcts, additionalPcts]:
                for id, p in enumerate(pcts):
                    if id != 0:
                        diffFlames += [
                            os.path.join(
                                tmpdirname,
                                f"flame-graph-P{x[0]}-{x[1]}vsP{p[0]}-{p[1]}.cct.svg",
                            )
                            for x in pcts[:id]
                        ]
            errFlamePairs = [
                (
                    f"P0-{x[1]}",
                    os.path.join(tmpdirname, f"err-flame-graph-P{x[1]}.cct.svg"),
                )
                for x in stdPcts
            ]

            errDiffFlames = []
            for pcts in [stdPcts]:
                for id, p in enumerate(pcts):
                    if id != 0:
                        errDiffFlames += [
                            os.path.join(
                                tmpdirname,
                                f"err-flame-graph-P0-{x[1]}vsP0-{p[1]}.cct.svg",
                            )
                            for x in pcts[:id]
                        ]
            result = flamegraph.flameGraph(
                m,
                tmpdirname,
                "S1",
                "O1",
                True,
                doRanges=True,
            )
            # the errored-API ones won't get generated as no metrics is marked as root errored
            expect = flamegraph.FlameGraphSet(
                flamePairs,
                diffFlames,
                errFlamePairs,
                errDiffFlames,
                [],
                [],
                [],
                [],
            )
            assert result == expect

    """ Function to test FlameGraphSet class"""

    def test_FlameGraphSet(self):
        flamePairs = [
            ("P50", "A.cct.svg"),
            ("P95", "B.cct.svg"),
            ("P99", "C.cct.svg"),
        ]
        diffFlames = [
            "D.cct.svg",
            "E.cct.svg",
            "F.cct.svg",
        ]
        errCPFlamePairs = [
            ("P50", "G.cct.svg"),
            ("P95", "H.cct.svg"),
            ("P99", "I.cct.svg"),
        ]
        errCPDiffFlames = [
            "J.cct.svg",
            "K.cct.svg",
            "L.cct.svg",
        ]
        errRootFlamePairs = [
            ("P50", "M.cct.svg"),
            ("P95", "N.cct.svg"),
            ("P99", "O.cct.svg"),
        ]
        errRootDiffFlames = [
            "P.cct.svg",
            "Q.cct.svg",
            "R.cct.svg",
        ]

        errPropToRootCountFgPctFilePair = [
            ("P100", "S.cct.svg"),
        ]
        diffErrPropToRootCountFgPctFilePair = []

        result = flamegraph.FlameGraphSet(
            flamePairs,
            diffFlames,
            errCPFlamePairs,
            errCPDiffFlames,
            errRootFlamePairs,
            errRootDiffFlames,
            errPropToRootCountFgPctFilePair,
            diffErrPropToRootCountFgPctFilePair,
        )
        assert result == copy.deepcopy(result)
        assert sorted(result.GetAllFiles()) == sorted(
            [x + ".cct.svg" for x in "ABCDEFGHIJKLMNOPQR"]
            + [x + ".cct" for x in "ABCDEFGHIJKLMNOPQR"],
        )

    def test_get_all_error_files(self):
        flamePairs = []
        diffFlames = []
        errCPFlamePairs = []
        errCPDiffFlames = []
        errRootFlamePairs = []
        errRootDiffFlames = []

        errPropToRootCountFgPctFilePair = [
            ("P100", "S.cct.svg"),
            ("P50", "T.cct.svg"),
        ]
        diffErrPropToRootCountFgPctFilePair = [
            "U.cct.svg",
            "V.cct.svg",
        ]

        flamegraph_set = flamegraph.FlameGraphSet(
            flamePairs,
            diffFlames,
            errCPFlamePairs,
            errCPDiffFlames,
            errRootFlamePairs,
            errRootDiffFlames,
            errPropToRootCountFgPctFilePair,
            diffErrPropToRootCountFgPctFilePair,
        )

        result = flamegraph_set.GetAllErrorFiles()

        expected_svg_files = ["S.cct.svg", "T.cct.svg", "U.cct.svg", "V.cct.svg"]
        expected_cct_files = ["S.cct", "T.cct", "U.cct", "V.cct"]

        assert sorted(result) == sorted(expected_svg_files + expected_cct_files)
