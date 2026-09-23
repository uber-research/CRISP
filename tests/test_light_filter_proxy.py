import json
import os
import tempfile
import unittest

import crisp.common as common
import crisp.utils.span_utils as span_utils
from crisp.process_trace import process

# svcA/opA (100us) -> svcProxy/opProxy (80us) -> svcC/opC (50us).
_TRACE = {
    "data": [
        {
            "traceID": "T1",
            "spans": [
                {"traceID": "T1", "spanID": "111", "operationName": "opA", "references": [],
                 "startTime": 1000000, "duration": 100, "processID": "p0", "warnings": None},
                {"traceID": "T1", "spanID": "222", "operationName": "opProxy",
                 "references": [{"traceID": "T1", "spanID": "111", "refType": "CHILD_OF"}],
                 "startTime": 1000010, "duration": 80, "processID": "p1", "warnings": None},
                {"traceID": "T1", "spanID": "333", "operationName": "opC",
                 "references": [{"traceID": "T1", "spanID": "222", "refType": "CHILD_OF"}],
                 "startTime": 1000020, "duration": 50, "processID": "p2", "warnings": None},
            ],
            "processes": {
                "p0": {"serviceName": "svcA"},
                "p1": {"serviceName": "svcProxy"},
                "p2": {"serviceName": "svcC"},
            },
            "warnings": None,
        }
    ]
}


class LightProcessFilterProxyTests(unittest.TestCase):
    """process() (the light-mode per-trace pipeline) forwards
    config.filterProxy to the Graph: with a registered proxy pair, the proxy
    span is short-wired out of the call paths; without the flag it is kept.
    Mirrors TestLightProcessFilterProxy in go/crisp/light_test.go."""

    def setUp(self):
        self._pairs = list(span_utils.PROXY_SERVICE_OP_PAIRS)
        span_utils.PROXY_SERVICE_OP_PAIRS.append(("svcProxy", "opProxy"))
        self._dir = tempfile.TemporaryDirectory()
        self._file = os.path.join(self._dir.name, "T1.json")
        with open(self._file, "w") as f:
            json.dump(_TRACE, f)

    def tearDown(self):
        span_utils.PROXY_SERVICE_OP_PAIRS[:] = self._pairs
        self._dir.cleanup()

    def _callPaths(self, filterProxy):
        config = common.Config(serviceName="svcA", operationName="opA", filterProxy=filterProxy)
        metrics = process(self._file, config)
        self.assertIsNotNone(metrics)
        return [p for p in metrics.opTimeInclusive if p != "totalTime"]

    def test_filter_off_keeps_proxy_span(self):
        paths = self._callPaths(filterProxy=False)
        self.assertTrue(any("opProxy" in p for p in paths), paths)

    def test_filter_on_short_wires_proxy_span(self):
        paths = self._callPaths(filterProxy=True)
        self.assertFalse(any("opProxy" in p for p in paths), paths)
        self.assertTrue(any("opC" in p for p in paths), paths)


if __name__ == "__main__":
    unittest.main()
