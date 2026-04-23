"""Unit tests for crisp.utils.span_utils."""

import unittest

from crisp.utils import span_utils
from crisp.utils.span_utils import (
    isErrPropNode,
    isProxyNode,
    isTestTraceByOpName,
    isTestTraceByServiceName,
)


class _ConfigIsolation(unittest.TestCase):
    """Base TestCase that snapshots & restores the module-level lists.

    span_utils exposes its classification lists as mutable module globals
    so users can extend them for their deployment. Tests extend those
    lists and must not leak state across tests, so we snapshot on setUp
    and restore on tearDown.
    """

    _LIST_ATTRS = (
        "PROXY_SERVICE_OP_PAIRS",
        "PROXY_ONLY_OPS",
        "ERR_PROP_SERVICE_OP_PAIRS",
        "TEST_TRACE_SERVICES",
        "TEST_TRACE_OP_PREFIXES",
    )

    def setUp(self):
        self._snapshots = {
            name: list(getattr(span_utils, name)) for name in self._LIST_ATTRS
        }

    def tearDown(self):
        for name, original in self._snapshots.items():
            getattr(span_utils, name)[:] = original


class DefaultsAreEmptyTests(_ConfigIsolation):
    """Out of the box, every classifier list is empty.

    This matters because the open-source distribution intentionally
    ships without any site-specific service / op names. Leaking real
    infrastructure identifiers here would be a regression.

    Inherits from ``_ConfigIsolation`` purely as defense-in-depth: the
    cases here are read-only today, but if a future contributor adds
    an ``.append(...)`` inside one of them the snapshot/restore will
    stop it from bleeding into other tests.
    """

    def test_proxy_lists_empty(self):
        self.assertEqual(span_utils.PROXY_SERVICE_OP_PAIRS, [])
        self.assertEqual(span_utils.PROXY_ONLY_OPS, [])

    def test_err_prop_list_empty(self):
        self.assertEqual(span_utils.ERR_PROP_SERVICE_OP_PAIRS, [])

    def test_test_trace_lists_empty(self):
        self.assertEqual(span_utils.TEST_TRACE_SERVICES, [])
        self.assertEqual(span_utils.TEST_TRACE_OP_PREFIXES, [])

    def test_no_matches_with_empty_config(self):
        # With empty configs, everything is non-proxy / non-err-prop /
        # non-test. Reviewers: if this starts failing after a port PR,
        # someone has re-introduced a hardcoded default.
        self.assertFalse(isProxyNode("any-service", "any-op"))
        self.assertFalse(isErrPropNode("any-service", "any-op"))
        self.assertFalse(isTestTraceByServiceName("any-service"))
        self.assertFalse(isTestTraceByOpName("any-op"))


class IsProxyNodeTests(_ConfigIsolation):
    def test_service_op_pair_match(self):
        span_utils.PROXY_SERVICE_OP_PAIRS.append(("svc-a", "op-a"))
        self.assertTrue(isProxyNode("svc-a", "op-a"))

    def test_service_op_pair_requires_both(self):
        span_utils.PROXY_SERVICE_OP_PAIRS.append(("svc-a", "op-a"))
        self.assertFalse(isProxyNode("svc-a", "different-op"))
        self.assertFalse(isProxyNode("different-svc", "op-a"))

    def test_only_ops_match_any_service(self):
        span_utils.PROXY_ONLY_OPS.append("op-x")
        self.assertTrue(isProxyNode("svc-a", "op-x"))
        self.assertTrue(isProxyNode("svc-b", "op-x"))

    def test_only_ops_exact_match(self):
        span_utils.PROXY_ONLY_OPS.append("op-x")
        self.assertFalse(isProxyNode("svc-a", "op-x-suffix"))

    def test_empty_strings_with_empty_config(self):
        self.assertFalse(isProxyNode("", ""))

    def test_empty_strings_with_empty_config_match(self):
        # An empty-string entry in the config would match empty inputs —
        # this documents the (somewhat sharp) behavior rather than
        # endorsing it.
        span_utils.PROXY_ONLY_OPS.append("")
        self.assertTrue(isProxyNode("any", ""))


class IsErrPropNodeTests(_ConfigIsolation):
    def test_service_op_pair_match(self):
        span_utils.ERR_PROP_SERVICE_OP_PAIRS.append(("svc-a", "op-a"))
        self.assertTrue(isErrPropNode("svc-a", "op-a"))

    def test_service_alone_does_not_match(self):
        span_utils.ERR_PROP_SERVICE_OP_PAIRS.append(("svc-a", "op-a"))
        self.assertFalse(isErrPropNode("svc-a", "other-op"))

    def test_op_alone_does_not_match(self):
        # Unlike isProxyNode, there is no op-only fallback list for
        # error propagation.
        span_utils.ERR_PROP_SERVICE_OP_PAIRS.append(("svc-a", "op-a"))
        self.assertFalse(isErrPropNode("other-svc", "op-a"))

    def test_multiple_entries(self):
        span_utils.ERR_PROP_SERVICE_OP_PAIRS.extend(
            [("svc-a", "op-a"), ("svc-b", "op-b")]
        )
        self.assertTrue(isErrPropNode("svc-a", "op-a"))
        self.assertTrue(isErrPropNode("svc-b", "op-b"))
        self.assertFalse(isErrPropNode("svc-a", "op-b"))


class IsTestTraceByServiceNameTests(_ConfigIsolation):
    def test_exact_match(self):
        span_utils.TEST_TRACE_SERVICES.append("test-svc")
        self.assertTrue(isTestTraceByServiceName("test-svc"))

    def test_prefix_does_not_match(self):
        # Intentionally exact equality, not startswith.
        span_utils.TEST_TRACE_SERVICES.append("test-svc")
        self.assertFalse(isTestTraceByServiceName("test-svc-extra"))

    def test_multiple_entries(self):
        span_utils.TEST_TRACE_SERVICES.extend(["a", "b", "c"])
        for name in ("a", "b", "c"):
            with self.subTest(name=name):
                self.assertTrue(isTestTraceByServiceName(name))
        self.assertFalse(isTestTraceByServiceName("d"))


class IsTestTraceByOpNameTests(_ConfigIsolation):
    def test_prefix_match(self):
        span_utils.TEST_TRACE_OP_PREFIXES.append("[prefix]")
        self.assertTrue(isTestTraceByOpName("[prefix]"))
        self.assertTrue(isTestTraceByOpName("[prefix]::detail"))

    def test_case_sensitive(self):
        # Matching is case-sensitive.
        span_utils.TEST_TRACE_OP_PREFIXES.append("[prefix]")
        self.assertFalse(isTestTraceByOpName("[PREFIX]"))

    def test_middle_of_string_does_not_match(self):
        # Must be a prefix, not an arbitrary substring.
        span_utils.TEST_TRACE_OP_PREFIXES.append("[prefix]")
        self.assertFalse(isTestTraceByOpName("leading[prefix]"))

    def test_multiple_prefixes(self):
        span_utils.TEST_TRACE_OP_PREFIXES.extend(["[a]", "[b]"])
        self.assertTrue(isTestTraceByOpName("[a]foo"))
        self.assertTrue(isTestTraceByOpName("[b]bar"))
        self.assertFalse(isTestTraceByOpName("[c]baz"))


if __name__ == "__main__":
    unittest.main()
