"""
Utility functions for span classification and validation.

Several of these helpers consult small per-deployment allow-lists
(e.g. "which spans should be treated as proxies that don't faithfully
forward their child's error?") that are inherently site-specific.

The lists ship empty by default. Downstream users can extend them
without monkey-patching by appending to the module-level globals:

    from crisp.utils import span_utils

    span_utils.PROXY_SERVICE_OP_PAIRS.append(("my-proxy", "relay"))
    span_utils.TEST_TRACE_OP_PREFIXES.append("[mytests.Run]")

Note that ``TEST_TRACE_OP_PREFIXES`` is matched with ``str.startswith``,
so a shorter entry like ``"[mytests."`` would match every op name in
that family (``[mytests.Run]``, ``[mytests.Setup]``, ...) in a single
line.

Entry shapes (enforced only at match time — appending the wrong shape
will silently do nothing until the first classification call):

* ``PROXY_SERVICE_OP_PAIRS``    — ``(serviceName: str, opName: str)``
* ``PROXY_ONLY_OPS``            — ``opName: str``
* ``ERR_PROP_SERVICE_OP_PAIRS`` — ``(serviceName: str, opName: str)``
* ``TEST_TRACE_SERVICES``       — ``serviceName: str``
* ``TEST_TRACE_OP_PREFIXES``    — ``opNamePrefix: str``
"""

# --- Proxy-node configuration ---------------------------------------------
# (serviceName, opName) pairs that should be treated as proxies whose span
# does not faithfully reflect the error state of its child.
PROXY_SERVICE_OP_PAIRS: list[tuple[str, str]] = []
# opNames that should be treated as proxies regardless of serviceName.
# Useful for instrumentation wrappers that appear under many services.
PROXY_ONLY_OPS: list[str] = []

# --- Error-propagation-node configuration ---------------------------------
# (serviceName, opName) pairs where errors are reported on the caller side
# rather than surfaced in the proxy's own span, so analysis should propagate
# the child's error upward.
ERR_PROP_SERVICE_OP_PAIRS: list[tuple[str, str]] = []

# --- Test-trace heuristics -------------------------------------------------
# Service names that identify synthetic/test traffic rather than production.
TEST_TRACE_SERVICES: list[str] = []
# Operation-name prefixes that identify synthetic/test traffic. Match is
# performed with str.startswith.
TEST_TRACE_OP_PREFIXES: list[str] = []


def isProxyNode(serviceName: str, opName: str) -> bool:
    """
    Check if a span represents a proxy node based on service and op name.

    A span is a "proxy node" when it forwards work to a child but does not
    faithfully reflect the child's timing or error state (e.g. an auth
    wrapper or instrumentation relay). The match is driven by
    ``PROXY_SERVICE_OP_PAIRS`` and ``PROXY_ONLY_OPS``; both are empty by
    default and intended to be populated per deployment.

    Args:
        serviceName: The name of the service.
        opName: The operation name.

    Returns:
        True if the span is identified as a proxy node, False otherwise.
    """
    for s, o in PROXY_SERVICE_OP_PAIRS:
        if (serviceName == s) and (opName == o):
            return True
    for o in PROXY_ONLY_OPS:
        if opName == o:
            return True
    return False


def isErrPropNode(serviceName: str, opName: str) -> bool:
    """
    Return True if this span should have its child's error propagated up.

    Some proxies always succeed at the network layer because the error is
    materialized on the client side. The child's error then has to be
    carried up through the proxy's span. The match is driven by
    ``ERR_PROP_SERVICE_OP_PAIRS``, empty by default.

    Args:
        serviceName: The name of the service.
        opName: The operation name.

    Returns:
        True if errors should be propagated from this node, False otherwise.
    """
    for s, o in ERR_PROP_SERVICE_OP_PAIRS:
        if (serviceName == s) and (opName == o):
            return True
    return False


def isTestTraceByServiceName(serviceName: str) -> bool:
    """
    Return True if ``serviceName`` identifies a synthetic/test trace.

    Match is exact equality against ``TEST_TRACE_SERVICES`` (empty by
    default). Extend that list to exclude your own test-framework
    services from production analysis.

    Args:
        serviceName: The name of the service to check.

    Returns:
        True if the service is identified as a test service, False otherwise.
    """
    for name in TEST_TRACE_SERVICES:
        if serviceName == name:
            return True
    return False


def isTestTraceByOpName(operationName: str) -> bool:
    """
    Return True if ``operationName`` identifies a synthetic/test trace.

    Match is ``startswith`` against ``TEST_TRACE_OP_PREFIXES`` (empty by
    default). Extend that list to exclude your own test-framework
    operations from production analysis.

    Args:
        operationName: The operation name to check.

    Returns:
        True if the operation is identified as a test operation, False otherwise.
    """
    for prefix in TEST_TRACE_OP_PREFIXES:
        if operationName.startswith(prefix):
            return True
    return False
