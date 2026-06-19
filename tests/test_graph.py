# ruff: noqa: I001
from unittest import TestCase

import crisp.common as common
import crisp.graph as graph
from crisp.graph import Graph, GraphNode, SpanKind
from crisp.constants import SYNTHETIC_ERR_CP_ROOT, SYNTHETIC_FULL_ERR_NON_CP_ROOT
from crisp.configuration import is_optimistic_enabled, get_server_lengthening_factor
from unittest import mock


# This creates a graph with call tree:
# A ([S1] O1) -> B ([S2] O2) -> C ([S3] O3)
#                            -> D ([S4] O4)
# A runs 0-100
# B runs 10-70
# C runs 25-45
# D runs 30-50, errors out
def sampleGraph():
    jsonData = {
        "data": [
            {
                "processes": {
                    "S1": {
                        "serviceName": "S1",
                        "tags": [],
                    },
                    "S2": {
                        "serviceName": "S2",
                        "tags": [],
                    },
                    "S3": {
                        "serviceName": "S3",
                        "tags": [],
                    },
                    "S4": {
                        "serviceName": "S4",
                        "tags": [],
                    },
                },
                "traceID": "A",
                "spans": [
                    {
                        "traceID": "A",
                        "spanID": "A",
                        "operationName": "O1",
                        "references": [],
                        "startTime": 0,
                        "duration": 100,
                        "processID": "S1",
                        "warnings": None,
                    },
                    {
                        "traceID": "A",
                        "spanID": "B",
                        "operationName": "O2",
                        "startTime": 10,
                        "duration": 60,
                        "processID": "S2",
                        "warnings": None,
                        "references": [
                            {
                                "refType": "CHILD_OF",
                                "traceID": "A",
                                "spanID": "A",
                            },
                        ],
                    },
                    {
                        "traceID": "A",
                        "spanID": "C",
                        "operationName": "O3",
                        "startTime": 25,
                        "duration": 20,
                        "processID": "S3",
                        "warnings": None,
                        "references": [
                            {
                                "refType": "CHILD_OF",
                                "traceID": "A",
                                "spanID": "B",
                            },
                        ],
                    },
                    {
                        "traceID": "A",
                        "spanID": "D",
                        "operationName": "O4",
                        "startTime": 30,
                        "duration": 20,
                        "processID": "S4",
                        "warnings": None,
                        "tags": [
                            {
                                "key": "error",
                                "type": "string",
                                "value": "ClientSideError",
                            },
                        ],
                        "references": [
                            {
                                "refType": "CHILD_OF",
                                "traceID": "A",
                                "spanID": "B",
                            },
                        ],
                    },
                ],
            },
        ],
    }

    return Graph(jsonData, "S1", "O1", "", "")


# This creates a graph with call tree:
# A ([S1] O1) -> B ([S2] proxy-relay) -> C ([S3] proxy-relay) -> D ([S4] O2)
def proxy_graph():
    jsonData = {
        "data": [
            {
                "processes": {
                    "S1": {
                        "serviceName": "S1",
                        "tags": [],
                    },
                    "S2": {
                        "serviceName": "S2",
                        "tags": [],
                    },
                    "S3": {
                        "serviceName": "S3",
                        "tags": [],
                    },
                    "S4": {
                        "serviceName": "S4",
                        "tags": [],
                    },
                },
                "traceID": "A",
                "spans": [
                    {
                        "traceID": "A",
                        "spanID": "A",
                        "operationName": "O1",
                        "references": [],
                        "startTime": 0,
                        "duration": 100,
                        "processID": "S1",
                    },
                    {
                        "traceID": "A",
                        "spanID": "B",
                        "operationName": "proxy-relay",
                        "startTime": 10,
                        "duration": 10,
                        "processID": "S2",
                        "warnings": None,
                        "references": [
                            {
                                "refType": "CHILD_OF",
                                "traceID": "A",
                                "spanID": "A",
                            },
                        ],
                    },
                    {
                        "traceID": "A",
                        "spanID": "C",
                        "operationName": "proxy-relay",
                        "startTime": 12,
                        "duration": 3,
                        "processID": "S3",
                        "warnings": None,
                        "references": [
                            {
                                "refType": "CHILD_OF",
                                "traceID": "A",
                                "spanID": "B",
                            },
                        ],
                    },
                    {
                        "traceID": "A",
                        "spanID": "D",
                        "operationName": "O2",
                        "startTime": 30,
                        "duration": 40,
                        "processID": "S4",
                        "warnings": None,
                        "references": [
                            {
                                "refType": "CHILD_OF",
                                "traceID": "A",
                                "spanID": "C",
                            },
                        ],
                    },
                ],
            },
        ],
    }
    return Graph(jsonData, "S1", "O1", "", "", filterProxy=True)


# This creates a graph with call tree:
# A ([S1] O1) -> P1 ([err-prop-svc] relay-op) -> P2 ([err-prop-svc] relay-op) -> B ([S2] O2)
def errprop_graph():
    jsonData = {
        "data": [
            {
                "processes": {
                    "S1": {
                        "serviceName": "S1",
                        "tags": [],
                    },
                    "S2": {
                        "serviceName": "S2",
                        "tags": [],
                    },
                    "err-prop-svc": {
                        "serviceName": "err-prop-svc",
                        "tags": [],
                    },
                },
                "traceID": "A",
                "spans": [
                    {
                        "traceID": "A",
                        "spanID": "A",
                        "operationName": "O1",
                        "references": [],
                        "startTime": 0,
                        "duration": 100,
                        "processID": "S1",
                    },
                    {
                        "traceID": "A",
                        "spanID": "P1",
                        "operationName": "relay-op",
                        "startTime": 10,
                        "duration": 80,
                        "processID": "err-prop-svc",
                        "warnings": None,
                        "references": [
                            {
                                "refType": "CHILD_OF",
                                "traceID": "A",
                                "spanID": "A",
                            },
                        ],
                    },
                    {
                        "traceID": "A",
                        "spanID": "P2",
                        "operationName": "relay-op",
                        "startTime": 20,
                        "duration": 50,
                        "processID": "err-prop-svc",
                        "warnings": None,
                        "references": [
                            {
                                "refType": "CHILD_OF",
                                "traceID": "A",
                                "spanID": "P1",
                            },
                        ],
                    },
                    {
                        "traceID": "A",
                        "spanID": "B",
                        "operationName": "O2",
                        "startTime": 30,
                        "duration": 40,
                        "processID": "S2",
                        "warnings": None,
                        "references": [
                            {
                                "refType": "CHILD_OF",
                                "traceID": "A",
                                "spanID": "P2",
                            },
                        ],
                        "tags": [
                            {
                                "key": "error",
                                "type": "bool",
                                "value": True,
                            },
                        ],
                    },
                ],
            },
        ],
    }

    return Graph(jsonData, "S1", "O1", "", "", filterProxy=True)


# This creates a graph with call tree:
# A ([S1] O1 TAG_K1:V1) -> B ([S2] O2 TAG_K2:V2) -> C ([S3] O3 TAG_K3:V3)
#                            -> D ([S4] O4 TAG_K4:V4)
# A runs 0-100
# B runs 10-70
# C runs 25-45
# D runs 30-50
def tagMatchGraph(tags):
    jsonData = {
        "data": [
            {
                "processes": {
                    "S1": {
                        "serviceName": "S1",
                        "tags": [],
                    },
                    "S2": {
                        "serviceName": "S2",
                        "tags": [],
                    },
                    "S3": {
                        "serviceName": "S3",
                        "tags": [],
                    },
                    "S4": {
                        "serviceName": "S4",
                        "tags": [],
                    },
                },
                "traceID": "A",
                "spans": [
                    {
                        "traceID": "A",
                        "spanID": "A",
                        "operationName": "O1",
                        "references": [],
                        "startTime": 0,
                        "duration": 100,
                        "processID": "S1",
                        "warnings": None,
                        "tags": [
                            {
                                "key": "k1",
                                "type": "string",
                                "value": "v1",
                            },
                        ],
                    },
                    {
                        "traceID": "A",
                        "spanID": "B",
                        "operationName": "O2",
                        "startTime": 10,
                        "duration": 60,
                        "processID": "S2",
                        "warnings": None,
                        "references": [
                            {
                                "refType": "CHILD_OF",
                                "traceID": "A",
                                "spanID": "A",
                            },
                        ],
                        "tags": [
                            {
                                "key": "k2",
                                "type": "string",
                                "value": "v2",
                            },
                        ],
                    },
                    {
                        "traceID": "A",
                        "spanID": "C",
                        "operationName": "O3",
                        "startTime": 25,
                        "duration": 20,
                        "processID": "S3",
                        "warnings": None,
                        "references": [
                            {
                                "refType": "CHILD_OF",
                                "traceID": "A",
                                "spanID": "B",
                            },
                        ],
                        "tags": [
                            {
                                "key": "k3",
                                "type": "string",
                                "value": "v3",
                            },
                        ],
                    },
                    {
                        "traceID": "A",
                        "spanID": "D",
                        "operationName": "O4",
                        "startTime": 30,
                        "duration": 20,
                        "processID": "S4",
                        "warnings": None,
                        "references": [
                            {
                                "refType": "CHILD_OF",
                                "traceID": "A",
                                "spanID": "B",
                            },
                        ],
                        "tags": [
                            {
                                "key": "k4",
                                "type": "string",
                                "value": "v4",
                            },
                        ],
                    },
                ],
            },
        ],
    }

    return Graph(
        data=jsonData,
        serviceName="S1",
        operationName="O1",
        filename="",
        rootTrace=True,  # Correctly set rootTrace as a boolean
        tags=tags,
        useParquet=False,
    )


class GraphTestCase(TestCase):
    def setUp(self):
        from crisp.utils import span_utils
        # Register generic proxy/errprop/test-trace names used by tests in this class.
        span_utils.PROXY_ONLY_OPS.append("proxy-relay")
        span_utils.ERR_PROP_SERVICE_OP_PAIRS.append(("err-prop-svc", "relay-op"))
        span_utils.TEST_TRACE_SERVICES.extend(
            ["test-svc-1", "test-executor", "test-accounts", "e2e-test-proxy"]
        )
        span_utils.TEST_TRACE_OP_PREFIXES.extend(
            ["[mytest.Root]", "[mytest.Action]", "[MYTEST.Run]", "[MYTEST.TestAccounts]", "[MYTEST.Teardown]"]
        )

    def tearDown(self):
        from crisp.utils import span_utils
        span_utils.PROXY_ONLY_OPS.remove("proxy-relay")
        span_utils.ERR_PROP_SERVICE_OP_PAIRS.remove(("err-prop-svc", "relay-op"))
        for s in ["test-svc-1", "test-executor", "test-accounts", "e2e-test-proxy"]:
            span_utils.TEST_TRACE_SERVICES.remove(s)
        for p in ["[mytest.Root]", "[mytest.Action]", "[MYTEST.Run]", "[MYTEST.TestAccounts]", "[MYTEST.Teardown]"]:
            span_utils.TEST_TRACE_OP_PREFIXES.remove(p)

    def test_proxyErrorProp(self):
        g = errprop_graph()
        assert g.rootNode.sid == "A"
        assert not g.rootNode.returnError
        children = list(g.rootNode.children.keys())
        assert len(children) == 1
        ch = children[0]
        assert ch.sid == "P1"
        assert ch.returnError
        ch = next(iter(ch.children.keys()))
        assert ch.sid == "P2"
        assert ch.returnError

    def test_proxy(self):
        g = proxy_graph()
        assert g.rootNode.sid == "A"
        children = list(g.rootNode.children.keys())
        assert len(children) == 1
        ch = children[0]
        assert ch.sid == "D"
        assert len(ch.children) == 0

    def test_isProxy(self):
        assert not graph.isProxyNode("x", "y")
        assert graph.isProxyNode("any-service", "proxy-relay")
        assert graph.isProxyNode("dummy", "proxy-relay")
        assert not graph.isProxyNode("proxy-relay", "dummy")

    def test_isErrorProp(self):
        assert not graph.isErrPropNode("x", "y")
        assert graph.isErrPropNode("err-prop-svc", "relay-op")

    def test_isTestTrace(self):
        assert graph.isTestTraceByServiceName("test-svc-1")
        assert graph.isTestTraceByServiceName("test-executor")
        assert graph.isTestTraceByServiceName("test-accounts")
        assert graph.isTestTraceByServiceName("e2e-test-proxy")
        assert not graph.isTestTraceByServiceName("foo")
        assert not graph.isTestTraceByServiceName("")

        assert graph.isTestTraceByOpName("[mytest.Root]")
        assert graph.isTestTraceByOpName("[mytest.Action] test:...")
        assert graph.isTestTraceByOpName("[mytest.Action] something")
        assert graph.isTestTraceByOpName("[MYTEST.Run] test:...")
        assert graph.isTestTraceByOpName("[MYTEST.TestAccounts] Populate")
        assert graph.isTestTraceByOpName("[MYTEST.Teardown]")
        assert not graph.isTestTraceByOpName("foo")
        assert not graph.isTestTraceByOpName("")

    def test_parseForErrorReturn(self):
        tag1 = [{"key": "error", "type": "bool", "value": True}]
        tag2 = [{"key": "http.status_code", "value": "403"}]
        tag3 = [{"key": "grpc.status", "value": "fail"}]
        log1 = [{"fields": [{"key": "error.object"}]}]
        log2 = [{"fields": [{"key": "error", "type": "string"}]}]
        log3 = [{"fields": [{"key": "event", "value": "error"}]}]

        m = sampleGraph()

        assert not m.parseForErrorReturn({}, [])
        assert m.parseForErrorReturn(tag1, [])
        assert m.parseForErrorReturn(tag2, [])
        assert m.parseForErrorReturn(tag3, [])
        assert m.parseForErrorReturn({}, log1)
        assert m.parseForErrorReturn({}, log2)
        assert m.parseForErrorReturn({}, log3)

    def test_isAcceptableParentChildDuration(self):
        p = graph.GraphNode(
            sid="A",
            startTime=0,
            duration=100,
            parentSpanId="",
            opName="O1",
            processID="P1",
            spanKind=graph.SpanKind.CLIENT,
            peerService=None,
            returnError=False,
        )
        c = graph.GraphNode(
            sid="B",
            startTime=1000,
            duration=100,
            parentSpanId="",
            opName="O2",
            processID="P2",
            spanKind=graph.SpanKind.SERVER,
            peerService=None,
            returnError=False,
        )

        c.duration = get_server_lengthening_factor() * p.duration - 1
        assert graph.isAcceptableParentChildDuration(p, c)

        c.duration = get_server_lengthening_factor() * p.duration + 1
        assert not graph.isAcceptableParentChildDuration(p, c)

    def test_isClientServerCall_with_invalid_span_combinations(self):
        p = graph.GraphNode(
            sid="A",
            startTime=0,
            duration=100,
            parentSpanId="",
            opName="O1",
            processID="P1",
            spanKind=graph.SpanKind.CLIENT,
            peerService=None,
            returnError=False,
        )
        c = graph.GraphNode(
            sid="B",
            startTime=1000,
            duration=10,
            parentSpanId="",
            opName="O2",
            processID="P2",
            spanKind=graph.SpanKind.SERVER,
            peerService=None,
            returnError=False,
        )
        d = graph.GraphNode(
            sid="C",
            startTime=0,
            duration=100,
            parentSpanId="",
            opName="O3",
            processID="P3",
            spanKind=graph.SpanKind.SERVER,
            peerService=None,
            returnError=False,
        )

        c.setParent(p)
        p.addChild(c)
        assert graph.isCleanClientServerCall(p, c)

        # client->client => false
        c.spanKind = graph.SpanKind.CLIENT
        p.spanKind = graph.SpanKind.CLIENT
        assert not graph.isCleanClientServerCall(p, c)

        # unknown->client => false
        p.spanKind = graph.SpanKind.UNKNOWN
        c.spanKind = graph.SpanKind.CLIENT
        assert not graph.isCleanClientServerCall(p, c)

        # unknown->unknown => false
        p.spanKind = graph.SpanKind.UNKNOWN
        c.spanKind = graph.SpanKind.UNKNOWN
        assert not graph.isCleanClientServerCall(p, c)

        # server->server => false
        c.spanKind = graph.SpanKind.SERVER
        c.spanKind = graph.SpanKind.SERVER
        assert not graph.isCleanClientServerCall(p, c)

        # unknown->server => false
        c.spanKind = graph.SpanKind.UNKNOWN
        c.spanKind = graph.SpanKind.SERVER
        assert not graph.isCleanClientServerCall(p, c)

        c.spanKind = graph.SpanKind.CLIENT
        c.spanKind = graph.SpanKind.SERVER
        # Multichild => false
        d.setParent(p)
        p.addChild(d)
        assert not graph.isClientServerCall({}, p, c)

    def test_isFuzzyServerCall(self):
        g = graph.GraphNode(
            sid="A",
            startTime=0,
            duration=1000,
            parentSpanId="",
            opName="O1",
            processID="P1",
            spanKind=graph.SpanKind.SERVER,
            peerService=None,
            returnError=False,
        )
        p = graph.GraphNode(
            sid="B",
            startTime=0,
            duration=100,
            parentSpanId="",
            opName="O2",
            processID="P2",
            spanKind=graph.SpanKind.CLIENT,
            peerService=None,
            returnError=False,
        )
        c = graph.GraphNode(
            sid="C",
            startTime=0,
            duration=10,
            parentSpanId="",
            opName="O3",
            processID="P3",
            spanKind=graph.SpanKind.SERVER,
            peerService=None,
            returnError=False,
        )
        d = graph.GraphNode(
            sid="C",
            startTime=0,
            duration=10,
            parentSpanId="",
            opName="O3",
            processID="P3",
            spanKind=graph.SpanKind.SERVER,
            peerService=None,
            returnError=False,
        )

        c.setParent(p)
        p.addChild(c)
        p.setParent(g)
        g.addChild(p)
        assert graph.isFuzzyClientServerCall(p, c)

        # client->unknown->server => True
        g.spanKind = graph.SpanKind.SERVER
        p.spanKind = graph.SpanKind.UNKNOWN
        c.spanKind = graph.SpanKind.SERVER
        assert graph.isFuzzyClientServerCall(p, c)

        # *->server->server => False
        p.spanKind = graph.SpanKind.SERVER
        c.spanKind = graph.SpanKind.SERVER
        assert not graph.isFuzzyClientServerCall(p, c)

        # unknown->unknown->server => False
        g.spanKind = graph.SpanKind.UNKNOWN
        p.spanKind = graph.SpanKind.UNKNOWN
        c.spanKind = graph.SpanKind.SERVER
        assert not graph.isFuzzyClientServerCall(p, c)

        # None->unknown->server => False
        p.parent = None
        p.spanKind = graph.SpanKind.UNKNOWN
        c.spanKind = graph.SpanKind.SERVER
        assert not graph.isFuzzyClientServerCall(p, c)

        p.parent = g

        # multichildren => false
        d.setParent(p)
        p.addChild(d)
        g.spanKind = graph.SpanKind.SERVER
        p.spanKind = graph.SpanKind.UNKNOWN
        c.spanKind = graph.SpanKind.SERVER
        assert not graph.isFuzzyClientServerCall(p, c)

    def test_onDifferentHosts(self):
        p = graph.GraphNode(
            sid="A",
            startTime=0,
            duration=100,
            parentSpanId="",
            opName="O1",
            processID="P1",
            spanKind=graph.SpanKind.UNKNOWN,
            peerService=None,
            returnError=False,
        )
        c = graph.GraphNode(
            sid="B",
            startTime=1000,
            duration=10,
            parentSpanId="",
            opName="O2",
            processID="P2",
            spanKind=graph.SpanKind.UNKNOWN,
            peerService=None,
            returnError=False,
        )
        d = graph.GraphNode(
            sid="C",
            startTime=0,
            duration=100,
            parentSpanId="",
            opName="O3",
            processID="P3",
            spanKind=graph.SpanKind.SERVER,
            peerService=None,
            returnError=False,
        )

        c.setParent(p)
        p.addChild(c)
        assert graph.onDifferentHosts({"P1": "H1", "P2": "H2"}, p, c)
        # On same host => false
        assert not graph.onDifferentHosts({"P1": "H1", "P2": "H1"}, p, c)
        # No host for parent => false
        assert not graph.onDifferentHosts({"P5": "H1", "P2": "H1"}, p, c)
        # No host for child => false
        assert not graph.onDifferentHosts({"P1": "H1", "P10": "H1"}, p, c)
        # No host for parent and child => false
        assert not graph.onDifferentHosts({}, p, c)
        # Multichild => false
        d.setParent(p)
        p.addChild(d)
        assert not graph.onDifferentHosts({"P1": "H1", "P2": "H2"}, p, c)

    def test_isClientServerCall_with_intermediate_unknown_span(self):
        p = graph.GraphNode(
            sid="A",
            startTime=0,
            duration=100,
            parentSpanId="",
            opName="O1",
            processID="P1",
            spanKind=graph.SpanKind.CLIENT,
            peerService=None,
            returnError=False,
        )
        c = graph.GraphNode(
            sid="B",
            startTime=1000,
            duration=10,
            parentSpanId="",
            opName="O2",
            processID="P2",
            spanKind=graph.SpanKind.SERVER,
            peerService=None,
            returnError=False,
        )

        c.setParent(p)
        p.addChild(c)
        assert graph.isClientServerCall({}, p, c)

        g = graph.GraphNode(
            sid="A",
            startTime=0,
            duration=1000,
            parentSpanId="",
            opName="O1",
            processID="P1",
            spanKind=graph.SpanKind.SERVER,
            peerService=None,
            returnError=False,
        )
        p = graph.GraphNode(
            sid="B",
            startTime=0,
            duration=100,
            parentSpanId="",
            opName="O2",
            processID="P2",
            spanKind=graph.SpanKind.UNKNOWN,
            peerService=None,
            returnError=False,
        )
        c = graph.GraphNode(
            sid="C",
            startTime=0,
            duration=10,
            parentSpanId="",
            opName="O3",
            processID="P3",
            spanKind=graph.SpanKind.SERVER,
            peerService=None,
            returnError=False,
        )

        c.setParent(p)
        p.addChild(c)
        p.setParent(g)
        g.addChild(p)
        assert graph.isClientServerCall({}, p, c)

        # *->server->server => False
        p.spanKind = graph.SpanKind.SERVER
        c.spanKind = graph.SpanKind.SERVER
        assert not graph.isClientServerCall({}, p, c)

        assert graph.isClientServerCall({"P2": "H1", "P3": "H2"}, p, c)

    def test_getCPSize(self):
        cct = {"A->B": 21, "A->B->C": 1}
        expect = 1 + 3 + 1 + 2 + 1 + 5 + 1 + 1
        assert graph.getCPSize(cct) == expect

    def test_GetRemainingTags(self):
        foundTags = [
            {common.TAG_NAME: "a", common.TAG_VALUE: "b", common.TAG_SEARCH_DEPTH: 5},
        ]
        allTags = [
            {common.TAG_NAME: "a", common.TAG_VALUE: "b", common.TAG_SEARCH_DEPTH: 5},
            {common.TAG_NAME: "a", common.TAG_VALUE: "b", common.TAG_SEARCH_DEPTH: 4},
            {common.TAG_NAME: "c", common.TAG_VALUE: "d", common.TAG_SEARCH_DEPTH: 10},
        ]
        remainingTags = graph.GetRemainingTags(foundTags, allTags, 5)
        expected = [
            {common.TAG_NAME: "c", common.TAG_VALUE: "d", common.TAG_SEARCH_DEPTH: 10},
        ]
        assert expected == remainingTags

    def test_GetMatchingTagsInTree(self):
        searchTags = [
            {common.TAG_NAME: "k1", common.TAG_VALUE: "v1", common.TAG_SEARCH_DEPTH: 1},
            {common.TAG_NAME: "k3", common.TAG_VALUE: "v3", common.TAG_SEARCH_DEPTH: 2},
            {
                common.TAG_NAME: "k4",
                common.TAG_VALUE: "v4",
                common.TAG_SEARCH_DEPTH: 10,
            },
        ]

        expected = [
            {common.TAG_NAME: "k1", common.TAG_VALUE: "v1", common.TAG_SEARCH_DEPTH: 1},
            {
                common.TAG_NAME: "k4",
                common.TAG_VALUE: "v4",
                common.TAG_SEARCH_DEPTH: 10,
            },
        ]
        g = tagMatchGraph(searchTags)
        assert expected == g.tags


class TestQuantizedMetrics(TestCase):
    def test_empty_histogram(self):
        instance = graph.QuantizedMetrics({})
        self.assertIsNone(instance.p100)
        self.assertIsNone(instance.p0)
        self.assertIsNone(instance.items)
        self.assertIsNone(instance.avg)
        self.assertIsNone(instance.p50)
        self.assertIsNone(instance.p90)
        self.assertIsNone(instance.p95)
        self.assertIsNone(instance.p99)

    def test_non_empty_histogram(self):
        h = {1: 5, 10: 8, 101: 1, 102: 2, 110: 3, 510: 2, 1000: 10, 2000: 1}

        data = sorted(
            [1] * 5
            + [10] * 8
            + [101] * 1
            + [102] * 2
            + [110] * 3
            + [510] * 2
            + [1000] * 10
            + [2000] * 1,
        )
        avg = sum(data) // len(data)
        p0 = data[0]
        p100 = data[-1]
        p50 = data[int(len(data) * 0.5)]
        p90 = data[int(len(data) * 0.9)]
        p95 = data[int(len(data) * 0.95)]
        p99 = data[int(len(data) * 0.99)]

        q = graph.QuantizedMetrics(h)
        self.assertEqual(2000, q.p100)
        self.assertEqual(1, q.p0)
        self.assertEqual(q.items, len(data))
        avg = 0
        items = 0
        for k, v in h.items():
            avg += k * v
            items += v

        self.assertEqual(int(avg / items), int(q.avg))
        self.assertEqual(p0, q.p0)
        self.assertEqual(p50, q.p50)
        self.assertEqual(p90, q.p90)
        self.assertEqual(p95, q.p95)
        self.assertEqual(p99, q.p99)
        self.assertEqual(p100, q.p100)

    @mock.patch('os.path.exists', return_value=False)
    def test_graph_init_file_does_not_exist(self, _mock_exists):
        # Arrange
        data = mock.MagicMock()
        serviceName = "TestService"
        operationName = "TestOperation"
        filename = "non_existent_file.json"

        # Act
        g = graph.Graph(
            data=data,
            serviceName=serviceName,
            operationName=operationName,
            filename=filename
        )

        # Assert
        self.assertEqual(g.filesz, 0)  # Since the file doesn't exist, size should be 0

    @mock.patch('os.path.exists', return_value=True)
    @mock.patch('os.path.getsize', return_value=1024)
    @mock.patch('crisp.graph.Graph.parseNode', side_effect=Exception("JSON parsing error"))
    @mock.patch('logging.warning')
    def test_graph_parse_exception(self, mock_logging_warning, mock_parseNode, _mock_getsize, _mock_exists):
        # Arrange
        data = mock.MagicMock()
        serviceName = "TestService"
        operationName = "TestOperation"
        filename = "test.json"

        # Act
        g = graph.Graph(
            data=data,
            serviceName=serviceName,
            operationName=operationName,
            filename=filename,
            useParquet=False
        )

        # Assert
        mock_parseNode.assert_called_once_with(data)
        mock_logging_warning.assert_any_call("self.parseNode failed in file test.json!")
        mock_logging_warning.assert_any_call("Exception: JSON parsing error")
        self.assertIsNone(g.rootNode)  # Should handle the exception gracefully

    @mock.patch('os.path.exists', return_value=True)
    @mock.patch('os.path.getsize', return_value=1024)
    @mock.patch('crisp.graph.Graph.parseNodeFromParquet', side_effect=Exception("Parquet parsing error"))
    @mock.patch('logging.warning')
    def test_graph_parquet_parse_exception(self, mock_logging_warning, mock_parseNodeFromParquet, _mock_getsize, _mock_exists):
        # Arrange
        data = mock.MagicMock()
        serviceName = "TestService"
        operationName = "TestOperation"
        filename = "test.parquet"

        # Act
        g = graph.Graph(
            data=data,
            serviceName=serviceName,
            operationName=operationName,
            filename=filename,
            useParquet=True
        )

        # Assert
        mock_parseNodeFromParquet.assert_called_once_with(data)
        mock_logging_warning.assert_any_call("self.parseNode failed in file test.parquet!")
        mock_logging_warning.assert_any_call("Exception: Parquet parsing error")
        self.assertIsNone(g.rootNode)  # Should handle the exception gracefully

    @mock.patch('os.path.exists', return_value=True)
    @mock.patch('os.path.getsize', return_value=1024)
    @mock.patch('crisp.graph.Graph.checkRootAndWarn', return_value=True)  # Mock checkRootAndWarn
    @mock.patch('crisp.graph.Graph.parseNode')
    @mock.patch('crisp.graph.Graph.parseNodeFromParquet')
    def test_graph_init_with_parquet(self, mock_parseNodeFromParquet, mock_parseNode, mock_checkRootAndWarn, _mock_getsize, _mock_exists):
        # Arrange
        data = mock.MagicMock()
        serviceName = "TestService"
        operationName = "TestOperation"
        filename = "test.parquet"

        # Create a mock root node with necessary attributes
        mock_root_node = mock.MagicMock()
        mock_root_node.pid = "S1"
        mock_root_node.opName = operationName

        # Mock the Parquet parsing function to return a valid root node
        mock_parseNodeFromParquet.return_value = ([mock_root_node], False)

        # Act
        g = graph.Graph(
            data=data,
            serviceName=serviceName,
            operationName=operationName,
            filename=filename,
            useParquet=True
        )

        # Assert
        mock_parseNodeFromParquet.assert_called_once_with(data)
        mock_parseNode.assert_not_called()  # parseNode should not be called
        mock_checkRootAndWarn.assert_called_once_with(mock_root_node, filename, True)  # checkRootAndWarn should be called
        self.assertEqual(g.filename, filename)
        self.assertEqual(g.filesz, 1024)
        self.assertEqual(g.rootNode, mock_root_node)  # Check that the root node is set


class TestParseForErrorReturnFromParquet(TestCase):
    @mock.patch('os.path.exists', return_value=True)
    @mock.patch('os.path.getsize', return_value=1024)
    def setUp(self, _mock_getsize, _mock_exists):
        # Initialize a Graph for testing
        self.graph = Graph([], "testService", "testOperation", filename="testfile.txt", skipInitializationForTest=True)

    def test_http_status_code_400(self):
        # Test typical HTTP error code (>= 400)
        self.assertTrue(self.graph.parseForErrorReturnFromParquet(statusCode=400, error=False, _errorMessage=None, rpcSystem="http"))

    def test_http_status_code_200(self):
        # Test HTTP success code
        self.assertFalse(self.graph.parseForErrorReturnFromParquet(statusCode=200, error=False, _errorMessage=None, rpcSystem="http"))

    def test_grpc_status_error(self):
        # Test gRPC non-zero status (error)
        self.assertTrue(self.graph.parseForErrorReturnFromParquet(statusCode=1, error=False, _errorMessage=None, rpcSystem="grpc"))

    def test_grpc_status_success(self):
        # Test gRPC zero status (success)
        self.assertFalse(self.graph.parseForErrorReturnFromParquet(statusCode=0, error=False, _errorMessage=None, rpcSystem="grpc"))

    def test_error_flag(self):
        # Test when the error flag is True
        self.assertTrue(self.graph.parseForErrorReturnFromParquet(statusCode=200, error=True, _errorMessage=None, rpcSystem="http"))

    def test_no_error_conditions(self):
        # Test when none of the error conditions are met
        self.assertFalse(self.graph.parseForErrorReturnFromParquet(statusCode=200, error=False, _errorMessage=None, rpcSystem="http"))

class MockDict(dict):
    def __getitem__(self, key):
        if key == "B":
            raise KeyError("B")
        return super().__getitem__(key)

class TestBuildParentChildRelationships(TestCase):

    @mock.patch('os.path.exists', return_value=True)
    @mock.patch('os.path.getsize', return_value=1024)
    def setUp(self, _mock_getsize, _mock_exists):
        # Initialize a mock Graph for testing, passing a valid filename
        self.graph = Graph([], "testService", "testOperation", filename="mockfile.txt", skipInitializationForTest=True)
        self.graph.nodeHT = {}
        self.graph.proxyNodes = {}

    def test_no_parent(self):
        # Arrange: A node with no parentSpanId
        node = mock.MagicMock()
        node.parentSpanId = None
        self.graph.nodeHT["A"] = node

        potentialRoots = []

        # Act
        self.graph.buildParentChildRelationships(potentialRoots)

        # Assert: Node "A" should be added to potentialRoots
        self.assertIn(node, potentialRoots)
        node.setParent.assert_not_called()  # No parent should be set
        node.addChild.assert_not_called()   # No children should be added

    def test_parent_not_in_nodeHT(self):
        # Arrange: A node whose parent is not in nodeHT
        node = mock.MagicMock()
        node.parentSpanId = "B"
        self.graph.nodeHT["A"] = node

        potentialRoots = []

        # Act
        self.graph.buildParentChildRelationships(potentialRoots)

        # Assert: Node "A" should be added to potentialRoots as parent is missing
        self.assertIn(node, potentialRoots)
        node.setParent.assert_not_called()
        node.addChild.assert_not_called()

    def test_proxy_node_with_no_parent(self):
        # Arrange: A proxy node whose parent is not in nodeHT
        node = mock.MagicMock()
        node.parentSpanId = "B"
        self.graph.nodeHT["A"] = node

        proxyNode = mock.MagicMock()
        proxyNode.parentSpanId = "C"
        self.graph.nodeHT["B"] = proxyNode
        self.graph.proxyNodes = {"B": 0}  # B is a proxy node

        potentialRoots = []

        # Act
        self.graph.buildParentChildRelationships(potentialRoots)

        # Assert: Node "A" should be added to potentialRoots as the proxy parent's parent is missing
        self.assertIn(node, potentialRoots)
        self.assertEqual(self.graph.numProxyRoots, 1)

class TestParseNodeFromParquet(TestCase):

    @mock.patch('os.path.exists', return_value=True)
    @mock.patch('os.path.getsize', return_value=1024)
    @mock.patch('crisp.graph.Graph.buildParentChildRelationships')
    @mock.patch('crisp.graph.Graph.propagateErrorsToRoots')
    @mock.patch('crisp.graph.Graph.storeNodeData')
    @mock.patch('crisp.graph.Graph.parseForErrorReturnFromParquet', return_value=False)
    def test_parse_node_from_parquet_success(self, _mock_parse_error, mock_store_node_data, mock_propagate_errors,
                                             mock_build_parent_child_relationships, _mock_getsize, _mock_exists):
        # Arrange
        g = Graph([], "testService", "testOperation", filename="mockfile.txt", skipInitializationForTest=True)

        # Corrected Sample parquetData input
        parquetData = {
            'span_set': [  # Corrected key
                {
                    'process': {  # Corrected key
                        'service_name': 'service1',  # Corrected key
                        'host_name': 'host1'  # Corrected key
                    },
                    'spans': [  # Corrected key
                        {
                            'span_id': 1,  # Corrected key
                            'parent_span_id': None,
                            'start_time_unix_nano': 1000,
                            'duration_nano': 500,
                            'kind': 1,
                            'operation_name': 'operation1',
                            'status_code': 200,
                        },
                        {
                            'span_id': 2,
                            'parent_span_id': 1,
                            'start_time_unix_nano': 2000,
                            'duration_nano': 300,
                            'kind': 2,
                            'operation_name': 'operation2',
                            'status_code': 200,
                        },
                    ]
                }
            ]
        }

        # Act
        potentialRoots, isCtfTest = g.parseNodeFromParquet(parquetData)

        # Debugging: Print out the contents of potentialRoots
        print(f"Potential roots: {potentialRoots}")

        # Assert
        self.assertFalse(isCtfTest)
        mock_store_node_data.assert_called()
        mock_build_parent_child_relationships.assert_called_once()
        mock_propagate_errors.assert_called_once()

    @mock.patch('os.path.exists', return_value=True)
    @mock.patch('os.path.getsize', return_value=1024)
    @mock.patch('crisp.graph.Graph.buildParentChildRelationships')
    @mock.patch('crisp.graph.Graph.propagateErrorsToRoots')
    @mock.patch('crisp.graph.Graph.storeNodeData')
    @mock.patch('crisp.graph.Graph.parseForErrorReturnFromParquet', return_value=True)
    def test_parse_node_from_parquet_with_errors(self, _mock_parse_error, mock_store_node_data, mock_propagate_errors,
                                                 mock_build_parent_child_relationships, _mock_getsize, _mock_exists):
        # Arrange
        g = Graph([], "testService", "testOperation", filename="mockfile.txt", skipInitializationForTest=True)

        # Corrected Sample parquetData with spans that have errors
        parquetData = {
            'span_set': [  # Corrected key
                {
                    'process': {  # Corrected key
                        'service_name': 'service1',  # Corrected key
                        'host_name': 'host1'  # Corrected key
                    },
                    'spans': [  # Corrected key
                        {
                            'span_id': 1,  # Corrected key
                            'parent_span_id': None,
                            'start_time_unix_nano': 1000,
                            'duration_nano': 500,
                            'kind': 1,
                            'operation_name': 'operation1',
                            'status_code': 500,  # Status code triggering an error
                        }
                    ]
                }
            ]
        }

        # Act
        potentialRoots, isCtfTest = g.parseNodeFromParquet(parquetData)

        # Assert
        self.assertEqual(g.numErrors, 1)  # Since the mock returns True for errors
        mock_store_node_data.assert_called()
        mock_build_parent_child_relationships.assert_called_once()
        mock_propagate_errors.assert_called_once()


class TestExtractRootSpanMetadata(TestCase):
    """Test cases for extract_root_span_metadata function."""

    def test_extract_root_span_metadata_with_valid_trace(self):
        """Test extracting metadata from a valid trace with root span."""
        trace_data = {
            "data": [
                {
                    "processes": {
                        "p1": {"serviceName": "test-service"},
                        "p2": {"serviceName": "other-service"}
                    },
                    "spans": [
                        {
                            "spanID": "root-span",
                            "processID": "p1",
                            "operationName": "GET /api/test",
                            "references": []  # No parent references = root span
                        },
                        {
                            "spanID": "child-span",
                            "processID": "p2",
                            "operationName": "db.query",
                            "references": [{"refType": "CHILD_OF", "spanID": "root-span"}]
                        }
                    ]
                }
            ]
        }

        service_name, operation_name = graph.extract_root_span_metadata(trace_data)

        self.assertEqual(service_name, "test-service")
        self.assertEqual(operation_name, "GET /api/test")

    def test_extract_root_span_metadata_with_multiple_roots(self):
        """Test extracting metadata when multiple root spans exist."""
        trace_data = {
            "data": [
                {
                    "processes": {
                        "p1": {"serviceName": "first-service"},
                        "p2": {"serviceName": "second-service"}
                    },
                    "spans": [
                        {
                            "spanID": "root1",
                            "processID": "p1",
                            "operationName": "operation1",
                            "references": []
                        },
                        {
                            "spanID": "root2",
                            "processID": "p2",
                            "operationName": "operation2",
                            "references": []
                        }
                    ]
                }
            ]
        }

        service_name, operation_name = graph.extract_root_span_metadata(trace_data)

        # Should return the first root span found
        self.assertEqual(service_name, "first-service")
        self.assertEqual(operation_name, "operation1")

    def test_extract_root_span_metadata_with_missing_process_name(self):
        """Test extracting metadata when process name is missing."""
        trace_data = {
            "data": [
                {
                    "processes": {
                        "p1": {}  # Missing serviceName
                    },
                    "spans": [
                        {
                            "spanID": "root-span",
                            "processID": "p1",
                            "operationName": "test-operation",
                            "references": []
                        }
                    ]
                }
            ]
        }

        service_name, operation_name = graph.extract_root_span_metadata(trace_data)

        self.assertEqual(service_name, "")
        self.assertEqual(operation_name, "test-operation")

    def test_extract_root_span_metadata_with_missing_operation_name(self):
        """Test extracting metadata when operation name is missing."""
        trace_data = {
            "data": [
                {
                    "processes": {
                        "p1": {"serviceName": "test-service"}
                    },
                    "spans": [
                        {
                            "spanID": "root-span",
                            "processID": "p1",
                            "references": []
                            # Missing operationName
                        }
                    ]
                }
            ]
        }

        service_name, operation_name = graph.extract_root_span_metadata(trace_data)

        self.assertEqual(service_name, "test-service")
        self.assertEqual(operation_name, "")

    def test_extract_root_span_metadata_with_no_root_spans(self):
        """Test extracting metadata when no root spans exist."""
        trace_data = {
            "data": [
                {
                    "processes": {
                        "p1": {"serviceName": "test-service"}
                    },
                    "spans": [
                        {
                            "spanID": "child-span",
                            "processID": "p1",
                            "operationName": "child-operation",
                            "references": [{"refType": "CHILD_OF", "spanID": "missing-parent"}]
                        }
                    ]
                }
            ]
        }

        service_name, operation_name = graph.extract_root_span_metadata(trace_data)

        # Should fallback to defaults
        self.assertEqual(service_name, "")
        self.assertEqual(operation_name, "")

    def test_extract_root_span_metadata_with_empty_trace(self):
        """Test extracting metadata from empty trace data."""
        trace_data = {"data": []}

        service_name, operation_name = graph.extract_root_span_metadata(trace_data)

        # Should fallback to defaults
        self.assertEqual(service_name, "")
        self.assertEqual(operation_name, "")

    def test_extract_root_span_metadata_with_malformed_trace(self):
        """Test extracting metadata from malformed trace data."""
        trace_data = {"invalid": "structure"}

        service_name, operation_name = graph.extract_root_span_metadata(trace_data)

        # Should fallback to defaults when parsing fails
        self.assertEqual(service_name, "")
        self.assertEqual(operation_name, "")

    def test_extract_root_span_metadata_with_missing_process_id(self):
        """Test extracting metadata when span references non-existent process ID."""
        trace_data = {
            "data": [
                {
                    "processes": {
                        "p1": {"serviceName": "test-service"}
                    },
                    "spans": [
                        {
                            "spanID": "root-span",
                            "processID": "p999",  # Non-existent process ID
                            "operationName": "test-operation",
                            "references": []
                        }
                    ]
                }
            ]
        }

        service_name, operation_name = graph.extract_root_span_metadata(trace_data)

        self.assertEqual(service_name, "")  # Fallback when process not found
        self.assertEqual(operation_name, "test-operation")


class TestCheckResults(TestCase):
    """Test the checkResults method which validates testing section."""

    def test_checkResults_with_valid_testing_data(self):
        """Test checkResults correctly validates opTimeExclusive using getLeafNodeFromCallPath."""
        from crisp.shared.models import MetricVals  # noqa: PLC0415
        from crisp.shared.models import ErrorCPMetrics  # noqa: PLC0415

        # Create a Graph instance with skipInitializationForTest=True
        g = Graph(
            data="test-trace",  # traceID
            serviceName="test-service",
            operationName="test-op",
            filename="test.json",
            skipInitializationForTest=True
        )

        # Set up testing expectations
        g.setTestResult({
            "opTimeExclusive": {
                "O1": 100,
                "O2": 50,
            },
            "totalWork": 200,
            "timeSavedOnWork": 30,
            "timeSavedOnCP": 25,
        })

        # Create call path profile with full paths
        # The checkResults method should use getLeafNodeFromCallPath to extract just the operation name
        cpp_dict = {
            "[S1] O1": MetricVals(inc=100, excl=100, freq=1, sid="trace1"),
            "[S1] O1->[S2] O2": MetricVals(inc=50, excl=50, freq=1, sid="trace1"),
        }

        # Create ErrorCPMetrics with required parameters
        errorCPMetrics = ErrorCPMetrics(
            errCPCallpathTimeExclusive={},
            errCPErrCounts={},
            savingPotential=0,
            numCPErrors=0,
            numRelatedToCPErrors=0
        )

        # Call checkResults - should use getLeafNodeFromCallPath at line 469
        result = g.checkResults(
            cpp=cpp_dict,
            work=200,
            timeSavedOnW=30,
            timeSavedOnCPAllSeries=25,
            errorCPMetrics=errorCPMetrics
        )

        # Should return True if validation passes
        self.assertTrue(result)

    def test_checkResults_returns_none_when_no_testing_data(self):
        """Test checkResults returns None when testing section is empty."""
        from crisp.shared.models import ErrorCPMetrics  # noqa: PLC0415

        g = Graph(
            data="test-trace",  # traceID
            serviceName="test-service",
            operationName="test-op",
            filename="test.json",
            skipInitializationForTest=True
        )
        # Don't set testing data (defaults to {})

        errorCPMetrics = ErrorCPMetrics(
            errCPCallpathTimeExclusive={},
            errCPErrCounts={},
            savingPotential=0,
            numCPErrors=0,
            numRelatedToCPErrors=0
        )

        result = g.checkResults(
            cpp={},
            work=0,
            timeSavedOnW=0,
            timeSavedOnCPAllSeries=0,
            errorCPMetrics=errorCPMetrics
        )

        # Should return None when no testing data
        self.assertIsNone(result)

    def test_checkResults_aggregates_duplicate_operations(self):
        """Test that checkResults correctly aggregates operations with same name from different call paths."""
        from crisp.shared.models import MetricVals  # noqa: PLC0415
        from crisp.shared.models import ErrorCPMetrics  # noqa: PLC0415

        g = Graph(
            data="test-trace",  # traceID
            serviceName="test-service",
            operationName="test-op",
            filename="test.json",
            skipInitializationForTest=True
        )

        # Set up testing expectations - O2 should have total of 75 (50+25)
        g.setTestResult({
            "opTimeExclusive": {
                "O1": 100,
                "O2": 75,  # Sum of two different call paths
            },
            "totalWork": 200,
            "timeSavedOnWork": 0,
            "timeSavedOnCP": 0,
        })

        # Create call path profile with multiple paths ending in O2
        cpp_dict = {
            "[S1] O1": MetricVals(inc=100, excl=100, freq=1, sid="trace1"),
            "[S1] O1->[S2] O2": MetricVals(inc=50, excl=50, freq=1, sid="trace1"),
            "[S1] O1->[S3] O2": MetricVals(inc=25, excl=25, freq=1, sid="trace1"),
        }

        errorCPMetrics = ErrorCPMetrics(
            errCPCallpathTimeExclusive={},
            errCPErrCounts={},
            savingPotential=0,
            numCPErrors=0,
            numRelatedToCPErrors=0
        )

        # Should correctly aggregate O2: 50 + 25 = 75
        result = g.checkResults(
            cpp=cpp_dict,
            work=200,
            timeSavedOnW=0,
            timeSavedOnCPAllSeries=0,
            errorCPMetrics=errorCPMetrics
        )

        self.assertTrue(result)
