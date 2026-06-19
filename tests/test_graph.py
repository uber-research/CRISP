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


def sampleGraph2():
    # graph from test_cases/25.json
    # then added X and Y spans that are logically in parallel with B->C with the same service and op
    # X and Y should not show up in any of the error counts nor critical path / error critical path
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
                },
                "traceID": "A",
                "spans": [
                    {
                        "traceID": "A",
                        "spanID": "A",
                        "operationName": "O1",
                        "startTime": 0,
                        "duration": 700,
                        "references": [],
                        "processID": "S1",
                    },
                    {
                        "traceID": "A",
                        "spanID": "B",
                        "operationName": "O2",
                        "startTime": 50,
                        "duration": 300,
                        "processID": "S2",
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
                        "startTime": 100,
                        "duration": 50,
                        "processID": "S3",
                        "tags": [
                            {
                                "key": "http.status_code",
                                "type": "int64",
                                "value": 404,
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
                    {
                        "traceID": "A",
                        "spanID": "D",
                        "operationName": "O3",
                        "startTime": 200,
                        "duration": 50,
                        "processID": "S3",
                        "tags": [
                            {
                                "key": "http.status_code",
                                "type": "int64",
                                "value": 404,
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
                    {
                        "traceID": "A",
                        "spanID": "E",
                        "operationName": "O3",
                        "startTime": 300,
                        "duration": 50,
                        "processID": "S3",
                        "tags": [
                            {
                                "key": "http.status_code",
                                "type": "int64",
                                "value": 504,
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
                    {
                        "traceID": "A",
                        "spanID": "F",
                        "operationName": "O2",
                        "startTime": 400,
                        "duration": 300,
                        "processID": "S2",
                        "tags": [
                            {
                                "key": "http.status_code",
                                "type": "int64",
                                "value": 404,
                            },
                        ],
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
                        "spanID": "G",
                        "operationName": "O3",
                        "startTime": 450,
                        "duration": 50,
                        "processID": "S3",
                        "references": [
                            {
                                "refType": "CHILD_OF",
                                "traceID": "A",
                                "spanID": "F",
                            },
                        ],
                    },
                    {
                        "traceID": "A",
                        "spanID": "H",
                        "operationName": "O3",
                        "startTime": 550,
                        "duration": 50,
                        "processID": "S3",
                        "references": [
                            {
                                "refType": "CHILD_OF",
                                "traceID": "A",
                                "spanID": "F",
                            },
                        ],
                    },
                    {
                        "traceID": "A",
                        "spanID": "I",
                        "operationName": "O3",
                        "startTime": 650,
                        "duration": 50,
                        "processID": "S3",
                        "references": [
                            {
                                "refType": "CHILD_OF",
                                "traceID": "A",
                                "spanID": "F",
                            },
                        ],
                    },
                    {
                        "traceID": "A",
                        "spanID": "X",
                        "operationName": "O2",
                        "startTime": 100,
                        "duration": 100,
                        "processID": "S2",
                        "tags": [
                            {
                                "key": "http.status_code",
                                "type": "int64",
                                "value": 404,
                            },
                        ],
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
                        "spanID": "Y",
                        "operationName": "O3",
                        "startTime": 150,
                        "duration": 50,
                        "processID": "S3",
                        "tags": [
                            {
                                "key": "http.status_code",
                                "type": "int64",
                                "value": 404,
                            },
                        ],
                        "references": [
                            {
                                "refType": "CHILD_OF",
                                "traceID": "A",
                                "spanID": "X",
                            },
                        ],
                    },
                ],
            },
        ],
    }

    return Graph(jsonData, "S1", "O1", "", "")


def sampleGraph3():
    # same as sampleGraph2 except that all errors along the critical path except for C
    # are removed (i.e., spans D, E, and F).  Errors outside of critical path (X and Y) stay the same.
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
                },
                "traceID": "A",
                "spans": [
                    {
                        "traceID": "A",
                        "spanID": "A",
                        "operationName": "O1",
                        "startTime": 0,
                        "duration": 700,
                        "references": [],
                        "processID": "S1",
                    },
                    {
                        "traceID": "A",
                        "spanID": "B",
                        "operationName": "O2",
                        "startTime": 50,
                        "duration": 300,
                        "processID": "S2",
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
                        "startTime": 100,
                        "duration": 50,
                        "processID": "S3",
                        "tags": [
                            {
                                "key": "http.status_code",
                                "type": "int64",
                                "value": 404,
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
                    {
                        "traceID": "A",
                        "spanID": "D",
                        "operationName": "O3",
                        "startTime": 200,
                        "duration": 50,
                        "processID": "S3",
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
                        "spanID": "E",
                        "operationName": "O3",
                        "startTime": 300,
                        "duration": 50,
                        "processID": "S3",
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
                        "spanID": "F",
                        "operationName": "O2",
                        "startTime": 400,
                        "duration": 300,
                        "processID": "S2",
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
                        "spanID": "G",
                        "operationName": "O3",
                        "startTime": 450,
                        "duration": 50,
                        "processID": "S3",
                        "references": [
                            {
                                "refType": "CHILD_OF",
                                "traceID": "A",
                                "spanID": "F",
                            },
                        ],
                    },
                    {
                        "traceID": "A",
                        "spanID": "H",
                        "operationName": "O3",
                        "startTime": 550,
                        "duration": 50,
                        "processID": "S3",
                        "references": [
                            {
                                "refType": "CHILD_OF",
                                "traceID": "A",
                                "spanID": "F",
                            },
                        ],
                    },
                    {
                        "traceID": "A",
                        "spanID": "I",
                        "operationName": "O3",
                        "startTime": 650,
                        "duration": 50,
                        "processID": "S3",
                        "references": [
                            {
                                "refType": "CHILD_OF",
                                "traceID": "A",
                                "spanID": "F",
                            },
                        ],
                    },
                    {
                        "traceID": "A",
                        "spanID": "X",
                        "operationName": "O2",
                        "startTime": 100,
                        "duration": 100,
                        "processID": "S2",
                        "tags": [
                            {
                                "key": "http.status_code",
                                "type": "int64",
                                "value": 404,
                            },
                        ],
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
                        "spanID": "Y",
                        "operationName": "O3",
                        "startTime": 150,
                        "duration": 50,
                        "processID": "S3",
                        "tags": [
                            {
                                "key": "http.status_code",
                                "type": "int64",
                                "value": 404,
                            },
                        ],
                        "references": [
                            {
                                "refType": "CHILD_OF",
                                "traceID": "A",
                                "spanID": "X",
                            },
                        ],
                    },
                ],
            },
        ],
    }

    return Graph(jsonData, "S1", "O1", "", "")


def timeskew_graph():
    """
    A->B->C call chain.
    C has time skew.
    C and B error out.
    """
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
                        "operationName": "O2",
                        "startTime": 10,
                        "duration": 50,
                        "processID": "S1",
                        "references": [
                            {
                                "refType": "CHILD_OF",
                                "traceID": "A",
                                "spanID": "A",
                            },
                        ],
                        "tags": [
                            {
                                "key": "error",
                                "type": "bool",
                                "value": True,
                            },
                            {
                                "key": "span.kind",
                                "type": "string",
                                "value": "client",
                            },
                        ],
                    },
                    {
                        "traceID": "A",
                        "spanID": "C",
                        "operationName": "O3",
                        "startTime": 1000,
                        "duration": 30,
                        "processID": "S2",
                        "references": [
                            {
                                "refType": "CHILD_OF",
                                "traceID": "A",
                                "spanID": "B",
                            },
                        ],
                        "tags": [
                            {
                                "key": "error",
                                "type": "bool",
                                "value": True,
                            },
                            {
                                "key": "span.kind",
                                "type": "string",
                                "value": "server",
                            },
                        ],
                    },
                ],
            },
        ],
    }

    return Graph(jsonData, "S1", "O1", "", "")


def mild_overlap_graph():
    """
    A->B
     ->C
    B and C overlap within the level of tolerance
    """
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
                },
                "traceID": "A",
                "spans": [
                    {
                        "traceID": "A",
                        "spanID": "A",
                        "operationName": "O1",
                        "references": [],
                        "startTime": 0,
                        "duration": 1000,
                        "processID": "S1",
                    },
                    {
                        "traceID": "A",
                        "spanID": "B",
                        "operationName": "O2",
                        "startTime": 0,
                        "duration": 500,
                        "processID": "S2",
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
                        "startTime": 495,
                        "duration": 505,
                        "processID": "S2",
                        "references": [
                            {
                                "refType": "CHILD_OF",
                                "traceID": "A",
                                "spanID": "A",
                            },
                        ],
                    },
                ],
            },
        ],
    }

    return Graph(jsonData, "S1", "O1", "", "")


def root_error_graph():
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
                        "tags": [
                            {
                                "key": "error",
                                "type": "bool",
                                "value": True,
                            },
                        ],
                    },
                    {
                        "traceID": "A",
                        "spanID": "B",
                        "operationName": "O2",
                        "startTime": 70,
                        "duration": 30,
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
                ],
            },
        ],
    }

    return Graph(jsonData, "S1", "O1", "", "")


def excludeGraph():
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
                        "operationName": "O2",
                        "startTime": 10,
                        "duration": 60,
                        "processID": "S2",
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
    exclude = {("S4", "O4")}
    return Graph(jsonData, "S1", "O1", "", "", exclusionSet=exclude)


def crossRegionSampleGraph():
    """Creates a sample graph with cross-region calls for testing"""
    jsonData = {
        "data": [
            {
                "processes": {
                    "P1": {
                        "serviceName": "ServiceA",
                        "tags": [
                            {"key": "region", "value": "us-west-1"},
                            {"key": "hostname", "value": "host1"}
                        ],
                    },
                    "P2": {
                        "serviceName": "ServiceB",
                        "tags": [
                            {"key": "region", "value": "us-east-1"},
                            {"key": "hostname", "value": "host2"}
                        ],
                    },
                    "P3": {
                        "serviceName": "ServiceC",
                        "tags": [
                            {"key": "zone", "value": "eu-west-1a"},
                            {"key": "hostname", "value": "host3"}
                        ],
                    },
                    "P4": {
                        "serviceName": "ServiceD",
                        "tags": [
                            {"key": "region", "value": "us-west-1"},
                            {"key": "hostname", "value": "host4"}
                        ],
                    },
                    "P5": {
                        "serviceName": "ServiceE",
                        "tags": [
                            {"key": "hostname", "value": "host5"}
                        ],
                    },
                },
                "traceID": "T1",
                "spans": [
                    {
                        "traceID": "T1",
                        "spanID": "S1",
                        "operationName": "GetUser",
                        "startTime": 0,
                        "duration": 1000,
                        "references": [],
                        "processID": "P1",
                    },
                    {
                        "traceID": "T1",
                        "spanID": "S2",
                        "operationName": "GetUser",
                        "startTime": 100,
                        "duration": 200,
                        "processID": "P2",
                        "references": [
                            {
                                "refType": "CHILD_OF",
                                "traceID": "T1",
                                "spanID": "S1",
                            },
                        ],
                    },
                    {
                        "traceID": "T1",
                        "spanID": "S3",
                        "operationName": "GetUser",
                        "startTime": 150,
                        "duration": 100,
                        "processID": "P3",
                        "references": [
                            {
                                "refType": "CHILD_OF",
                                "traceID": "T1",
                                "spanID": "S2",
                            },
                        ],
                    },
                    {
                        "traceID": "T1",
                        "spanID": "S4",
                        "operationName": "GetUser",
                        "startTime": 300,
                        "duration": 50,
                        "processID": "P4",
                        "references": [
                            {
                                "refType": "CHILD_OF",
                                "traceID": "T1",
                                "spanID": "S1",
                            },
                        ],
                    },
                    {
                        "traceID": "T1",
                        "spanID": "S5",
                        "operationName": "GetData",
                        "startTime": 400,
                        "duration": 75,
                        "processID": "P2",
                        "references": [
                            {
                                "refType": "CHILD_OF",
                                "traceID": "T1",
                                "spanID": "S1",
                            },
                        ],
                    },
                    {
                        "traceID": "T1",
                        "spanID": "S6",
                        "operationName": "GetUser",
                        "startTime": 500,
                        "duration": 25,
                        "processID": "P5",
                        "references": [
                            {
                                "refType": "CHILD_OF",
                                "traceID": "T1",
                                "spanID": "S1",
                            },
                        ],
                    },
                ],
            },
        ],
    }

    return Graph(jsonData, "ServiceA", "GetUser", "", "")


class GraphTestCaseDeferred(TestCase):
    """Tests unblocked in PR 9d — previously deferred."""

    def test_findCriticalPath(self):
        cp = sampleGraph().findCriticalPath()
        assert len(cp) == 3
        assert cp[0].sid == "A"
        assert cp[1].sid == "B"
        assert cp[2].sid == "D"

    def test_findErrorsOnCriticalPath(self):
        fullErrCP = sampleGraph().findErrorsOnCriticalPath()

        assert len(fullErrCP) == 3
        assert fullErrCP[0].sid == "A"
        assert fullErrCP[1].sid == "B"
        assert fullErrCP[2].sid == "D"

    def test_computeTimeSaved(self):
        (
            work,
            timeSavedOnW,
            tsPessimistic,
            tsOptimistic,
            tsAllSeries,
        ) = sampleGraph().computeTimeSaved()
        assert work == 200
        assert timeSavedOnW == 20
        assert tsAllSeries == 5
        if is_optimistic_enabled():
            assert tsPessimistic == 5
        else:
            assert tsPessimistic == 0
        if is_optimistic_enabled():
            assert tsOptimistic == 5
        else:
            assert tsOptimistic == 0

    def test_rootReturnError(self):
        g = root_error_graph()
        assert g.rootNode.returnError
        fullErrCP = g.findErrorsOnCriticalPath()
        assert len(fullErrCP) == 1
        assert fullErrCP[0].sid == "A"

    def test_metrics1(self):
        g = sampleGraph2()

        tsPessimistic = g.computeTimeSavedOnCPPessimistic(g.rootNode)
        assert tsPessimistic == 450

        tsOptimistic = g.computeTimeSavedOnCPOptimistic(g.rootNode)
        assert tsOptimistic == 450

        tsAllSeries = g.ComputeAllSeriesTimeSaved(g.rootNode)
        assert tsAllSeries == 450

        cp = g.findCriticalPath()
        fullErrCP = g.findErrorsOnCriticalPath()
        cpMetrics, sidTimeEx = g.accumeCPMetrics(cp, "0x0a", g.rootNode)
        errCPMetrics = g.accumeErrorCPMetrics(fullErrCP, sidTimeEx)

        assert len(cp) == 9
        assert cp[0].sid == "A"
        assert cp[1].sid == "F"
        assert cp[2].sid == "I"
        assert cp[3].sid == "H"
        assert cp[4].sid == "G"
        assert cp[5].sid == "B"
        assert cp[6].sid == "E"
        assert cp[7].sid == "D"
        assert cp[8].sid == "C"

        assert len(fullErrCP) == 6
        assert fullErrCP[0].sid == "A"
        assert fullErrCP[1].sid == "F"
        assert fullErrCP[2].sid == "B"
        assert fullErrCP[3].sid == "E"
        assert fullErrCP[4].sid == "D"
        assert fullErrCP[5].sid == "C"

        prefix = SYNTHETIC_ERR_CP_ROOT + "->"
        errCPCallPathTimeEx = errCPMetrics.errCPCallpathTimeExclusive
        assert errCPCallPathTimeEx[prefix + "[S1] O1->[S2] O2"] == 150
        assert errCPCallPathTimeEx[prefix + "[S1] O1->[S2] O2->[S3] O3"] == 150

        errCPErrCounts = errCPMetrics.errCPErrCounts
        assert errCPErrCounts[prefix + "[S1] O1"].toArray() == [0, 0, 1]
        assert errCPErrCounts[prefix + "[S1] O1->[S2] O2"].toArray() == [1, 0, 1]
        assert errCPErrCounts[prefix + "[S1] O1->[S2] O2->[S3] O3"].toArray() == [
            3,
            0,
            0,
        ]

        cpp = cpMetrics.profile
        assert len(cpp) == 3
        assert cpp["[S1] O1"].freq == 1
        assert cpp["[S1] O1->[S2] O2"].freq == 2
        assert cpp["[S1] O1->[S2] O2->[S3] O3"].freq == 6

        saving = errCPMetrics.savingPotential
        assert len(saving.items()) == 2
        assert saving["[S2] O2"].timeSaved == 300
        assert saving["[S3] O3"].timeSaved == 150

        assert errCPMetrics.numCPErrors == 4
        assert errCPMetrics.numRelatedToCPErrors == 0

        errCounts = {}
        errCallChainCounts = {}
        selfErrDepthList = []
        stoppedErrDepthList = []
        depthMap = {}
        propLengthMap = {}
        resiliencyMap = {}

        numAllErrors = g.computeErrorStats(
            g.rootNode,
            1,
            1,
            errCounts,
            errCallChainCounts,
            selfErrDepthList,
            stoppedErrDepthList,
            depthMap,
            propLengthMap,
            resiliencyMap,
        )
        assert numAllErrors == 6

        maxErrDepth = g.computeMaxErrDepthPropagatedToRoot(g.rootNode, 1)
        assert maxErrDepth == -1

        assert len(errCounts) == 3
        assert errCounts["[S1] O1"].toArray() == [0, 0, 1]
        assert errCounts["[S1] O1->[S2] O2"].toArray() == [1, 1, 1]
        assert errCounts["[S1] O1->[S2] O2->[S3] O3"].toArray() == [4, 0, 0]

        assert len(errCallChainCounts) == 2
        assert errCallChainCounts["[S1] O1->[S2] O2"] == 1
        assert errCallChainCounts["[S1] O1->[S2] O2->[S3] O3"] == 4

        selfErrDepthList.sort()
        assert len(selfErrDepthList) == 5
        assert selfErrDepthList[0] == 2
        assert selfErrDepthList[1] == 3
        assert selfErrDepthList[2] == 3
        assert selfErrDepthList[3] == 3
        assert selfErrDepthList[4] == 3

        stoppedErrDepthList.sort()
        assert len(stoppedErrDepthList) == 2
        assert stoppedErrDepthList[0] == 1
        assert stoppedErrDepthList[1] == 2

        assert len(depthMap) == 3
        assert depthMap[1].toArray() == [0, 0, 1]
        assert depthMap[2].toArray() == [1, 1, 1]
        assert depthMap[3].toArray() == [4, 0, 0]

        assert len(propLengthMap) == 2
        assert propLengthMap[1] == 4
        assert propLengthMap[2] == 1

        assert len(resiliencyMap) == 2
        assert resiliencyMap["[S1] O1"].toArray() == [0, 0, 1]
        assert resiliencyMap["[S2] O2"].toArray() == [0, 1, 1]

    def test_metrics2(self):
        g = sampleGraph3()

        tsPessimistic = g.computeTimeSavedOnCPPessimistic(g.rootNode)
        assert tsPessimistic == 50

        tsOptimistic = g.computeTimeSavedOnCPOptimistic(g.rootNode)
        assert tsOptimistic == 50

        tsAllSeries = g.ComputeAllSeriesTimeSaved(g.rootNode)
        assert tsAllSeries == 50

        cp = g.findCriticalPath()
        fullErrCP = g.findErrorsOnCriticalPath()
        cpMetrics, sidTimeEx = g.accumeCPMetrics(cp, "0x0a", g.rootNode)
        errCPMetrics = g.accumeErrorCPMetrics(fullErrCP, sidTimeEx)

        assert len(cp) == 9
        assert cp[0].sid == "A"
        assert cp[1].sid == "F"
        assert cp[2].sid == "I"
        assert cp[3].sid == "H"
        assert cp[4].sid == "G"
        assert cp[5].sid == "B"
        assert cp[6].sid == "E"
        assert cp[7].sid == "D"
        assert cp[8].sid == "C"

        assert len(fullErrCP) == 3
        assert fullErrCP[0].sid == "A"
        assert fullErrCP[1].sid == "B"
        assert fullErrCP[2].sid == "C"

        prefix = SYNTHETIC_ERR_CP_ROOT + "->"
        errCPErrCounts = errCPMetrics.errCPErrCounts
        assert (prefix + "[S1] O1") not in errCPErrCounts
        assert errCPErrCounts[prefix + "[S1] O1->[S2] O2"].toArray() == [0, 0, 1]
        assert errCPErrCounts[prefix + "[S1] O1->[S2] O2->[S3] O3"].toArray() == [
            1,
            0,
            0,
        ]

        saving = errCPMetrics.savingPotential
        assert len(saving.items()) == 1
        assert saving["[S3] O3"].timeSaved == 50

        assert errCPMetrics.numCPErrors == 1
        assert errCPMetrics.numRelatedToCPErrors == 0

        errCounts = {}
        errCallChainCounts = {}
        selfErrDepthList = []
        stoppedErrDepthList = []
        depthMap = {}
        propLengthMap = {}
        resiliencyMap = {}

        numAllErrors = g.computeErrorStats(
            g.rootNode,
            1,
            1,
            errCounts,
            errCallChainCounts,
            selfErrDepthList,
            stoppedErrDepthList,
            depthMap,
            propLengthMap,
            resiliencyMap,
        )
        assert numAllErrors == 3

        maxErrDepth = g.computeMaxErrDepthPropagatedToRoot(g.rootNode, 1)
        assert maxErrDepth == -1

        assert len(errCounts) == 3
        assert errCounts["[S1] O1"].toArray() == [0, 0, 1]
        assert errCounts["[S1] O1->[S2] O2"].toArray() == [0, 1, 1]
        assert errCounts["[S1] O1->[S2] O2->[S3] O3"].toArray() == [2, 0, 0]

        assert len(errCallChainCounts) == 1
        assert errCallChainCounts["[S1] O1->[S2] O2->[S3] O3"] == 2

        selfErrDepthList.sort()
        assert len(selfErrDepthList) == 2
        assert selfErrDepthList[0] == 3
        assert selfErrDepthList[1] == 3

        stoppedErrDepthList.sort()
        assert len(stoppedErrDepthList) == 2
        assert stoppedErrDepthList[0] == 1
        assert stoppedErrDepthList[1] == 2

        assert len(depthMap) == 3
        assert depthMap[1].toArray() == [0, 0, 1]
        assert depthMap[2].toArray() == [0, 1, 1]
        assert depthMap[3].toArray() == [2, 0, 0]

        assert len(propLengthMap) == 2
        assert propLengthMap[1] == 1
        assert propLengthMap[2] == 1

        assert len(resiliencyMap) == 2
        assert resiliencyMap["[S1] O1"].toArray() == [0, 0, 1]
        assert resiliencyMap["[S2] O2"].toArray() == [0, 1, 1]

    def test_timeskewMetrics(self):
        g = timeskew_graph()

        tsPessimistic = g.computeTimeSavedOnCPPessimistic(g.rootNode)
        assert tsPessimistic == 50

        tsOptimistic = g.computeTimeSavedOnCPOptimistic(g.rootNode)
        assert tsOptimistic == 50

        tsAllSeries = g.ComputeAllSeriesTimeSaved(g.rootNode)
        assert tsAllSeries == 50

        cp = g.findCriticalPath()
        fullErrCP = g.findErrorsOnCriticalPath()
        cpMetrics, sidTimeEx = g.accumeCPMetrics(cp, "0x0a", g.rootNode)
        errCPMetrics = g.accumeErrorCPMetrics(fullErrCP, sidTimeEx)

        assert len(cp) == 3
        assert cp[0].sid == "A"
        assert cp[1].sid == "B"
        assert cp[2].sid == "C"
        assert cpMetrics.profile["[S1] O1"].excl == 50
        assert cpMetrics.profile["[S1] O1->[S1] O2"].excl == 20
        assert cpMetrics.profile["[S1] O1->[S1] O2->[S2] O3"].excl == 30

        # same as errCP
        assert len(fullErrCP) == 3
        assert fullErrCP[0].sid == "A"
        assert fullErrCP[1].sid == "B"
        assert fullErrCP[2].sid == "C"

        prefix = SYNTHETIC_ERR_CP_ROOT + "->"
        errCPCallpathTimeEx = errCPMetrics.errCPCallpathTimeExclusive
        assert errCPCallpathTimeEx[prefix + "[S1] O1->[S1] O2"] == 20
        assert errCPCallpathTimeEx[prefix + "[S1] O1->[S1] O2->[S2] O3"] == 30

        errCPErrCounts = errCPMetrics.errCPErrCounts
        assert errCPErrCounts[prefix + "[S1] O1"].toArray() == [0, 0, 1]
        assert errCPErrCounts[prefix + "[S1] O1->[S1] O2"].toArray() == [0, 1, 0]
        assert errCPErrCounts[prefix + "[S1] O1->[S1] O2->[S2] O3"].toArray() == [
            1,
            0,
            0,
        ]

        saving = errCPMetrics.savingPotential
        assert len(saving.items()) == 2
        assert saving["[S1] O2"].timeSaved == 50
        assert saving["[S2] O3"].timeSaved == 0

        assert errCPMetrics.numCPErrors == 2
        assert errCPMetrics.numRelatedToCPErrors == 0

    def test_overlapedSerialCalls(self):
        """
        This test shows that some CP component can become negative.
        An upcoming diff will fix this issue.
        """
        g = mild_overlap_graph()
        cp = g.findCriticalPath()
        cpMetrics, _ = g.accumeCPMetrics(cp, "0x0a", g.rootNode)

        assert len(cp) == 3
        assert cp[0].sid == "A"
        assert cp[1].sid == "C"
        assert cp[2].sid == "B"
        # normal calculation would have resulted -5, but we sanitize out negative time entries
        assert cpMetrics.profile["[S1] O1"].excl == 0
        assert cpMetrics.profile["[S1] O1->[S2] O2"].excl == 500
        assert cpMetrics.profile["[S1] O1->[S2] O3"].excl == 505

    def test_exclusion(self):
        cp = excludeGraph().findCriticalPath()
        assert len(cp) == 3
        assert cp[0].sid == "A"
        assert cp[1].sid == "B"
        assert cp[2].sid == "C"

    def test_computeErrDepthHisto(self):
        g = graph.Graph(
            [],
            "testService",
            "testOperation",
            "nofile.txt",
            skipInitializationForTest=True,
        )
        #            root
        #            /  \
        #     (err) c1    c2
        #          /  \     \
        #       c3    c4    c5 (err)
        #      /  \          \
        # err (c6)  c7        c8 (err)
        #     |
        #     c9
        root = graph.GraphNode(
            sid=0,
            startTime=0,
            duration=1000,
            parentSpanId=-1,
            opName="root",
            processID=1,
            spanKind=graph.SpanKind.SERVER,
            peerService="testService",
            returnError=False,
        )
        c1 = graph.GraphNode(
            sid=1,
            startTime=10,
            duration=50,
            parentSpanId=0,
            opName="o1",
            processID=1,
            spanKind=graph.SpanKind.SERVER,
            peerService="testService",
            returnError=True,
        )
        c2 = graph.GraphNode(
            sid=2,
            startTime=100,
            duration=150,
            parentSpanId=0,
            opName="o2",
            processID=1,
            spanKind=graph.SpanKind.SERVER,
            peerService="testService",
            returnError=False,
        )
        c3 = graph.GraphNode(
            sid=3,
            startTime=10,
            duration=10,
            parentSpanId=1,
            opName="o3",
            processID=1,
            spanKind=graph.SpanKind.SERVER,
            peerService="testService",
            returnError=False,
        )
        c4 = graph.GraphNode(
            sid=4,
            startTime=20,
            duration=10,
            parentSpanId=1,
            opName="o4",
            processID=1,
            spanKind=graph.SpanKind.SERVER,
            peerService="testService",
            returnError=False,
        )
        c5 = graph.GraphNode(
            sid=5,
            startTime=110,
            duration=10,
            parentSpanId=2,
            opName="o5",
            processID=1,
            spanKind=graph.SpanKind.SERVER,
            peerService="testService",
            returnError=True,
        )
        c6 = graph.GraphNode(
            sid=6,
            startTime=10,
            duration=1,
            parentSpanId=3,
            opName="o6",
            processID=1,
            spanKind=graph.SpanKind.SERVER,
            peerService="testService",
            returnError=True,
        )
        c7 = graph.GraphNode(
            sid=7,
            startTime=10,
            duration=1,
            parentSpanId=3,
            opName="o7",
            processID=1,
            spanKind=graph.SpanKind.SERVER,
            peerService="testService",
            returnError=False,
        )
        c8 = graph.GraphNode(
            sid=8,
            startTime=110,
            duration=10,
            parentSpanId=5,
            opName="o6",
            processID=1,
            spanKind=graph.SpanKind.SERVER,
            peerService="testService",
            returnError=True,
        )
        c9 = graph.GraphNode(
            sid=9,
            startTime=10,
            duration=1,
            parentSpanId=6,
            opName="o9",
            processID=1,
            spanKind=graph.SpanKind.SERVER,
            peerService="testService",
            returnError=False,
        )

        c9.parent = c6
        c6.addChild(c9)
        c8.parent = c5
        c5.addChild(c8)
        c7.parent = c3
        c3.addChild(c7)
        c6.parent = c3
        c3.addChild(c6)
        c5.parent = c2
        c2.addChild(c5)
        c4.parent = c1
        c1.addChild(c4)
        c3.parent = c1
        c1.addChild(c3)
        c2.parent = root
        root.addChild(c2)
        c1.parent = root
        root.addChild(c1)

        g.rootNode = root
        propToRootHisto = {}
        notPropToRootHisto = {}
        notPropToRootHistoExpected = {1: 1, 3: 2}
        depth = 0
        g.computeErrDepthHisto(
            g.rootNode,
            depth,
            propToRootHisto,
            notPropToRootHisto,
            True,
            lambda x: True,  # noqa: ARG005
        )
        self.assertDictEqual({}, propToRootHisto)
        self.assertDictEqual(notPropToRootHistoExpected, notPropToRootHisto)

        supressHisto = {}
        supressHistotExpected = {0: 1, 1: 1, 2: 1}
        g.computeSupressErrDepthHisto(g.rootNode, depth, supressHisto, lambda x: True)  # noqa: ARG005
        self.assertDictEqual(supressHistotExpected, supressHisto)

        # Now fail the root
        root.returnError = True
        propToRootHisto = {}
        notPropToRootHisto = {}
        propToRootHistoExpected = {1: 1}
        notPropToRootHistoExpected = {3: 2}
        depth = 0
        g.computeErrDepthHisto(
            g.rootNode,
            depth,
            propToRootHisto,
            notPropToRootHisto,
            True,
            lambda x: True,  # noqa: ARG005
        )
        self.assertDictEqual(propToRootHistoExpected, propToRootHisto)
        self.assertDictEqual(notPropToRootHistoExpected, notPropToRootHisto)

        supressHisto = {}
        supressHistotExpected = {1: 1, 2: 1}
        g.computeSupressErrDepthHisto(g.rootNode, depth, supressHisto, lambda x: True)  # noqa: ARG005
        self.assertDictEqual(supressHistotExpected, supressHisto)

    def test_getOutboundCount_root_matches(self):
        # Create a simple graph where the root node matches the service name
        jsonData = {
            "data": [
                {
                    "processes": {
                        "P1": {"serviceName": "S1", "tags": []},
                        "P2": {"serviceName": "S2", "tags": []},
                        "P3": {"serviceName": "S3", "tags": []},
                    },
                    "traceID": "0x1234abcd",
                    "spans": [
                        {
                            "traceID": "0x1234abcd",
                            "spanID": "A",
                            "operationName": "O1",
                            "references": [],
                            "startTime": 0,
                            "duration": 100,
                            "processID": "P1",
                            "warnings": None,
                        },
                        {
                            "traceID": "0x1234abcd",
                            "spanID": "B",
                            "operationName": "O2",
                            "references": [{"refType": "CHILD_OF", "traceID": "0x1234abcd", "spanID": "A"}],
                            "startTime": 10,
                            "duration": 60,
                            "processID": "P2",
                            "warnings": None,
                        },
                        {
                            "traceID": "0x1234abcd",
                            "spanID": "C",
                            "operationName": "O3",
                            "references": [{"refType": "CHILD_OF", "traceID": "0x1234abcd", "spanID": "A"}],
                            "startTime": 20,
                            "duration": 40,
                            "processID": "P3",
                            "warnings": None,
                        },
                    ],
                }
            ]
        }

        g = Graph(jsonData, "S1", "O1", filename="test.json", rootTrace=True)
        self.assertEqual(g.getOutboundCount("S1"), [2])  # Root has 2 children

    def test_getOutboundCount_child_matches(self):
        # Create a graph where a child node matches the service name
        jsonData = {
            "data": [
                {
                    "processes": {
                        "P1": {"serviceName": "S1", "tags": []},
                        "P2": {"serviceName": "S2", "tags": []},
                        "P3": {"serviceName": "S3", "tags": []},
                        "P4": {"serviceName": "S4", "tags": []},
                    },
                    "traceID": "0x5678def0",
                    "spans": [
                        {
                            "traceID": "0x5678def0",
                            "spanID": "A",
                            "operationName": "O1",
                            "references": [],
                            "startTime": 0,
                            "duration": 100,
                            "processID": "P1",
                            "warnings": None,
                        },
                        {
                            "traceID": "0x5678def0",
                            "spanID": "B",
                            "operationName": "O2",
                            "references": [{"refType": "CHILD_OF", "traceID": "0x5678def0", "spanID": "A"}],
                            "startTime": 10,
                            "duration": 60,
                            "processID": "P2",
                            "warnings": None,
                        },
                        {
                            "traceID": "0x5678def0",
                            "spanID": "C",
                            "operationName": "O3",
                            "references": [{"refType": "CHILD_OF", "traceID": "0x5678def0", "spanID": "B"}],
                            "startTime": 20,
                            "duration": 40,
                            "processID": "P3",
                            "warnings": None,
                        },
                        {
                            "traceID": "0x5678def0",
                            "spanID": "D",
                            "operationName": "O4",
                            "references": [{"refType": "CHILD_OF", "traceID": "0x5678def0", "spanID": "B"}],
                            "startTime": 25,
                            "duration": 35,
                            "processID": "P4",
                            "warnings": None,
                        },
                    ],
                }
            ]
        }

        g = Graph(jsonData, "S1", "O1", filename="test.json", rootTrace=True)
        self.assertEqual(g.getOutboundCount("S2"), [2])  # S2 has 2 children

    def test_getOutboundCount_no_match(self):
        # Create a graph where no node matches the service name
        jsonData = {
            "data": [
                {
                    "processes": {
                        "P1": {"serviceName": "S1", "tags": []},
                        "P2": {"serviceName": "S2", "tags": []},
                        "P3": {"serviceName": "S3", "tags": []},
                    },
                    "traceID": "0x9abc1234",
                    "spans": [
                        {
                            "traceID": "0x9abc1234",
                            "spanID": "A",
                            "operationName": "O1",
                            "references": [],
                            "startTime": 0,
                            "duration": 100,
                            "processID": "P1",
                            "warnings": None,
                        },
                        {
                            "traceID": "0x9abc1234",
                            "spanID": "B",
                            "operationName": "O2",
                            "references": [{"refType": "CHILD_OF", "traceID": "0x9abc1234", "spanID": "A"}],
                            "startTime": 10,
                            "duration": 60,
                            "processID": "P2",
                            "warnings": None,
                        },
                        {
                            "traceID": "0x9abc1234",
                            "spanID": "C",
                            "operationName": "O3",
                            "references": [{"refType": "CHILD_OF", "traceID": "0x9abc1234", "spanID": "B"}],
                            "startTime": 20,
                            "duration": 40,
                            "processID": "P3",
                            "warnings": None,
                        },
                    ],
                }
            ]
        }

        g = Graph(jsonData, "S1", "O1", filename="test.json", rootTrace=True)
        self.assertEqual(g.getOutboundCount("S4"), [])  # No node with service name S4

    def test_getOutboundCount_empty_graph(self):
        # Test with an empty graph
        g = Graph({"data": []}, "S1", "O1", filename="test.json", rootTrace=True)
        self.assertEqual(g.getOutboundCount("S1"), [])  # Empty graph has no nodes

    def test_getOutboundCount_deep_match(self):
        # Create a graph where a deep node matches the service name
        jsonData = {
            "data": [
                {
                    "processes": {
                        "P1": {"serviceName": "S1", "tags": []},
                        "P2": {"serviceName": "S2", "tags": []},
                        "P3": {"serviceName": "S3", "tags": []},
                        "P4": {"serviceName": "S4", "tags": []},
                    },
                    "traceID": "0xdef05678",
                    "spans": [
                        {
                            "traceID": "0xdef05678",
                            "spanID": "A",
                            "operationName": "O1",
                            "references": [],
                            "startTime": 0,
                            "duration": 100,
                            "processID": "P1",
                            "warnings": None,
                        },
                        {
                            "traceID": "0xdef05678",
                            "spanID": "B",
                            "operationName": "O2",
                            "references": [{"refType": "CHILD_OF", "traceID": "0xdef05678", "spanID": "A"}],
                            "startTime": 10,
                            "duration": 60,
                            "processID": "P2",
                            "warnings": None,
                        },
                        {
                            "traceID": "0xdef05678",
                            "spanID": "C",
                            "operationName": "O3",
                            "references": [{"refType": "CHILD_OF", "traceID": "0xdef05678", "spanID": "B"}],
                            "startTime": 20,
                            "duration": 40,
                            "processID": "P3",
                            "warnings": None,
                        },
                        {
                            "traceID": "0xdef05678",
                            "spanID": "D",
                            "operationName": "O4",
                            "references": [{"refType": "CHILD_OF", "traceID": "0xdef05678", "spanID": "C"}],
                            "startTime": 25,
                            "duration": 35,
                            "processID": "P4",
                            "warnings": None,
                        },
                    ],
                }
            ]
        }

        g = Graph(jsonData, "S1", "O1", filename="test.json", rootTrace=True)
        self.assertEqual(g.getOutboundCount("S3"), [1])  # S3 has 1 child

    def test_getAllOutboundCounts(self):
        # Test getAllOutboundCounts function with a multi-service graph
        jsonData = {
            "data": [
                {
                    "processes": {
                        "P1": {"serviceName": "S1", "tags": []},
                        "P2": {"serviceName": "S2", "tags": []},
                        "P3": {"serviceName": "S3", "tags": []},
                        "P4": {"serviceName": "S2", "tags": []},  # Another S2 span
                    },
                    "traceID": "0xtest1234",
                    "spans": [
                        {
                            "traceID": "0xtest1234",
                            "spanID": "A",
                            "operationName": "O1",
                            "references": [],
                            "startTime": 0,
                            "duration": 100,
                            "processID": "P1",
                            "warnings": None,
                        },
                        {
                            "traceID": "0xtest1234",
                            "spanID": "B",
                            "operationName": "O2",
                            "references": [{"refType": "CHILD_OF", "traceID": "0xtest1234", "spanID": "A"}],
                            "startTime": 10,
                            "duration": 60,
                            "processID": "P2",
                            "warnings": None,
                        },
                        {
                            "traceID": "0xtest1234",
                            "spanID": "C",
                            "operationName": "O3",
                            "references": [{"refType": "CHILD_OF", "traceID": "0xtest1234", "spanID": "A"}],
                            "startTime": 20,
                            "duration": 40,
                            "processID": "P3",
                            "warnings": None,
                        },
                        {
                            "traceID": "0xtest1234",
                            "spanID": "D",
                            "operationName": "O4",
                            "references": [{"refType": "CHILD_OF", "traceID": "0xtest1234", "spanID": "B"}],
                            "startTime": 30,
                            "duration": 20,
                            "processID": "P4",
                            "warnings": None,
                        },
                    ],
                }
            ]
        }

        g = Graph(jsonData, "S1", "O1", filename="test.json", rootTrace=True)
        all_counts = g.getAllOutboundCounts()

        # S1 has 2 children, S2 has 1 child (first S2 span) and 0 children (second S2 span), S3 has 0 children
        expected = {"S1": [2], "S2": [1, 0], "S3": [0]}
        self.assertEqual(all_counts, expected)

    def test_getCycles(self):
        # Test case 1: Simple cycle with two nodes
        jsonData = {
            "data": [
                {
                    "processes": {
                        "pid1": {
                            "serviceName": "serviceA",
                            "tags": [],
                        },
                    },
                    "traceID": "A",
                    "spans": [],
                },
            ],
        }
        g = Graph(jsonData, "serviceA", "opA", filename="test.json")
        node1 = GraphNode(
            sid="1",
            startTime=0,
            duration=100,
            parentSpanId="",
            opName="opA",
            processID="pid1",
            spanKind=SpanKind.CLIENT,
            peerService=None,
            returnError=False,
        )
        node2 = GraphNode(
            sid="2",
            startTime=10,
            duration=80,
            parentSpanId="1",
            opName="opB",
            processID="pid1",
            spanKind=SpanKind.CLIENT,
            peerService=None,
            returnError=False,
        )
        node3 = GraphNode(
            sid="3",
            startTime=20,
            duration=60,
            parentSpanId="2",
            opName="opA",
            processID="pid1",
            spanKind=SpanKind.CLIENT,
            peerService=None,
            returnError=False,
        )
        node4 = GraphNode(
            sid="4",
            startTime=40,
            duration=20,
            parentSpanId="3",
            opName="opA",
            processID="pid1",
            spanKind=SpanKind.CLIENT,
            peerService=None,
            returnError=False,
        )
        node1.addChild(node2)
        node2.addChild(node3)
        node3.addChild(node4)
        g.rootNode = node1

        cycles = {}
        g.getCycles(node1, [], [], cycles)
        self.assertEqual(len(cycles), 2)
        self.assertIn("3", cycles)
        self.assertIn("4", cycles)
        self.assertEqual(cycles["3"], ["[serviceA] opA", "[serviceA] opB", "[serviceA] opA"])
        self.assertEqual(cycles["4"], ["[serviceA] opA", "[serviceA] opA"])
        # Test case 2: No cycle
        jsonData = {
            "data": [
                {
                    "processes": {
                        "pid1": {
                            "serviceName": "serviceA",
                            "tags": [],
                        },
                    },
                    "traceID": "A",
                    "spans": [],
                },
            ],
        }
        g = Graph(jsonData, "serviceA", "opA", filename="test.json")
        node1 = GraphNode(
            sid="1",
            startTime=0,
            duration=100,
            parentSpanId="",
            opName="opA",
            processID="pid1",
            spanKind=SpanKind.CLIENT,
            peerService=None,
            returnError=False,
        )
        node2 = GraphNode(
            sid="2",
            startTime=10,
            duration=80,
            parentSpanId="1",
            opName="opB",
            processID="pid1",
            spanKind=SpanKind.CLIENT,
            peerService=None,
            returnError=False,
        )
        node1.addChild(node2)
        g.rootNode = node1

        cycles = {}
        g.getCycles(node1, [], [], cycles)
        self.assertEqual(len(cycles), 0)


class TestCrossRegionDetection(TestCase):
    """Test cases for cross-region call detection functionality"""

    def test_region_map_parsing(self):
        """Test that region information is correctly parsed from process tags"""
        g = crossRegionSampleGraph()

        # Verify region map is populated correctly
        self.assertEqual(g.regionMap["P1"], "us-west-1")
        self.assertEqual(g.regionMap["P2"], "us-east-1")
        self.assertEqual(g.regionMap["P3"], "eu-west-1a")  # Zone fallback
        self.assertEqual(g.regionMap["P4"], "us-west-1")
        self.assertNotIn("P5", g.regionMap)  # No region/zone data

    def test_zone_fallback_logic(self):
        """Test that zone information is used when region is not available"""
        jsonData = {
            "data": [
                {
                    "processes": {
                        "P1": {
                            "serviceName": "ServiceA",
                            "tags": [
                                {"key": "zone", "value": "us-west-1a"},
                            ],
                        },
                        "P2": {
                            "serviceName": "ServiceB",
                            "tags": [
                                {"key": "region", "value": "us-east-1"},
                                {"key": "zone", "value": "us-east-1b"},
                            ],
                        },
                    },
                    "traceID": "T1",
                    "spans": [
                        {
                            "traceID": "T1",
                            "spanID": "S1",
                            "operationName": "TestOp",
                            "startTime": 0,
                            "duration": 100,
                            "references": [],
                            "processID": "P1",
                        },
                    ],
                },
            ],
        }

        g = Graph(jsonData, "ServiceA", "TestOp", "", "")

        # Zone should be used for P1 since no region tag
        self.assertEqual(g.regionMap["P1"], "us-west-1a")
        # Region should be used for P2, zone ignored
        self.assertEqual(g.regionMap["P2"], "us-east-1")

    def test_cross_region_detection_same_operation(self):
        """Test detection of cross-region calls with same operation name"""
        g = crossRegionSampleGraph()
        crossRegionCalls = {}
        g.getCrossRegionCalls(g.rootNode, crossRegionCalls)

        # Should detect cross-region calls
        self.assertGreater(len(crossRegionCalls), 0)

        # Check that we have the expected cross-region calls
        s2_found = False
        s3_found = False

        for callData in crossRegionCalls.values():
            if callData['operationName'] == 'GetUser':
                if callData['parentRegion'] == 'uswest' and callData['childRegion'] == 'useast':
                    s2_found = True
                    self.assertEqual(callData['parentService'], 'ServiceA')
                    self.assertEqual(callData['childService'], 'ServiceB')
                    self.assertEqual(callData['parentDuration'], 1000)
                    self.assertEqual(callData['childDuration'], 200)
                elif callData['parentRegion'] == 'useast' and callData['childRegion'] == 'euwesta':
                    s3_found = True
                    self.assertEqual(callData['parentService'], 'ServiceB')
                    self.assertEqual(callData['childService'], 'ServiceC')

        self.assertTrue(s2_found, "Cross-region call S2 not detected")
        self.assertTrue(s3_found, "Cross-region call S3 not detected")

    def test_same_region_calls_ignored(self):
        """Test that same-region calls are not detected as cross-region"""
        g = crossRegionSampleGraph()
        crossRegionCalls = {}
        g.getCrossRegionCalls(g.rootNode, crossRegionCalls)

        # Should not detect S4 (uswest -> uswest)
        for callData in crossRegionCalls.values():
            if callData['operationName'] == 'GetUser':
                self.assertFalse(
                    callData['parentRegion'] == 'uswest' and callData['childRegion'] == 'uswest',
                    "Same-region call incorrectly detected as cross-region"
                )

    def test_different_operation_ignored(self):
        """Test that calls with different operation names are ignored"""
        g = crossRegionSampleGraph()
        crossRegionCalls = {}
        g.getCrossRegionCalls(g.rootNode, crossRegionCalls)

        # Should not detect S5 (GetData operation, different from parent GetUser)
        for callData in crossRegionCalls.values():
            self.assertNotEqual(callData['operationName'], 'GetData',
                              "Different operation incorrectly detected as cross-region")

    def test_missing_region_data_ignored(self):
        """Test that spans without region data are ignored"""
        g = crossRegionSampleGraph()
        crossRegionCalls = {}
        g.getCrossRegionCalls(g.rootNode, crossRegionCalls)

        # Should not detect S6 (no region data for P5)
        for callData in crossRegionCalls.values():
            self.assertNotEqual(callData['childService'], 'ServiceE',
                              "Span without region data incorrectly detected")

    def test_duration_ratio_calculation(self):
        """Test that duration ratios are calculated correctly"""
        g = crossRegionSampleGraph()
        crossRegionCalls = {}
        g.getCrossRegionCalls(g.rootNode, crossRegionCalls)

        for callData in crossRegionCalls.values():
            expected_ratio = callData['parentDuration'] / callData['childDuration']
            self.assertEqual(callData['durationRatio'], expected_ratio)
            self.assertGreater(callData['durationRatio'], 0)

    def test_call_path_tracking(self):
        """Test that call paths are correctly tracked"""
        g = crossRegionSampleGraph()
        crossRegionCalls = {}
        g.getCrossRegionCalls(g.rootNode, crossRegionCalls)

        for callData in crossRegionCalls.values():
            self.assertIsInstance(callData['callPath'], str)
            self.assertGreater(len(callData['callPath']), 0)
            # Call path should contain the operation name
            self.assertIn(callData['operationName'], callData['callPath'])

    def test_empty_trace_handling(self):
        """Test handling of empty trace data"""
        jsonData = {"data": [{"processes": {}, "traceID": "T1", "spans": []}]}
        g = Graph(jsonData, "NonExistentService", "NonExistentOp", "", "")

        # Should handle gracefully without errors
        crossRegionCalls = {}
        if g.rootNode:
            g.getCrossRegionCalls(g.rootNode, crossRegionCalls)

        self.assertEqual(len(crossRegionCalls), 0)

    def test_single_span_trace(self):
        """Test handling of single span traces"""
        jsonData = {
            "data": [
                {
                    "processes": {
                        "P1": {
                            "serviceName": "ServiceA",
                            "tags": [{"key": "region", "value": "us-west-1"}],
                        },
                    },
                    "traceID": "T1",
                    "spans": [
                        {
                            "traceID": "T1",
                            "spanID": "S1",
                            "operationName": "GetUser",
                            "startTime": 0,
                            "duration": 100,
                            "references": [],
                            "processID": "P1",
                        },
                    ],
                },
            ],
        }

        g = Graph(jsonData, "ServiceA", "GetUser", "", "")
        crossRegionCalls = {}
        g.getCrossRegionCalls(g.rootNode, crossRegionCalls)

        # No cross-region calls possible with single span
        self.assertEqual(len(crossRegionCalls), 0)
