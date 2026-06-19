# ruff: noqa: I001
import heapq
import logging
import os
from typing import Callable

import crisp.common as common
from crisp.shared.models import (
    MetricVals,
    CallPathProfile,
    QuantizedMetrics,
    ErrCountsData,
    SavingData,
    SpanKind,
    ErrorCPMetrics,
    ErrorMetrics,
    Metrics,
)
from crisp.shared.utils import getLeafNodeFromCallPath
from crisp.models import (
    PBlock, GraphNode, GetRemainingTags,
)
from crisp.constants import (
    SPANS, SPAN_ID, REFERENCES, START_TIME, SPAN_KIND, PEER_SERVICE,
    DURATION, OPERATION_NAME, PROCESS_ID, REF_TYPE, CHILD_OF,
    TAGS, LOGS, FIELDS, PROCESSES, HOSTNAME, TESTING, OP_TIME_EXCLUSIVE,
    TOTAL_WORK, TIME_SAVED_ON_WORK, TIME_SAVED_ON_CP, ERR_CP_CALLPATH_EXCLUSIVE,
    ERR_CP_ERR_COUNTS, SERVER, CLIENT, PARQUET_PROCESS, PARQUET_SERVICE_NAME,
    PARQUET_OPERATION_NAME, PARQUET_HOSTNAME, PARQUET_START_TIME, PARQUET_DURATION,
    PARQUET_SPAN_SET, PARQUET_KIND, PARQUET_SPAN_ID, PARQUET_PARENT_SPAN_ID,
    PARQUET_SPANS, PARQUET_TAGS, PARQUET_ERROR, PARQUET_RPC_STATUS_CODE,
    PARQUET_RPC_SYSTEM, PARQUET_ERROR_MESSAGE, SYNTHETIC_ERR_CP_ROOT, SYNTHETIC_FULL_ERR_NON_CP_ROOT, Colors,
    DEFAULT_MAX_DEPTH
)
from crisp.configuration import (
    get_overlap_allowance, get_server_lengthening_factor, is_optimistic_enabled,
    is_pessimistic_enabled
)
from crisp.utils.dict_utils import (
    accumulateInDict, getCPSize
)

# Re-export for backward compatibility
__all__ = ['Graph', 'accumulateInDict', 'bcolors', 'getCPSize']
from crisp.utils.span_utils import (
    isProxyNode, isErrPropNode, isTestTraceByServiceName, isTestTraceByOpName
)


# bcolors class moved to constants.py - use Colors instead
bcolors = Colors


logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)
debug_on = logging.getLogger(__name__).isEnabledFor(logging.DEBUG)

def isAcceptableParentChildDuration(parent, me):
    if me.duration > get_server_lengthening_factor() * parent.duration:
        return False
    return True


# detects server1 -> [^server] -> server2, where middleone has one and only one child (server2)
def isFuzzyClientServerCall(parent, me):
    if me.spanKind != SpanKind.SERVER:
        return False
    if parent.spanKind == SpanKind.SERVER:
        return False
    if len(parent.children) != 1:
        return False
    grandParent = parent.parent
    if not grandParent or (grandParent.spanKind != SpanKind.SERVER):
        return False
    return isAcceptableParentChildDuration(parent, me)


# detects client1 -> server2, where client1 has one and only one child (server2)
def isCleanClientServerCall(parent, me):
    if me.spanKind != SpanKind.SERVER:
        return False
    if parent.spanKind != SpanKind.CLIENT:
        return False
    if len(parent.children) != 1:
        return False
    return isAcceptableParentChildDuration(parent, me)


# Detects if parent and child are running on different hosts.
def onDifferentHosts(hostDict, parent, me):
    parentHost = hostDict[parent.pid] if parent.pid in hostDict else None
    meHost = hostDict[me.pid] if me.pid in hostDict else None
    if not parentHost or not meHost:
        return False
    if parentHost == meHost:
        return False
    if len(parent.children) != 1:
        return False
    return isAcceptableParentChildDuration(parent, me)


# adjust duration to be no longer than the parent. This helps us not get -ve numbers.
def adjustChildDuration(parent, me):
    if me.duration > parent.duration:
        me.duration = parent.duration


# Checks if "parent" is a client(ish) and "me" is server.
def isClientServerCall(hostDict, parent, me, adjustChild=True):
    if (
        isCleanClientServerCall(parent, me)
        or isFuzzyClientServerCall(parent, me)
        or onDifferentHosts(hostDict, parent, me)
    ):
        if isAcceptableParentChildDuration(parent, me):
            adjustChild and adjustChildDuration(parent, me)
            return True
    return False


def get_split_child_trace_ids(trace_data: dict) -> list[dict]:
    """
    Identify child trace IDs from split point markers in a parent trace.

    When Jaeger splits large traces due to size limits, it creates spans with
    special tags (internal.splittrace.traceID / spanID) that indicate the child
    trace ID. This scans all spans for those markers.

    Args:
        trace_data: Jaeger trace JSON data

    Returns:
        List of dicts, each containing child_trace_id and metadata
    """
    split_children = []
    seen_child_ids = set()

    try:
        for item in trace_data.get("data", []):
            for span in item.get(SPANS, []):
                child_trace_id = None
                child_span_id = None

                for tag in span.get(TAGS, []):
                    tag_key = tag.get("key", "")
                    if tag_key == "internal.splittrace.traceID":
                        child_trace_id = tag.get("value")
                    elif tag_key == "internal.splittrace.spanID":
                        child_span_id = tag.get("value")

                if child_trace_id and child_trace_id not in seen_child_ids:
                    seen_child_ids.add(child_trace_id)
                    split_children.append({
                        'child_trace_id': child_trace_id,
                        'child_span_id': child_span_id,
                        'split_point_span_id': span.get(SPAN_ID),
                        'split_point_operation': span.get(OPERATION_NAME, ""),
                    })
    except Exception as e:
        logging.warning(f"Failed to parse split trace markers from trace data: {e}")

    return split_children


def extract_root_span_metadata(trace_data: dict) -> tuple[str, str]:
    """
    Extract the actual service name and operation name from trace root span.

    This function parses Jaeger trace data to find the root span (span without parent references)
    and extracts the service name and operation name from it. This is useful for creating
    Graph instances with the correct serviceName and operationName parameters when those are not available in the config.

    Args:
        trace_data: Parsed JSON trace data from Jaeger containing 'data' field with
                   processes and spans information

    Returns:
        Tuple of (service_name, operation_name) extracted from the root span.
        Falls back to ("", "") if extraction fails.
    """
    try:
        # Parse process names from trace data (similar to Graph.parseNode)
        process_names = {}
        for item in trace_data.get("data", []):
            for process_id, process_info in item.get(PROCESSES, {}).items():
                process_names[process_id] = process_info.get("serviceName", "")

        # Find potential root spans (spans without parent references)
        potential_roots = []
        for item in trace_data.get("data", []):
            for span in item.get(SPANS, []):
                has_parent = False
                for ref in span.get(REFERENCES, []):
                    if ref.get(REF_TYPE) == CHILD_OF:
                        has_parent = True
                        break

                if not has_parent:
                    potential_roots.append(span)

        # Use the first root span found
        if potential_roots:
            root_span = potential_roots[0]
            process_id = root_span.get(PROCESS_ID)
            service_name = process_names.get(process_id, "")
            operation_name = root_span.get(OPERATION_NAME, "")
            return service_name, operation_name

    except Exception as e:
        logging.warning(f"Failed to extract root span metadata from trace: {e}")

    # Fallback to defaults
    return "", ""

class Graph:
    """
    Graph represents a Jaeger trace composed of spans (represented by GraphNodes).
    It is the central data structure on which we compute the critical path.
    Each Graph is built from some Jaeger JSON file represented by its filename.
    A well-formed Jaeger trace should have one and only one rootNode.
    """

    def __init__(
        self,
        data,
        serviceName,
        operationName,
        filename=None,
        rootTrace=True,
        filterProxy=False,
        tags=None,
        exclusionSet=None,
        skipInitializationForTest=False,
        useParquet=False,
    ) -> None:
        self.operationName = operationName
        self.serviceName = serviceName
        self.tags = []
        self.filename = filename
        self.filesz = os.path.getsize(filename) if os.path.exists(filename) else 0
        self.rootNode = None
        self.nodeHT = {}
        self.tagHT = {}  # nullify once its use is over
        self.processName = {}
        self.hostMap = {}  # maps service to host name
        self.regionMap = {}  # maps process ID to region/zone info
        self.totalShrink = 0
        self.totalDrop = 0
        self.shrinkCounter = 0
        self.testing = {}
        self.exclusiveExampleMap = {}
        self.inclusiveExampleMap = {}
        self.numErrors = 0
        self.proxyNodes = {}  # maps sid to child count, for sanity checks
        self.filterProxy = filterProxy
        self.numProxyRoots = 0  # number of proxy nodes that are roots
        self.exclusionSet = (
            exclusionSet if exclusionSet else {}
        )  # operations to exlude from the graph

        # Used for testing to skip initialization.
        if skipInitializationForTest:
            return

        try:
            if useParquet:
                potentialRoots, isCtfTest = self.parseNodeFromParquet(data)
            else:
                potentialRoots, isCtfTest = self.parseNode(data)
            self.isCtfTest = isCtfTest
        except Exception as e:
            logging.warning(f"self.parseNode failed in file {filename}!")
            logging.warning(f"Exception: {e}")
            return

        if len(potentialRoots) == 0:
            logging.warning(f"no root node in file {filename}!")
            return

        if rootTrace:  # the root span must be the service+operation
            if len(potentialRoots) != 1:
                logging.warning(f"{len(potentialRoots)} roots node in file {filename}!")
                return
            if not potentialRoots[0] or not self.checkRootAndWarn(
                potentialRoots[0],
                filename,
                rootTrace,
            ):
                return
            else:
                self.rootNode = potentialRoots[0]
        else:  # randomly choose some one node whose service and op names match
            for candidate in potentialRoots:
                someRoot = self.findARoot(candidate)
                if not someRoot or not self.checkRootAndWarn(
                    someRoot,
                    filename,
                    rootTrace,
                ):
                    continue
                # make someRoot's parent as None
                someRoot.parent = None
                someRoot.parentSpanId = None
                # set someRoot as the rootNode
                self.rootNode = someRoot
                break

            if not self.rootNode:
                logging.warning(
                    f"rootTrace == {rootTrace} but no matching node found in file {filename}!",
                )
                return

        self.sanitizeOverflowingChildren(self.rootNode)
        # Remove operation in exclusionDict from the graph
        self.removeExcludedOps(self.rootNode, self.exclusionSet)
        # record each tag that is found in the node.
        self.tags = self.GetMatchingTagsInTree(tags if tags else [], self.rootNode)
        self.tagHT = {}  # No needed anymore.

        if debug_on:
            logging.debug(f"{self.totalShrink} of duation compressed")
            logging.debug(f"{self.shrinkCounter} spans shrank")
            logging.debug(f"{self.totalDrop} spans dropped")
            logging.debug(f"total executionTime {self.rootNode.duration}")

    def GetMatchingTagsInNode(self, tags, node):
        if not node:
            return []
        if len(tags) == 0:
            return []
        if node.sid not in self.tagHT:
            return []
        matchingTags = []
        for dictionary in self.tagHT[node.sid]:
            k = dictionary["key"] if "key" in dictionary else ""
            v = dictionary["value"] if "value" in dictionary else ""
            matchingTags += [
                item
                for item in tags
                if (k == item[common.TAG_NAME]) and (v == item[common.TAG_VALUE])
            ]
        debug_on and (len(matchingTags) > 0) and logging.debug(
            f"matching tags found: {self.filename}: {node.sid}",
        )
        return matchingTags

    def GetMatchingTagsInTree(self, tags, rootNode, curSearchDepth=1):
        matchingTags = self.GetMatchingTagsInNode(tags, rootNode)
        if len(matchingTags) == len(tags):
            return matchingTags

        newDepth = curSearchDepth + 1
        remainingTags = GetRemainingTags(matchingTags, tags, newDepth)
        for child in rootNode.children:
            moreMatches = self.GetMatchingTagsInTree(remainingTags, child, newDepth)
            if len(moreMatches) > 0:
                matchingTags += moreMatches
                remainingTags = GetRemainingTags(moreMatches, remainingTags, newDepth)
        return matchingTags

    def findARoot(self, node):
        # a DFS for finding the first node that matches the required service and operation name.
        if (
            self.processName[node.pid] == self.serviceName
            and node.opName == self.operationName
        ):
            return node
        for c in node.children:
            found = self.findARoot(c)
            if found:
                return found
        return None

    # input args:
    # - node: curr node
    # - depth: the depth of curr node
    # - stopErrNodeDepth: the depth of the closest ancestor (to this node) that
    #                     did not error out
    # - errCounts: a map from opCallpath to error counts
    # - errCallChainCounts: a map from opCallPath to error call-chain counts
    # - selfErrDepthList: a list of self error depth
    # - depthMap: a map from depth (key) to ErrCountsData
    # - propLengthMap: a map from propagation length (key) to count of self errors
    # - resiliencyMap: a map from canonicalOpName to ErrCountsData
    #        though we only update the propagated and stopped errors in this case
    #
    # return:
    # numAllErrors: the number of all errored out nodes in the program
    #
    # For computing error stats, we assume that the root node has depth 1
    def computeErrorStats(
        self,
        node,
        depth,
        stopErrNodeDepth,
        errCounts,
        errCallChainCounts,
        selfErrDepthList,
        stoppedErrDepthList,
        depthMap,
        propLengthMap,
        resiliencyMap,
    ):
        numAllErrors = 0
        childHasError = False

        if node.returnError:
            newStopErrNodeDepth = stopErrNodeDepth
        else:
            newStopErrNodeDepth = depth

        # a DFS traversal
        for c in node.children:
            if c.returnError:
                childHasError = True
            numAllErrors += self.computeErrorStats(
                c,
                depth + 1,
                newStopErrNodeDepth,
                errCounts,
                errCallChainCounts,
                selfErrDepthList,
                stoppedErrDepthList,
                depthMap,
                propLengthMap,
                resiliencyMap,
            )

        # update maps with my own stats
        op = self.canonicalOpName(node)
        opCallpath = self.getCallPath(node)
        if node.returnError:
            numAllErrors = numAllErrors + 1
            if childHasError:  # node has propagated error
                accumulateInDict(
                    errCounts,
                    opCallpath,
                    ErrCountsData(propagatedErrors=1),
                )
                accumulateInDict(depthMap, depth, ErrCountsData(propagatedErrors=1))
                accumulateInDict(resiliencyMap, op, ErrCountsData(propagatedErrors=1))
            else:  # node has self error
                selfErrDepthList.append(depth)
                accumulateInDict(errCounts, opCallpath, ErrCountsData(selfErrors=1))
                accumulateInDict(errCallChainCounts, opCallpath, 1)
                accumulateInDict(depthMap, depth, ErrCountsData(selfErrors=1))
                # my depth should always be greater than stopErrNodeDepth,
                # which is the depth of one of my proper ancestor; note that
                # the root node starts with depth 1
                accumulateInDict(propLengthMap, (depth - stopErrNodeDepth), 1)
        elif childHasError:  # node has suppressed error
            stoppedErrDepthList.append(depth)
            accumulateInDict(errCounts, opCallpath, ErrCountsData(stoppedErrors=1))
            accumulateInDict(depthMap, depth, ErrCountsData(stoppedErrors=1))
            accumulateInDict(resiliencyMap, op, ErrCountsData(stoppedErrors=1))

        return numAllErrors

    def computeGraphStats(self, node):
        # a DFS
        descendants = 0
        depth = 0  # the root node has depth 1 (+1 at return)
        for c in node.children:
            moreDescendants, newDepth = self.computeGraphStats(c)
            descendants = descendants + moreDescendants
            depth = newDepth if newDepth > depth else depth
        return descendants + 1, depth + 1

    def checkRootAndWarn(self, node, filename, rootTrace):
        if (
            self.processName[node.pid] != self.serviceName
            or node.opName != self.operationName
        ):
            logging.warning(
                (
                    f"rootTrace == {rootTrace}, expected serviceName={self.serviceName} and found {self.processName[node.pid]}. "
                    f"Expected operationName={self.operationName} and found {node.opName} in file {filename}",
                ),
            )
            return False
        return True

    def setTestResult(self, result):
        self.testing = result

    def checkMapContent(self, expected, result, mapName):
        for k, v in expected.items():
            if k not in result:
                return f"missing key {k!s} in {mapName}"
            if v != result[k]:
                # special handling of user-defined type
                if isinstance(result[k], ErrCountsData):
                    if v == result[k].toArray():
                        continue
                return f"in {mapName} key {k!s}: expected {v!s} found {result[k]!s}"
        for k in result:
            if k not in expected:
                return f"extra key {k!s} in {mapName}"
        return True

    def checkResults(
        self,
        cpp,
        work,
        timeSavedOnW,
        timeSavedOnCPAllSeries,
        errorCPMetrics,
    ):
        # Check if the critical path found by our algorithms matches the one
        # recorded in the "testing" section.
        if self.testing == {}:
            return None  # nothing to validate

        opTimeMap = {}
        for k, v in cpp.items():
            op = getLeafNodeFromCallPath(k)
            if op in opTimeMap:
                opTimeMap[op] += v.excl
            else:
                opTimeMap[op] = v.excl

        map_tag_res_msg_tuples = [
            (OP_TIME_EXCLUSIVE, opTimeMap, "opTimeExclusive"),
            (
                ERR_CP_CALLPATH_EXCLUSIVE,
                errorCPMetrics.errCPCallpathTimeExclusive,
                "errCPCallpathTimeExclusive",
            ),
            (ERR_CP_ERR_COUNTS, errorCPMetrics.errCPErrCounts, "errCPErrCounts"),
        ]
        val_tag_res_msg_tuples = [
            (TOTAL_WORK, work, "total work"),
            (TIME_SAVED_ON_WORK, timeSavedOnW, "time saved on work"),
            (TIME_SAVED_ON_CP, timeSavedOnCPAllSeries, "time saved on CP"),
        ]
        for tag, res, msg in map_tag_res_msg_tuples:
            if tag not in self.testing:
                continue
            expected = self.testing[tag]
            check = self.checkMapContent(expected, res, msg)
            if not check:
                return check

        for tag, res, msg in val_tag_res_msg_tuples:
            expected = self.testing[tag]
            if expected != res:
                return f"wrong {msg}: expected {expected!s} found {res!s}"

        return True

    def getPeerService(self, tags):
        for dictionary in tags:
            k = dictionary["key"].lower() if "key" in dictionary else None
            v = dictionary["value"] if "value" in dictionary else None
            if k == PEER_SERVICE:
                return v
        return None

    def getSpanKind(self, tags):
        for dictionary in tags:
            k = dictionary["key"].lower() if "key" in dictionary else ""
            v = dictionary["value"] if "value" in dictionary else ""
            if k == SPAN_KIND:
                if v.lower() == SERVER:
                    return SpanKind.SERVER
                if v.lower() == CLIENT:
                    return SpanKind.CLIENT
                return SpanKind.UNKNOWN
        return SpanKind.UNKNOWN

    def parseForErrorReturn(self, tags, logs):
        for dictionary in tags:
            k = dictionary["key"].lower() if "key" in dictionary else None
            t = dictionary["type"].lower() if "type" in dictionary else None
            v = dictionary["value"] if "value" in dictionary else False
            if k == "error" and (t == "string" or v):
                return True
            if k == "http.status_code" and int(v) >= 400:
                return True
            if k == "grpc.status" and (v != "OK" and v):
                return True

        for entry in logs:
            fields = entry[FIELDS] if FIELDS in entry else []
            for dictionary in fields:
                k = dictionary["key"].lower() if "key" in dictionary else None
                if k == "error.object":
                    return True
                if k == "error" and (
                    "type" in dictionary and dictionary["type"].lower() == "string"
                ):
                    return True
                if k == "event" and (
                    "value" in dictionary and dictionary["value"].lower() == "error"
                ):
                    return True
        return False

    def parseForErrorReturnFromParquet(self, statusCode, error, _errorMessage, rpcSystem):
        # TODO: errorMessage is not used yet, YARPC is not landed yet
        if error:
            return True
        if rpcSystem and rpcSystem.lower() == "grpc" and statusCode != 0:
            return True
        if rpcSystem and rpcSystem.lower() == "http" and statusCode >= 400:
            return True
        return False

    # For all nodes that are identified as isErrPropNode(), pass through the child's error to this node.
    # We expect the node to have a single child. Violations will be logged.
    # Post order traversal.
    def propagateErrors(self, me, errPropSpans):
        if len(me.children) == 0:
            return
        for c in list(me.children.keys()):
            self.propagateErrors(c, errPropSpans)

        if me.sid not in errPropSpans:
            return

        if len(me.children) != 1:
            debug_on and logging.debug(
                f"Expected 1 child, but Span {me.sid}'s has {len(me.children)} children in: file = {self.filename}",
            )
            return

        child = next(iter(me.children.keys()))
        if (not me.returnError) and (child.returnError):
            me.returnError = child.returnError

    def buildParentChildRelationships(self, potentialRoots):
        for spanId in self.nodeHT:
            me = self.nodeHT[spanId]
            parentId = me.parentSpanId
            if not parentId:
                potentialRoots.append(me)
                continue
            if parentId not in self.nodeHT:
                potentialRoots.append(me)
                continue

            # Parent is a proxy node; short wire it
            if parentId in self.proxyNodes:
                self.proxyNodes[parentId] += 1
                proxyGraphNode = self.nodeHT[parentId]
                parentId = proxyGraphNode.parentSpanId
                if parentId not in self.nodeHT:
                    self.numProxyRoots += 1
                    potentialRoots.append(me)
                    continue

            parent = self.nodeHT[parentId]
            me.setParent(parent)
            if spanId not in self.proxyNodes:
                parent.addChild(me)


    def propagateErrorsToRoots(self, potentialRoots, errPropNodes):
        for root in potentialRoots:
            self.propagateErrors(root, errPropNodes)

    def storeNodeData(self, thisSpan, node, serviceName, operationName, errPropNodes, spanTags):
        self.nodeHT[thisSpan] = node
        self.tagHT[thisSpan] = spanTags
        if self.filterProxy:
            if isProxyNode(serviceName, operationName):
                self.proxyNodes[thisSpan] = 0
            if isErrPropNode(serviceName, operationName):
                errPropNodes[thisSpan] = 0

    def parseNode(self, jsonData):
        # given jaeger jsonData blob, build the Graph().

        potentialRoots = []
        numErrors = 0
        isCtfTest = False
        errPropNodes = {}

        # pass 1: record service names and other KV data first
        for item in jsonData["data"]:
            for p in item[PROCESSES]:
                self.processName[p] = item[PROCESSES][p]["serviceName"]
                if isTestTraceByServiceName(self.processName[p]):
                    isCtfTest = True
                if TAGS in item[PROCESSES][p]:
                    for dictionary in item[PROCESSES][p][TAGS]:
                        if dictionary["key"] == HOSTNAME:
                            self.hostMap[p] = dictionary["value"]
                        elif dictionary["key"] == "region":
                            self.regionMap[p] = dictionary["value"]
                        elif dictionary["key"] == "zone" and p not in self.regionMap:
                            self.regionMap[p] = dictionary["value"]

        # pass 2: extract all spans and create one GraphNode for each.
        for item in jsonData["data"]:
            for span in item[SPANS]:
                thisSpan = span[SPAN_ID]
                parentSpanId = None
                # We only care about CHILD_OF spans reachable from the root.
                for parent in span[REFERENCES]:
                    if parent[REF_TYPE] == CHILD_OF:
                        parentSpanId = parent[SPAN_ID]

                spanTags = span[TAGS] if (TAGS in span) else {}
                spanLogs = span[LOGS] if (LOGS in span) else {}
                hasError = self.parseForErrorReturn(spanTags, spanLogs)
                if hasError:
                    numErrors += 1

                spanKind = self.getSpanKind(spanTags)
                peerService = self.getPeerService(spanTags)

                opName = span[OPERATION_NAME]
                pid = span[PROCESS_ID]
                if isTestTraceByOpName(opName):
                    isCtfTest = True

                node = GraphNode(
                    thisSpan,
                    span[START_TIME],
                    span[DURATION],
                    parentSpanId,  # no parent YET, only spanID is available.
                    opName,
                    pid,
                    spanKind,
                    peerService,
                    hasError,
                )
                self.storeNodeData(thisSpan, node, self.processName[pid], opName, errPropNodes, spanTags)

        self.numErrors = numErrors

        # pass 3: build parent-child relationships
        self.buildParentChildRelationships(potentialRoots)

        # pass 4. Propagate errors for nodes identified as errorProps.
        self.propagateErrorsToRoots(potentialRoots, errPropNodes)

        # for testing only, we keep the expected results in TESTING section of JSON.
        # pass 5: record test results
        if TESTING in jsonData and len(jsonData[TESTING]) > 0:
            self.setTestResult(jsonData[TESTING])

        return potentialRoots, isCtfTest

    def parseNodeFromParquet(self, parquetData):
        # Builds graph from Parquet data and returns potential roots.
        '''
        parquetData: a dictionary containing the data from a parquet file.
        '''
        # TODO: we need to map 'Kind' from integer back into 'server'/'client' in order to do error analysis.
        # Note: we don't really care about 'Links' https://opentelemetry.io/docs/concepts/signals/traces/#span-links so far.
        # Instead we assume if the ParentSpanID is non empty then it's a valid parent-child relationship.
        potentialRoots = []
        numErrors = 0
        isCtfTest = False
        errPropNodes = {}
        pidMap = {}  # Mapping from serviceName to a unique pid, required by findAllRoots() and other functions.

        parquetSpanSets = parquetData[PARQUET_SPAN_SET]

        # Pass 1: Record service names, hostnames, and generate unique pids based on (serviceName, hostName) tuple
        for item in parquetSpanSets:
            process = item[PARQUET_PROCESS]
            serviceName = process[PARQUET_SERVICE_NAME]
            hostName = process[PARQUET_HOSTNAME]

            # Use a tuple (serviceName, hostName) as the unique key
            service_host_key = (serviceName, hostName)

            # If this service + host combination hasn't been assigned a pid yet, assign a new one
            if service_host_key not in pidMap:
                pid = len(pidMap) + 1  # Generate a unique pid
                pidMap[service_host_key] = pid
                self.processName[pid] = serviceName
                self.hostMap[pid] = hostName

            if isTestTraceByServiceName(serviceName):
                isCtfTest = True

        # Pass 2: Extract spans and create one GraphNode for each
        for item in parquetSpanSets:
            spans = item[PARQUET_SPANS]
            for span in spans:
                # Convert SpanID and ParentSpanID to hex strings
                thisSpan = common.intToHexString(span[PARQUET_SPAN_ID])
                parentSpanId = common.intToHexString(span[PARQUET_PARENT_SPAN_ID]) if span.get(PARQUET_PARENT_SPAN_ID) else None

                # Extract other span information
                spanKind = span[PARQUET_KIND]
                operationName = span[PARQUET_OPERATION_NAME]
                serviceName = item[PARQUET_PROCESS][PARQUET_SERVICE_NAME]
                hostName = item[PARQUET_PROCESS][PARQUET_HOSTNAME]

                if isTestTraceByOpName(operationName):
                    isCtfTest = True

                # Use the tuple (serviceName, hostName) as the key to get the pid from pidMap
                service_host_key = (serviceName, hostName)
                pid = pidMap[service_host_key]

                # Error checking
                statusCode = span.get(PARQUET_RPC_STATUS_CODE, None)
                error = span.get(PARQUET_ERROR, None)
                errorMessage = span.get(PARQUET_ERROR_MESSAGE, None)
                rpcSystem = span.get(PARQUET_RPC_SYSTEM, None)

                hasError = self.parseForErrorReturnFromParquet(statusCode, error, errorMessage, rpcSystem)

                if hasError:
                    numErrors += 1

                spanTags = span.get(PARQUET_TAGS, []) # TODO: this needs to be verified. For now we are not going to test on user-defined tags.

                # Create the GraphNode for the span, and assign the pid
                node = GraphNode(
                    thisSpan,
                    span[PARQUET_START_TIME],
                    span[PARQUET_DURATION],
                    parentSpanId,
                    operationName,
                    pid,
                    spanKind,
                    None,  # Peer service not present in the data structure
                    hasError
                )

                # Store the node and span data
                self.storeNodeData(thisSpan, node, serviceName, operationName, errPropNodes, spanTags)


        self.numErrors = numErrors

        # Pass 3: Build parent-child relationships
        self.buildParentChildRelationships(potentialRoots)

        # Pass 4: Propagate errors
        self.propagateErrorsToRoots(potentialRoots, errPropNodes)

        return potentialRoots, isCtfTest

    def removeExcludedOps(self, curNode, exclusionSet):
        removeList = []
        for c in curNode.children:
            serviceName = self.processName[c.pid]
            opName = c.opName
            if (serviceName, opName) in exclusionSet:
                removeList.append(c)
                # logging.info(f"dropping { (serviceName, opName)}")
            else:
                # recursion
                self.removeExcludedOps(c, exclusionSet)
        # now delete the list of items marked for deletion.
        for r in removeList:
            r.parent = None
            del curNode.children[r]

    def sanitizeOverflowingChildren(self, curNode):
        # if a child overflows or underflows its parent, it will be truncated/deleted to match/adhere to parent timeline.
        parentStart = curNode.startTime
        parentEnd = curNode.endTime

        removeList = []
        for c in curNode.children:
            childStart = c.startTime
            childEnd = c.endTime

            debug_on and logging.debug(f"working on parent {curNode}, child {c}")
            debug_on and logging.debug(
                f"parent start {parentStart}, parent end {parentEnd}",
            )
            debug_on and logging.debug(
                f"child start {childStart}, child end {childEnd}",
            )

            if isClientServerCall(self.hostMap, curNode, c):
                debug_on and logging.debug(
                    f"{curNode.sid}, {curNode.opName}, {c.sid}, {c.opName}, case 0",
                )
                # So, curNode is client and c is a server.
                # Don't chop c to adjust to curNode's times.
                # continue recursion
                self.sanitizeOverflowingChildren(c)
            elif childStart >= parentStart and childEnd <= parentEnd:
                # case 1: everything looks good
                # |----parent----|
                #   |----child--|
                debug_on and logging.debug("Case 1")
                # continue recursion
                self.sanitizeOverflowingChildren(c)
            elif (
                childStart < parentStart
                and childEnd <= parentEnd
                and childEnd > parentStart
            ):
                # case 2: child start before parent, truncate is needed
                #      |----parent----|
                #   |----child--|
                debug_on and logging.debug("Case 2")
                shrunk = parentStart - childStart
                self.totalShrink += shrunk
                self.shrinkCounter += 1
                c.startTime = parentStart
                c.duration -= shrunk
                debug_on and self.dumpShrinkStats(curNode, c, shrunk)
                # continue recursion
                self.sanitizeOverflowingChildren(c)
            elif (
                childStart >= parentStart
                and childEnd > parentEnd
                and childStart < parentEnd
            ):
                # case 3: child end after parent, truncate is needed
                #      |----parent----|
                #              |----child--|
                debug_on and logging.debug("Case 3")
                shrunk = childEnd - parentEnd
                self.totalShrink += shrunk
                self.shrinkCounter += 1
                c.duration -= shrunk
                c.endTime -= shrunk
                debug_on and self.dumpShrinkStats(curNode, c, shrunk)
                # continue recursion
                self.sanitizeOverflowingChildren(c)
            else:
                # case 4: child outside of parent rantge =>  drop the child span
                #      |----parent----|
                #                        |----child--|
                # or
                #                      |----parent----|
                #       |----child--|
                debug_on and logging.debug("Case 4")
                debug_on and self.dumpDeletionStats(curNode, c, c.duration)
                removeList.append(c)
                # no recursion; all descendants will become unreachable from the root.

        # now delete the list of items marked for deletion.
        for r in removeList:
            r.parent = None
            del curNode.children[r]

    def dumpShrinkStats(self, curNode, child, shrunk):
        logging.debug(
            (
                f" shrunk node {curNode} id:{curNode.sid} duration {curNode.duration} by {shrunk}: child {child} "
                f"id:{child.sid} => {shrunk / curNode.duration * 100}  reduction",
            ),
        )

    def dumpDeletionStats(self, curNode, child, shrunk):
        if curNode.duration > 0:
            logging.debug(
                (
                    f" delete: parent node {curNode} id:{curNode.sid}  duration {curNode.duration} with child {child} id:{child.sid} "
                    f"of {shrunk} => {shrunk / curNode.duration * 100}  reduction",
                ),
            )

    def computeCriticalPath(self, curNode):
        # Recursively find the critical path for curNode.
        debug_on and logging.debug(
            f"Working on CP parent {curNode} {self.canonicalOpName(curNode)}",
        )

        # step 0. curNode is obviously on the critical path.
        criticalPath = [curNode]

        if len(curNode.children) == 0:
            debug_on and logging.debug(f"{curNode} has no children")
            return criticalPath

        # step 1. reverse sort all children of curNode by their end time.
        sortedChildren = sorted(curNode.children, key=lambda x: x.endTime)[::-1]

        # step 2. begin by the child who finishes last
        lrc = sortedChildren[0]
        criticalPath.extend(self.computeCriticalPath(lrc))
        lastStartTime = lrc.startTime

        for cn in sortedChildren[
            1:
        ]:  # first one (actually the last one) is already added
            # step 3. get the child who finished just before the start of lrc's start
            if self.happensBefore(curNode, sortedChildren, cn, lrc):
                debug_on and logging.debug(
                    f"Adding child {cn} {self.canonicalOpName(cn)} to CP",
                )
                # # step 4. recur on cn, which is on the critical path.
                criticalPath.extend(self.computeCriticalPath(cn))
                lrc = cn
                lastStartTime = min(lastStartTime, cn.startTime)
            else:
                debug_on and logging.debug(
                    f"NOT adding child {cn} {self.canonicalOpName(cn)} to CP",
                )

            debug_on and logging.debug(f"lastStartTime = {lastStartTime}")
        return criticalPath

    # isRPCNode returns true if the node is a server or client node, not a user-defined span.
    def isRPCNode(self, node):
        return node.spanKind == SpanKind.SERVER or node.spanKind == SpanKind.CLIENT

    # computePropToRootGraph is used to build an error flamegraph
    def computePropToRootGraph(self, rootNode=None):
        """Compute error propagation to root graph.

        Args:
            rootNode: Optional root node to start from. If None, uses self.rootNode.
        """
        node = self._get_root_node(rootNode)

        def callerCalleeSpansInTheSameService(parent, child):
            return self.processName[parent.pid] == self.processName[child.pid]

        def visitChildrenOfErroringParent(errMap, myPath, parentNode, parentSvcName):
            # Nodes that return an error (both internal and user-defined)
            haveErrChild = sum(computePropToRootGraphInternal(errMap, myPath, cn, parentSvcName)
                               for cn in parentNode.children)
            # Accumulate error if none of the children reported error
            if haveErrChild == 0:
                accumulateInDict(errMap, myPath, 1)
            return 1  # Since parentNode.returnError is True

        def computePropToRootGraphInternal(errMap, parent, curNode, currentService):
            nodeService = self.processName[curNode.pid]
            myPath = self.appendCallPath(parent, curNode)

            if self.isRPCNode(curNode):
                # Internal node (client/server span)
                currentService = nodeService
                if not curNode.returnError:
                    # Internal node without error: stop propagation
                    return 0
                # Proceed to traverse children with error
                return visitChildrenOfErroringParent(errMap, myPath, curNode, currentService)

            if curNode.returnError:
                # User-defined span that returns an error
                currentService = nodeService  # Update current service
                # Proceed to traverse children with error
                return visitChildrenOfErroringParent(errMap, myPath, curNode, currentService)

            # User-defined span that does not return an error
            # Will proceed to a child only if the child is within the same process boundary.
            haveErrChild = 0
            for cn in curNode.children:
                if callerCalleeSpansInTheSameService(curNode, cn):
                    haveErrChild += computePropToRootGraphInternal(
                        errMap, myPath, cn, currentService
                    )
            # Since curNode.returnError is False, we don't accumulate error here
            # Return 1 if any child reported an error
            return 1 if haveErrChild else 0

        errMap = {}
        rootService = self.processName[node.pid]
        computePropToRootGraphInternal(errMap, "", node, rootService)
        return errMap

    # This function computes errors along the critical path (errCP).  That is, we add a node to the errCP
    # if and only if the node is on the critical path and a) returning an error or b) a ancestor of another
    # node on the critical path and returning an error
    def computeErrorsOnCriticalPath(self, curNode):
        # Recursively find the critical path for curNode.
        debug_on and logging.debug(
            f"Working on errCP parent {curNode} {self.canonicalOpName(curNode)}",
        )

        # step 0. base case
        if len(curNode.children) == 0:
            debug_on and logging.debug(f"{curNode} has no children")
            errCP = [curNode] if curNode.returnError else []
            return errCP

        # step 1. reverse sort all children of curNode by their end time.
        sortedChildren = sorted(curNode.children, key=lambda x: x.endTime)[::-1]

        # step 2. begin by the child who finishes last
        errCP = []
        lrc = sortedChildren[0]
        errCP.extend(self.computeErrorsOnCriticalPath(lrc))
        lastStartTime = lrc.startTime

        # first one (actually the last one) is already added
        for cn in sortedChildren[1:]:
            # step 3. get the child who finished just before the start of lrc's start
            if self.happensBefore(curNode, sortedChildren, cn, lrc):
                debug_on and logging.debug(
                    f"Adding child {cn} {self.canonicalOpName(cn)} to errCP",
                )
                # step 4. recur on cn, which is on the critical path.
                errCP.extend(self.computeErrorsOnCriticalPath(cn))
                lrc = cn
                lastStartTime = min(lastStartTime, cn.startTime)
            else:
                debug_on and logging.debug(
                    f"NOT adding child {cn} {self.canonicalOpName(cn)} to errCP",
                )

            debug_on and logging.debug(f"lastStartTime = {lastStartTime}")

        # step 5. add curNode to front assuming some descendant contains error
        #         or if curNode itself returns error
        if errCP != [] or curNode.returnError:
            errCP.insert(0, curNode)

        return errCP

    # This function computes full errors on the critical path (fullErrCP).  The computation is
    # similar to computErrorsOnCriticalPath except that the computed fullErrCP additionally includes
    # some nodes F that is not necessarily on the critical path.  A node F can be in fullErrCP
    # but not in errCP if and only if F has some acenstor G who is on the critical path, G errors
    # out, and F and G are connected via a chain of nodes who all errored out.
    #
    # Example:
    #   |--- root ---------------------------------|
    #    |--- A ---------------------------------|
    #      |--- B ------|--|           |
    #        |--D ---|  |--- C --------|
    #
    # The graph shows that
    # root -> A -> B -> D
    #           -> C
    #
    # here -> indicates call, and B and C overlap (in parallel).
    # Assume that A, B, D errored out
    # Then, the function returns [root, A, B, D].  Note that B and D are not on the critrical path
    #   but they are included because they are connected to A (which is on the critical path) via a
    #   chain of nodes that errored out.  Also note that if A hadn't errored out, this function
    #   would have returned [] (i.e., no errors along the critical path, which is root -> A -> C.
    #
    # We should only recur on a curNode if and only if either curNode is on CP or curNode is not on
    # CP but is connected to an ancestor on CP via a chain of nodes that errored out
    # Inupt: self (the graph), curNode that we are considering, and onCP that indicate whether the
    # curNode is onCP or not.
    def computeFullErrorsOnCriticalPath(self, curNode, onCP):
        # Recursively find the critical path for curNode.
        debug_on and logging.debug(
            f"Working on fullErrCP parent {curNode} {self.canonicalOpName(curNode)}",
        )

        # step 0. base case
        # we can stop recurring if curNode does not error out and is not on the critical path
        if not curNode.returnError and not onCP:
            return []

        # otherwise curNode either errored out or is on the critical path
        if len(curNode.children) == 0:
            debug_on and logging.debug(f"{curNode} has no children")
            if curNode.returnError:
                return [curNode]
            return []

        # step 1. reverse sort all children of curNode by their end time.
        sortedChildren = sorted(curNode.children, key=lambda x: x.endTime)[::-1]

        # step 2. begin by the child who finishes last
        fullErrCP = []
        lrc = sortedChildren[0]
        # propagate the onCP from curNode: lrc is on the CP if and only if curNode is on the CP
        fullErrCP.extend(self.computeFullErrorsOnCriticalPath(lrc, onCP))
        lastStartTime = lrc.startTime

        # first one (actually the last one) is already added
        for cn in sortedChildren[1:]:
            # step 3. get the child who finished just before the start of lrc's start
            if self.happensBefore(curNode, sortedChildren, cn, lrc):
                debug_on and logging.debug(
                    f"Recur on child {cn} {self.canonicalOpName(cn)} for fullErrCP, on CP? {onCP}",
                )
                # step 4a. recur on the new lrc, who gets parent's onCP
                fullErrCP.extend(self.computeFullErrorsOnCriticalPath(cn, onCP))
                lrc = cn
                lastStartTime = min(lastStartTime, cn.startTime)
            else:
                if curNode.returnError:
                    debug_on and logging.debug(
                        f"Recur on child {cn} {self.canonicalOpName(cn)} for fullErrCP, on CP? False",
                    )
                    # step 4b. recur on cn because curNode (its parent) errored out and is on the critical path.
                    fullErrCP.extend(
                        self.computeFullErrorsOnCriticalPath(cn, onCP=False),
                    )
            debug_on and logging.debug(f"lastStartTime = {lastStartTime}")

        # step 5. add curNode to front assuming some descendant contains error
        #         or if curNode itself returns error
        #         Note that if we got to this point, the curNode must be on the CP or returns error
        if fullErrCP != [] or curNode.returnError:
            fullErrCP.insert(0, curNode)

        return fullErrCP

    # This function returns the (total work, work saved) under curNode
    # When span A invokes span B, we don't know whether span A is doing other useful work
    # concurrently before B returns or A is simply blocked on B.  There is no way to tell
    # based on the trace info in the jaeger file.  For now, we will assume A is doing useful work.
    # As such, we might be overcounting the total work and undercounting timeSavedOnWork due
    # to error.
    def computeTimeSavedOnWork(self, curNode):
        # base case
        totalWork = curNode.duration
        timeSavedOnWork = curNode.duration if curNode.returnError else 0

        # will skip if no children
        for cn in curNode.children:
            cnWork, cnTimeSaved = self.computeTimeSavedOnWork(cn)
            totalWork += cnWork
            timeSavedOnWork += cnTimeSaved
        return totalWork, timeSavedOnWork

    def calcTimeSaved(self, origLrcEndTime, newLrcEndTime, overlap):
        saving = origLrcEndTime - newLrcEndTime
        assert saving >= 0
        if saving > overlap:
            timeSaved = saving - overlap
        else:
            timeSaved = 0
        return timeSaved

    # getPBlocks returns a list of PBlocks, where each PBlock is a set of transitively overlapping children.
    # The list is sorted by the end time of the PBlocks.
    # For example, consider we have 4 children A, B, C, and D, where A overlaps with B, B overlaps with C, but A
    # does not overlap with C and finally D does not overlap with any of A, B, or C.
    # Then, getPBlocks will return a list of 3 PBlocks: [A, B, C], [D].
    def getPBlocks(self, curNode):
        pBlocks = []
        if len(curNode.children) == 0:
            return pBlocks

        # Put all children in in heapq by their end time.
        pq = []
        for c in curNode.children:
            # Since we need a MAX heap, we use negative end time.
            heapq.heappush(pq, (-c.endTime, c))

        _, lrc = heapq.heappop(pq)
        overlap = 0
        curPBlock = PBlock(curNode, lrc, overlap)

        while len(pq) > 0:
            # Pop the next child from the heap.
            _, lrc = heapq.heappop(pq)
            if curPBlock.HappensAfter(self, lrc):
                # overlap how much pred overlaps with pBlock
                overlap = max(0, lrc.endTime - curPBlock.startTime)
                pBlocks.append(curPBlock)
                # Start a new block.
                curPBlock = PBlock(curNode, lrc, overlap)
            else:  # span is in this block.
                curPBlock.Add(lrc)

        # Add the last (first in time order) block.
        pBlocks.append(curPBlock)
        return pBlocks

    # computeTimeSavedForPBlockPessimistic computes the time saved on the critical path for a PBlock if the error-returing chidren are
    # eliminated (recursively).
    # The algorithm is recursive, hence, some child may shrink in length instead of being eliminated.
    # The "pessimism" comes from the fact that we only consider those children that will end the block.
    # Time reduced by any of their predecessors (if any) is not considered.
    # In the following figure: where the pBlock contains 5 children X, A, B, Y, and C.
    # X runs from 0 to 20; A runs from 20 to 50; B runs from 50 to 100; Y runs from 10 to 30; C runs from 30 to 80.
    #   |(0)--- root (pBlock) --------------------------------------------(100)|
    #   |(0)---X---(20)||(20) --- A ---(50)||(50)------------B------------(100)|
    #         |(10)---Y---(30)||(30) ---------------C------------(80)|
    # Assume, X shrinks by 10 time units; A shrinks by 20 time units; B shrinks by 30 time units; Y shrinks by 10 time units; C shrinks by 5
    # time units.
    # Then, the time saved considers only B and C's shrinkage, ignoring the rest because they are the only two canidates to end the block.
    # The time saved due to B's shrinkage is 30 time units => 30 time units shirnk for the pBlock.
    # The time saved due to C's shrinkage is 5 time units => 5 + 20 (distance from the end of C to end of the pBlock)= 25 time units shirnk
    # for the pBlock.
    # The resulting time saved is the lesser of the two => 25 time units.
    # The algorithm proceeds by identifying the last retunring child LRC in the pBlock.
    # It then computes the time saved for the LRC and checks if a new child happens to become the LRC after the shrinkage.
    # As soon as we find an a child whose shrunk (if any) time is the LRC, we stop processing any more children because they will only
    # shrink to a smaller time, where as this one ends later than the rest.
    # The end time of such "processed" LRC subtracted from the end time of the pBlock is the time saved on the pBlock.
    # Another way to look at the pessimistic algorithm is that the "start time" of any span does not change; only the end times change, leaving  # noqa: E501
    # only the last child to dictate the time reduction.
    def computeTimeSavedForPBlockPessimistic(
        self,
        pBlock: PBlock,
        timeSavedAttribute: str,
        perBlockFunctionPtr,
    ):
        assert len(pBlock.spanSet) > 0
        # Put all children in to heapq by their end time.
        UNPROCESSED = 1
        PROCESSED = 0
        pq = []
        for c in pBlock.spanSet:
            # Since we need a MAX heap, we use negative end time.
            # Here we want the PROCESSED ones to come out first when times are equal, initially, all are UNPROCESSED.
            heapq.heappush(pq, (-c.endTime, UNPROCESSED, c))

        while len(pq) > 0:
            # Pop the next child from the heap.
            # If it is PROCESSED, we are done with this block.
            # If it is UNPROCESSED, we need to process it.
            curEndTime, isProcessed, lrc = heapq.heappop(pq)
            # if lrc is PROCESSED, we are done with this block.
            if isProcessed == PROCESSED:
                # this is the LRC of the block.
                # Compute the time saved on the block.
                timeSaved = self.calcTimeSaved(
                    pBlock.endTime,
                    -curEndTime,
                    pBlock.overlap,
                )
                assert timeSaved <= (pBlock.endTime - pBlock.startTime)
                return timeSaved
            # else => UNPROCESSED
            lrcTimeSaved = self.computeTimeSavedOnCPReal(
                lrc,
                timeSavedAttribute,
                perBlockFunctionPtr,
            )
            assert lrcTimeSaved <= lrc.duration
            lrcNewEndTime = lrc.endTime - lrcTimeSaved
            # Push it back into the pq.
            heapq.heappush(pq, (-lrcNewEndTime, PROCESSED, lrc))

        # should never reach here
        raise AssertionError

    # Traverse the children block by block, where each block is a set of overlapping children.
    # For each block, compute the time saved and add it to the total time saved.
    def computeTimeSavedOnCPReal(
        self,
        curNode,
        timeSavedAttribute: str,
        perBlockFunctionPtr,
    ):
        # Using setattr to make the function generic.
        setattr(curNode, timeSavedAttribute, 0)

        # step 0, base case
        # if curNode itself returns error, its time saved is the entire duration
        if curNode.returnError:
            v = curNode.duration
            setattr(curNode, timeSavedAttribute, v)
            return v

        # if curNode has no children, we are done
        if len(curNode.children) == 0:
            return 0

        pBlocks = self.getPBlocks(curNode)
        assert len(pBlocks) > 0
        timeSaved = 0
        for pBlock in pBlocks:
            v = perBlockFunctionPtr(pBlock, timeSavedAttribute, perBlockFunctionPtr)
            assert v <= (pBlock.endTime - pBlock.startTime)
            assert v >= 0
            timeSaved += v

        # While the time saved in each PBlock is guaranteed to be less than or equal to the duration of the PBlock,
        # the total time saved is not guaranteed to be less than or equal to the duration of the curNode since the
        # PBlocks may slightly overlap by the definition of "HappensBefore".
        # Adjust the time saved to be less than or equal to the duration of the curNode.
        assert timeSaved >= 0
        # assert timeSaved <= curNode.duration
        timeSaved = min(timeSaved, curNode.duration)
        setattr(curNode, timeSavedAttribute, timeSaved)
        return timeSaved

    def computeTimeSavedOnCPPessimistic(self, curNode):
        return self.computeTimeSavedOnCPReal(
            curNode,
            "timeSavedOnCPPessimistic",
            self.computeTimeSavedForPBlockPessimistic,
        )

    # computeTimeSavedForChain recursively computes the time saved for a chain of spans that were originally in series.
    # The chain in formed end to start. For each node, its immediate predecessor is the one that happens before it.
    def computeTimeSavedForChain(
        self,
        chainEndNode,
        pBlock,
        resultCache,
        sortedpeers,
        timeSavedAttribute: str,
        perBlockFunctionPtr,
    ):
        assert chainEndNode

        if chainEndNode in resultCache:
            return resultCache[chainEndNode]

        timesavedOptimistic = self.computeTimeSavedOnCPReal(
            chainEndNode,
            timeSavedAttribute,
            perBlockFunctionPtr,
        )
        setattr(chainEndNode, timeSavedAttribute, timesavedOptimistic)

        # Find predecessors of chainEndNode in sortedpeers.
        for idx, p in enumerate(sortedpeers):
            if self.happensBefore(pBlock.parent, pBlock.spanSet, p, chainEndNode):
                timesavedOptimistic += self.computeTimeSavedForChain(
                    p,
                    pBlock,
                    resultCache,
                    sortedpeers[idx + 1 :],
                    timeSavedAttribute,
                    perBlockFunctionPtr,
                )
                break

        resultCache[chainEndNode] = timesavedOptimistic
        return timesavedOptimistic

    # Returns the spanset of Pblock ordered by thier end time (high to low),
    # and returns a dict of span -> index in the sorted list.
    def GetCandidateBlockEndingSpans(self, pBlock: PBlock):
        assert len(pBlock.spanSet) > 0

        # 1. Sort by span end time to get the LRC in the pBlock.
        # 2. Get all spans in parallel with LRC.

        # 1. Sort by span end time to get the LRC in the pBlock.
        sortedSpans = sorted(pBlock.spanSet, key=lambda x: x.endTime)[::-1]
        lrc = sortedSpans[0]

        # 2. Get all spans in parallel with LRC.
        blockEndCandidateSpans = {}
        blockEndCandidateSpans[lrc] = 0

        for idx, s in enumerate(sortedSpans[1:]):
            # We have visited all spans that are in parallel with LRC.
            if self.happensBefore(pBlock.parent, sortedSpans, s, lrc):
                break
            # Skip the span if it is in series with any of the blockEndCandidateSpans.
            for k in blockEndCandidateSpans.keys():
                if self.happensBefore(pBlock.parent, sortedSpans, s, k):
                    continue
            blockEndCandidateSpans[s] = idx
        return sortedSpans, blockEndCandidateSpans

    # This function computes the time saved in a PBlock if the error-returing chidren were eliminated (recursively).
    # The algorithm is recursive, hence, some child may shrink in length instead of being eliminated.
    # This is similar to computeTimeSavedForPBlockPessimistic but the time saved is computed for all children forming a chain of successors/predecessor,  # noqa: E501
    # not just the last ones.
    # Similar to computeTimeSavedForPBlockPessimistic, it is seeded with the set of block-ending spans that are in parallel.
    # The "optimism" comes from the fact that by such shrinkage of the chain, spans that were previously in series may now become in parallel.  # noqa: E501
    # In the following figure: where the pBlock contains 5 children X, A, B, Y, and C.
    # X runs from 0 to 20; A runs from 20 to 50; B runs from 50 to 100; Y runs from 0 to 30; C runs from 30 to 80.
    #   |(0)--- root (pBlock) --------------------------------------------(100)|
    #   |(0)---X---(20)||(20) --- A ---(50)||(50)------------B------------(100)|
    #   |(0)----Y------(30)||(30) ---------------C------------(80)|
    # Assume, X shrinks by 10 time units; A shrinks by 20 time units; B shrinks by 30 time units; Y shrinks by 20 time units; C shrinks by 5 time units.  # noqa: E501
    # There are two chains: X->A->B and Y->C. The time saved considers both chains.
    # The time saved in X->A->B is 10 + 20 + 30 = 60 time units. This would make the pBlock end at 40 time units.
    # The time saved in Y->C is 20 + 5 = 25 time units. This would make the pBlock end at 55 time units.
    # The resulting time saved is the longest chain Y->C and the time saved is 100 - 55 = 45 time units.
    # Notice here that X was originally in series with C but after the shrinkage, they are in parallel, which is allowed in this optimistic algorithm.  # noqa: E501
    def computeTimeSavedForPBlockOptimistic(
        self,
        pBlock: PBlock,
        timeSavedAttribute: str,
        perBlockFunctionPtr,
    ):
        # 1. Get the sorted spans and the candidate block ending spans.
        # 2. Get the longest chains ending in each of blockEndCandidateSpans.
        # 3. Compute the time saved for each chain.
        # 4. Return the time saved via the longest chain after reducing their lenths.

        # 1. Get the sorted spans and the candidate block ending spans.
        sortedSpans, blockEndCandidateSpans = self.GetCandidateBlockEndingSpans(pBlock)

        # 2. Get the longest chains ending in each of blockEndCandidateSpans.
        #    Each chain is a list of spans.
        longestSofar = -1
        resultCache = {}

        for span, idx in blockEndCandidateSpans.items():
            peerStartIdx = idx + 1
            timeSaved = self.computeTimeSavedForChain(
                span,
                pBlock,
                resultCache,
                sortedSpans[peerStartIdx:],
                timeSavedAttribute,
                perBlockFunctionPtr,
            )
            longestSofar = max(longestSofar, span.endTime - timeSaved)

        assert longestSofar >= 0
        return max(0, pBlock.endTime - longestSofar - pBlock.overlap)

    def computeTimeSavedOnCPOptimistic(self, curNode):
        return self.computeTimeSavedOnCPReal(
            curNode,
            "timeSavedOnCPOptimistic",
            self.computeTimeSavedForPBlockOptimistic,
        )

    # GetNonTransitiveHBInPBlock returns a dictionary of non-transitive happens-before (hb) relationships for each span in the pBlock.
    # The dictionary is keyed by the span and the value is a set of spans that are in series with the key span.
    # Consider the following pBlock with 4 span A, B, C, and D.
    #  |------A------|  |------B------|
    #         |------C------------------|  |------D------|
    # The dictionary will be:
    #  A -> {} # no predecessors.
    #  B -> {A} # A happend before B.
    #  C -> {} # no predecessors.
    #  D -> {C, B} # C and B happen before D. Although A happens before B, it is already in the set of B hence not added.

    def GetNonTransitiveHBInPBlock(self, pBlock: PBlock) -> dict[GraphNode, set]:
        # A dictionary of non-transitive happens-before (hb) relationships for each span in the pBlock.
        hbCache = {}
        # Sort by span end time to process from end to start.
        sortedSpans = sorted(pBlock.spanSet, key=lambda x: x.endTime)[::-1]
        ws = {sortedSpans[0]}
        hbCache[sortedSpans[0]] = set()
        for s in sortedSpans[1:]:
            removeList = []
            for inflight in ws:
                if not self.happensBeforeSimple(s, inflight):
                    continue
                # s is in series with inflight (directly or transitively)

                # Easy case for adding to the set if it is directly in series with inflight.
                if len(hbCache[inflight]) == 0:
                    hbCache[inflight].add(s)
                    continue

                inseriesCount = 0
                for span in hbCache[inflight]:
                    if self.happensBeforeSimple(s, span):
                        inseriesCount += 1
                # if s is in series with all inflight spans, remove inflight from ws.
                if inseriesCount == len(hbCache[inflight]):
                    removeList.append(inflight)
                elif inseriesCount == 0:
                    # No transitivity found, hence add it.
                    hbCache[inflight].add(s)
                else:
                    # if s is in series with some but not all inflight spans, do nothing
                    pass

            # Remove finished from the ws.
            for r in removeList:
                ws.remove(r)

            ws.add(s)
            hbCache[s] = set()
        return hbCache

    # ComputeAllSeriesTimeSavedForPBlock and ComputeAllSeriesTimeSavedForNode together compute the time saved in a PBlock if the
    # error-returing chidren were eliminated (recursively).
    # The algorithm is recursive, hence, some child may shrink in length instead of being eliminated.
    # This is similar to computeTimeSavedForPBlockOptimistic but it honors all series relationships observed originally.
    # When a span shrinks, it will drag its successors with it but the grad is capped by the next span in series.
    # In the following figure: where the pBlock contains 4 children X, A, B, and Y.
    # X runs from 0 to 20; A runs from 20 to 50; B runs from 60 to 100; Y runs from 0 to 30.
    #   |(0)--- root (pBlock) --------------------------------------------(100)|
    #   |(0)---X---(20)||(20) --- A ---(50)|  |(60)----------B------------(100)|
    #   |(0)----Y------(30)|
    # Assume, X shrinks by 10 time units; A shrinks by 20 time units; B shrinks by 30 time units; Y shrinks by 5 time units.
    # Note: there is a 10 time unit gap between A and B.
    # The time saved considers all series relationships.
    # The algorithm computes that Y (25) + (10 space betwee B and its original immediate predecessor) + B (30) = 65 time units as the longest chain.  # noqa: E501
    # The resulting time saved is 100 - 65 = 35 time units.
    # Notice here that shrinking of X  and A  makes them X (10) + A (10) = 20 time units.
    # Naively dragging B left by 20 time units would make lose its series relationship with Y.
    # After Y shrinks it is 25 time units whereas X+A are 20 time units. Hence, Y is the new immediate predecessor of B.
    # Further more, since there was a 10 time unit gap between A and B originally, that gap is preserved now between Y and B.
    # The resulting graph can be seen as below:
    #   |(0)--- root (pBlock) --------------------------------------------(100)|
    #   |(0)X(10)||(10)A(20)|         |(35)----B---------(65)|
    #   |(0)----Y------------(25)|

    def ComputeAllSeriesTimeSavedForNode(
        self,
        pBlock,
        hbCache,
        timeSavedCache,
        node,
        timeSavedAttribute: str,
        perBlockFunctionPtr,
    ):
        # Compute the time saved for all spans in series with node (recursively).
        # hbCache is a dictionary of non-transitive happens-before (hb) relationships for each span in the pBlock.
        # timeSavedCache is a dictionary of time saved for each span in the pBlock.
        # node is the span for which we are computing the time saved.
        if node in timeSavedCache:
            return timeSavedCache[node]

        nodeTimeSaved = self.computeTimeSavedOnCPReal(
            node,
            timeSavedAttribute,
            perBlockFunctionPtr,
        )
        assert nodeTimeSaved >= 0
        assert nodeTimeSaved <= node.duration
        minDistance = node.startTime - pBlock.startTime
        endTimes = {}

        # Obtain how much the predecessors can save.
        for pred in hbCache[node]:
            minDistance = max(0, min(minDistance, node.startTime - pred.endTime))
            predSaved = self.ComputeAllSeriesTimeSavedForNode(
                pBlock,
                hbCache,
                timeSavedCache,
                pred,
                timeSavedAttribute,
                perBlockFunctionPtr,
            )
            endTimes[pred] = pred.endTime - predSaved

        # Pick the last finishing predecessor to compute the time saved.
        logestPredEndTime = pBlock.startTime
        for endTime in endTimes.values():
            logestPredEndTime = max(logestPredEndTime, endTime)

        nodeExecutionTime = node.duration - nodeTimeSaved
        nodeNewEndTime = logestPredEndTime + minDistance + nodeExecutionTime
        assert nodeNewEndTime <= node.endTime
        nodeTotalTimeSaved = node.endTime - nodeNewEndTime
        timeSavedCache[node] = nodeTotalTimeSaved
        return nodeTotalTimeSaved

    # ComputeAllSeriesTimeSavedForPBlock finds all block ending spans in the pBlock
    # and computes the time saved for each of them using ComputeAllSeriesTimeSavedForNode.
    # The time saved is based on least time saved among all candidates.
    def ComputeAllSeriesTimeSavedForPBlock(
        self,
        pBlock,
        timeSavedAttribute,
        perBlockFunctionPtr,
    ):
        hbCache = self.GetNonTransitiveHBInPBlock(pBlock)
        _, blockEndCandidateSpans = self.GetCandidateBlockEndingSpans(pBlock)

        # Get the new endtimes for each of blockEndCandidateSpans.
        longestSofar = -1

        timeSavedCache = {}
        for span in blockEndCandidateSpans.keys():
            timeSaved = self.ComputeAllSeriesTimeSavedForNode(
                pBlock,
                hbCache,
                timeSavedCache,
                span,
                timeSavedAttribute,
                perBlockFunctionPtr,
            )
            assert timeSaved >= 0
            assert timeSaved <= pBlock.endTime - pBlock.startTime
            longestSofar = max(longestSofar, span.endTime - timeSaved)

        assert longestSofar >= 0
        assert longestSofar <= pBlock.endTime
        return max(0, pBlock.endTime - longestSofar - pBlock.overlap)

    def ComputeAllSeriesTimeSaved(self, curNode):
        return self.computeTimeSavedOnCPReal(
            curNode,
            "timeSavedOnCPAllSeries",
            self.ComputeAllSeriesTimeSavedForPBlock,
        )



    # ComputeAllSeriesTimeChangeForNode is similar to ComputeAllSeriesTimeSavedForNode.
    # Instead of computing the time saved, it computes the time change (-ve means saved, +ve means grown).
    # The time growth is the amount of time that the pBlock would have grown if we injected a delay of deltaDurationMicroSec in each span.
    # The time saved in the pBlock is the negative of the time growth.
    # The algorithm is recursive.
    # When a span grows, it will delay its successors with it.
    def ComputeAllSeriesTimeChangeForNode(
        self,
        pBlock,
        hbCache,
        timeDeltaCache,
        node,
        timeDeltaAttribute: str,
        perBlockFunctionPtr,
        deltaDurationMicroSec: int,
        targetService=None,
        targetOperation=None,
    ):
        # Compute the time delta for all spans in series with node (recursively).
        # hbCache is a dictionary of non-transitive happens-before (hb) relationships for each span in the pBlock.
        # timeDeltaCache is a dictionary of time saved for each span in the pBlock.
        # node is the span for which we are computing the time saved.
        if node in timeDeltaCache:
            return timeDeltaCache[node]

        nodeTimeDelta = self.computeTimeChangeOnCPReal(
            node,
            timeDeltaAttribute,
            perBlockFunctionPtr,
            deltaDurationMicroSec,
            targetService,
            targetOperation,
        )
        if deltaDurationMicroSec < 0:
            # cannot reduce the time of a span by more than its duration.
            assert nodeTimeDelta >= -node.duration
        else:
            # cannot increase the time of a span by a negative value.
            assert nodeTimeDelta >= 0

        # We need to respect the minimum distance between the start of the span and the start of the pBlock.
        minDistance = node.startTime - pBlock.startTime
        endTimes = {}

        # Obtain how much the predecessors can change.
        for pred in hbCache[node]:
            # What ever is the least distance between one of the in-series predecessors and the node, use that, but not less than 0.
            minDistance = max(0, min(minDistance, node.startTime - pred.endTime))
            predDelta = self.ComputeAllSeriesTimeChangeForNode(
                pBlock,
                hbCache,
                timeDeltaCache,
                pred,
                timeDeltaAttribute,
                perBlockFunctionPtr,
                deltaDurationMicroSec,
                targetService,
                targetOperation,
            )
            endTimes[pred] = pred.endTime + predDelta # -ve predDelta will reduce the end time.

        # Pick the last finishing predecessor to compute the time saved.
        logestPredEndTime = pBlock.startTime
        for endTime in endTimes.values():
            logestPredEndTime = max(logestPredEndTime, endTime)

        nodeExecutionTime = node.duration + nodeTimeDelta # -ve nodeTimeDelta will reduce the end time.
        nodeNewEndTime = logestPredEndTime + minDistance + nodeExecutionTime
        if deltaDurationMicroSec >= 0:
            assert nodeNewEndTime >= node.endTime
        else:
            assert nodeNewEndTime <= node.endTime
        nodeTotalTimeDelta = nodeNewEndTime - node.endTime
        timeDeltaCache[node] = nodeTotalTimeDelta
        return nodeTotalTimeDelta

    # ComputeAllSeriesTimeGrowthForPBlock finds all block ending spans in the pBlock
    # and computes the time changed for each of them using ComputeAllSeriesTimeChangeForNode.
    # The time growth is the maximum growth and time reduction is the minimum reduction based on the in-series candidates.
    def ComputeAllSeriesTimeChangeForPBlock(
        self,
        pBlock,
        timeDeltaAttribute,
        perBlockFunctionPtr,
        deltaDurationMicroSec: int,
        targetService=None,
        targetOperation=None,
    ):
        hbCache = self.GetNonTransitiveHBInPBlock(pBlock)
        _, blockEndCandidateSpans = self.GetCandidateBlockEndingSpans(pBlock)

        # Get the new endtimes for each of blockEndCandidateSpans.
        longestSofar = -1

        timeDeltaCache = {}
        for span in blockEndCandidateSpans.keys():
            timeDelta = self.ComputeAllSeriesTimeChangeForNode(
                pBlock,
                hbCache,
                timeDeltaCache,
                span,
                timeDeltaAttribute,
                perBlockFunctionPtr,
                deltaDurationMicroSec,
                targetService,
                targetOperation,
            )
            if deltaDurationMicroSec >= 0:
                assert timeDelta >= 0
            else:
                assert -timeDelta <= pBlock.endTime - pBlock.startTime
            longestSofar = max(longestSofar, span.endTime + timeDelta)

        assert longestSofar >= 0
        if deltaDurationMicroSec >= 0:
            assert longestSofar >= pBlock.endTime
            # use max to keep the number +ve.
            finalChange = longestSofar - pBlock.endTime - pBlock.overlap # since we assumed series, if there was an overlap, discount it.
            return max(0, finalChange)
        else:
            assert longestSofar <= pBlock.endTime
            finalChange = pBlock.endTime - longestSofar + pBlock.overlap # since we assumed series, if there was an overlap, discount it.
        # pBlock.endTime - longestSofar will be a +ve number because longestSofar is smaller than pBlock.endTime
        # use min to keep the number -ve.
            return min(0, -finalChange)

    def ComputeAllSeriesTimeChange(self,
                                   curNode,
                                   deltaDurationMicroSec: int,
                                   targetService=None,
                                   targetOperation=None):
        return self.computeTimeChangeOnCPReal(
            curNode,
            "timeChangeOnCPAllSeries",
            self.ComputeAllSeriesTimeChangeForPBlock,
            deltaDurationMicroSec,
            targetService,
            targetOperation,
        )
    # Traverse the children block by block, where each block is a set of overlapping children.
    # For each block, compute the time changed and add it to the total time changed.
    def computeTimeChangeOnCPReal(
        self,
        curNode,
        timeDeltaAttribute: str,
        perBlockFunctionPtr,
        deltaDurationMicroSec: int, # this is the amount of time that we are going to inject in each span.
        # positive value means that we are going to delay the span by that amount of time.
        # negative value means that we are going to shrink the span by that amount of time.
        targetService=None,
        targetOperation=None,
    ):
        # Using setattr to make the function generic.
        setattr(curNode, timeDeltaAttribute, 0)

        # Determine whether the delta applies to this node.
        # When targetService/targetOperation are specified, only matching SERVER spans get the delta.
        # When they are None, all SERVER spans get the delta (original behavior).
        if targetService is not None and targetOperation is not None:
            node_matches = (
                curNode.spanKind == SpanKind.SERVER
                and self.processName.get(curNode.pid, '') == targetService
                and curNode.opName == targetOperation
            )
        else:
            node_matches = curNode.spanKind == SpanKind.SERVER

        deltaDuration = deltaDurationMicroSec if node_matches else 0

        # cannot reduce the time of a span by more than its duration.
        if deltaDuration < 0:
            assert -deltaDuration <= curNode.duration

        if len(curNode.children) == 0:
            setattr(curNode, timeDeltaAttribute, deltaDuration)
            return deltaDuration

        pBlocks = self.getPBlocks(curNode)
        assert len(pBlocks) > 0
        for pBlock in pBlocks:
            v = perBlockFunctionPtr(pBlock, timeDeltaAttribute, perBlockFunctionPtr, deltaDurationMicroSec, targetService, targetOperation)
            if deltaDurationMicroSec < 0:
                # cannot reduce the time of a span by more than its duration.
                assert v <= (pBlock.endTime - pBlock.startTime)
            else:
                # cannot increase the time of a span by a negative value.
                assert v >= 0
            # accumulate the time deltas in each PBlock.
            # for +ve  deltaDurationMicroSec it will be growing and for -ve deltaDurationMicroSec it will be shrinking.
            deltaDuration += v

        # The total time saved  (-ve deltaDuration) is not guaranteed to be less than or equal to the duration of the curNode since the
        # PBlocks may slightly overlap by the definition of "HappensBefore".
        # Adjust the time saved to be less than or equal to the duration of the curNode.
        if deltaDurationMicroSec < 0:
            deltaDuration = max(deltaDuration, -curNode.duration)
        # there is no limit to how much a span can grow.
        setattr(curNode, timeDeltaAttribute, deltaDuration)
        return deltaDuration

    def numSyncEventsInWindowInclusive(self, children, startTime, endTime):
        numEvents = 0

        for c in children:
            if c.startTime >= startTime and c.startTime <= endTime:
                numEvents = numEvents + 1
            if c.endTime >= startTime and c.endTime <= endTime:
                numEvents = numEvents + 1
        return numEvents

    # happensBeforeSimple returns true if the end of childBefore happens before the start of childLater.
    def happensBeforeSimple(self, childBefore, childLater):
        # happensBefore returns true if the end of childBefore happens before
        # the start of childLater.

        # Astart------Aend Bstart-----Bend
        if childBefore.endTime <= childLater.startTime:
            return True
        return False

    def happensBefore(self, parent, children, childBefore, childLater):
        # happensBefore returns true if the end of childBefore happens before
        # the start of childLater. however, there is some heuristic to
        # accomodate clock skew.

        # obviously A HB B if
        # Astart------Aend Bstart-----Bend
        if childBefore.endTime < childLater.startTime:
            return True

        # allow a 1 % overlap with earlier child starting prior to the later child
        # Allow this:
        # Astart------Aend
        #            Bstart-----Bend
        # Don't allow this
        # Astart-------Aend
        #            Bstart-----Bend
        #     Cstart---Cend
        if (
            (childBefore.endTime < childLater.endTime)
            and (childBefore.startTime < childLater.startTime)
            and (
                (childBefore.endTime - childLater.startTime) / parent.duration
                < get_overlap_allowance()
            )
        ):
            # Now check that there is no other overlapping child in this region
            nEvt = self.numSyncEventsInWindowInclusive(
                children,
                childLater.startTime,
                childBefore.endTime,
            )
            debug_on and logging.debug(
                f"nEvt for {self.canonicalOpName(childBefore)} = {nEvt}",
            )
            if nEvt == 2:  # there can two and only 2 events in this window
                return True
        return False

    # --- _get_root_node and subsequent methods will be added in PR 9d ---
