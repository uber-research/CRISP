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

    # --- computeCriticalPath and subsequent methods will be added in PR 9c ---
