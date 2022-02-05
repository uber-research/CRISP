#Copyright (c) 2021  Uber Technologies, Inc.
#
#Licensed under the Uber Non-Commercial License (the "License");
#you may not use this file except in compliance with the License.
#You may obtain a copy of the License at the root directory of this project.
#
#See the License for the specific language governing permissions and
#limitations under the License.

import logging

logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')
debug_on = logging.getLogger(__name__).isEnabledFor(logging.DEBUG)

_SPANS = 'spans'
_SPAN_ID = 'spanID'
_REFERENCES = 'references'
_START_TIME = 'startTime'
_DURATION = 'duration'
_OPERATION_NAME = 'operationName'
_PROCESS_ID = 'processID'
_TRACE_ID = 'traceID'
_REF_TYPE = 'refType'
_CHILD_OF = 'CHILD_OF'
_TAGS = 'tags'
_PROCESSES = 'processes'
_HOSTNAME = 'hostname'
_TESTING = 'testing'
# how much can spans overlap as a fraction of the total execution time of thier common parent.
_OVERLAP_ALLOWANCE_FRACTION = 0.01
'''
An example Jaeger JSON data looks like the one below.
The testing section is added by us to keep the expected results.
In this trace spanID A is the root span and spanID B is its child.

{
    "data": [
        {
            "processes": {
                "S1": {
                    "serviceName": "S1",
                    "tags": [
                    ]
                },
                "S2": {
                    "serviceName": "S2",
                    "tags": [
                    ]
                }
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
                    "warnings": null
                },
                {
                    "traceID": "A",
                    "spanID": "B",
                    "operationName": "O2",
                    "startTime": 10,
                    "duration": 50,
                    "processID": "S2",
                    "warnings": null,
                    "references": [
                        {
                            "refType": "CHILD_OF",
                            "traceID": "A",
                            "spanID": "A"
                        }
                    ]
                }
            ]
        }
        ],
    "testing": [
        {
            "[S1] O1": 50,
            "[S2] O2": 50
        }
    ],
    "total": 0,
    "limit": 0,
    "offset": 0,
    "errors": null
}

'''


class GraphNode():
    """
    GraphNode is a node in the Graph. It is a representative of a span in an Jaeger trace.
    It has other GraphNode children and a GraphNode parent.
    Additionally, it has the start time, duration, and end time (starttime + duration).
    Since, sometimes we edit these time values, we record originalStartTime and originalDuration.
    """
    def __init__(self, sid, startTime, duration, parentSpanId, opName,
                 processID):
        self.sid = sid
        self.startTime = startTime
        self.originalStartTime = startTime
        self.duration = duration
        self.originalDuration = duration
        self.parentSpanId = parentSpanId
        self.endTime = startTime + duration
        self.parent = None
        self.opName = opName
        self.pid = processID
        self.children = {}

    def setParent(self, parent):
        self.parent = parent

    def addChild(self, child):
        self.children[child] = True

    def __repr__(self):
        return f'Node(SpanID={self.sid}, startTime={self.startTime}, duration={self.duration}, parent={self.parent}, opName={self.opName})'


class Graph():
    """
    Graph represents a Jaeger trace composed of spans (represented by GraphNodes).
    It is the central data structure on which we compute the critical path.
    Each Graph is built from some Jaeger JSON file represented by its filename.
    A well-formed Jaeger trace should have one and only one rootNode.
    """
    def __init__(self, data, serviceName, operationName, filename,
                 rootTrace) -> None:
        self.operationName = operationName
        self.serviceName = serviceName
        self.filename = filename
        self.rootNode = None
        self.nodeHT = {}
        self.processName = {}
        self.hostMap = {}  # maps service to host name
        self.totalShrink = 0
        self.totalDrop = 0
        self.shrinkCounter = 0
        self.testing = {}
        self.exclusiveExampleMap = {}
        self.inclusiveExampleMap = {}
        self.callChain = {}
        potentialRoots = self.parseNode(data)
        if len(potentialRoots) == 0:
            logging.warning(f"no root node in file {filename}!")
            return
        if rootTrace == True:  # the root span must be the service+operation
            if len(potentialRoots) != 1:
                logging.warning(
                    f"{len(potentialRoots)} roots node in file {filename}!")
                return
            if potentialRoots[0] == None or self.checkRootAndWarn(
                    potentialRoots[0], filename, rootTrace) == False:
                return
            else:
                self.rootNode = potentialRoots[0]
        else:  # randomly choose some one node whose service and op names match
            for candidate in potentialRoots:
                someRoot = self.findARoot(candidate)
                if someRoot == None or self.checkRootAndWarn(
                        someRoot, filename, rootTrace) == False:
                    continue
                # make someRoot's parent as None
                someRoot.parent = None
                someRoot.parentId = None
                # set someRoot as the rootNode
                self.rootNode = someRoot
                break

            if self.rootNode == None:
                logging.warning(
                    f"rootTrace == {rootTrace} but no matching node found in file {filename}!"
                )
                return

        self.sanitizeOverflowingChildren(self.rootNode)

        if debug_on:
            logging.debug(f"{self.totalShrink} of duation compressed")
            logging.debug(f"{self.shrinkCounter} spans shrank")
            logging.debug(f"{self.totalDrop} spans dropped")
            logging.debug(f"total executionTime {self.rootNode.duration}")

    def findARoot(self, node):
        # a DFS for finding the first node that matches the required service and operation name.
        if self.processName[
                node.
                pid] == self.serviceName and node.opName == self.operationName:
            return node
        for c in node.children:
            found = self.findARoot(c)
            if found != None:
                return found
        return None

    def computeGraphStats(self, node):
        # a DFS
        descendants = 0
        depth = 0
        for c in node.children:
            moreDescendants, newDepth = self.computeGraphStats(c)
            descendants = descendants + moreDescendants
            depth = newDepth if newDepth > depth else depth
        return descendants + 1, depth + 1

    def checkRootAndWarn(self, node, filename, rootTrace):
        if self.processName[
                node.
                pid] != self.serviceName or node.opName != self.operationName:
            logging.warning(
                f"rootTrace == {rootTrace}, expected serviceName={self.serviceName} and found {self.processName[node.pid]}. Expected operationName={self.operationName} and found {node.opName} in file {filename}"
            )
            return False
        return True

    def setTestResult(self, result):
        self.testing = result

    def checkResultsWithoutQuantity(self, cp):
        # Check if the critical path found by our algorithms matches the one recorded in the "testing" section.
        # This routine ignores the weights of each operation.
        return checkResults(self, cp, False)

    def checkResults(self, cp, checkValue=True):
        # Check if the critical path found by our algorithms matches the one recorded in the "testing" section.
        if self.testing == None:
            return None  # nothing to validate

        for k, v in self.testing.items():
            if k not in cp:
                return "missing key {} in cp".format(str(k))
            if checkValue and (v != cp[k]):
                return "expected {} found {}".format((str(v), str(cp[k])))

        for k in cp:
            if k not in self.testing:
                return "extra key {} in cp".format(str(k))
        return True

    # Builds graph and returns potential roots.
    def parseNode(self, jsonData):
        # given jaeger jsonData blob, build the Graph().

        potentialRoots = []

        # pass 1: extract all spans and create one GraphNode for each.
        for item in jsonData['data']:
            for span in item[_SPANS]:
                thisSpan = span[_SPAN_ID]
                parentSpanId = None
                # We only care about _CHILD_OF spans reachable from the root.
                for parent in span[_REFERENCES]:
                    if parent[_REF_TYPE] == _CHILD_OF:
                        parentSpanId = parent[_SPAN_ID]

                node = GraphNode(
                    thisSpan,
                    span[_START_TIME],
                    span[_DURATION],
                    parentSpanId,  # no parent YET, only spanID is available.
                    span[_OPERATION_NAME],
                    span[_PROCESS_ID])

                self.nodeHT[thisSpan] = node

        # pass 2: add parent-child relations to GraphNodes.
        for spanId in self.nodeHT:
            me = self.nodeHT[spanId]
            parentId = me.parentSpanId
            if parentId == None:
                potentialRoots.append(me)
                continue
            if parentId not in self.nodeHT:
                debug_on and logging.debug(
                    f"Span {spanId}'s parent {parentId} not present in nodeHT: file = {self.filename}"
                )
                potentialRoots.append(me)
                continue

            parent = self.nodeHT[parentId]
            me.setParent(parent)
            parent.addChild(me)

        # pass 3: record service names and other KV data
        for item in jsonData['data']:
            for p in item[_PROCESSES]:
                self.processName[p] = item[_PROCESSES][p]['serviceName']
                for dictionary in item[_PROCESSES][p][_TAGS]:
                    if dictionary['key'] == _HOSTNAME:
                        self.hostMap[p] = dictionary['value']

        # for testing only, we keep the expected results in _TESTING section of JSON.
        # pass 4 : record test results
        if _TESTING in jsonData and len(jsonData[_TESTING]) > 0:
            results = {}
            for k, v in jsonData[_TESTING][0].items():
                results[k] = v
            self.setTestResult(results)

        return potentialRoots

    def sanitizeOverflowingChildren(self, curNode):
        # if a child overflows or underflows its parent, it will be truncated/deleted to match/adhere to parent timeline.
        parentStart = curNode.startTime
        parentEnd = curNode.endTime

        removeList = []
        for c in curNode.children:
            childStart = c.startTime
            childEnd = c.endTime
            debug_on and logging.debug(
                f"working on parent {curNode}, child {c}")
            debug_on and logging.debug(
                f"parent start {parentStart}, parent end {parentEnd}")
            debug_on and logging.debug(
                f"child start {childStart}, child end {childEnd}")
            if childStart >= parentStart and childEnd <= parentEnd:
                # case 1: everything looks good
                # |----parent----|
                #   |----child--|
                debug_on and logging.debug(f"Case 1")
                # continue recursion
                self.sanitizeOverflowingChildren(c)
            elif childStart < parentStart and childEnd <= parentEnd and childEnd > parentStart:
                # case 2: child start before parent, truncate is needed
                #      |----parent----|
                #   |----child--|
                debug_on and logging.debug(f"Case 2")
                shrunk = (parentStart - childStart)
                self.totalShrink += shrunk
                self.shrinkCounter += 1
                c.startTime = parentStart
                c.duration -= shrunk
                debug_on and self.dumpShrinkStats(curNode, c, shrunk)
                # continue recursion
                self.sanitizeOverflowingChildren(c)
            elif childStart >= parentStart and childEnd > parentEnd and childStart < parentEnd:
                # case 3: child end after parent, truncate is needed
                #      |----parent----|
                #              |----child--|
                debug_on and logging.debug(f"Case 3")
                shrunk = (childEnd - parentEnd)
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
                debug_on and logging.debug(f"Case 4")
                debug_on and self.dumpDeletionStats(curNode, c, c.duration)
                removeList.append(c)
                # no recursion; all descendants will become unreachable from the root.

        # now delete the list of items marked for deletion.
        for r in removeList:
            r.parent = None
            del curNode.children[r]

    def dumpShrinkStats(self, curNode, child, shrunk):
        logging.debug(
            f" shrunk node {curNode} duration {curNode.duration} by {shrunk}: child {child} => {shrunk / curNode.duration * 100}  reduction"
        )

    def dumpDeletionStats(self, curNode, child, shrunk):
        if curNode.duration > 0:
            logging.debug(
                f" delete: parent node {curNode} duration {curNode.duration} with child {child} of {shrunk} => {shrunk / curNode.duration * 100}  reduction"
            )

    def computeCriticalPath(self, curNode):
        # Recursively find the critical path for curNode.
        debug_on and logging.debug(
            f"Working on CP parent {curNode} {self.canonicalOpName(curNode)}")

        # step 0. curNode is obviously on the critical path.
        criticalPath = [curNode]

        if len(curNode.children) == 0:
            debug_on and logging.debug(f"{curNode} has no children")
            return criticalPath

        # step 1. reverse sort all children of curNode by their end time.
        sortedChildren = sorted(curNode.children,
                                key=lambda x: x.endTime)[::-1]

        # step 2. begin by the child who finishes last
        lrc = sortedChildren[0]
        criticalPath.extend(self.computeCriticalPath(lrc))
        lastStartTime = lrc.startTime

        for cn in sortedChildren[
                1:]:  # first one (actually the last one) is already added
            # step 3. get the child who finished just before the start of lrc's start
            if self.happensBefore(curNode, sortedChildren, cn, lrc):
                debug_on and logging.debug(
                    f"Adding child {cn} {self.canonicalOpName(cn)} to CP")
                # # step 4. recur on cn, which is on the critical path.
                criticalPath.extend(self.computeCriticalPath(cn))
                lrc = cn
                lastStartTime = min(lastStartTime, cn.startTime)
            else:
                debug_on and logging.debug(
                    f"NOT adding child {cn} {self.canonicalOpName(cn)} to CP")

            debug_on and logging.debug(f"lastStartTime = {lastStartTime}")
        return criticalPath

    def numSyncEventsInWindowInclusive(self, children, startTime, endTime):
        numEvents = 0

        for c in children:
            if c.startTime >= startTime and c.startTime <= endTime:
                numEvents = numEvents + 1
            if c.endTime >= startTime and c.endTime <= endTime:
                numEvents = numEvents + 1
        return numEvents

    def happensBefore(self, parent, reverseSortedChildren, childBefore,
                      childLater):
        # happensBefore returns true if the end of childBefore happens before the start of childLater.
        # however, there is some heuristic to accomodate clock skew.

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
        if (childBefore.endTime < childLater.endTime) and (
                childBefore.startTime < childLater.startTime) and (
                    (childBefore.endTime - childLater.startTime) /
                    parent.duration < _OVERLAP_ALLOWANCE_FRACTION):
            # Now check that there is no other overlapping child in this region
            nEvt = self.numSyncEventsInWindowInclusive(reverseSortedChildren,
                                                       childLater.startTime,
                                                       childBefore.endTime)
            debug_on and logging.debug(
                f"nEvt for {self.canonicalOpName(childBefore)} = {nEvt}")
            if nEvt == 2:  # there can two and only 2 events in this window
                return True
        return False

    def findCriticalPath(self):
        # return a list a critical path sid for all subgraph root
        return self.computeCriticalPath(self.rootNode)

    def canonicalOpName(self, node):
        # return the canonical name of the span in "[serviceName] operationName" fashion
        return '[' + self.processName[node.pid] + '] ' + node.opName

    def getCallPath(self, graphNode):
        # getCallPath obtains the stringified form of how the rootnode reaches graphNode
        # the operation names are joined with "->".
        # TODO: this can be optimized via memoization.
        str = self.canonicalOpName(graphNode)
        while graphNode.parent != None:
            str = self.canonicalOpName(graphNode.parent) + "->" + str
            graphNode = graphNode.parent
        return str

    def getMetrics(self, criticalPath):
        # Compute inclusive and exclustive metrics.
        # Include both flat (just the operation no call path) and call path profiles.
        # Maintain an example of worst case span seen for each call path.

        opTimeExclusive = {}
        opTimeInclusive = {}
        callpathTimeExlusive = {}
        callpathTimeInclusive = {}
        exclusiveExampleMap = {}
        inclusiveExampleMap = {}
        callChain = {}
        for n in reversed(criticalPath):
            sid = n.sid
            op = self.canonicalOpName(n)
            opCallapth = self.getCallPath(n)

            # record in the set of callChains reaching this operation.
            if op not in self.callChain:
                callChain[op] = []
            callChain[op].append(opCallapth)

            # flat profile
            accumulateInDict(opTimeExclusive, op, n.duration)
            accumulateInDict(opTimeInclusive, op, n.duration)

            # callpath profile
            accumulateInDict(callpathTimeExlusive, opCallapth, n.duration)
            accumulateInDict(callpathTimeInclusive, opCallapth, n.duration)

            # maintain the worst case example
            maxExample(exclusiveExampleMap, opCallapth, n.sid, n.duration)
            maxExample(inclusiveExampleMap, opCallapth, n.sid, n.duration)

            # no parent for root
            if n == self.rootNode:
                continue

            # for exclusive metrics, subtract the child's duration from its parent.
            # if the parent is visited after the child(ren) we will have an existing -ve value to which a positive value will be added above.
            parentName = self.canonicalOpName(n.parent)
            # -ve duration is added or inserted
            accumulateInDict(opTimeExclusive, parentName, -n.duration)
            parentCC = self.getCallPath(n.parent)
            # -ve duration is added or inserted
            accumulateInDict(callpathTimeExlusive, parentCC, -n.duration)

        descendants, depth = self.computeGraphStats(self.rootNode)
        return Metrics(opTimeExclusive, callpathTimeExlusive,
                       exclusiveExampleMap, opTimeInclusive,
                       callpathTimeInclusive, inclusiveExampleMap, callChain,
                       self.rootNode.sid, descendants, depth)


def accumulateInDict(dictName, key, value):
    if key in dictName:
        # add value to existing key in dictName
        dictName[key] = dictName[key] + value
    else:
        # insert new key and value into dictName
        dictName[key] = value


def maxExample(dictName, key, sid, value):
    if key in dictName:
        if value > dictName[key][1]:
            # remember if this is the worst case seen
            dictName[key] = (sid, value)
    else:
        # remember since this is the first case seen
        dictName[key] = (sid, value)


class Metrics():
    """
    Metric represents the following measurements as dictionaries
    1. opTimeExclusive: the flat profile with exclusive operation times.
    2. callpathTimeExlusive: the call-path profile with exclusive callpath times.
    3. exclusiveExampleMap: per callpath worst case example of exclusive time.
    4. opTimeInclusive: the flat profile with inclusive operation times.
    5. callpathTimeInclusive: the call-path profile with inclusive callpath times.
    6. inclusiveExampleMap: per callpath worst case example of inclusive time.
    """
    def __init__(self, opTimeExclusive, callpathTimeExlusive,
                 exclusiveExampleMap, opTimeInclusive, callpathTimeInclusive,
                 inclusiveExampleMap, callChain, rootSpanID, descendants,
                 depth):
        self.opTimeExclusive = opTimeExclusive
        self.callpathTimeExlusive = callpathTimeExlusive
        self.exclusiveExampleMap = exclusiveExampleMap
        self.opTimeInclusive = opTimeInclusive
        self.callpathTimeInclusive = callpathTimeInclusive
        self.inclusiveExampleMap = inclusiveExampleMap
        self.callChain = callChain
        self.rootSpanID = rootSpanID
        self.numNodes = descendants
        self.depth = depth
