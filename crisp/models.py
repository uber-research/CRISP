# ruff: noqa: I001
"""Heavy graph-shaped types: PBlock, GraphNode, GetRemainingTags (layer 5).

Lighter metric containers live in crisp.shared.models (layer 2).
"""

from crisp.shared.constants import TAG_SEARCH_DEPTH
from crisp.shared.models import SpanKind


class PBlock:
    """
    PBlock is a Parallel block of a set of spans.
    """

    # parent: the parent GraphNode this Pblock belongs to.
    # node: the lastNode (by time order) node in the Pblock. All others will be earlier than this.
    # overlap: the amount of overlap this Pblock has with its immediate successor Pblock (if any).
    def __init__(self, parent, node, overlap: float = 0.0):
        self.startTime = node.startTime
        self.endTime = node.endTime
        self.parent = parent
        self.firstNode = node
        self.spanSet = {node}
        self.overlap = overlap

    def GetOverlap(self):
        return self.overlap

    # Additions are done in later to earlier endtime order.
    def Add(self, node):
        # ensure the Addition invariant is maintained.
        assert node.endTime <= self.endTime
        self.spanSet.add(node)
        if node.startTime < self.firstNode.startTime:
            self.firstNode = node
            self.startTime = node.startTime

    # Checks if this Pblock happens after the given childBefore node.
    def HappensAfter(self, graph, childBefore):
        return graph.happensBefore(
            self.parent,
            self.parent.children,
            childBefore,
            self.firstNode,
        )

    def __repr__(self):
        return f"startTime={self.startTime}, endTime={self.endTime}, overlap={self.overlap}, spanSet={self.spanSet}"


class GraphNode:
    """
    GraphNode is a node in the Graph. It is a representative of a span in an Jaeger trace.
    It has other GraphNode children and a GraphNode parent.
    Additionally, it has the start time, duration, and end time (starttime + duration).
    Since, sometimes we edit these time values, we record originalStartTime and originalDuration.
    """

    def __init__(
        self,
        sid,
        startTime,
        duration,
        parentSpanId,
        opName,
        processID,
        spanKind: SpanKind,
        peerService,
        returnError,
    ):
        self.sid = sid
        self.startTime = startTime
        self.originalStartTime = startTime
        self.duration = duration
        self.originalDuration = duration
        self.parentSpanId = parentSpanId
        self.endTime = startTime + duration
        self.returnError = returnError
        self.timeSavedOnCPPessimistic = 0
        self.timeSavedOnCPOptimistic = 0
        self.timeSavedOnCPAllSeries = 0
        self.timeChangeOnCPAllSeries = 0
        self.parent = None
        self.opName = opName
        self.pid = processID
        self.children = {}
        self.peerService = peerService
        self.spanKind = spanKind

    def setParent(self, parent):
        self.parent = parent
        self.parentSpanId = parent.sid

    def addChild(self, child):
        self.children[child] = True

    def __lt__(self, other):
        if self.endTime < other.endTime:
            return True
        if self.endTime > other.endTime:
            return False

        if self.startTime < other.startTime:
            return True
        if self.startTime > other.startTime:
            return False

        if self.sid < other.sid:
            return True
        if self.sid > other.sid:
            return False

        if self.opName < other.opName:
            return True
        return False

    def __repr__(self):
        return f"Node(SpanID={self.sid}, startTime={self.startTime}, duration={self.duration}, parent={self.parent}, opName={self.opName})"


def GetRemainingTags(foundTags, allTags, curSearchDepth):
    remainingTags = []
    for m in allTags:
        if curSearchDepth > m[TAG_SEARCH_DEPTH]:
            continue
        if m not in foundTags:
            remainingTags.append(m)
    return remainingTags
