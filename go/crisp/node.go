package crisp

// Node mirrors crisp.models.GraphNode: a span in the trace, linked into a
// parent/child tree by Graph construction (buildParentChildRelationships in
// graph.py). StartTime/Duration/EndTime may be rewritten by
// sanitizeOverflowingChildren; OriginalStartTime/OriginalDuration always
// hold the as-decoded values.
type Node struct {
	SID               string
	StartTime         int64
	OriginalStartTime int64
	Duration          int64
	OriginalDuration  int64
	EndTime           int64
	ParentSpanID      *string
	OpName            string
	ProcessID         string
	SpanKind          SpanKind
	PeerService       *string
	ReturnError       bool

	// Parent is the linked parent node, nil for roots. Children mirrors
	// Python's insertion-ordered dict keyed by child node: order is the
	// order addChild was called in, which follows span document order (see
	// buildParentChildRelationships), and deletions preserve the order of
	// the remaining children.
	Parent   *Node
	Children []*Node
}

func newNode(sid string, startTime, duration int64, parentSpanID *string, opName, processID string, spanKind SpanKind, peerService *string, returnError bool) Node {
	return Node{
		SID:               sid,
		StartTime:         startTime,
		OriginalStartTime: startTime,
		Duration:          duration,
		OriginalDuration:  duration,
		EndTime:           startTime + duration,
		ParentSpanID:      parentSpanID,
		OpName:            opName,
		ProcessID:         processID,
		SpanKind:          spanKind,
		PeerService:       peerService,
		ReturnError:       returnError,
	}
}

// setParent mirrors GraphNode.setParent.
func (n *Node) setParent(parent *Node) {
	n.Parent = parent
	sid := parent.SID // copy: Python assigns the immutable string value
	n.ParentSpanID = &sid
}

// addChild mirrors GraphNode.addChild (children[child] = True): appending a
// child that is already present is a no-op, preserving dict semantics.
func (n *Node) addChild(child *Node) {
	for _, c := range n.Children {
		if c == child {
			return
		}
	}
	n.Children = append(n.Children, child)
}

// removeChild mirrors del parent.children[child]: removes the child and
// clears its parent pointer, preserving the order of remaining children.
func (n *Node) removeChild(child *Node) {
	for i, c := range n.Children {
		if c == child {
			n.Children = append(n.Children[:i], n.Children[i+1:]...)
			child.Parent = nil
			return
		}
	}
}

// Less mirrors GraphNode.__lt__: ordering by (endTime, startTime, sid,
// opName). Used by the critical-path stage's priority queues.
func (n *Node) Less(other *Node) bool {
	if n.EndTime != other.EndTime {
		return n.EndTime < other.EndTime
	}
	if n.StartTime != other.StartTime {
		return n.StartTime < other.StartTime
	}
	if n.SID != other.SID {
		return n.SID < other.SID
	}
	return n.OpName < other.OpName
}
