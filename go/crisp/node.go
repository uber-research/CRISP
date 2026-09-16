package crisp

// Node mirrors the fields crisp.models.GraphNode's constructor sets directly
// from a decoded span, before any parent/child tree-linking is applied.
// Tree-linking (Parent/Children, buildParentChildRelationships in graph.py)
// is added by a later port stage -- this type intentionally stops at what
// ParseTrace alone can produce.
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
