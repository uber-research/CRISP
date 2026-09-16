package crisp

import (
	"fmt"

	"github.com/uber-research/CRISP/go/crisp/jaeger"
)

const (
	refTypeChildOf     = "CHILD_OF"
	processTagHostname = "hostname"
	processTagRegion   = "region"
	processTagZone     = "zone"
)

// ParsedTrace is the result of decoding a Jaeger trace and building one Node
// per span, in span-array order, with process/host/region lookup tables.
// It does not yet link nodes into a parent/child tree -- see the Node doc
// comment.
type ParsedTrace struct {
	ProcessName map[string]string // processID -> serviceName
	HostMap     map[string]string // processID -> hostname
	RegionMap   map[string]string // processID -> region (falls back to zone)
	Nodes       []Node            // span-array order, preserved across all data[] items
	NumErrors   int
	IsTestTrace bool
}

// ParseTrace mirrors graph.py Graph.parseNode's pass 1 and pass 2: it builds
// the process/host/region lookup tables, then decodes every span into a Node.
// It does not yet perform pass 3 (buildParentChildRelationships) -- that is a
// later port stage.
//
// An error here corresponds to Python's parseNode raising inside the
// try/except in Graph.__init__: the caller should treat it as "no graph could
// be built for this trace" rather than a partial result.
func ParseTrace(t *jaeger.Trace) (*ParsedTrace, error) {
	pt := &ParsedTrace{
		ProcessName: map[string]string{},
		HostMap:     map[string]string{},
		RegionMap:   map[string]string{},
	}

	// pass 1: record service names and other KV data first.
	for _, item := range t.Data {
		for pid, proc := range item.Processes {
			pt.ProcessName[pid] = proc.ServiceName
			if isTestTraceByServiceName(proc.ServiceName) {
				pt.IsTestTrace = true
			}
			_, hasRegion := pt.RegionMap[pid]
			for _, tag := range proc.Tags {
				switch tag.Key {
				case processTagHostname:
					if v, ok := tagString(tag.Value); ok {
						pt.HostMap[pid] = v
					}
				case processTagRegion:
					if v, ok := tagString(tag.Value); ok {
						pt.RegionMap[pid] = v
						hasRegion = true
					}
				case processTagZone:
					if !hasRegion {
						if v, ok := tagString(tag.Value); ok {
							pt.RegionMap[pid] = v
						}
					}
				}
			}
		}
	}

	// pass 2: extract all spans and create one Node for each, preserving
	// span-array order (semantically significant -- see CONFORMANCE.md).
	for _, item := range t.Data {
		for _, span := range item.Spans {
			var parentSpanID *string
			for _, ref := range span.References {
				if ref.RefType == refTypeChildOf {
					id := ref.SpanID
					parentSpanID = &id // last matching CHILD_OF ref wins, matching Python
				}
			}

			hasError, err := parseForErrorReturn(span.Tags, span.Logs)
			if err != nil {
				return nil, fmt.Errorf("span %s: %w", span.SpanID, err)
			}
			if hasError {
				pt.NumErrors++
			}

			spanKind, err := getSpanKind(span.Tags)
			if err != nil {
				return nil, fmt.Errorf("span %s: %w", span.SpanID, err)
			}
			peerService := getPeerService(span.Tags)

			if isTestTraceByOpName(span.OperationName) {
				pt.IsTestTrace = true
			}

			startTime, err := span.StartTimeMicros()
			if err != nil {
				return nil, fmt.Errorf("span %s: startTime: %w", span.SpanID, err)
			}
			duration, err := span.DurationMicros()
			if err != nil {
				return nil, fmt.Errorf("span %s: duration: %w", span.SpanID, err)
			}

			pt.Nodes = append(pt.Nodes, newNode(
				span.SpanID,
				startTime,
				duration,
				parentSpanID,
				span.OperationName,
				span.ProcessID,
				spanKind,
				peerService,
				hasError,
			))
		}
	}

	return pt, nil
}
