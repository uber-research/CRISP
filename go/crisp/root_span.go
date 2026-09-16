package crisp

import (
	"errors"
	"fmt"

	"github.com/uber-research/CRISP/go/crisp/jaeger"
)

// DeriveRootSpan mirrors crisp/conformance.py derive_root_span: it returns the
// (serviceName, operationName) of the span with no in-trace CHILD_OF parent,
// with ties broken by earliest startTime, then spanID (CONFORMANCE.md rule 5).
//
// A span whose CHILD_OF reference points outside the trace (an orphan) has no
// in-trace parent and is therefore a root candidate, exactly as in Python.
// Like the Python original, only the first element of the trace's data array
// is considered.
func DeriveRootSpan(t *jaeger.Trace) (service, operation string, err error) {
	if len(t.Data) == 0 {
		return "", "", errors.New("no root span found in trace: empty data array")
	}
	data := t.Data[0]

	spanIDs := make(map[string]struct{}, len(data.Spans))
	for _, s := range data.Spans {
		spanIDs[s.SpanID] = struct{}{}
	}

	var root *jaeger.Span
	var rootStart int64
	for i := range data.Spans {
		s := &data.Spans[i]
		hasParent := false
		for _, r := range s.References {
			if r.RefType == refTypeChildOf {
				if _, ok := spanIDs[r.SpanID]; ok {
					hasParent = true
					break
				}
			}
		}
		if hasParent {
			continue
		}
		start, err := s.StartTimeMicros()
		if err != nil {
			return "", "", fmt.Errorf("span %s: startTime: %w", s.SpanID, err)
		}
		// Python's min keeps the first candidate on a full (startTime, spanID)
		// tie; the strict comparisons here do the same.
		if root == nil || start < rootStart || (start == rootStart && s.SpanID < root.SpanID) {
			root = s
			rootStart = start
		}
	}
	if root == nil {
		return "", "", errors.New("no root span found in trace")
	}
	proc, ok := data.Processes[root.ProcessID]
	if !ok {
		return "", "", fmt.Errorf("root span %s: processID %q not found in processes", root.SpanID, root.ProcessID)
	}
	return proc.ServiceName, root.OperationName, nil
}
