package crisp

// fragment.go ports crisp/fragment.py: a trace reduced to what the light
// pipeline needs to analyze one service and operation, and the checks behind
// LightConfig.Fragment. The contract is in CONFORMANCE.md ("Trace
// fragments"): with Fragment set, a fragment gives the same outputs as the
// trace it was made from.

import (
	"errors"

	"github.com/uber-research/CRISP/go/crisp/jaeger"
)

// lastChildOf mirrors fragment.py _lastChildOf: the parent span ID as
// ParseTrace reads it, the last CHILD_OF reference.
func lastChildOf(span *jaeger.Span) (string, bool) {
	parent, ok := "", false
	for _, ref := range span.References {
		if ref.RefType == refTypeChildOf {
			parent, ok = ref.SpanID, true
		}
	}
	return parent, ok
}

// fragmentReferences mirrors fragment.py _fragmentReferences: the references
// of a kept span.
//
// A reference stays when its target is kept, or was never in the trace (the
// span is then an orphan in the fragment too). Of the CHILD_OF references
// only the last one can stay: it is the parent, and an earlier one must not
// become the parent when the last one is removed.
func fragmentReferences(span *jaeger.Span, keep, present map[string]bool) []jaeger.Reference {
	last := -1
	for i, ref := range span.References {
		if ref.RefType == refTypeChildOf {
			last = i
		}
	}
	out := []jaeger.Reference{}
	for i, ref := range span.References {
		if ref.RefType == refTypeChildOf && i != last {
			continue
		}
		if keep[ref.SpanID] || !present[ref.SpanID] {
			out = append(out, ref)
		}
	}
	return out
}

// PruneToFragment mirrors fragment.py pruneToFragment: the fragment of a
// trace for one service and operation.
//
// The fragment keeps, in their original order:
//
//   - every span that matches the service and operation, with its
//     descendants;
//   - the ancestors of every match, so that a match that has a parent keeps
//     it and does not become an analysis root;
//   - every orphan span (its CHILD_OF parent is not in the trace), so that
//     incomplete subtrees are still detected;
//   - the first span whose operation marks a test trace, and all processes,
//     so that a test trace is still one.
//
// trace is not modified. The fragment shares the process maps and the kept
// spans' tags and logs with it.
func PruneToFragment(trace *jaeger.Trace, serviceName, operationName string) *jaeger.Trace {
	processName := map[string]string{}
	present := map[string]bool{}
	parent := map[string]string{}
	children := map[string][]string{}
	for i := range trace.Data {
		item := &trace.Data[i]
		for pid, process := range item.Processes {
			processName[pid] = process.ServiceName
		}
	}
	var matches []string
	for i := range trace.Data {
		for j := range trace.Data[i].Spans {
			span := &trace.Data[i].Spans[j]
			present[span.SpanID] = true
			// For a repeated span ID the last span wins, as in the Graph.
			delete(parent, span.SpanID)
			if parentID, ok := lastChildOf(span); ok {
				parent[span.SpanID] = parentID
				children[parentID] = append(children[parentID], span.SpanID)
			}
			if service, ok := processName[span.ProcessID]; ok && service == serviceName && span.OperationName == operationName {
				matches = append(matches, span.SpanID)
			}
		}
	}

	keep := map[string]bool{}
	stack := append([]string(nil), matches...)
	for len(stack) > 0 {
		spanID := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if keep[spanID] {
			continue
		}
		keep[spanID] = true
		stack = append(stack, children[spanID]...)
	}
	// The walk up stops at a span that is already kept, which also ends it on
	// a CHILD_OF cycle.
	for _, spanID := range matches {
		ancestor, ok := parent[spanID]
		for ok && present[ancestor] && !keep[ancestor] {
			keep[ancestor] = true
			ancestor, ok = parent[ancestor]
		}
	}
	marker := false
	for i := range trace.Data {
		for j := range trace.Data[i].Spans {
			span := &trace.Data[i].Spans[j]
			if parentID, ok := lastChildOf(span); ok && !present[parentID] {
				keep[span.SpanID] = true
			}
			if !marker && isTestTraceByOpName(span.OperationName) {
				keep[span.SpanID] = true
				marker = true
			}
		}
	}

	fragment := &jaeger.Trace{Data: make([]jaeger.TraceData, 0, len(trace.Data))}
	for i := range trace.Data {
		item := trace.Data[i]
		spans := []jaeger.Span{}
		for j := range item.Spans {
			if !keep[item.Spans[j].SpanID] {
				continue
			}
			span := item.Spans[j]
			span.References = fragmentReferences(&span, keep, present)
			spans = append(spans, span)
		}
		item.Spans = spans
		fragment.Data = append(fragment.Data, item)
	}
	return fragment
}

// CheckFragmentConfig mirrors process_trace.py checkFragmentConfig: an error
// when Fragment is combined with an option that needs the whole trace.
//
// A fragment only has the subtrees of the matching spans, their ancestors
// and the orphan spans. The light pipeline without RootTrace reads nothing
// else, except for the trace-root error breakdown.
func CheckFragmentConfig(c *LightConfig) error {
	if !c.Fragment {
		return nil
	}
	if c.RootTrace {
		return errors.New("--fragment cannot be used with --rootTrace: a fragment does not have the roots of its trace")
	}
	if c.ErrorBreakdown != nil && c.ErrorBreakdown.Root == ErrorBreakdownTraceRoot {
		return errors.New("--fragment cannot be used with --errorBreakdownRoot trace: a fragment does not have " +
			"the whole trace (use --errorBreakdownRoot analysis)")
	}
	return nil
}
