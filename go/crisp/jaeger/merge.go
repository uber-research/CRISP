// Trace merging for split Jaeger traces -- a port of crisp/trace_merger.py.
//
// Large traces can be split by Jaeger into multiple trace files due to size
// limits. The child trace is created as a separate trace with its own
// traceID, linked to the parent via a FOLLOWS_FROM reference (Jaeger does
// this for UI reasons; semantically it should be CHILD_OF). The graph
// construction pass only follows CHILD_OF references within one trace, so
// split traces must be merged before analysis:
//
//  1. Find the FOLLOWS_FROM reference from child to parent trace.
//  2. Combine all spans from both traces.
//  3. Update all child span traceIDs to the parent's.
//  4. Convert the cross-trace FOLLOWS_FROM reference to CHILD_OF.
//  5. Merge the process dictionaries, renumbering child processes whose ID
//     collides with a different parent process definition.
//
// create_merged_graph is intentionally not ported here; it belongs to the
// graph-construction stage.
package jaeger

import (
	"fmt"
	"os"
	"sort"
	"strconv"
)

// TraceMergeError is returned when trace merge validation fails.
type TraceMergeError struct {
	Message string
}

func (e *TraceMergeError) Error() string { return e.Message }

// CrossTraceLink describes the FOLLOWS_FROM reference from a child trace to
// its parent trace.
type CrossTraceLink struct {
	ChildSpanID    string
	ParentSpanID   string
	RefType        string
	ChildOperation string
}

// ExternalRef describes a span reference that points outside its own trace,
// indicating the trace is part of a split.
type ExternalRef struct {
	SpanID            string
	ReferencedTraceID string
	ReferencedSpanID  string
	RefType           string
}

// firstTraceData returns data[0], mirroring the Python code's unconditional
// trace_data["data"][0] indexing.
func firstTraceData(trace *Trace) (*TraceData, error) {
	if len(trace.Data) == 0 {
		return nil, &TraceMergeError{Message: "trace has empty data array"}
	}
	return &trace.Data[0], nil
}

// FindCrossTraceLink finds the FOLLOWS_FROM reference from the child trace to
// the parent trace, returning nil if none exists. When several spans have
// such a reference, the first in document order wins (as in Python).
func FindCrossTraceLink(child *Trace, parentTraceID string) *CrossTraceLink {
	childData, err := firstTraceData(child)
	if err != nil {
		return nil
	}
	for _, span := range childData.Spans {
		for _, ref := range span.References {
			if ref.TraceID == parentTraceID && ref.RefType == "FOLLOWS_FROM" {
				return &CrossTraceLink{
					ChildSpanID:    span.SpanID,
					ParentSpanID:   ref.SpanID,
					RefType:        ref.RefType,
					ChildOperation: span.OperationName,
				}
			}
		}
	}
	return nil
}

// ValidateMergePreconditions checks that a merge can proceed safely: the
// referenced parent span must exist. Span ID collisions between parent and
// child are not fatal; they are returned as warnings (Python logs them).
func ValidateMergePreconditions(parent, child *Trace, link *CrossTraceLink) (warnings []string, err error) {
	parentData, err := firstTraceData(parent)
	if err != nil {
		return nil, err
	}
	childData, err := firstTraceData(child)
	if err != nil {
		return nil, err
	}

	parentSpanIDs := make(map[string]bool, len(parentData.Spans))
	for _, span := range parentData.Spans {
		parentSpanIDs[span.SpanID] = true
	}
	if !parentSpanIDs[link.ParentSpanID] {
		return nil, &TraceMergeError{
			Message: fmt.Sprintf(
				"Parent span %s referenced by child trace not found in parent trace %s",
				link.ParentSpanID, parentData.TraceID,
			),
		}
	}

	var collisions []string
	for _, span := range childData.Spans {
		if parentSpanIDs[span.SpanID] {
			collisions = append(collisions, span.SpanID)
		}
	}
	if len(collisions) > 0 {
		sort.Strings(collisions)
		warnings = append(warnings, fmt.Sprintf(
			"Found %d span ID collisions between parent and child traces: %v",
			len(collisions), collisions,
		))
	}
	return warnings, nil
}

// maxProcessID mirrors Python's max([int(pid[1:]) for pid in processes]):
// the first character of every process ID is stripped (whatever it is) and
// the remainder must parse as an integer. It is an error for the map to be
// empty or for any ID to have a non-numeric suffix.
func maxProcessID(processes map[string]Process) (int, error) {
	if len(processes) == 0 {
		return 0, &TraceMergeError{Message: "trace has no processes"}
	}
	maxID := -1
	for pid := range processes {
		if len(pid) == 0 {
			return 0, &TraceMergeError{Message: "empty process ID"}
		}
		n, err := strconv.Atoi(pid[1:])
		if err != nil {
			return 0, &TraceMergeError{
				Message: fmt.Sprintf("process ID %q has no numeric suffix: %v", pid, err),
			}
		}
		if n > maxID {
			maxID = n
		}
	}
	return maxID, nil
}

// processEqual mirrors Python's dict equality on process definitions:
// serviceName and the tags list must match exactly (order-sensitive).
func processEqual(a, b Process) bool {
	if a.ServiceName != b.ServiceName || len(a.Tags) != len(b.Tags) {
		return false
	}
	for i := range a.Tags {
		if !tagEqual(a.Tags[i], b.Tags[i]) {
			return false
		}
	}
	return true
}

func tagEqual(a, b Tag) bool {
	if a.Key != b.Key || a.Type != b.Type {
		return false
	}
	return valueEqual(a.Value, b.Value)
}

// valueEqual compares dynamically-typed tag values the way Python's == does
// for the JSON-decoded shapes involved (strings, numbers, bools, nil, and
// nested lists/maps).
func valueEqual(a, b any) bool {
	switch av := a.(type) {
	case nil:
		return b == nil
	case string:
		bv, ok := b.(string)
		return ok && av == bv
	case bool:
		bv, ok := b.(bool)
		return ok && av == bv
	case float64:
		// Python 1 == 1.0 is True; compare numerically across numeric types.
		return numericEqual(av, b)
	case int64:
		return numericEqual(av, b)
	case []any:
		bv, ok := b.([]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !valueEqual(av[i], bv[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		bv, ok := b.(map[string]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for k, v := range av {
			if !valueEqual(v, bv[k]) {
				return false
			}
		}
		return true
	default:
		// json.Number and any other string-like value compare by string form.
		return fmt.Sprint(a) == fmt.Sprint(b)
	}
}

// numericEqual reports whether a (a float64 or int64) equals b numerically,
// accepting b as float64, int64, or json.Number-like string.
func numericEqual(a any, b any) bool {
	af, aok := toFloat(a)
	bf, bok := toFloat(b)
	return aok && bok && af == bf
}

func toFloat(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case int64:
		return float64(x), true
	case string:
		f, err := strconv.ParseFloat(x, 64)
		return f, err == nil
	case fmt.Stringer:
		f, err := strconv.ParseFloat(x.String(), 64)
		return f, err == nil
	default:
		return 0, false
	}
}

// deepCopyTrace returns a deep copy of the trace so merges never mutate the
// caller's data (matching Python's copy.deepcopy of the parent trace).
func deepCopyTrace(t *Trace) *Trace {
	out := &Trace{Data: make([]TraceData, len(t.Data))}
	for i := range t.Data {
		out.Data[i] = deepCopyTraceData(&t.Data[i])
	}
	return out
}

func deepCopyTraceData(d *TraceData) TraceData {
	out := TraceData{
		TraceID:   d.TraceID,
		Processes: make(map[string]Process, len(d.Processes)),
		Spans:     make([]Span, len(d.Spans)),
	}
	for pid, p := range d.Processes {
		out.Processes[pid] = deepCopyProcess(p)
	}
	for i := range d.Spans {
		out.Spans[i] = deepCopySpan(&d.Spans[i])
	}
	return out
}

func deepCopyProcess(p Process) Process {
	return Process{ServiceName: p.ServiceName, Tags: deepCopyTags(p.Tags)}
}

func deepCopySpan(s *Span) Span {
	out := *s
	if s.References != nil {
		out.References = make([]Reference, len(s.References))
		copy(out.References, s.References)
	}
	out.Tags = deepCopyTags(s.Tags)
	if s.Logs != nil {
		out.Logs = make([]Log, len(s.Logs))
		for i := range s.Logs {
			out.Logs[i] = Log{
				Timestamp: s.Logs[i].Timestamp,
				Fields:    deepCopyTags(s.Logs[i].Fields),
			}
		}
	}
	return out
}

func deepCopyTags(tags []Tag) []Tag {
	if tags == nil {
		return nil
	}
	out := make([]Tag, len(tags))
	for i, t := range tags {
		out[i] = Tag{Key: t.Key, Type: t.Type, Value: deepCopyValue(t.Value)}
	}
	return out
}

// deepCopyValue deep-copies the JSON-decoded dynamic shapes a tag value can
// hold. Scalars (string, bool, float64, json.Number) are immutable and are
// returned as-is.
func deepCopyValue(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, v := range x {
			out[k] = deepCopyValue(v)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i := range x {
			out[i] = deepCopyValue(x[i])
		}
		return out
	default:
		return v
	}
}

// mergeChildIntoParent is the shared core of MergeTraceData and
// MergeMultipleChildTraces: it merges childData into the (already
// deep-copied) merged trace, whose data[0] has trace ID parentTraceID.
// Python inlines this logic in both functions; behavior is identical.
//
// One deliberate divergence: Python iterates the child process dictionary in
// JSON document order; Go maps are unordered, so child process IDs are
// visited in sorted order. When multiple child processes collide with parent
// IDs, the renumbering assignments can therefore differ from Python's, but
// the merged graph is isomorphic either way -- every child span's processID
// is remapped consistently, and all downstream outputs key on service and
// operation names, never on process IDs.
func mergeChildIntoParent(merged *Trace, child *Trace) (warnings []string, err error) {
	parentData, err := firstTraceData(merged)
	if err != nil {
		return nil, err
	}
	parentTraceID := parentData.TraceID
	childData, err := firstTraceData(child)
	if err != nil {
		return nil, err
	}
	childTraceID := childData.TraceID

	link := FindCrossTraceLink(child, parentTraceID)
	if link == nil {
		return nil, &TraceMergeError{
			Message: fmt.Sprintf(
				"No FOLLOWS_FROM reference found from child trace %s to parent trace %s",
				childTraceID, parentTraceID,
			),
		}
	}

	warnings, err = ValidateMergePreconditions(merged, child, link)
	if err != nil {
		return nil, err
	}

	// Renumber child processes whose ID collides with a different parent
	// process definition.
	maxParentProcessID, err := maxProcessID(parentData.Processes)
	if err != nil {
		return nil, err
	}
	processIDMapping := make(map[string]string, len(childData.Processes))

	childProcessIDs := make([]string, 0, len(childData.Processes))
	for pid := range childData.Processes {
		childProcessIDs = append(childProcessIDs, pid)
	}
	sort.Strings(childProcessIDs)

	for _, processID := range childProcessIDs {
		processData := childData.Processes[processID]
		if parentProcess, collides := parentData.Processes[processID]; collides {
			if processEqual(parentProcess, processData) {
				// Identical definition, no need to renumber.
				processIDMapping[processID] = processID
			} else {
				maxParentProcessID++
				newID := fmt.Sprintf("p%d", maxParentProcessID)
				processIDMapping[processID] = newID
				parentData.Processes[newID] = processData
			}
		} else {
			processIDMapping[processID] = processID
			parentData.Processes[processID] = processData
		}
	}

	// Copy and update child spans.
	for _, span := range childData.Spans {
		mergedSpan := deepCopySpan(&span)
		mergedSpan.TraceID = parentTraceID
		if newPID, ok := processIDMapping[mergedSpan.ProcessID]; ok {
			mergedSpan.ProcessID = newPID
		}
		for i := range mergedSpan.References {
			ref := &mergedSpan.References[i]
			if ref.TraceID == childTraceID {
				ref.TraceID = parentTraceID
			}
			// Convert FOLLOWS_FROM to CHILD_OF for the cross-trace link.
			if mergedSpan.SpanID == link.ChildSpanID &&
				ref.SpanID == link.ParentSpanID &&
				ref.RefType == "FOLLOWS_FROM" {
				ref.RefType = "CHILD_OF"
			}
		}
		parentData.Spans = append(parentData.Spans, mergedSpan)
	}

	return warnings, nil
}

// MergeTraceData merges a child trace into a parent trace, returning the
// merged trace. Neither input is modified.
func MergeTraceData(parent, child *Trace) (*Trace, []string, error) {
	merged := deepCopyTrace(parent)
	warnings, err := mergeChildIntoParent(merged, child)
	if err != nil {
		return nil, nil, err
	}
	return merged, warnings, nil
}

// MergeMultipleChildTraces merges several child traces into a parent trace,
// processing the parent only once. Each child is validated and renumbered
// against the merged-so-far trace, exactly as in the Python original.
func MergeMultipleChildTraces(parent *Trace, children []*Trace) (*Trace, []string, error) {
	merged := deepCopyTrace(parent)
	var allWarnings []string
	for _, child := range children {
		warnings, err := mergeChildIntoParent(merged, child)
		if err != nil {
			return nil, nil, err
		}
		allWarnings = append(allWarnings, warnings...)
	}
	return merged, allWarnings, nil
}

// LoadAndMergeTraces loads trace files from disk and merges them. A single
// child path takes the single-child merge path; multiple child paths take the
// optimized multi-child path (the parent file is read once either way).
func LoadAndMergeTraces(parentPath string, childPaths []string) (*Trace, []string, error) {
	parentBytes, err := os.ReadFile(parentPath)
	if err != nil {
		return nil, nil, err
	}
	parent, err := Decode(parentBytes)
	if err != nil {
		return nil, nil, fmt.Errorf("decoding parent trace %s: %w", parentPath, err)
	}

	children := make([]*Trace, len(childPaths))
	for i, childPath := range childPaths {
		childBytes, err := os.ReadFile(childPath)
		if err != nil {
			return nil, nil, err
		}
		children[i], err = Decode(childBytes)
		if err != nil {
			return nil, nil, fmt.Errorf("decoding child trace %s: %w", childPath, err)
		}
	}

	if len(children) == 1 {
		return MergeTraceData(parent, children[0])
	}
	return MergeMultipleChildTraces(parent, children)
}

// IdentifySplitTraces returns the external trace references of a trace --
// references whose traceID differs from the trace's own -- indicating the
// trace is part of a split.
func IdentifySplitTraces(trace *Trace) ([]ExternalRef, error) {
	data, err := firstTraceData(trace)
	if err != nil {
		return nil, err
	}
	var externalRefs []ExternalRef
	for _, span := range data.Spans {
		for _, ref := range span.References {
			if ref.TraceID != "" && ref.TraceID != data.TraceID {
				refType := ref.RefType
				if refType == "" {
					refType = "UNKNOWN"
				}
				externalRefs = append(externalRefs, ExternalRef{
					SpanID:            span.SpanID,
					ReferencedTraceID: ref.TraceID,
					ReferencedSpanID:  ref.SpanID,
					RefType:           refType,
				})
			}
		}
	}
	return externalRefs, nil
}
