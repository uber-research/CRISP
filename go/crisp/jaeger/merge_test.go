package jaeger

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func makeSpan(traceID, spanID, op, processID string, refs ...Reference) Span {
	return Span{
		TraceID:       traceID,
		SpanID:        spanID,
		OperationName: op,
		ProcessID:     processID,
		References:    refs,
		StartTime:     json.Number("0"),
		Duration:      json.Number("1"),
	}
}

func makeTrace(traceID string, processes map[string]Process, spans ...Span) *Trace {
	return &Trace{Data: []TraceData{{
		TraceID:   traceID,
		Processes: processes,
		Spans:     spans,
	}}}
}

func svcProcess(name string) Process {
	return Process{ServiceName: name, Tags: []Tag{{Key: "k", Type: "string", Value: "v"}}}
}

// parentFixture: trace "parent" with one process p1=svcA and one span P1.
func parentFixture() *Trace {
	return makeTrace("parent",
		map[string]Process{"p1": svcProcess("svcA")},
		makeSpan("parent", "P1", "opA", "p1"),
	)
}

// childFixture: trace "child" with process p1=svcB (collides with the
// parent's p1=svcA) and two spans: C1 linked FOLLOWS_FROM to the parent's P1,
// C2 linked CHILD_OF to C1 within the child trace.
func childFixture() *Trace {
	return makeTrace("child",
		map[string]Process{"p1": svcProcess("svcB")},
		makeSpan("child", "C1", "opB", "p1",
			Reference{RefType: "FOLLOWS_FROM", TraceID: "parent", SpanID: "P1"}),
		makeSpan("child", "C2", "opC", "p1",
			Reference{RefType: "CHILD_OF", TraceID: "child", SpanID: "C1"}),
	)
}

func TestFindCrossTraceLink(t *testing.T) {
	link := FindCrossTraceLink(childFixture(), "parent")
	if link == nil {
		t.Fatal("expected a cross-trace link")
	}
	if link.ChildSpanID != "C1" || link.ParentSpanID != "P1" ||
		link.RefType != "FOLLOWS_FROM" || link.ChildOperation != "opB" {
		t.Errorf("unexpected link: %+v", link)
	}

	// CHILD_OF to the parent and FOLLOWS_FROM to another trace are ignored.
	other := makeTrace("child",
		map[string]Process{"p1": svcProcess("svcB")},
		makeSpan("child", "C1", "opB", "p1",
			Reference{RefType: "CHILD_OF", TraceID: "parent", SpanID: "P1"}),
		makeSpan("child", "C2", "opC", "p1",
			Reference{RefType: "FOLLOWS_FROM", TraceID: "unrelated", SpanID: "X"}),
	)
	if got := FindCrossTraceLink(other, "parent"); got != nil {
		t.Errorf("expected no link, got %+v", got)
	}

	if got := FindCrossTraceLink(&Trace{}, "parent"); got != nil {
		t.Errorf("empty trace: expected nil, got %+v", got)
	}
}

func TestValidateMergePreconditions(t *testing.T) {
	parent := parentFixture()
	child := childFixture()
	link := FindCrossTraceLink(child, "parent")

	warnings, err := ValidateMergePreconditions(parent, child, link)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(warnings) != 0 {
		t.Errorf("expected no warnings, got %v", warnings)
	}

	// Referenced parent span missing -> fatal.
	badLink := &CrossTraceLink{ChildSpanID: "C1", ParentSpanID: "NOPE"}
	if _, err := ValidateMergePreconditions(parent, child, badLink); err == nil {
		t.Fatal("expected error for missing parent span")
	} else if !strings.Contains(err.Error(), "NOPE") {
		t.Errorf("error should name the missing span: %v", err)
	}

	// Span ID collision -> warning, not fatal.
	colliding := makeTrace("child",
		map[string]Process{"p9": svcProcess("svcB")},
		makeSpan("child", "P1", "opB", "p9",
			Reference{RefType: "FOLLOWS_FROM", TraceID: "parent", SpanID: "P1"}),
	)
	warnings, err = ValidateMergePreconditions(parent, colliding, link)
	if err != nil {
		t.Fatalf("collision should not be fatal: %v", err)
	}
	if len(warnings) != 1 || !strings.Contains(warnings[0], "P1") {
		t.Errorf("expected one collision warning naming P1, got %v", warnings)
	}
}

func TestMergeTraceData_Basic(t *testing.T) {
	merged, warnings, err := MergeTraceData(parentFixture(), childFixture())
	if err != nil {
		t.Fatalf("merge failed: %v", err)
	}
	if len(warnings) != 0 {
		t.Errorf("unexpected warnings: %v", warnings)
	}

	data := merged.Data[0]
	if data.TraceID != "parent" {
		t.Errorf("merged traceID = %q, want parent", data.TraceID)
	}
	if len(data.Spans) != 3 {
		t.Fatalf("merged span count = %d, want 3", len(data.Spans))
	}

	// Processes: parent p1=svcA kept; child p1=svcB renumbered to p2.
	if len(data.Processes) != 2 {
		t.Fatalf("merged process count = %d, want 2", len(data.Processes))
	}
	if data.Processes["p1"].ServiceName != "svcA" {
		t.Errorf("p1 = %v, want svcA", data.Processes["p1"].ServiceName)
	}
	if data.Processes["p2"].ServiceName != "svcB" {
		t.Errorf("p2 = %v, want svcB (renumbered child)", data.Processes["p2"].ServiceName)
	}

	c1 := data.Spans[1]
	if c1.SpanID != "C1" {
		t.Fatalf("span order: got %q at index 1, want C1", c1.SpanID)
	}
	if c1.TraceID != "parent" {
		t.Errorf("child span traceID = %q, want parent", c1.TraceID)
	}
	if c1.ProcessID != "p2" {
		t.Errorf("child span processID = %q, want p2", c1.ProcessID)
	}
	if len(c1.References) != 1 || c1.References[0].RefType != "CHILD_OF" ||
		c1.References[0].TraceID != "parent" || c1.References[0].SpanID != "P1" {
		t.Errorf("cross-trace link not converted to CHILD_OF: %+v", c1.References)
	}

	c2 := data.Spans[2]
	if c2.TraceID != "parent" || c2.ProcessID != "p2" {
		t.Errorf("C2 identity not updated: %+v", c2)
	}
	if c2.References[0].RefType != "CHILD_OF" || c2.References[0].TraceID != "parent" ||
		c2.References[0].SpanID != "C1" {
		t.Errorf("intra-child reference not rewritten to parent trace: %+v", c2.References)
	}
}

func TestMergeTraceData_IdenticalProcessNotRenumbered(t *testing.T) {
	child := makeTrace("child",
		map[string]Process{"p1": svcProcess("svcA")}, // identical to parent's p1
		makeSpan("child", "C1", "opB", "p1",
			Reference{RefType: "FOLLOWS_FROM", TraceID: "parent", SpanID: "P1"}),
	)
	merged, _, err := MergeTraceData(parentFixture(), child)
	if err != nil {
		t.Fatalf("merge failed: %v", err)
	}
	data := merged.Data[0]
	if len(data.Processes) != 1 {
		t.Fatalf("process count = %d, want 1 (no renumbering)", len(data.Processes))
	}
	if got := data.Spans[1].ProcessID; got != "p1" {
		t.Errorf("child span processID = %q, want p1", got)
	}
}

func TestMergeTraceData_NoCollisionKeepsChildProcessID(t *testing.T) {
	child := makeTrace("child",
		map[string]Process{"p9": svcProcess("svcB")},
		makeSpan("child", "C1", "opB", "p9",
			Reference{RefType: "FOLLOWS_FROM", TraceID: "parent", SpanID: "P1"}),
	)
	merged, _, err := MergeTraceData(parentFixture(), child)
	if err != nil {
		t.Fatalf("merge failed: %v", err)
	}
	data := merged.Data[0]
	if len(data.Processes) != 2 || data.Processes["p9"].ServiceName != "svcB" {
		t.Errorf("child process not kept as p9: %v", data.Processes)
	}
	if got := data.Spans[1].ProcessID; got != "p9" {
		t.Errorf("child span processID = %q, want p9", got)
	}
}

func TestMergeTraceData_NoLink(t *testing.T) {
	child := makeTrace("child",
		map[string]Process{"p1": svcProcess("svcB")},
		makeSpan("child", "C1", "opB", "p1"),
	)
	if _, _, err := MergeTraceData(parentFixture(), child); err == nil {
		t.Fatal("expected TraceMergeError for missing FOLLOWS_FROM link")
	} else if _, ok := err.(*TraceMergeError); !ok {
		t.Errorf("error type = %T, want *TraceMergeError", err)
	}
}

func TestMergeTraceData_BadProcessID(t *testing.T) {
	parent := makeTrace("parent",
		map[string]Process{"px": svcProcess("svcA")}, // no numeric suffix
		makeSpan("parent", "P1", "opA", "px"),
	)
	if _, _, err := MergeTraceData(parent, childFixture()); err == nil {
		t.Fatal("expected error for non-numeric process ID suffix")
	}
}

func TestMergeTraceData_DoesNotMutateInputs(t *testing.T) {
	parent := parentFixture()
	child := childFixture()
	if _, _, err := MergeTraceData(parent, child); err != nil {
		t.Fatalf("merge failed: %v", err)
	}
	if len(parent.Data[0].Spans) != 1 || len(parent.Data[0].Processes) != 1 {
		t.Error("parent trace was mutated")
	}
	childC1 := child.Data[0].Spans[0]
	if childC1.TraceID != "child" || childC1.References[0].RefType != "FOLLOWS_FROM" {
		t.Error("child trace was mutated")
	}
}

func TestMergeMultipleChildTraces(t *testing.T) {
	child2 := makeTrace("child2",
		map[string]Process{"p1": svcProcess("svcC")}, // also collides
		makeSpan("child2", "D1", "opD", "p1",
			Reference{RefType: "FOLLOWS_FROM", TraceID: "parent", SpanID: "P1"}),
	)

	merged, _, err := MergeMultipleChildTraces(parentFixture(), []*Trace{childFixture(), child2})
	if err != nil {
		t.Fatalf("multi-merge failed: %v", err)
	}
	data := merged.Data[0]
	if len(data.Spans) != 4 {
		t.Fatalf("span count = %d, want 4", len(data.Spans))
	}
	// p1=svcA (parent), p2=svcB (child 1), p3=svcC (child 2): renumbering
	// accumulates across children.
	if len(data.Processes) != 3 || data.Processes["p3"].ServiceName != "svcC" {
		t.Errorf("processes = %v, want p1/p2/p3", data.Processes)
	}
	if got := data.Spans[3].ProcessID; got != "p3" {
		t.Errorf("child2 span processID = %q, want p3", got)
	}

	// Empty child list returns a deep copy of the parent.
	solo, warnings, err := MergeMultipleChildTraces(parentFixture(), nil)
	if err != nil || len(warnings) != 0 {
		t.Fatalf("empty children: err=%v warnings=%v", err, warnings)
	}
	if len(solo.Data[0].Spans) != 1 || len(solo.Data[0].Processes) != 1 {
		t.Error("empty children should return an unchanged copy of the parent")
	}
}

func TestLoadAndMergeTraces(t *testing.T) {
	dir := t.TempDir()
	write := func(name string, trace *Trace) string {
		t.Helper()
		b, err := json.Marshal(trace)
		if err != nil {
			t.Fatalf("marshal %s: %v", name, err)
		}
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, b, 0o644); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
		return path
	}
	parentPath := write("parent.json", parentFixture())
	childPath := write("child.json", childFixture())

	merged, _, err := LoadAndMergeTraces(parentPath, []string{childPath})
	if err != nil {
		t.Fatalf("load+merge failed: %v", err)
	}
	if len(merged.Data[0].Spans) != 3 {
		t.Errorf("span count = %d, want 3", len(merged.Data[0].Spans))
	}

	merged2, _, err := LoadAndMergeTraces(parentPath, []string{childPath, childPath})
	if err != nil {
		t.Fatalf("multi load+merge failed: %v", err)
	}
	if len(merged2.Data[0].Spans) != 5 {
		t.Errorf("multi span count = %d, want 5", len(merged2.Data[0].Spans))
	}

	if _, _, err := LoadAndMergeTraces(parentPath, []string{filepath.Join(dir, "missing.json")}); err == nil {
		t.Fatal("expected error for missing child file")
	}
}

func TestIdentifySplitTraces(t *testing.T) {
	refs, err := IdentifySplitTraces(childFixture())
	if err != nil {
		t.Fatalf("identify failed: %v", err)
	}
	if len(refs) != 1 {
		t.Fatalf("expected 1 external ref, got %d", len(refs))
	}
	want := ExternalRef{
		SpanID:            "C1",
		ReferencedTraceID: "parent",
		ReferencedSpanID:  "P1",
		RefType:           "FOLLOWS_FROM",
	}
	if refs[0] != want {
		t.Errorf("external ref = %+v, want %+v", refs[0], want)
	}

	// All-internal references are not splits.
	refs, err = IdentifySplitTraces(parentFixture())
	if err != nil || len(refs) != 0 {
		t.Errorf("internal-only trace: refs=%v err=%v", refs, err)
	}

	// A missing refType surfaces as UNKNOWN.
	weird := makeTrace("t",
		map[string]Process{"p1": svcProcess("svcA")},
		makeSpan("t", "S1", "op", "p1", Reference{TraceID: "other", SpanID: "X"}),
	)
	refs, err = IdentifySplitTraces(weird)
	if err != nil || len(refs) != 1 || refs[0].RefType != "UNKNOWN" {
		t.Errorf("missing refType: refs=%v err=%v", refs, err)
	}
}
