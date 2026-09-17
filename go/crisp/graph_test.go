package crisp

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/uber-research/CRISP/go/crisp/jaeger"
)

func mkSpan(sid, op, pid string, start, dur int64, refs []jaeger.Reference, tags ...jaeger.Tag) jaeger.Span {
	return jaeger.Span{
		TraceID:       "t",
		SpanID:        sid,
		OperationName: op,
		ProcessID:     pid,
		References:    refs,
		StartTime:     json.Number(strconv.FormatInt(start, 10)),
		Duration:      json.Number(strconv.FormatInt(dur, 10)),
		Tags:          tags,
	}
}

// childOf is declared in root_span_test.go.

func kindTag(kind string) jaeger.Tag {
	return jaeger.Tag{Key: "span.kind", Type: "string", Value: kind}
}

func mkProc(service string, tags ...jaeger.Tag) jaeger.Process {
	return jaeger.Process{ServiceName: service, Tags: tags}
}

func mkTrace(processes map[string]jaeger.Process, spans ...jaeger.Span) *jaeger.Trace {
	return &jaeger.Trace{Data: []jaeger.TraceData{{
		TraceID:   "t",
		Processes: processes,
		Spans:     spans,
	}}}
}

func boolPtr(b bool) *bool { return &b }

// childIDs returns the children's span IDs in order.
func childIDs(n *Node) []string {
	ids := make([]string, 0, len(n.Children))
	for _, c := range n.Children {
		ids = append(ids, c.SID)
	}
	return ids
}

func TestNewGraph_BasicTree(t *testing.T) {
	trace := mkTrace(
		map[string]jaeger.Process{"p1": mkProc("svcA"), "p2": mkProc("svcB")},
		mkSpan("R", "opA", "p1", 0, 100, nil),
		mkSpan("C1", "opB", "p2", 10, 40, []jaeger.Reference{childOf("R")},
			jaeger.Tag{Key: "error", Type: "bool", Value: true}),
		mkSpan("C2", "opB", "p2", 50, 40, []jaeger.Reference{childOf("R")}),
	)

	g, err := NewGraph(trace, "svcA", "opA", nil)
	if err != nil {
		t.Fatalf("NewGraph failed: %v", err)
	}
	root := g.RootNode
	if root.SID != "R" {
		t.Fatalf("root = %q, want R", root.SID)
	}
	if got := childIDs(root); len(got) != 2 || got[0] != "C1" || got[1] != "C2" {
		t.Errorf("children = %v, want [C1 C2]", got)
	}
	c1 := g.NodeHT["C1"]
	if c1.Parent != root || c1.ParentSpanID == nil || *c1.ParentSpanID != "R" {
		t.Errorf("C1 parent linkage broken: %+v", c1)
	}
	if g.NumErrors != 1 || !c1.ReturnError {
		t.Errorf("error accounting: NumErrors=%d C1.ReturnError=%v", g.NumErrors, c1.ReturnError)
	}
	if g.IsTestTrace {
		t.Error("IsTestTrace should be false with default (empty) heuristics")
	}
}

func TestNewGraph_ChildrenFollowDocumentOrder(t *testing.T) {
	// Children declared before the parent, and out of sid order: the
	// children list must follow span document order (Python dict insertion
	// order), not sid order.
	trace := mkTrace(
		map[string]jaeger.Process{"p1": mkProc("svcA")},
		mkSpan("C2", "opB", "p1", 50, 40, []jaeger.Reference{childOf("R")}),
		mkSpan("C1", "opB", "p1", 10, 40, []jaeger.Reference{childOf("R")}),
		mkSpan("R", "opA", "p1", 0, 100, nil),
	)
	g, err := NewGraph(trace, "svcA", "opA", nil)
	if err != nil {
		t.Fatalf("NewGraph failed: %v", err)
	}
	if got := childIDs(g.RootNode); len(got) != 2 || got[0] != "C2" || got[1] != "C1" {
		t.Errorf("children = %v, want document order [C2 C1]", got)
	}
}

func TestNewGraph_RootTraceRequiresSingleRoot(t *testing.T) {
	trace := mkTrace(
		map[string]jaeger.Process{"p1": mkProc("svcA")},
		mkSpan("R1", "opA", "p1", 0, 100, nil),
		mkSpan("R2", "opA", "p1", 0, 100, nil),
	)
	// Python logs a warning and leaves rootNode None (the trace is
	// skipped); it does not raise.
	g, err := NewGraph(trace, "svcA", "opA", nil)
	if err != nil {
		t.Fatalf("NewGraph: %v", err)
	}
	if g.RootNode != nil {
		t.Errorf("RootNode = %v, want nil (multi-root trace skipped)", g.RootNode.SID)
	}
}

func TestNewGraph_NoRoots(t *testing.T) {
	// A cycle: every span has a parent present in the trace.
	trace := mkTrace(
		map[string]jaeger.Process{"p1": mkProc("svcA")},
		mkSpan("A", "opA", "p1", 0, 100, []jaeger.Reference{childOf("B")}),
		mkSpan("B", "opA", "p1", 0, 100, []jaeger.Reference{childOf("A")}),
	)
	g, err := NewGraph(trace, "svcA", "opA", nil)
	if err != nil {
		t.Fatalf("NewGraph: %v", err)
	}
	if g.RootNode != nil {
		t.Errorf("RootNode = %v, want nil (rootless trace skipped)", g.RootNode.SID)
	}
}

func TestNewGraph_RootMismatch(t *testing.T) {
	trace := mkTrace(
		map[string]jaeger.Process{"p1": mkProc("svcA")},
		mkSpan("R", "opA", "p1", 0, 100, nil),
	)
	// Python's checkRootAndWarn returns False and __init__ returns with
	// rootNode None.
	g, err := NewGraph(trace, "svcX", "opY", nil)
	if err != nil {
		t.Fatalf("NewGraph: %v", err)
	}
	if g.RootNode != nil {
		t.Errorf("RootNode = %v, want nil (mismatched root skipped)", g.RootNode.SID)
	}
}

func TestNewGraph_DanglingParentBecomesRoot(t *testing.T) {
	trace := mkTrace(
		map[string]jaeger.Process{"p1": mkProc("svcA")},
		mkSpan("C", "opA", "p1", 0, 100, []jaeger.Reference{childOf("missing")}),
	)
	g, err := NewGraph(trace, "svcA", "opA", nil)
	if err != nil {
		t.Fatalf("NewGraph failed: %v", err)
	}
	if g.RootNode.SID != "C" {
		t.Errorf("root = %q, want C (dangling parent treated as root)", g.RootNode.SID)
	}
}

func TestNewGraph_FindARoot(t *testing.T) {
	trace := mkTrace(
		map[string]jaeger.Process{"p1": mkProc("svcA"), "p2": mkProc("svcB")},
		mkSpan("R", "opA", "p1", 0, 100, nil),
		mkSpan("M", "opB", "p2", 10, 80, []jaeger.Reference{childOf("R")}),
		mkSpan("N", "opC", "p2", 20, 30, []jaeger.Reference{childOf("M")}),
	)
	opts := &GraphOptions{RootTrace: boolPtr(false)}
	g, err := NewGraph(trace, "svcB", "opB", opts)
	if err != nil {
		t.Fatalf("NewGraph failed: %v", err)
	}
	if g.RootNode.SID != "M" {
		t.Fatalf("root = %q, want M", g.RootNode.SID)
	}
	if g.RootNode.Parent != nil || g.RootNode.ParentSpanID != nil {
		t.Error("chosen root was not detached from its parent")
	}
	// Bug-for-bug: Python does not remove the chosen root from its former
	// parent's children dict.
	if got := childIDs(g.NodeHT["R"]); len(got) != 1 || got[0] != "M" {
		t.Errorf("former parent's children = %v, want [M] (Python keeps the stale entry)", got)
	}

	// No matching node anywhere -> rootNode stays None in Python (skipped).
	g2, err := NewGraph(trace, "svcZ", "opZ", opts)
	if err != nil {
		t.Fatalf("NewGraph: %v", err)
	}
	if g2.RootNode != nil {
		t.Errorf("RootNode = %v, want nil (no matching node)", g2.RootNode.SID)
	}
}

func TestNewGraph_ProxyShortWire(t *testing.T) {
	ProxyServiceOpPairs = append(ProxyServiceOpPairs, [2]string{"svcProxy", "opProxy"})
	defer func() { ProxyServiceOpPairs = ProxyServiceOpPairs[:len(ProxyServiceOpPairs)-1] }()

	trace := mkTrace(
		map[string]jaeger.Process{
			"p1": mkProc("svcA"), "p2": mkProc("svcProxy"), "p3": mkProc("svcC"),
		},
		mkSpan("R", "opA", "p1", 0, 100, nil),
		mkSpan("P", "opProxy", "p2", 10, 80, []jaeger.Reference{childOf("R")}),
		mkSpan("C", "opC", "p3", 20, 30, []jaeger.Reference{childOf("P")}),
	)
	g, err := NewGraph(trace, "svcA", "opA", &GraphOptions{FilterProxy: true})
	if err != nil {
		t.Fatalf("NewGraph failed: %v", err)
	}
	// C is short-wired to R; P never appears as a child.
	if got := childIDs(g.RootNode); len(got) != 1 || got[0] != "C" {
		t.Errorf("root children = %v, want [C]", got)
	}
	if g.NodeHT["C"].Parent != g.RootNode {
		t.Error("C should be parented to R, skipping the proxy")
	}
	if g.ProxyNodes["P"] != 1 {
		t.Errorf("ProxyNodes[P] = %d, want 1", g.ProxyNodes["P"])
	}
	// The proxy's own parent pointer is still set (Python calls setParent
	// on proxy nodes even though they are never added as children).
	if g.NodeHT["P"].Parent != g.RootNode {
		t.Error("proxy node P should still have Parent set to R")
	}
}

func TestNewGraph_ProxyWithDanglingParent(t *testing.T) {
	ProxyServiceOpPairs = append(ProxyServiceOpPairs, [2]string{"svcProxy", "opProxy"})
	defer func() { ProxyServiceOpPairs = ProxyServiceOpPairs[:len(ProxyServiceOpPairs)-1] }()

	trace := mkTrace(
		map[string]jaeger.Process{"p1": mkProc("svcProxy"), "p2": mkProc("svcC")},
		mkSpan("P", "opProxy", "p1", 0, 100, []jaeger.Reference{childOf("missing")}),
		mkSpan("C", "opC", "p2", 10, 50, []jaeger.Reference{childOf("P")}),
	)
	// P's parent is dangling, so P is a root; C is short-wired past P, finds
	// no grandparent, and becomes a root too (numProxyRoots). Two roots
	// total, so rootTrace=true leaves RootNode nil (Python warns + skips)...
	g0, err := NewGraph(trace, "svcC", "opC", &GraphOptions{FilterProxy: true})
	if err != nil {
		t.Fatalf("NewGraph: %v", err)
	}
	if g0.RootNode != nil {
		t.Errorf("RootNode = %v, want nil (multi-root skipped under rootTrace)", g0.RootNode.SID)
	}
	// ...but rootTrace=false can select C.
	g, err := NewGraph(trace, "svcC", "opC", &GraphOptions{FilterProxy: true, RootTrace: boolPtr(false)})
	if err != nil {
		t.Fatalf("NewGraph failed: %v", err)
	}
	if g.RootNode.SID != "C" || g.NumProxyRoots != 1 {
		t.Errorf("root=%q NumProxyRoots=%d, want C/1", g.RootNode.SID, g.NumProxyRoots)
	}
}

func TestNewGraph_ErrorPropagation(t *testing.T) {
	ErrPropServiceOpPairs = append(ErrPropServiceOpPairs, [2]string{"svcE", "opE"})
	defer func() { ErrPropServiceOpPairs = ErrPropServiceOpPairs[:len(ErrPropServiceOpPairs)-1] }()

	errTag := jaeger.Tag{Key: "error", Type: "bool", Value: true}
	trace := mkTrace(
		map[string]jaeger.Process{"p1": mkProc("svcA"), "p2": mkProc("svcE")},
		mkSpan("R", "opA", "p1", 0, 100, nil),
		mkSpan("E", "opE", "p2", 10, 80, []jaeger.Reference{childOf("R")}),
		mkSpan("C", "opC", "p2", 20, 30, []jaeger.Reference{childOf("E")}, errTag),
		// A second err-prop node with two children must NOT propagate.
		mkSpan("E2", "opE", "p2", 10, 80, []jaeger.Reference{childOf("R")}),
		mkSpan("D1", "opD", "p2", 20, 30, []jaeger.Reference{childOf("E2")}, errTag),
		mkSpan("D2", "opD", "p2", 20, 30, []jaeger.Reference{childOf("E2")}),
	)
	// R has two children (E, E2); E has one child C.
	g, err := NewGraph(trace, "svcA", "opA", &GraphOptions{FilterProxy: true})
	if err != nil {
		t.Fatalf("NewGraph failed: %v", err)
	}
	if !g.NodeHT["E"].ReturnError {
		t.Error("single-child err-prop node should adopt the child's error")
	}
	if g.NodeHT["E2"].ReturnError {
		t.Error("multi-child err-prop node must not adopt the child's error")
	}
	// Without FilterProxy no err-prop nodes are recorded, so no propagation.
	g2, err := NewGraph(trace, "svcA", "opA", nil)
	if err != nil {
		t.Fatalf("NewGraph failed: %v", err)
	}
	if g2.NodeHT["E"].ReturnError {
		t.Error("propagation should not happen without FilterProxy")
	}
}

func TestSanitize_Cases1to4(t *testing.T) {
	trace := mkTrace(
		map[string]jaeger.Process{"p1": mkProc("svcA")},
		mkSpan("R", "opA", "p1", 100, 100, nil),                               // [100, 200]
		mkSpan("C1", "opB", "p1", 120, 30, []jaeger.Reference{childOf("R")}),  // case 1: inside
		mkSpan("C2", "opB", "p1", 50, 100, []jaeger.Reference{childOf("R")}),  // case 2: [50,150] overflows left
		mkSpan("C3", "opB", "p1", 150, 100, []jaeger.Reference{childOf("R")}), // case 3: [150,250] overflows right
		mkSpan("C4", "opB", "p1", 300, 100, []jaeger.Reference{childOf("R")}), // case 4: entirely after
		mkSpan("C5", "opB", "p1", 0, 60, []jaeger.Reference{childOf("R")}),    // case 4: entirely before
	)

	g, err := NewGraph(trace, "svcA", "opA", nil)
	if err != nil {
		t.Fatalf("NewGraph failed: %v", err)
	}
	root := g.RootNode
	if got := childIDs(root); len(got) != 3 || got[0] != "C1" || got[1] != "C2" || got[2] != "C3" {
		t.Errorf("children after sanitize = %v, want [C1 C2 C3]", got)
	}

	c1, c2, c3 := g.NodeHT["C1"], g.NodeHT["C2"], g.NodeHT["C3"]
	if c1.StartTime != 120 || c1.Duration != 30 || c1.EndTime != 150 {
		t.Errorf("case 1 child mutated: %+v", c1)
	}
	// Case 2: start clamped to parent start, duration reduced, endTime
	// untouched (stays consistent: 100+50 == 150).
	if c2.StartTime != 100 || c2.Duration != 50 || c2.EndTime != 150 {
		t.Errorf("case 2 shrink wrong: %+v", c2)
	}
	// Case 3: duration and endTime reduced.
	if c3.StartTime != 150 || c3.Duration != 50 || c3.EndTime != 200 {
		t.Errorf("case 3 shrink wrong: %+v", c3)
	}
	if g.TotalShrink != 100 || g.ShrinkCounter != 2 {
		t.Errorf("TotalShrink=%d ShrinkCounter=%d, want 100/2", g.TotalShrink, g.ShrinkCounter)
	}
}

func TestSanitize_Case0_CleanClientServer(t *testing.T) {
	trace := mkTrace(
		map[string]jaeger.Process{"p1": mkProc("svcA"), "p2": mkProc("svcB")},
		mkSpan("R", "opA", "p1", 100, 100, nil, kindTag("client")),                              // [100,200] dur=100
		mkSpan("S", "opB", "p2", 100, 101, []jaeger.Reference{childOf("R")}, kindTag("server")), // dur=101 <= 1.01*100
	)
	g, err := NewGraph(trace, "svcA", "opA", nil)
	if err != nil {
		t.Fatalf("NewGraph failed: %v", err)
	}
	s := g.NodeHT["S"]
	// Case 0: not chopped by case 3, but duration capped at the parent's --
	// and, bug-for-bug, endTime is NOT updated by adjustChildDuration.
	if s.Duration != 100 {
		t.Errorf("server duration = %d, want capped 100", s.Duration)
	}
	if s.EndTime != 201 {
		t.Errorf("server endTime = %d, want stale 201 (Python does not update it)", s.EndTime)
	}
	if g.TotalShrink != 0 {
		t.Errorf("TotalShrink = %d, want 0 (case 0 is not a shrink)", g.TotalShrink)
	}
}

func TestSanitize_Case0_UnacceptableDurationFallsThrough(t *testing.T) {
	trace := mkTrace(
		map[string]jaeger.Process{"p1": mkProc("svcA"), "p2": mkProc("svcB")},
		mkSpan("R", "opA", "p1", 100, 100, nil, kindTag("client")),
		mkSpan("S", "opB", "p2", 100, 150, []jaeger.Reference{childOf("R")}, kindTag("server")), // 150 > 1.01*100
	)
	g, err := NewGraph(trace, "svcA", "opA", nil)
	if err != nil {
		t.Fatalf("NewGraph failed: %v", err)
	}
	s := g.NodeHT["S"]
	// Not an acceptable client-server call, so case 3 chops it to the
	// parent's end.
	if s.Duration != 100 || s.EndTime != 200 {
		t.Errorf("server = duration %d endTime %d, want 100/200 (case 3)", s.Duration, s.EndTime)
	}
	if g.TotalShrink != 50 {
		t.Errorf("TotalShrink = %d, want 50", g.TotalShrink)
	}
}

func TestSanitize_Case0_Fuzzy(t *testing.T) {
	trace := mkTrace(
		map[string]jaeger.Process{"p1": mkProc("svcA"), "p2": mkProc("svcB"), "p3": mkProc("svcC")},
		mkSpan("G", "opA", "p1", 100, 100, nil, kindTag("server")),
		mkSpan("P", "opB", "p2", 100, 100, []jaeger.Reference{childOf("G")}), // no kind
		mkSpan("S", "opC", "p3", 100, 100, []jaeger.Reference{childOf("P")}, kindTag("server")),
	)
	g, err := NewGraph(trace, "svcA", "opA", nil)
	if err != nil {
		t.Fatalf("NewGraph failed: %v", err)
	}
	s := g.NodeHT["S"]
	if s.StartTime != 100 || s.Duration != 100 || s.EndTime != 200 {
		t.Errorf("fuzzy client-server server was mutated: %+v", s)
	}
}

func TestSanitize_Case0_DifferentHosts(t *testing.T) {
	trace := mkTrace(
		map[string]jaeger.Process{
			"p1": mkProc("svcA", jaeger.Tag{Key: "hostname", Type: "string", Value: "h1"}),
			"p2": mkProc("svcB", jaeger.Tag{Key: "hostname", Type: "string", Value: "h2"}),
		},
		mkSpan("R", "opA", "p1", 100, 100, nil),
		mkSpan("C", "opB", "p2", 100, 101, []jaeger.Reference{childOf("R")}),
	)
	g, err := NewGraph(trace, "svcA", "opA", nil)
	if err != nil {
		t.Fatalf("NewGraph failed: %v", err)
	}
	c := g.NodeHT["C"]
	if c.Duration != 100 || c.EndTime != 201 {
		t.Errorf("cross-host child = duration %d endTime %d, want capped 100 / stale 201",
			c.Duration, c.EndTime)
	}
}

func TestNewGraph_RemoveExcludedOps(t *testing.T) {
	trace := mkTrace(
		map[string]jaeger.Process{"p1": mkProc("svcA"), "p2": mkProc("svcX")},
		mkSpan("R", "opA", "p1", 0, 100, nil),
		mkSpan("A", "opX", "p2", 10, 50, []jaeger.Reference{childOf("R")}),
		mkSpan("B", "opB", "p2", 20, 20, []jaeger.Reference{childOf("A")}),
		mkSpan("C", "opC", "p1", 60, 30, []jaeger.Reference{childOf("R")}),
	)
	g, err := NewGraph(trace, "svcA", "opA", &GraphOptions{
		ExclusionSet: []ServiceOp{{Service: "svcX", Op: "opX"}},
	})
	if err != nil {
		t.Fatalf("NewGraph failed: %v", err)
	}
	// A and its whole subtree (B) are dropped; C survives.
	if got := childIDs(g.RootNode); len(got) != 1 || got[0] != "C" {
		t.Errorf("children = %v, want [C]", got)
	}
}

func TestNewGraph_TagMatching(t *testing.T) {
	trace := mkTrace(
		map[string]jaeger.Process{"p1": mkProc("svcA")},
		mkSpan("R", "opA", "p1", 0, 100, nil,
			jaeger.Tag{Key: "http.url", Type: "string", Value: "/a"}),
		mkSpan("C", "opB", "p1", 10, 50, []jaeger.Reference{childOf("R")},
			jaeger.Tag{Key: "http.status_code", Type: "int64", Value: json.Number("500")}),
	)
	filters := []TagFilter{
		{Name: "http.url", Value: "/a", SearchDepth: 1},
		{Name: "http.status_code", Value: json.Number("500"), SearchDepth: 2},
	}
	g, err := NewGraph(trace, "svcA", "opA", &GraphOptions{Tags: filters})
	if err != nil {
		t.Fatalf("NewGraph failed: %v", err)
	}
	if len(g.MatchedTags) != 2 {
		t.Errorf("MatchedTags = %v, want both filters matched", g.MatchedTags)
	}

	// With search depth 1 the child's tag is out of reach.
	filters[1].SearchDepth = 1
	g, err = NewGraph(trace, "svcA", "opA", &GraphOptions{Tags: filters})
	if err != nil {
		t.Fatalf("NewGraph failed: %v", err)
	}
	if len(g.MatchedTags) != 1 || g.MatchedTags[0].Name != "http.url" {
		t.Errorf("depth-limited MatchedTags = %v, want only http.url", g.MatchedTags)
	}
}

func TestNewGraph_DuplicateSpanIDs(t *testing.T) {
	// Python's nodeHT is a dict: a duplicate spanID overwrites the node
	// (last wins) but is visited exactly once in pass 3, at its first
	// document position. Regression test for duplicate roots producing a
	// spurious multi-root failure and duplicate children double-counting.
	trace := mkTrace(
		map[string]jaeger.Process{"p1": mkProc("svcA"), "p2": mkProc("svcB")},
		mkSpan("R", "opWRONG", "p1", 0, 100, nil),
		mkSpan("R", "opA", "p1", 0, 100, nil), // duplicate root ID, last wins
		mkSpan("C", "opX", "p2", 10, 10, []jaeger.Reference{childOf("R")}),
		mkSpan("C", "opY", "p2", 30, 10, []jaeger.Reference{childOf("R")}), // duplicate child ID
	)
	g, err := NewGraph(trace, "svcA", "opA", nil)
	if err != nil {
		t.Fatalf("NewGraph failed: %v", err)
	}
	if g.RootNode.OpName != "opA" {
		t.Errorf("root op = %q, want opA (last duplicate wins)", g.RootNode.OpName)
	}
	children := childIDs(g.RootNode)
	if len(children) != 1 || children[0] != "C" {
		t.Fatalf("children = %v, want exactly [C]", children)
	}
	if got := g.NodeHT["C"].OpName; got != "opY" {
		t.Errorf("child op = %q, want opY (last duplicate wins)", got)
	}
}

func TestNewGraph_UnknownProcessID(t *testing.T) {
	trace := mkTrace(
		map[string]jaeger.Process{"p1": mkProc("svcA")},
		mkSpan("R", "opA", "pUnknown", 0, 100, nil),
	)
	if _, err := NewGraph(trace, "svcA", "opA", nil); err == nil {
		t.Fatal("expected error for span referencing an unknown processID")
	}
}

// TestNewGraph_AllFixtures mirrors what the Python CLI does in conformance
// mode for every committed fixture: derive the (service, operation) root and
// build the graph with rootTrace semantics. Every fixture must construct.
func TestNewGraph_AllFixtures(t *testing.T) {
	for _, path := range discoverFixtures(t) {
		path := path
		t.Run(filepath.Base(path), func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("reading %s: %v", path, err)
			}
			trace, err := jaeger.Decode(data)
			if err != nil {
				t.Fatalf("decoding %s: %v", path, err)
			}
			service, operation, err := DeriveRootSpan(trace)
			if err != nil {
				t.Fatalf("DeriveRootSpan(%s): %v", path, err)
			}
			g, err := NewGraph(trace, service, operation, nil)
			if err != nil {
				t.Fatalf("NewGraph(%s): %v", path, err)
			}
			if g.RootNode == nil {
				t.Fatalf("no root for %s", path)
			}
			// Every node reachable from the root must fit within its
			// parent's timeline unless it is a client-server case-0 child.
			assertSanitized(t, g, g.RootNode)
		})
	}
}

func assertSanitized(t *testing.T, g *Graph, n *Node) {
	t.Helper()
	for _, c := range n.Children {
		fits := c.StartTime >= n.StartTime && c.EndTime <= n.EndTime
		if !fits && c.SpanKind != SpanKindServer {
			t.Errorf("node %s [%d,%d] escapes parent %s [%d,%d] without server kind",
				c.SID, c.StartTime, c.EndTime, n.SID, n.StartTime, n.EndTime)
		}
		assertSanitized(t, g, c)
	}
}
