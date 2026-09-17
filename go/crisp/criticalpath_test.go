package crisp

import (
	"errors"
	"testing"

	"github.com/uber-research/CRISP/go/crisp/jaeger"
)

func cpIDs(t *testing.T, g *Graph) []string {
	t.Helper()
	cp, err := g.FindCriticalPath(nil)
	if err != nil {
		t.Fatalf("FindCriticalPath: %v", err)
	}
	ids := make([]string, 0, len(cp))
	for _, n := range cp {
		ids = append(ids, n.SID)
	}
	return ids
}

func equalIDs(got []string, want ...string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func TestCriticalPath_LinearChain(t *testing.T) {
	trace := mkTrace(
		map[string]jaeger.Process{"p1": mkProc("svcA")},
		mkSpan("R", "opA", "p1", 0, 100, nil),
		mkSpan("A", "opB", "p1", 10, 80, []jaeger.Reference{childOf("R")}),
		mkSpan("B", "opC", "p1", 20, 50, []jaeger.Reference{childOf("A")}),
	)
	g, err := NewGraph(trace, "svcA", "opA", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := cpIDs(t, g); !equalIDs(got, "R", "A", "B") {
		t.Errorf("CP = %v, want [R A B]", got)
	}
}

func TestCriticalPath_SequentialSiblings(t *testing.T) {
	// Two non-overlapping children: both are on the critical path, the
	// later-finishing one first.
	trace := mkTrace(
		map[string]jaeger.Process{"p1": mkProc("svcA")},
		mkSpan("R", "opA", "p1", 0, 1000, nil),
		mkSpan("A", "opB", "p1", 0, 400, []jaeger.Reference{childOf("R")}),   // [0,400]
		mkSpan("B", "opC", "p1", 500, 500, []jaeger.Reference{childOf("R")}), // [500,1000]
	)
	g, err := NewGraph(trace, "svcA", "opA", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := cpIDs(t, g); !equalIDs(got, "R", "B", "A") {
		t.Errorf("CP = %v, want [R B A]", got)
	}
}

func TestCriticalPath_ConcurrentSiblingExcluded(t *testing.T) {
	// A overlaps B well beyond the 1% allowance: A is concurrent work, not
	// on the critical path.
	trace := mkTrace(
		map[string]jaeger.Process{"p1": mkProc("svcA")},
		mkSpan("R", "opA", "p1", 0, 1000, nil),
		mkSpan("A", "opB", "p1", 0, 600, []jaeger.Reference{childOf("R")}),   // [0,600]
		mkSpan("B", "opC", "p1", 500, 500, []jaeger.Reference{childOf("R")}), // [500,1000]
	)
	g, err := NewGraph(trace, "svcA", "opA", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := cpIDs(t, g); !equalIDs(got, "R", "B") {
		t.Errorf("CP = %v, want [R B]", got)
	}
}

func TestCriticalPath_SkewedSiblingWithinAllowance(t *testing.T) {
	// A ends 5us after B starts: within the 1% allowance (10us of the
	// parent's 1000us), and exactly two sync events in the window, so A is
	// treated as happens-before B and joins the path.
	trace := mkTrace(
		map[string]jaeger.Process{"p1": mkProc("svcA")},
		mkSpan("R", "opA", "p1", 0, 1000, nil),
		mkSpan("A", "opB", "p1", 0, 505, []jaeger.Reference{childOf("R")}),   // [0,505]
		mkSpan("B", "opC", "p1", 500, 500, []jaeger.Reference{childOf("R")}), // [500,1000]
	)
	g, err := NewGraph(trace, "svcA", "opA", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := cpIDs(t, g); !equalIDs(got, "R", "B", "A") {
		t.Errorf("CP = %v, want [R B A]", got)
	}
}

func TestCriticalPath_ThirdSyncEventBlocks(t *testing.T) {
	// Same skewed pair, but a third sibling starts inside the overlap
	// window: three sync events in the window means real concurrency, so A
	// is excluded. (C itself is excluded on the allowance check.)
	trace := mkTrace(
		map[string]jaeger.Process{"p1": mkProc("svcA")},
		mkSpan("R", "opA", "p1", 0, 1000, nil),
		mkSpan("A", "opB", "p1", 0, 505, []jaeger.Reference{childOf("R")}),   // [0,505]
		mkSpan("B", "opC", "p1", 500, 500, []jaeger.Reference{childOf("R")}), // [500,1000]
		mkSpan("C", "opD", "p1", 502, 8, []jaeger.Reference{childOf("R")}),   // [502,510]
	)
	g, err := NewGraph(trace, "svcA", "opA", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := cpIDs(t, g); !equalIDs(got, "R", "B") {
		t.Errorf("CP = %v, want [R B]", got)
	}
}

func TestCriticalPath_OverlapAllowanceBoundary(t *testing.T) {
	// Overlap exactly equal to the allowance is NOT within it (strict <).
	trace := mkTrace(
		map[string]jaeger.Process{"p1": mkProc("svcA")},
		mkSpan("R", "opA", "p1", 0, 1000, nil),
		mkSpan("A", "opB", "p1", 0, 510, []jaeger.Reference{childOf("R")}),   // [0,510]: overlap 10 == 1% of 1000
		mkSpan("B", "opC", "p1", 500, 500, []jaeger.Reference{childOf("R")}), // [500,1000]
	)
	g, err := NewGraph(trace, "svcA", "opA", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := cpIDs(t, g); !equalIDs(got, "R", "B") {
		t.Errorf("CP = %v, want [R B] (boundary overlap is not within allowance)", got)
	}
}

func TestCriticalPath_EndTimeTieReverseDocumentOrder(t *testing.T) {
	// X and Y tie on end time. Python's stable-sort-then-reverse visits the
	// LATER document child first, making Y (declared after X) the
	// last-finishing-child pivot; X then fails the overlap preconditions
	// against Y and is excluded along with its subtree.
	trace := mkTrace(
		map[string]jaeger.Process{"p1": mkProc("svcA")},
		mkSpan("R", "opA", "p1", 0, 1000, nil),
		mkSpan("X", "opX", "p1", 500, 500, []jaeger.Reference{childOf("R")}), // [500,1000]
		mkSpan("Y", "opY", "p1", 600, 400, []jaeger.Reference{childOf("R")}), // [600,1000]
		mkSpan("XC", "opC", "p1", 500, 200, []jaeger.Reference{childOf("X")}),
		mkSpan("YC", "opC", "p1", 600, 200, []jaeger.Reference{childOf("Y")}),
	)
	g, err := NewGraph(trace, "svcA", "opA", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := cpIDs(t, g); !equalIDs(got, "R", "Y", "YC") {
		t.Errorf("CP = %v, want [R Y YC] (tie broken against X)", got)
	}
}

func TestCriticalPath_NestedSubPaths(t *testing.T) {
	// The last finisher's sub-path is spliced in before earlier siblings.
	trace := mkTrace(
		map[string]jaeger.Process{"p1": mkProc("svcA")},
		mkSpan("R", "opA", "p1", 0, 1000, nil),
		mkSpan("A", "opB", "p1", 0, 400, []jaeger.Reference{childOf("R")}),
		mkSpan("B", "opC", "p1", 500, 500, []jaeger.Reference{childOf("R")}),
		mkSpan("B1", "opD", "p1", 900, 100, []jaeger.Reference{childOf("B")}), // [900,1000]
		mkSpan("B2", "opD", "p1", 500, 300, []jaeger.Reference{childOf("B")}), // [500,800]
	)
	g, err := NewGraph(trace, "svcA", "opA", nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := cpIDs(t, g); !equalIDs(got, "R", "B", "B1", "B2", "A") {
		t.Errorf("CP = %v, want [R B B1 B2 A]", got)
	}
}

func TestHappensBefore_ZeroDurationParent(t *testing.T) {
	trace := mkTrace(
		map[string]jaeger.Process{"p1": mkProc("svcA")},
		mkSpan("R", "opA", "p1", 0, 100, nil),
	)
	g, err := NewGraph(trace, "svcA", "opA", nil)
	if err != nil {
		t.Fatal(err)
	}
	parent := &Node{SID: "Z", StartTime: 100, Duration: 0, EndTime: 100}
	before := &Node{SID: "A", StartTime: 0, Duration: 5, EndTime: 5}
	later := &Node{SID: "B", StartTime: 3, Duration: 7, EndTime: 10}
	_, err = g.happensBefore(parent, []*Node{before, later}, before, later)
	var zde *ZeroDurationError
	if !errors.As(err, &zde) {
		t.Errorf("err = %v, want ZeroDurationError", err)
	}
}

func TestHappensBeforeSimple(t *testing.T) {
	g := &Graph{}
	a := &Node{EndTime: 10}
	b := &Node{StartTime: 10}
	if !g.happensBeforeSimple(a, b) {
		t.Error("endTime == startTime should be happens-before (<=)")
	}
	b.StartTime = 9
	if g.happensBeforeSimple(a, b) {
		t.Error("endTime > startTime should not be happens-before")
	}
}

func TestNumSyncEventsInWindowInclusive(t *testing.T) {
	g := &Graph{}
	children := []*Node{
		{SID: "A", StartTime: 0, EndTime: 5},
		{SID: "B", StartTime: 3, EndTime: 10},
		{SID: "C", StartTime: 20, EndTime: 30},
	}
	// Window [3,5]: A.end(5) and B.start(3) are inside; inclusive bounds.
	if got := g.numSyncEventsInWindowInclusive(children, 3, 5); got != 2 {
		t.Errorf("nEvt = %d, want 2", got)
	}
	// Window [0,30]: all six events.
	if got := g.numSyncEventsInWindowInclusive(children, 0, 30); got != 6 {
		t.Errorf("nEvt = %d, want 6", got)
	}
}
