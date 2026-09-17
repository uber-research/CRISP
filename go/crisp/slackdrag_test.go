package crisp

import (
	"math"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/uber-research/CRISP/go/crisp/jaeger"
)

// buildDragGraph builds root R [0,100) with children A [0,80) and B [10,60).
// The critical path is R -> A (A ends later).
func buildDragGraph(t *testing.T) *Graph {
	t.Helper()
	trace := mkTrace(
		map[string]jaeger.Process{"p1": mkProc("S")},
		mkSpan("R", "OR", "p1", 0, 100, nil, kindTag("server")),
		mkSpan("A", "OA", "p1", 0, 80, []jaeger.Reference{childOf("R")}),
		mkSpan("B", "OB", "p1", 10, 50, []jaeger.Reference{childOf("R")}),
	)
	g, err := NewGraph(trace, "S", "OR", &GraphOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func TestCalculateDrag(t *testing.T) {
	g := buildDragGraph(t)
	cp, err := g.FindCriticalPath(nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := cpIDs(t, g); !equalIDs(got, "R", "A") {
		t.Fatalf("cp = %v, want [R A]", got)
	}

	drag := g.CalculateDrag(cp, false)
	// Root: no parent -> full duration.
	if drag.PerSpan["R"] != 100 {
		t.Errorf("R drag = %v, want 100", drag.PerSpan["R"])
	}
	// A: siblings sorted by endTime desc = [A(80), B(60)]; A is not the
	// earliest, so capped by the gap to the next sibling: 80-60 = 20.
	if drag.PerSpan["A"] != 20 {
		t.Errorf("A drag = %v, want 20", drag.PerSpan["A"])
	}
	// B is not on the CP: no entry (Python omits the key).
	if _, ok := drag.PerSpan["B"]; ok {
		t.Errorf("B should have no drag entry")
	}
	if drag.Total != 120 {
		t.Errorf("total = %v, want 120", drag.Total)
	}
}

func TestCalculateDragSingleChildUncapped(t *testing.T) {
	trace := mkTrace(
		map[string]jaeger.Process{"p1": mkProc("S")},
		mkSpan("R", "OR", "p1", 0, 100, nil, kindTag("server")),
		mkSpan("A", "OA", "p1", 10, 50, []jaeger.Reference{childOf("R")}),
	)
	g, err := NewGraph(trace, "S", "OR", &GraphOptions{})
	if err != nil {
		t.Fatal(err)
	}
	cp, err := g.FindCriticalPath(nil)
	if err != nil {
		t.Fatal(err)
	}
	drag := g.CalculateDrag(cp, false)
	// Single child: no sibling to compete with -> full duration.
	if drag.PerSpan["A"] != 50 {
		t.Errorf("A drag = %v, want 50", drag.PerSpan["A"])
	}
}

func TestCalculateDragEarliestSiblingUncapped(t *testing.T) {
	// B ends earliest; if B were on the CP it would be uncapped. Construct
	// a CP through the earliest-ending sibling by making it the only child
	// of a second root child... simpler: two siblings where the CP child
	// is the earliest-ending because the later-ending one is off-CP is not
	// possible (CP picks latest-ending). Instead verify via three siblings:
	// the earliest-ending of the sorted order is uncapped.
	trace := mkTrace(
		map[string]jaeger.Process{"p1": mkProc("S")},
		mkSpan("R", "OR", "p1", 0, 100, nil, kindTag("server")),
		mkSpan("A", "OA", "p1", 0, 90, []jaeger.Reference{childOf("R")}),
		mkSpan("B", "OB", "p1", 0, 70, []jaeger.Reference{childOf("R")}),
		mkSpan("C", "OC", "p1", 0, 50, []jaeger.Reference{childOf("R")}),
	)
	g, err := NewGraph(trace, "S", "OR", &GraphOptions{})
	if err != nil {
		t.Fatal(err)
	}
	cp, err := g.FindCriticalPath(nil)
	if err != nil {
		t.Fatal(err)
	}
	// CP = [R, A] (A ends last). A's drag: gap to next sibling B = 90-70=20.
	drag := g.CalculateDrag(cp, false)
	if drag.PerSpan["A"] != 20 {
		t.Errorf("A drag = %v, want 20", drag.PerSpan["A"])
	}
	// Sanity: B and C off CP -> no entries.
	if len(drag.PerSpan) != 2 {
		t.Errorf("entries = %v, want exactly R and A", drag.PerSpan)
	}
}

func TestCalculateDragExclusive(t *testing.T) {
	// R [0,100) -> A [0,80) -> C [0,60); A has sibling B [10,60).
	trace := mkTrace(
		map[string]jaeger.Process{"p1": mkProc("S")},
		mkSpan("R", "OR", "p1", 0, 100, nil, kindTag("server")),
		mkSpan("A", "OA", "p1", 0, 80, []jaeger.Reference{childOf("R")}),
		mkSpan("B", "OB", "p1", 10, 50, []jaeger.Reference{childOf("R")}),
		mkSpan("C", "OC", "p1", 0, 60, []jaeger.Reference{childOf("A")}),
	)
	g, err := NewGraph(trace, "S", "OR", &GraphOptions{})
	if err != nil {
		t.Fatal(err)
	}
	cp, err := g.FindCriticalPath(nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := cpIDs(t, g); !equalIDs(got, "R", "A", "C") {
		t.Fatalf("cp = %v, want [R A C]", got)
	}

	drag := g.CalculateDrag(cp, true)
	// Exclusive CP times: C=60, A=80-60=20, R=100-80=20.
	// C: single child of A -> uncapped: own exclusive = 60.
	if drag.PerSpan["C"] != 60 {
		t.Errorf("C drag = %v, want 60", drag.PerSpan["C"])
	}
	// A: siblings [A(80), B(60)]; next=B(60). Exclusive branch with own CP
	// child C(60): dragVal = 80 - max(60, 60) = 20; ownCPChild.start(0) >
	// nextSibling.end(60)? No. drag = min(own exclusive 20, 20) = 20.
	if drag.PerSpan["A"] != 20 {
		t.Errorf("A drag = %v, want 20", drag.PerSpan["A"])
	}
	// R: root -> own exclusive = 20.
	if drag.PerSpan["R"] != 20 {
		t.Errorf("R drag = %v, want 20", drag.PerSpan["R"])
	}
}

func TestPyFloatRepr(t *testing.T) {
	// All expectations from CPython 3.11 repr().
	cases := []struct {
		in   float64
		want string
	}{
		{0.0, "0.0"},
		{math.Copysign(0, -1), "-0.0"},
		{100.0, "100.0"},
		{33.333333333333336, "33.333333333333336"},
		{1e-05, "1e-05"},
		{0.0001, "0.0001"},
		{1e16, "1e+16"},
		{1e15, "1000000000000000.0"},
		{9999999999999998.0, "9999999999999998.0"},
		{2.5, "2.5"},
		{7.0, "7.0"},
		{0.1, "0.1"},
		{123456.789, "123456.789"},
		{3.0, "3.0"},
		{0.5, "0.5"},
		{-2.5, "-2.5"},
	}
	for _, c := range cases {
		if got := pyFloatRepr(c.in); got != c.want {
			t.Errorf("pyFloatRepr(%v) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestAggregateAndMergeSlackDrag(t *testing.T) {
	g := buildDragGraph(t)
	cp, err := g.FindCriticalPath(nil)
	if err != nil {
		t.Fatal(err)
	}
	drag := g.CalculateDrag(cp, false)
	agg := g.AggregateDragSlackByCallpath(drag)

	// Three nodes, three distinct call paths; insertion order follows
	// document order (R, A, B spans).
	wantOrder := []string{"[S] OR", "[S] OR->[S] OA", "[S] OR->[S] OB"}
	if !reflect.DeepEqual(agg.Order, wantOrder) {
		t.Errorf("Order = %v, want %v", agg.Order, wantOrder)
	}
	if agg.ByPath["[S] OR"].TotalDrag != 100 || agg.ByPath["[S] OR"].SpanCount != 1 {
		t.Errorf("root agg = %+v", agg.ByPath["[S] OR"])
	}
	if agg.ByPath["[S] OR->[S] OA"].AvgDrag != 20 {
		t.Errorf("A agg = %+v", agg.ByPath["[S] OR->[S] OA"])
	}
	// B is off-CP: drag 0.
	if agg.ByPath["[S] OR->[S] OB"].TotalDrag != 0 {
		t.Errorf("B agg = %+v", agg.ByPath["[S] OR->[S] OB"])
	}

	// Merge two copies: span counts and totals double; averages unchanged.
	merged := MergePerMethodSlackDrag([]*SlackDragByCallpath{agg, agg})
	r := merged.ByPath["[S] OR"]
	if r.SpanCount != 2 || r.TotalDrag != 200 || r.AvgDrag != 100 {
		t.Errorf("merged root = %+v, want count=2 total=200 avg=100", r)
	}
}

func TestGenSlackDragCSV(t *testing.T) {
	agg := newSlackDragByCallpath()
	agg.sumEntry("[S1] O1", 2, 100.0, 0.0) // avg 50
	agg.sumEntry("[S1] O1->[S2] O2", 1, 33.0, 0.0)
	agg.sumEntry("[S1] O1", 1, 50.0, 0.0) // totals: count 3, drag 150, avg 50
	agg.deriveAverages()

	dir := t.TempDir()
	path, err := GenSlackDragCSV(agg, dir, "")
	if err != nil {
		t.Fatal(err)
	}
	if path != filepath.Join(dir, SlackDragCSV) {
		t.Errorf("path = %s", path)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	// Sorted by avgDrag desc: [S1] O1 (50) before child (33).
	want := "callPath,spanCount,avgDrag,totalDrag,avgSlack,totalSlack\n" +
		"[S1] O1,3,50.0,150.0,0.0,0.0\n" +
		"[S1] O1->[S2] O2,1,33.0,33.0,0.0,0.0\n"
	if string(data) != want {
		t.Errorf("got:\n%s\nwant:\n%s", data, want)
	}
}

func TestGenSlackDragCSVQuoting(t *testing.T) {
	agg := newSlackDragByCallpath()
	agg.sumEntry("[S] a,b", 1, 1.0, 0.0)
	agg.sumEntry("[S] q\"x", 1, 2.0, 0.0)
	agg.sumEntry(" plain", 1, 3.0, 0.0)
	agg.deriveAverages()

	dir := t.TempDir()
	path, err := GenSlackDragCSV(agg, dir, "")
	if err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	// Python csv QUOTE_MINIMAL: comma/quote quoted, leading space NOT.
	want := "callPath,spanCount,avgDrag,totalDrag,avgSlack,totalSlack\n" +
		" plain,1,3.0,3.0,0.0,0.0\n" +
		"\"[S] q\"\"x\",1,2.0,2.0,0.0,0.0\n" +
		"\"[S] a,b\",1,1.0,1.0,0.0,0.0\n"
	if string(data) != want {
		t.Errorf("got:\n%s\nwant:\n%s", data, want)
	}
}

func TestGenSlackDragCSVEmpty(t *testing.T) {
	dir := t.TempDir()
	path, err := GenSlackDragCSV(newSlackDragByCallpath(), dir, "")
	if err != nil {
		t.Fatal(err)
	}
	if path != "" {
		t.Errorf("path = %q, want empty (no file written)", path)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Errorf("dir not empty: %v", entries)
	}
}
