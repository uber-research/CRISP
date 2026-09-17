package crisp

import (
	"testing"

	"github.com/uber-research/CRISP/go/crisp/jaeger"
)

func TestFloorDiv(t *testing.T) {
	// Python // floors toward -inf; Go / truncates toward zero.
	cases := []struct{ a, b, want int64 }{
		{7, 2, 3},
		{-7, 2, -4}, // Python: -7 // 2 == -4
		{7, -2, -4},
		{-7, -2, 3},
		{6, 3, 2},
		{0, 5, 0},
	}
	for _, c := range cases {
		if got := floorDiv(c.a, c.b); got != c.want {
			t.Errorf("floorDiv(%d, %d) = %d, want %d", c.a, c.b, got, c.want)
		}
	}
}

func TestMetricValsAdd(t *testing.T) {
	a := NewMetricVals(10, 5, 1, "s1")
	b := NewMetricVals(20, 3, 2, "s2")
	a.Add(b)
	if a.Inc != 30 || a.Excl != 8 || a.Freq != 3 {
		t.Errorf("sums = (%d, %d, %d), want (30, 8, 3)", a.Inc, a.Excl, a.Freq)
	}
	// Worst-case exemplars: b.incExVal=20 > 10, b.exclExVal=3 < 5.
	if a.IncEx != "s2" || a.IncExVal != 20 {
		t.Errorf("IncEx = (%s, %d), want (s2, 20)", a.IncEx, a.IncExVal)
	}
	if a.ExclEx != "s1" || a.ExclExVal != 5 {
		t.Errorf("ExclEx = (%s, %d), want (s1, 5)", a.ExclEx, a.ExclExVal)
	}

	// Strict >: equal values keep the first sid (Python tie behavior).
	c := NewMetricVals(10, 5, 1, "s3")
	c.Add(NewMetricVals(10, 5, 1, "s4"))
	if c.IncEx != "s3" || c.ExclEx != "s3" {
		t.Errorf("tie: got (%s, %s), want (s3, s3)", c.IncEx, c.ExclEx)
	}
}

func TestMetricValsFloorDiv(t *testing.T) {
	m := NewMetricVals(-7, 7, 3, "s1")
	m.FloorDiv(2)
	if m.Inc != -4 || m.Excl != 3 || m.Freq != 1 {
		t.Errorf("got (%d, %d, %d), want (-4, 3, 1)", m.Inc, m.Excl, m.Freq)
	}
}

func TestCallPathProfileUpsertOrder(t *testing.T) {
	cpp := NewCallPathProfile(1, "t1")
	cpp.Upsert("b", NewMetricVals(1, 1, 1, "s1"))
	cpp.Upsert("a", NewMetricVals(2, 2, 1, "s2"))
	cpp.Upsert("b", NewMetricVals(3, 3, 1, "s3")) // merges, keeps position
	if len(cpp.Order) != 2 || cpp.Order[0] != "b" || cpp.Order[1] != "a" {
		t.Errorf("Order = %v, want [b a]", cpp.Order)
	}
	if cpp.Profile["b"].Inc != 4 {
		t.Errorf("b.Inc = %d, want 4", cpp.Profile["b"].Inc)
	}
	// Insert is a copy: mutating the source metric must not affect the profile.
	src := NewMetricVals(5, 5, 1, "s4")
	cpp.Upsert("c", src)
	src.Inc = 99
	if cpp.Profile["c"].Inc != 5 {
		t.Errorf("c.Inc = %d, want 5 (insert must copy)", cpp.Profile["c"].Inc)
	}
}

func TestCallPathProfileAdd(t *testing.T) {
	a := NewCallPathProfile(1, "t1")
	a.Upsert("x", NewMetricVals(10, 10, 1, "s1"))
	b := NewCallPathProfile(1, "t2")
	b.Upsert("y", NewMetricVals(20, 20, 1, "s2"))
	b.Upsert("x", NewMetricVals(5, 5, 1, "s3"))
	a.Add(b)
	if a.Count != 2 {
		t.Errorf("Count = %d, want 2", a.Count)
	}
	if len(a.Order) != 2 || a.Order[0] != "x" || a.Order[1] != "y" {
		t.Errorf("Order = %v, want [x y]", a.Order)
	}
	if a.Profile["x"].Inc != 15 {
		t.Errorf("x.Inc = %d, want 15", a.Profile["x"].Inc)
	}
}

func TestCallPathProfileNormalizeAndSanitize(t *testing.T) {
	cpp := NewCallPathProfile(2, "")
	cpp.Upsert("p", NewMetricVals(10, 7, 2, "s1"))
	cpp.Upsert("q", NewMetricVals(4, -3, 0, "s2"))
	cpp.SanitizeExcl()
	if cpp.Profile["q"].Excl != 0 {
		t.Errorf("q.Excl = %d, want 0 after sanitize", cpp.Profile["q"].Excl)
	}
	cpp.NormalizeExcl()
	if cpp.Profile["p"].Excl != 3 { // 7 // 2
		t.Errorf("p.Excl = %d, want 3", cpp.Profile["p"].Excl)
	}
	// Inc/Freq untouched by NormalizeExcl.
	if cpp.Profile["p"].Inc != 10 || cpp.Profile["p"].Freq != 2 {
		t.Errorf("p = (%d, %d), want (10, 2)", cpp.Profile["p"].Inc, cpp.Profile["p"].Freq)
	}
}

// buildAccumeGraph builds: root R (svc S, op OR) [0,100) with children A
// (op OA) [10,60) and B (op OB) [60,90); A has child C (op OC) [20,40).
func buildAccumeGraph(t *testing.T) *Graph {
	t.Helper()
	trace := mkTrace(
		map[string]jaeger.Process{"p1": mkProc("S")},
		mkSpan("R", "OR", "p1", 0, 100, nil, kindTag("server")),
		mkSpan("A", "OA", "p1", 10, 50, []jaeger.Reference{childOf("R")}),
		mkSpan("C", "OC", "p1", 20, 20, []jaeger.Reference{childOf("A")}),
		mkSpan("B", "OB", "p1", 60, 30, []jaeger.Reference{childOf("R")}),
	)
	g, err := NewGraph(trace, "S", "OR", &GraphOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if g.RootNode == nil {
		t.Fatal("no root")
	}
	return g
}

func TestAccumeCPMetrics(t *testing.T) {
	g := buildAccumeGraph(t)
	// Critical path: R, then children by end time desc: B (90) before A (60);
	// B is a leaf; A's subtree adds C. CP = [R, B, A, C].
	cp, err := g.FindCriticalPath(nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := cpIDs(t, g); !equalIDs(got, "R", "B", "A", "C") {
		t.Fatalf("cp = %v, want [R B A C]", got)
	}
	_ = cp

	cpp, sidExcl := g.AccumeCPMetrics(cp, "trace1", nil)

	// Walk is reversed: C, A, B, R. Insertion order of profile keys:
	// C: "[S] OR->[S] OA->[S] OC" then parent "[S] OR->[S] OA"
	// A: "[S] OR->[S] OA" exists; parent "[S] OR"
	// B: "[S] OR->[S] OB" then parent "[S] OR" exists
	// R: "[S] OR" exists
	wantOrder := []string{"[S] OR->[S] OA->[S] OC", "[S] OR->[S] OA", "[S] OR", "[S] OR->[S] OB"}
	if len(cpp.Order) != len(wantOrder) {
		t.Fatalf("Order = %v, want %v", cpp.Order, wantOrder)
	}
	for i, k := range wantOrder {
		if cpp.Order[i] != k {
			t.Fatalf("Order[%d] = %q, want %q (full %v)", i, cpp.Order[i], k, cpp.Order)
		}
	}

	// [S] OR: inc=100 (R), excl=100 -50(A) -30(B) = 20, freq=1.
	r := cpp.Profile["[S] OR"]
	if r.Inc != 100 || r.Excl != 20 || r.Freq != 1 {
		t.Errorf("root = (%d, %d, %d), want (100, 20, 1)", r.Inc, r.Excl, r.Freq)
	}
	// [S] OR->[S] OA: inc=50, excl=50-20=30, freq=1.
	a := cpp.Profile["[S] OR->[S] OA"]
	if a.Inc != 50 || a.Excl != 30 || a.Freq != 1 {
		t.Errorf("A = (%d, %d, %d), want (50, 30, 1)", a.Inc, a.Excl, a.Freq)
	}
	// Leaves: inc == excl.
	c := cpp.Profile["[S] OR->[S] OA->[S] OC"]
	if c.Inc != 20 || c.Excl != 20 || c.Freq != 1 {
		t.Errorf("C = (%d, %d, %d), want (20, 20, 1)", c.Inc, c.Excl, c.Freq)
	}
	b := cpp.Profile["[S] OR->[S] OB"]
	if b.Inc != 30 || b.Excl != 30 || b.Freq != 1 {
		t.Errorf("B = (%d, %d, %d), want (30, 30, 1)", b.Inc, b.Excl, b.Freq)
	}
	// Exemplar sids: each path's own node sid.
	if r.ExclEx != "R" || a.ExclEx != "A" || c.ExclEx != "C" || b.ExclEx != "B" {
		t.Errorf("exemplar sids = (%s, %s, %s, %s), want (R, A, C, B)", r.ExclEx, a.ExclEx, c.ExclEx, b.ExclEx)
	}

	// Per-span exclusive times.
	if sidExcl["R"] != 20 || sidExcl["A"] != 30 || sidExcl["C"] != 20 || sidExcl["B"] != 30 {
		t.Errorf("sidExcl = %v, want {R:20 A:30 C:20 B:30}", sidExcl)
	}
}

func TestAccumeCPMetricsSanitizesNegativeExcl(t *testing.T) {
	// Child duration exceeds parent's (unsanitized hand-built tree): the
	// parent's exclusive time goes negative and is clamped to 0.
	trace := mkTrace(
		map[string]jaeger.Process{"p1": mkProc("S")},
		mkSpan("R", "OR", "p1", 0, 10, nil, kindTag("server")),
	)
	g, err := NewGraph(trace, "S", "OR", &GraphOptions{})
	if err != nil {
		t.Fatal(err)
	}
	root := g.RootNode
	child := &Node{SID: "K", StartTime: 0, Duration: 50, EndTime: 50, OpName: "OK", ProcessID: "p1"}
	child.setParent(root)
	root.addChild(child)
	cpp, sidExcl := g.AccumeCPMetrics([]*Node{root, child}, "t", root)
	if got := cpp.Profile["[S] OR"].Excl; got != 0 {
		t.Errorf("root excl = %d, want 0 (sanitized)", got)
	}
	if sidExcl["R"] != 0 {
		t.Errorf("sidExcl[R] = %d, want 0 (sanitized)", sidExcl["R"])
	}
}
