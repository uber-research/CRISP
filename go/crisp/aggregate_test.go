package crisp

import (
	"reflect"
	"testing"
)

func TestGetParentCallPath(t *testing.T) {
	cases := []struct{ in, want string }{
		{"a", ""},
		{"a->b", "a"},
		{"a->b->c", "a->b"},
		{"a->", "a"},
	}
	for _, c := range cases {
		if got := GetParentCallPath(c.in); got != c.want {
			t.Errorf("GetParentCallPath(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestAggregateCallPathProfiles(t *testing.T) {
	// Two traces sharing a root path; check merge, averaging, and emission.
	a := NewCallPathProfile(1, "t1")
	a.Upsert("[S] R", NewMetricVals(100, 20, 1, "s1"))
	a.Upsert("[S] R->[S] A", NewMetricVals(80, 80, 1, "s2"))
	b := NewCallPathProfile(1, "t2")
	b.Upsert("[S] R", NewMetricVals(50, 11, 1, "s3"))
	b.Upsert("[S] R->[S] B", NewMetricVals(39, 39, 1, "s4"))

	out, err := AggregateCallPathProfiles([]*CallPathProfile{a, b})
	if err != nil {
		t.Fatal(err)
	}
	// [S] R: excl (20+11)//2 = 15, freq 2; A: 80//2=40; B: 39//2=19.
	// Insertion order: [S] R, [S] R->[S] A, [S] R->[S] B.
	want := "[S] R 15 <<2>>\n[S] R;[S] A 40 <<1>>\n[S] R;[S] B 19 <<1>>\n"
	if out != want {
		t.Errorf("got %q, want %q", out, want)
	}
}

func TestAggregateCallPathProfilesLeafZeroFixup(t *testing.T) {
	// A leaf path with zero exclusive time is bumped to 1; a zero-value
	// PARENT path is left alone.
	a := NewCallPathProfile(1, "t1")
	a.Upsert("[S] R", NewMetricVals(10, 0, 1, "s1"))
	a.Upsert("[S] R->[S] A", NewMetricVals(10, 0, 1, "s2"))
	out, err := AggregateCallPathProfiles([]*CallPathProfile{a})
	if err != nil {
		t.Fatal(err)
	}
	want := "[S] R 0 <<1>>\n[S] R;[S] A 1 <<1>>\n"
	if out != want {
		t.Errorf("got %q, want %q", out, want)
	}
}

func TestAggregateCallPathProfilesKeyReplacementOrder(t *testing.T) {
	// ";" in op names becomes "_" before "->" becomes ";", so a ";" produced
	// by the arrow replacement must not be re-replaced.
	a := NewCallPathProfile(1, "t1")
	a.Upsert("[S] o;p->[S] x", NewMetricVals(5, 5, 1, "s1"))
	out, err := AggregateCallPathProfiles([]*CallPathProfile{a})
	if err != nil {
		t.Fatal(err)
	}
	want := "[S] o_p;[S] x 5 <<1>>\n"
	if out != want {
		t.Errorf("got %q, want %q", out, want)
	}
}

func TestAggregateCallPathProfilesZeroCount(t *testing.T) {
	if _, err := AggregateCallPathProfiles(nil); err == nil {
		t.Error("expected error for zero count, got nil")
	}
}

func TestExemplarHeapMatchesPython(t *testing.T) {
	// The heap-array order leaks through the final stable sort for equal
	// values, so the heap operations must replicate CPython heapq exactly.
	// Sequence (max 2): (5,a), (5,b), (6,c). Python: push 5a -> [5a];
	// push 5b -> [5a,5b]; 6c > 5a -> heapreplace -> [5b,6c].
	// sorted desc by val (stable) -> [6c,5b]. A naive top-N by
	// (value desc, arrival asc) would give [6c,5a] instead.
	var h []exemplarEntry
	push := func(val int64, tid, sid string, max int) {
		e := exemplarEntry{val, tid, sid}
		if len(h) < max {
			h = heapPush(h, e)
		} else if e.val > h[0].val {
			heapReplace(h, e)
		}
	}
	push(5, "t", "a", 2)
	push(5, "t", "b", 2)
	push(6, "t", "c", 2)
	if len(h) != 2 || h[0].spanID != "b" || h[1].spanID != "c" {
		t.Fatalf("heap = %v, want [(5,b) (6,c)]", h)
	}

	// Larger mixed sequence cross-checked against CPython 3.11 heapq:
	// push (3,a),(1,b),(4,c); (1,d) rejected (not > heap min 1);
	// (5,e) replaces -> [3a,5e,4c]; (2,f) rejected; (6,g) replaces ->
	// [(4,'t','c'), (5,'t','e'), (6,'t','g')].
	h = nil
	for _, e := range []exemplarEntry{
		{3, "t", "a"}, {1, "t", "b"}, {4, "t", "c"}, {1, "t", "d"},
		{5, "t", "e"}, {2, "t", "f"}, {6, "t", "g"},
	} {
		if len(h) < 3 {
			h = heapPush(h, e)
		} else if e.val > h[0].val {
			heapReplace(h, e)
		}
	}
	got := []string{h[0].spanID, h[1].spanID, h[2].spanID}
	if !reflect.DeepEqual(got, []string{"c", "e", "g"}) {
		t.Errorf("heap array = %v, want [c e g] (CPython heapq order)", got)
	}
}

func TestMergeCallPathProfilesWithExemplars(t *testing.T) {
	mk := func(traceID string, entries ...struct {
		path   string
		metric *MetricVals
	}) *TraceMetrics {
		cpp := NewCallPathProfile(1, traceID)
		for _, e := range entries {
			cpp.Upsert(e.path, e.metric)
		}
		return &TraceMetrics{TraceID: traceID, CPMetrics: cpp}
	}
	type ent = struct {
		path   string
		metric *MetricVals
	}

	m1 := mk("t1",
		ent{"[S] R", NewMetricVals(100, 20, 1, "s1")},
		ent{"[S] R->[S] A", NewMetricVals(80, 80, 1, "s2")},
	)
	m2 := mk("t2",
		ent{"[S] R", NewMetricVals(50, 30, 1, "s3")},
		ent{"[S] R->[S] A", NewMetricVals(20, 20, 1, "s4")},
	)

	merged := MergeCallPathProfilesWithExemplars([]*TraceMetrics{m1, m2}, 3)
	if merged.Count != 2 {
		t.Errorf("Count = %d, want 2", merged.Count)
	}
	r := merged.Profile["[S] R"]
	if r.Inc != 150 || r.Excl != 50 || r.Freq != 2 {
		t.Errorf("R = (%d, %d, %d), want (150, 50, 2)", r.Inc, r.Excl, r.Freq)
	}
	// Worst exclusive for R: t2's 30 > t1's 20.
	if r.ExclEx != "s3" || r.ExclTrace != "t2" {
		t.Errorf("R worst excl = (%s, %s), want (s3, t2)", r.ExclEx, r.ExclTrace)
	}
	// Exemplars sorted by exclusive value desc.
	a := merged.Profile["[S] R->[S] A"]
	want := [][2]string{{"t1", "s2"}, {"t2", "s4"}}
	if !reflect.DeepEqual(a.Exemplars, want) {
		t.Errorf("A exemplars = %v, want %v", a.Exemplars, want)
	}
	// Insertion order preserved across traces.
	if !reflect.DeepEqual(merged.Order, []string{"[S] R", "[S] R->[S] A"}) {
		t.Errorf("Order = %v", merged.Order)
	}
}

func TestMergeCallPathProfilesTopN(t *testing.T) {
	// maxExemplars=2 with three traces: the worst two by exclusive value.
	var ms []*TraceMetrics
	for _, tc := range []struct {
		tid, sid string
		excl     int64
	}{{"t1", "s1", 10}, {"t2", "s2", 50}, {"t3", "s3", 30}} {
		cpp := NewCallPathProfile(1, tc.tid)
		cpp.Upsert("p", NewMetricVals(tc.excl, tc.excl, 1, tc.sid))
		ms = append(ms, &TraceMetrics{TraceID: tc.tid, CPMetrics: cpp})
	}
	merged := MergeCallPathProfilesWithExemplars(ms, 2)
	want := [][2]string{{"t2", "s2"}, {"t3", "s3"}}
	if !reflect.DeepEqual(merged.Profile["p"].Exemplars, want) {
		t.Errorf("exemplars = %v, want %v", merged.Profile["p"].Exemplars, want)
	}
}

func TestMergeCallPathProfilesExemplarsDisabled(t *testing.T) {
	cpp := NewCallPathProfile(1, "t1")
	cpp.Upsert("p", NewMetricVals(10, 10, 1, "s1"))
	merged := MergeCallPathProfilesWithExemplars([]*TraceMetrics{{TraceID: "t1", CPMetrics: cpp}}, 0)
	if merged.Profile["p"].Exemplars != nil {
		t.Errorf("exemplars = %v, want nil when maxExemplars=0", merged.Profile["p"].Exemplars)
	}
}
