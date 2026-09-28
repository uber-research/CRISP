package crisp

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/uber-research/CRISP/go/crisp/jaeger"
)

// segSpan is one span of a single-process trace: [start, start+duration),
// child of parent ("" for a root).
type segSpan struct {
	id, op, parent  string
	start, duration int64
}

func segTrace(t *testing.T, spans ...segSpan) *jaeger.Trace {
	t.Helper()
	parts := make([]string, len(spans))
	for i, s := range spans {
		refs := "[]"
		if s.parent != "" {
			refs = fmt.Sprintf(`[{"refType": "CHILD_OF", "spanID": %q}]`, s.parent)
		}
		parts[i] = fmt.Sprintf(`{"spanID": %q, "operationName": %q, "references": %s, "startTime": %d, "duration": %d, "processID": "P1"}`,
			s.id, s.op, refs, s.start, s.duration)
	}
	return decodeTrace(t, []byte(`{"data": [{"processes": {"P1": {"serviceName": "S1"}}, "spans": [`+strings.Join(parts, ",")+`]}]}`))
}

func segGraph(t *testing.T, trace *jaeger.Trace, service, operation string, rootTrace bool) *Graph {
	t.Helper()
	g, err := NewGraph(trace, service, operation, &GraphOptions{RootTrace: &rootTrace})
	if err != nil {
		t.Fatal(err)
	}
	if g.RootNode == nil {
		t.Fatal("no root node")
	}
	return g
}

func graphSegments(t *testing.T, g *Graph) []CriticalPathSpan {
	t.Helper()
	cp, err := g.FindCriticalPath(nil)
	if err != nil {
		t.Fatal(err)
	}
	return g.CriticalPathSegments(cp)
}

// manualSegGraph builds a graph without sanitization, so children may extend
// outside their parent the way unsanitized client/server spans do. specs are
// (sid, start, duration, parent); the first is the root.
func manualSegGraph(specs ...segSpan) *Graph {
	g := &Graph{ParsedTrace: &ParsedTrace{ProcessName: map[string]string{"P1": "S1"}}, NodeHT: map[string]*Node{}}
	for _, s := range specs {
		n := newNode(s.id, s.start, s.duration, nil, s.id, "P1", SpanKindUnknown, nil, false)
		g.NodeHT[s.id] = &n
		if s.parent != "" {
			n.setParent(g.NodeHT[s.parent])
			g.NodeHT[s.parent].addChild(&n)
		}
	}
	g.RootNode = g.NodeHT[specs[0].id]
	return g
}

// segMicros renders spans as sid -> [[start, end], ...] in microseconds.
func segMicros(spans []CriticalPathSpan) map[string][][2]int64 {
	out := map[string][][2]int64{}
	for _, s := range spans {
		segs := [][2]int64{}
		for _, seg := range s.Segments {
			segs = append(segs, [2]int64{seg.Start.UnixMicro(), seg.End.UnixMicro()})
		}
		out[s.SpanID] = segs
	}
	return out
}

func exclMicros(spans []CriticalPathSpan) map[string]int64 {
	out := map[string]int64{}
	for _, s := range spans {
		out[s.SpanID] = s.Exclusive.Microseconds()
	}
	return out
}

func TestCPSegmentsLongSpanPartlyOnCriticalPath(t *testing.T) {
	spans := graphSegments(t, segGraph(t, segTrace(t,
		segSpan{"R", "root", "", 0, 1000},
		segSpan{"A", "a", "R", 0, 1000},
		segSpan{"B", "b", "A", 100, 200},
		segSpan{"C", "c", "A", 600, 100},
	), "S1", "root", true))
	var ids, parents []string
	for _, s := range spans {
		ids, parents = append(ids, s.SpanID), append(parents, s.ParentSpanID)
	}
	if want := []string{"R", "A", "C", "B"}; !reflect.DeepEqual(ids, want) {
		t.Errorf("order = %v, want %v", ids, want)
	}
	if want := []string{"", "R", "A", "A"}; !reflect.DeepEqual(parents, want) {
		t.Errorf("parents = %v, want %v", parents, want)
	}
	wantSegs := map[string][][2]int64{"R": {}, "A": {{0, 100}, {300, 600}, {700, 1000}}, "B": {{100, 300}}, "C": {{600, 700}}}
	if got := segMicros(spans); !reflect.DeepEqual(got, wantSegs) {
		t.Errorf("segments = %v, want %v", got, wantSegs)
	}
	if got, want := exclMicros(spans), map[string]int64{"R": 0, "A": 700, "B": 200, "C": 100}; !reflect.DeepEqual(got, want) {
		t.Errorf("exclusive = %v, want %v", got, want)
	}
	a := spans[1]
	if a.Start != time.UnixMicro(0).UTC() || a.End != time.UnixMicro(1000).UTC() || a.Service != "S1" || a.Operation != "a" {
		t.Errorf("A = %+v", a)
	}
}

func TestCPSegmentsBackToBackChildrenTileTheParent(t *testing.T) {
	spans := graphSegments(t, segGraph(t, segTrace(t,
		segSpan{"R", "root", "", 0, 300},
		segSpan{"A", "a", "R", 0, 100},
		segSpan{"B", "b", "R", 100, 100},
		segSpan{"C", "c", "R", 200, 100},
	), "S1", "root", true))
	want := map[string][][2]int64{"R": {}, "A": {{0, 100}}, "B": {{100, 200}}, "C": {{200, 300}}}
	if got := segMicros(spans); !reflect.DeepEqual(got, want) {
		t.Errorf("segments = %v, want %v", got, want)
	}
}

func TestCPSegmentsLeafRootIsOneSegment(t *testing.T) {
	spans := graphSegments(t, segGraph(t, segTrace(t, segSpan{"R", "root", "", 5, 10}), "S1", "root", true))
	want := []CriticalPathSpan{{
		SpanID: "R", Service: "S1", Operation: "root",
		Start: time.UnixMicro(5).UTC(), End: time.UnixMicro(15).UTC(), Exclusive: 10 * time.Microsecond,
		Segments: []Segment{{time.UnixMicro(5).UTC(), time.UnixMicro(15).UTC()}},
	}}
	if !reflect.DeepEqual(spans, want) {
		t.Errorf("got %+v, want %+v", spans, want)
	}
}

func TestCPSegmentsOverlappingSiblingsShareTheOverlap(t *testing.T) {
	// A [100,505] and B [500,800] overlap by 5us, under 1% of R, so both are
	// on the critical path and [500,505] is in both.
	spans := graphSegments(t, segGraph(t, segTrace(t,
		segSpan{"R", "root", "", 0, 1000},
		segSpan{"A", "a", "R", 100, 405},
		segSpan{"B", "b", "R", 500, 300},
	), "S1", "root", true))
	want := map[string][][2]int64{"R": {{0, 100}, {800, 1000}}, "A": {{100, 505}}, "B": {{500, 800}}}
	if got := segMicros(spans); !reflect.DeepEqual(got, want) {
		t.Errorf("segments = %v, want %v", got, want)
	}
	if got := exclMicros(spans)["R"]; got != 295 {
		t.Errorf("R exclusive = %d, want 295", got)
	}
}

func TestCPSegmentsClipping(t *testing.T) {
	for _, tc := range []struct {
		name  string
		specs []segSpan
		want  map[string][][2]int64
		excl  map[string]int64
	}{
		{
			name:  "child ends after parent",
			specs: []segSpan{{"R", "", "", 0, 1000}, {"A", "", "R", 0, 500}, {"S", "", "A", 400, 300}},
			want:  map[string][][2]int64{"R": {{500, 1000}}, "A": {{0, 400}}, "S": {{400, 500}}},
			excl:  map[string]int64{"R": 500, "A": 200, "S": 300},
		},
		{
			name:  "child starts before parent",
			specs: []segSpan{{"R", "", "", 100, 900}, {"A", "", "R", 0, 300}},
			want:  map[string][][2]int64{"R": {{300, 1000}}, "A": {{100, 300}}},
		},
		{
			name:  "child entirely outside parent",
			specs: []segSpan{{"R", "", "", 0, 100}, {"A", "", "R", 200, 100}, {"B", "", "A", 250, 10}},
			want:  map[string][][2]int64{"R": {{0, 100}}, "A": {}, "B": {}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spans := graphSegments(t, manualSegGraph(tc.specs...))
			if got := segMicros(spans); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("segments = %v, want %v", got, tc.want)
			}
			if tc.excl != nil && !reflect.DeepEqual(exclMicros(spans), tc.excl) {
				t.Errorf("exclusive = %v, want %v", exclMicros(spans), tc.excl)
			}
		})
	}
}

func TestCPSegmentsExplicitCPWithNestedSiblings(t *testing.T) {
	// FindCriticalPath never yields nested siblings, but a caller-supplied cp
	// can: B [200,300] lies inside its sibling A [100,900].
	g := manualSegGraph(segSpan{"R", "", "", 0, 1000}, segSpan{"A", "", "R", 100, 800}, segSpan{"B", "", "R", 200, 100})
	spans := g.CriticalPathSegments([]*Node{g.NodeHT["R"], g.NodeHT["A"], g.NodeHT["B"]})
	want := map[string][][2]int64{"R": {{0, 100}, {900, 1000}}, "A": {{100, 900}}, "B": {{200, 300}}}
	if got := segMicros(spans); !reflect.DeepEqual(got, want) {
		t.Errorf("segments = %v, want %v", got, want)
	}
}

func TestCPSegmentsEmptyCP(t *testing.T) {
	if got := (&Graph{}).CriticalPathSegments(nil); got == nil || len(got) != 0 {
		t.Errorf("got %#v, want empty non-nil", got)
	}
}

// Fixture 26: server S runs past its client A (504us under a 500us client,
// so sanitization shortens S's duration but keeps its EndTime), S's child X
// is clipped with S, server T starts before its client B, and the
// zero-duration Z splits R's time into adjacent windows.
func TestCPSegmentsFixture26(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(fixturesDir(t), "26.json"))
	if err != nil {
		t.Fatal(err)
	}
	spans := graphSegments(t, segGraph(t, decodeTrace(t, data), "S1", "O1", true))
	type row struct {
		id         string
		start, end int64
		excl       int64
		segs       [][2]int64
	}
	var got []row
	for _, s := range spans {
		got = append(got, row{s.SpanID, s.Start.UnixMicro(), s.End.UnixMicro(), s.Exclusive.Microseconds(), segMicros([]CriticalPathSpan{s})[s.SpanID]})
	}
	want := []row{
		{"R", 0, 1000, 300, [][2]int64{{0, 100}, {600, 700}, {700, 750}, {950, 1000}}},
		{"B", 750, 950, 0, [][2]int64{{945, 950}}},
		{"T", 745, 945, 200, [][2]int64{{750, 945}}},
		{"Z", 700, 700, 0, [][2]int64{}},
		{"A", 100, 600, 0, [][2]int64{{100, 105}}},
		{"S", 105, 609, 441, [][2]int64{{105, 550}}},
		{"X", 550, 609, 59, [][2]int64{{550, 600}}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got  %v\nwant %v", got, want)
	}
}

func TestCPSegmentsAnalysisRootBelowTraceRoot(t *testing.T) {
	trace := segTrace(t,
		segSpan{"R", "root", "", 0, 1000},
		segSpan{"A", "a", "R", 100, 500},
		segSpan{"B", "b", "A", 200, 100},
	)
	byName := graphSegments(t, segGraph(t, trace, "S1", "a", false))
	analysis, err := AnalyzeTrace(context.Background(), trace, "A", nil)
	if err != nil {
		t.Fatal(err)
	}
	for name, spans := range map[string][]CriticalPathSpan{"by name": byName, "by span ID": analysis.Spans} {
		if spans[0].SpanID != "A" || spans[0].ParentSpanID != "" || spans[1].ParentSpanID != "A" {
			t.Errorf("%s: spans = %+v", name, spans)
		}
		want := map[string][][2]int64{"A": {{100, 200}, {300, 600}}, "B": {{200, 300}}}
		if got := segMicros(spans); !reflect.DeepEqual(got, want) {
			t.Errorf("%s: segments = %v, want %v", name, got, want)
		}
	}
}

func TestCanonicalCPSegmentsJSON(t *testing.T) {
	us := func(v int64) time.Time { return time.UnixMicro(v).UTC() }
	spans := []CriticalPathSpan{
		{SpanID: "R", Service: "S1", Operation: "root", Start: us(0), End: us(10), Exclusive: 4 * time.Microsecond,
			Segments: []Segment{{us(0), us(2)}, {us(8), us(10)}}},
		{SpanID: "A", ParentSpanID: "R", Service: "S1", Operation: "a", Start: us(2), End: us(8), Exclusive: 6 * time.Microsecond,
			Segments: []Segment{{us(2), us(8)}}},
	}
	want := "{\n  \"spans\": [\n" +
		"    {\n      \"endTime\": 10,\n      \"exclusive\": 4,\n      \"operation\": \"root\",\n" +
		"      \"parentSpanID\": null,\n      \"segments\": [\n        [\n          0,\n          2\n        ],\n" +
		"        [\n          8,\n          10\n        ]\n      ],\n      \"service\": \"S1\",\n" +
		"      \"spanID\": \"R\",\n      \"startTime\": 0\n    },\n" +
		"    {\n      \"endTime\": 8,\n      \"exclusive\": 6,\n      \"operation\": \"a\",\n" +
		"      \"parentSpanID\": \"R\",\n      \"segments\": [\n        [\n          2,\n          8\n        ]\n" +
		"      ],\n      \"service\": \"S1\",\n      \"spanID\": \"A\",\n      \"startTime\": 2\n    }\n" +
		"  ]\n}\n"
	for name, tc := range map[string]struct {
		spans []CriticalPathSpan
		want  string
	}{
		"two spans":      {spans, want},
		"empty":          {nil, "{\n  \"spans\": []\n}\n"},
		"empty segments": {[]CriticalPathSpan{{SpanID: "Z", Start: us(1), End: us(1)}}, "{\n  \"spans\": [\n    {\n      \"endTime\": 1,\n      \"exclusive\": 0,\n      \"operation\": \"\",\n      \"parentSpanID\": null,\n      \"segments\": [],\n      \"service\": \"\",\n      \"spanID\": \"Z\",\n      \"startTime\": 1\n    }\n  ]\n}\n"},
		"no html escape": {[]CriticalPathSpan{{SpanID: "<&>", Operation: "é", Start: us(0), End: us(0)}}, "{\n  \"spans\": [\n    {\n      \"endTime\": 0,\n      \"exclusive\": 0,\n      \"operation\": \"é\",\n      \"parentSpanID\": null,\n      \"segments\": [],\n      \"service\": \"\",\n      \"spanID\": \"<&>\",\n      \"startTime\": 0\n    }\n  ]\n}\n"},
	} {
		got, err := CanonicalCPSegmentsJSON(tc.spans)
		if err != nil {
			t.Fatal(err)
		}
		if got != tc.want {
			t.Errorf("%s:\n got %q\nwant %q", name, got, tc.want)
		}
	}
}

// TestCPSegmentsMatchGolden is the Go port of
// tests/test_conformance.py::test_cp_segments_match_golden.
func TestCPSegmentsMatchGolden(t *testing.T) {
	dir := fixturesDir(t)
	for _, fixture := range discoverFixtures(t) {
		name := goldenName(t, dir, fixture)
		t.Run(name, func(t *testing.T) {
			data, err := os.ReadFile(fixture)
			if err != nil {
				t.Fatal(err)
			}
			trace := decodeTrace(t, data)
			service, operation, err := DeriveRootSpan(trace)
			if err != nil {
				t.Fatal(err)
			}
			got, err := CanonicalCPSegmentsJSON(graphSegments(t, segGraph(t, trace, service, operation, true)))
			if err != nil {
				t.Fatal(err)
			}
			golden, err := os.ReadFile(filepath.Join(dir, "golden", name, CPSegmentsFile))
			if err != nil {
				t.Fatal(err)
			}
			if got != string(golden) {
				t.Errorf("%s diverged from golden\n--- go ---\n%s\n--- golden ---\n%s", CPSegmentsFile, got, golden)
			}
		})
	}
}

// checkCoverage asserts the invariants CONFORMANCE.md states for any
// critical path: segments lie within the root and cover it exactly, and
// overlap only where critical-path siblings overlap.
func checkCoverage(t *testing.T, label string, spans []CriticalPathSpan) {
	t.Helper()
	root := spans[0]
	lo, hi := root.Start.UnixMicro(), root.End.UnixMicro()
	var all [][2]int64
	children := map[string][][2]int64{}
	for _, s := range spans {
		all = append(all, segMicros([]CriticalPathSpan{s})[s.SpanID]...)
		if s.ParentSpanID != "" {
			children[s.ParentSpanID] = append(children[s.ParentSpanID], [2]int64{s.Start.UnixMicro(), s.End.UnixMicro()})
		}
	}
	siblingOverlap := false
	for _, kids := range children {
		slices.SortFunc(kids, func(a, b [2]int64) int { return cmp.Compare(a[0], b[0]) })
		for i := 1; i < len(kids); i++ {
			siblingOverlap = siblingOverlap || kids[i-1][1] > kids[i][0]
		}
	}
	slices.SortFunc(all, func(a, b [2]int64) int { return cmp.Or(cmp.Compare(a[0], b[0]), cmp.Compare(a[1], b[1])) })
	cursor, overlap := lo, false
	for _, seg := range all {
		if seg[0] < lo || seg[1] > hi || seg[0] >= seg[1] {
			t.Fatalf("%s: segment %v outside root [%d,%d] or empty", label, seg, lo, hi)
		}
		if seg[0] > cursor {
			t.Fatalf("%s: gap [%d,%d] in coverage", label, cursor, seg[0])
		}
		overlap = overlap || seg[0] < cursor
		cursor = max(cursor, seg[1])
	}
	if cursor != hi {
		t.Fatalf("%s: coverage ends at %d, root ends at %d", label, cursor, hi)
	}
	if overlap && !siblingOverlap {
		t.Fatalf("%s: segments overlap without overlapping critical-path siblings", label)
	}
}

// criticalPathReference computes CriticalPath's result directly from the
// graph, independently of AnalyzeTrace.
func criticalPathReference(t *testing.T, trace *jaeger.Trace, rootSpanID string) []CriticalPathContributor {
	t.Helper()
	g, err := NewGraph(trace, "", "", &GraphOptions{RootSpanID: rootSpanID})
	if err != nil {
		t.Fatal(err)
	}
	cp, err := g.FindCriticalPath(nil)
	if err != nil {
		t.Fatal(err)
	}
	_, exclusive := g.AccumeCPMetrics(cp, "", nil)
	want := make([]CriticalPathContributor, len(cp))
	for i, n := range cp {
		want[i] = CriticalPathContributor{n.SID, g.ProcessName[n.ProcessID], n.OpName, time.Duration(exclusive[n.SID]) * time.Microsecond}
	}
	slices.SortFunc(want, func(a, b CriticalPathContributor) int {
		return cmp.Or(cmp.Compare(b.Duration, a.Duration), strings.Compare(a.SpanID, b.SpanID))
	})
	return want
}

// Every span of every fixture as the AnalyzeTrace root: the coverage
// invariants hold, and CriticalPath matches the graph-level computation.
func TestAnalyzeTraceEveryRoot(t *testing.T) {
	ctx := context.Background()
	for _, fixture := range discoverFixtures(t) {
		data, err := os.ReadFile(fixture)
		if err != nil {
			t.Fatal(err)
		}
		trace := decodeTrace(t, data)
		for _, span := range trace.Data[0].Spans {
			label := fmt.Sprintf("%s root %s", filepath.Base(fixture), span.SpanID)
			analysis, err := AnalyzeTrace(ctx, trace, span.SpanID, nil)
			if errors.Is(err, ErrRootNotFound) {
				continue
			}
			if err != nil {
				t.Fatalf("%s: %v", label, err)
			}
			if analysis.Spans[0].SpanID != span.SpanID {
				t.Fatalf("%s: root = %s", label, analysis.Spans[0].SpanID)
			}
			checkCoverage(t, label, analysis.Spans)

			contributors, err := CriticalPath(ctx, trace, span.SpanID)
			if err != nil {
				t.Fatalf("%s: CriticalPath: %v", label, err)
			}
			if want := criticalPathReference(t, trace, span.SpanID); !reflect.DeepEqual(contributors, want) {
				t.Fatalf("%s: CriticalPath = %+v, want %+v", label, contributors, want)
			}
		}
	}
}

func TestMicroDurationSaturates(t *testing.T) {
	for us, want := range map[int64]time.Duration{
		0:                      0,
		1500:                   1500 * time.Microsecond,
		math.MaxInt64 / 1000:   time.Duration(math.MaxInt64/1000) * time.Microsecond,
		math.MaxInt64/1000 + 1: math.MaxInt64,
		math.MaxInt64:          math.MaxInt64,
	} {
		if got := microDuration(us); got != want {
			t.Errorf("microDuration(%d) = %d, want %d", us, got, want)
		}
	}
}

func TestAnalyzeTraceErrors(t *testing.T) {
	trace := segTrace(t, segSpan{"R", "root", "", 0, 10})
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for name, tc := range map[string]struct {
		ctx  context.Context
		root string
		want error
	}{
		"empty root":   {context.Background(), "", ErrRootNotFound},
		"unknown root": {context.Background(), "nope", ErrRootNotFound},
		"canceled":     {canceled, "R", context.Canceled},
	} {
		if _, err := AnalyzeTrace(tc.ctx, trace, tc.root, &AnalyzeOptions{}); !errors.Is(err, tc.want) {
			t.Errorf("%s: got %v, want %v", name, err, tc.want)
		}
	}
}
