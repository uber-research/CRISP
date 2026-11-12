package crisp

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"

	"github.com/uber-research/CRISP/go/crisp/jaeger"
)

// MergeAllRoots: which spans are analysis roots, and how a trace with several
// roots is processed (see Graph.AnalysisRoots and CONFORMANCE.md). Mirrors
// tests/test_merge_all_roots.py on the same traces.

// marSpan is one span of a test trace. parent "" means no reference.
type marSpan struct {
	id, op, pid, parent string
	start, dur          int64
	err                 bool
}

func marTrace(t *testing.T, spans ...marSpan) []byte {
	t.Helper()
	services := map[string]string{"pA": "svcA", "pB": "svcB", "pX": "svcX"}
	processes := map[string]any{}
	out := make([]map[string]any, 0, len(spans))
	for _, s := range spans {
		refs := []map[string]string{}
		if s.parent != "" {
			refs = append(refs, map[string]string{"traceID": "T1", "spanID": s.parent, "refType": "CHILD_OF"})
		}
		span := map[string]any{
			"traceID": "T1", "spanID": s.id, "operationName": s.op, "references": refs,
			"startTime": s.start, "duration": s.dur, "processID": s.pid,
		}
		if s.err {
			span["tags"] = []map[string]any{{"key": "error", "type": "bool", "value": true}}
		}
		out = append(out, span)
		processes[s.pid] = map[string]string{"serviceName": services[s.pid]}
	}
	b, err := json.Marshal(map[string]any{"data": []any{map[string]any{"traceID": "T1", "spans": out, "processes": processes}}})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// A trace root svcX/opX with two svcA/opA children (A is the first match, S
// its sibling), a second trace root B, and C, the head of an orphaned
// fragment. A, B and C are analysis roots; S has a parent and is not.
var marMultiRoot = []marSpan{
	{id: "X", op: "opX", pid: "pX", start: 0, dur: 500},
	{id: "A", op: "opA", pid: "pA", parent: "X", start: 10, dur: 100},
	{id: "S", op: "opA", pid: "pA", parent: "X", start: 120, dur: 100},
	{id: "B", op: "opA", pid: "pA", start: 600, dur: 200},
	{id: "C", op: "opA", pid: "pA", parent: "missing", start: 900, dur: 300},
}

func marConfig() LightConfig {
	return LightConfig{ServiceName: "svcA", OperationName: "opA", MergeAllRoots: true}
}

func marRoots(t *testing.T, spans []marSpan, c LightConfig) []*lightTraceResult {
	t.Helper()
	results, err := processTraceRoots(marTrace(t, spans...), "T1", "T1.json", &c)
	if err != nil {
		t.Fatal(err)
	}
	return results
}

func marRootIDs(results []*lightTraceResult) []string {
	ids := []string{}
	for _, res := range results {
		ids = append(ids, res.rootSpanID)
	}
	return ids
}

func marPaths(res *lightTraceResult) []string {
	paths := []string{}
	for path := range res.cpp.Profile {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths
}

func marExpectRoots(t *testing.T, results []*lightTraceResult, want ...string) {
	t.Helper()
	if want == nil {
		want = []string{}
	}
	if got := marRootIDs(results); !reflect.DeepEqual(got, want) {
		t.Errorf("roots = %v, want %v", got, want)
	}
}

func marUS(v int64) *int64 { return &v }

// --- which spans are analysis roots ------------------------------------------

func TestMergeAllRootsEveryParentlessMatchIsARoot(t *testing.T) {
	results := marRoots(t, marMultiRoot, marConfig())
	marExpectRoots(t, results, "A", "B", "C")
	for i, want := range []int64{100, 200, 300} {
		if got := results[i].cpp.Profile["[svcA] opA"].Inc; got != want {
			t.Errorf("root %d inclusive = %d, want %d", i, got, want)
		}
	}
}

func TestNoMergeAllRootsKeepsTheFirstMatch(t *testing.T) {
	c := marConfig()
	c.MergeAllRoots = false
	marExpectRoots(t, marRoots(t, marMultiRoot, c), "A")
}

func TestMergeAllRootsRootTracePinsASingleRoot(t *testing.T) {
	c := marConfig()
	c.RootTrace = true
	marExpectRoots(t, marRoots(t, []marSpan{{id: "A", op: "opA", pid: "pA", start: 1000, dur: 100}}, c), "A")
}

// Sibling and nested matches count only through the critical path of the
// root above them.
func TestMergeAllRootsMatchesWithAParentAreNotRoots(t *testing.T) {
	results := marRoots(t, []marSpan{
		{id: "X", op: "opX", pid: "pX", start: 0, dur: 1000},
		{id: "A", op: "opA", pid: "pA", parent: "X", start: 10, dur: 400},
		{id: "N", op: "opA", pid: "pA", parent: "A", start: 20, dur: 100},
		{id: "S", op: "opA", pid: "pA", parent: "X", start: 500, dur: 100},
	}, marConfig())
	marExpectRoots(t, results, "A")
	profile := results[0].cpp.Profile
	for _, path := range []string{"[svcA] opA", "[svcA] opA->[svcA] opA"} {
		if got := profile[path].Freq; got != 1 {
			t.Errorf("freq of %q = %d, want 1", path, got)
		}
	}
}

// N starts after its parent A ended, so sanitization drops it.
func TestMergeAllRootsMatchDroppedBySanitizationIsNotARoot(t *testing.T) {
	marExpectRoots(t, marRoots(t, []marSpan{
		{id: "X", op: "opX", pid: "pX", start: 0, dur: 1000},
		{id: "A", op: "opA", pid: "pA", parent: "X", start: 0, dur: 100},
		{id: "N", op: "opA", pid: "pA", parent: "A", start: 200, dur: 50},
	}, marConfig()), "A")
}

func marGraph(t *testing.T, spans []marSpan, opts *GraphOptions) *Graph {
	t.Helper()
	trace, err := jaeger.Decode(marTrace(t, spans...))
	if err != nil {
		t.Fatal(err)
	}
	g, err := NewGraph(trace, "", "", opts)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func TestMergeAllRootsRootSpanIDPinsASingleRoot(t *testing.T) {
	g := marGraph(t, marMultiRoot, &GraphOptions{Filename: "T1.json", RootSpanID: "B"})
	roots := g.AnalysisRoots(true)
	if len(roots) != 1 || roots[0].SID != "B" {
		t.Errorf("AnalysisRoots = %v, want [B]", roots)
	}
}

// --- every root is prepared the same way -------------------------------------

// L starts after root B ended. It must not reach B's critical path.
func TestMergeAllRootsEveryRootIsSanitized(t *testing.T) {
	results := marRoots(t, []marSpan{
		{id: "A", op: "opA", pid: "pA", start: 0, dur: 100},
		{id: "B", op: "opA", pid: "pA", start: 1000, dur: 100},
		{id: "L", op: "late", pid: "pB", parent: "B", start: 2000, dur: 5000},
	}, marConfig())
	marExpectRoots(t, results, "A", "B")
	for _, res := range results {
		if got := marPaths(res); !reflect.DeepEqual(got, []string{"[svcA] opA"}) {
			t.Errorf("paths = %v, want only the root", got)
		}
		if got := res.cpp.Profile["[svcA] opA"].Excl; got != 100 {
			t.Errorf("root exclusive = %d, want 100", got)
		}
	}
}

// Each merged root gives the critical path of a run pinned to that span.
func TestMergeAllRootsEqualRunsPinnedToEachRoot(t *testing.T) {
	for _, res := range marRoots(t, marMultiRoot, marConfig()) {
		rootID := res.rootSpanID
		pinned := marGraph(t, marMultiRoot, &GraphOptions{Filename: "T1.json", RootSpanID: rootID})
		cp, err := pinned.FindCriticalPath(nil)
		if err != nil {
			t.Fatal(err)
		}
		want := []string{}
		for _, n := range cp {
			want = append(want, pinned.getCallPath(n))
		}
		sort.Strings(want)
		if got := marPaths(res); !reflect.DeepEqual(got, want) {
			t.Errorf("root %s: paths = %v, want %v", rootID, got, want)
		}
		if got := res.cpp.Profile["[svcA] opA"].Inc; got != pinned.RootNode.Duration {
			t.Errorf("root %s: inclusive = %d, want %d", rootID, got, pinned.RootNode.Duration)
		}
	}
}

// --- duration window ---------------------------------------------------------

func TestMergeAllRootsFirstRootIsTheFirstMatchInTheDurationWindow(t *testing.T) {
	spans := []marSpan{
		{id: "X", op: "opX", pid: "pX", start: 0, dur: 9000},
		{id: "A", op: "opA", pid: "pA", parent: "X", start: 10, dur: 10},
		{id: "S", op: "opA", pid: "pA", parent: "X", start: 100, dur: 200},
	}
	c := marConfig()
	marExpectRoots(t, marRoots(t, spans, c), "A")
	c.MinDuration = marUS(50)
	marExpectRoots(t, marRoots(t, spans, c), "S")
	c.MinDuration = marUS(500)
	marExpectRoots(t, marRoots(t, spans, c))
}

func TestMergeAllRootsDurationWindowAppliesToEachRoot(t *testing.T) {
	c := marConfig()
	c.MinDuration = marUS(150)
	marExpectRoots(t, marRoots(t, marMultiRoot, c), "B", "C")
	c = marConfig()
	c.MaxDuration = marUS(250)
	marExpectRoots(t, marRoots(t, marMultiRoot, c), "A", "B")
}

// --- outputs -----------------------------------------------------------------

func marRunLight(t *testing.T, spans []marSpan, c LightConfig) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "T1.json")
	if err := os.WriteFile(path, marTrace(t, spans...), 0o644); err != nil {
		t.Fatal(err)
	}
	c.TraceFiles = []string{path}
	c.OutputDir = dir
	c.MaxExemplars = 3
	if err := LightProcess(&c); err != nil {
		t.Fatal(err)
	}
	return dir
}

func marReadCCT(t *testing.T, dir string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, LightCCTFile))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestMergeAllRootsLightCCTAveragesOverRoots(t *testing.T) {
	// Three roots of 100, 200 and 300us: mean exclusive time 200, frequency 3.
	if got, want := marReadCCT(t, marRunLight(t, marMultiRoot, marConfig())), "[svcA] opA 200 <<3>>\n"; got != want {
		t.Errorf("cct = %q, want %q", got, want)
	}
}

func TestNoMergeAllRootsLightCCT(t *testing.T) {
	c := marConfig()
	c.MergeAllRoots = false
	if got, want := marReadCCT(t, marRunLight(t, marMultiRoot, c)), "[svcA] opA 100 <<1>>\n"; got != want {
		t.Errorf("cct = %q, want %q", got, want)
	}
}

func TestMergeAllRootsErrorBreakdownIsOnePerTrace(t *testing.T) {
	spans := []marSpan{
		{id: "A", op: "opA", pid: "pA", start: 0, dur: 100, err: true},
		{id: "B", op: "opA", pid: "pA", start: 1000, dur: 100, err: true},
	}
	for _, tt := range []struct {
		name      string
		root      ErrorBreakdownRoot
		wantCount int
	}{
		{name: "analysis root: summed over the roots", root: ErrorBreakdownAnalysisRoot, wantCount: 2},
		{name: "trace root: once per trace", root: ErrorBreakdownTraceRoot, wantCount: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := marConfig()
			c.ErrorBreakdown = &ErrorBreakdownOptions{Mode: ErrorBreakdownOrigins, Root: tt.root}
			b, err := os.ReadFile(filepath.Join(marRunLight(t, spans, c), ErrorBreakdownFile))
			if err != nil {
				t.Fatal(err)
			}
			var doc ErrorBreakdown
			if err := json.Unmarshal(b, &doc); err != nil {
				t.Fatal(err)
			}
			count := 0
			for _, p := range doc.Paths {
				count += p.Count
			}
			if doc.Traces != 1 || count != tt.wantCount {
				t.Errorf("traces = %d, count = %d; want 1, %d", doc.Traces, count, tt.wantCount)
			}
		})
	}
}

func TestMergeAllRootsDragReportCoversEachRootSubtreeOnce(t *testing.T) {
	results := marRoots(t, []marSpan{
		{id: "A", op: "opA", pid: "pA", start: 0, dur: 100},
		{id: "A1", op: "opB", pid: "pB", parent: "A", start: 10, dur: 50},
		{id: "B", op: "opA", pid: "pA", start: 1000, dur: 100},
	}, marConfig())
	want := []map[string]int64{
		{"[svcA] opA": 1, "[svcA] opA->[svcB] opB": 1},
		{"[svcA] opA": 1},
	}
	got := []map[string]int64{}
	for _, res := range results {
		counts := map[string]int64{}
		for path, agg := range res.slackDrag.ByPath {
			counts[path] = agg.SpanCount
		}
		got = append(got, counts)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("span counts = %v, want %v", got, want)
	}
}
