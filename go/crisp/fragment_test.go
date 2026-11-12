package crisp

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/uber-research/CRISP/go/crisp/jaeger"
)

// Trace fragments: PruneToFragment and LightConfig.Fragment (see fragment.go
// and CONFORMANCE.md, "Trace fragments"). Mirrors tests/test_fragment.py on
// the same traces.

func fragProcs() map[string]jaeger.Process {
	return map[string]jaeger.Process{"pA": mkProc("svcA"), "pB": mkProc("svcB"), "pX": mkProc("svcX"), "pZ": mkProc("svcZ"), "pP": mkProc("svcProxy")}
}

func fragRefs(refType string, parents ...string) []jaeger.Reference {
	refs := []jaeger.Reference{}
	for _, parent := range parents {
		refs = append(refs, jaeger.Reference{RefType: refType, TraceID: "t", SpanID: parent})
	}
	return refs
}

func fragChildOf(parents ...string) []jaeger.Reference { return fragRefs("CHILD_OF", parents...) }

// X is the trace root. Its child A matches svcA/opA and calls B; its child U
// and U's child V are unrelated. M is a second trace root that matches. O is
// an orphan: its parent is not in the trace. W is a child of the orphan.
func fragTrace() []jaeger.Span {
	return []jaeger.Span{
		mkSpan("X", "opX", "pX", 0, 1000, nil),
		mkSpan("U", "opU", "pZ", 10, 100, fragChildOf("X")),
		mkSpan("V", "opV", "pZ", 20, 50, fragChildOf("U")),
		mkSpan("A", "opA", "pA", 200, 500, fragChildOf("X")),
		mkSpan("B", "opB", "pB", 250, 300, fragChildOf("A")),
		mkSpan("M", "opA", "pA", 2000, 400, nil),
		mkSpan("O", "opO", "pZ", 3000, 100, fragChildOf("missing")),
		mkSpan("W", "opW", "pZ", 3010, 50, fragChildOf("O")),
	}
}

// fragSpans returns the fragment's spans as "ID<-ref,ref", in order.
func fragSpans(spans []jaeger.Span, operation string) []string {
	fragment := PruneToFragment(mkTrace(fragProcs(), spans...), "svcA", operation)
	out := []string{}
	for _, s := range fragment.Data[0].Spans {
		refs := []string{}
		for _, r := range s.References {
			refs = append(refs, r.SpanID)
		}
		out = append(out, s.SpanID+"<-"+strings.Join(refs, ","))
	}
	return out
}

func fragExpect(t *testing.T, got []string, want ...string) {
	t.Helper()
	if want == nil {
		want = []string{}
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("fragment = %v, want %v", got, want)
	}
}

// --- PruneToFragment ---------------------------------------------------------

// U, V and W are gone. Every kept span keeps its parent reference, and the
// spans keep their order.
func TestFragmentKeepsMatchesSubtreesAncestorsAndOrphans(t *testing.T) {
	fragExpect(t, fragSpans(fragTrace(), "opA"), "X<-", "A<-X", "B<-A", "M<-", "O<-missing")
}

func TestFragmentKeepsProcessesAndDoesNotModifyTheTrace(t *testing.T) {
	trace := mkTrace(fragProcs(), fragTrace()...)
	before, err := json.Marshal(trace)
	if err != nil {
		t.Fatal(err)
	}
	fragment := PruneToFragment(trace, "svcA", "opA")
	after, err := json.Marshal(trace)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Error("PruneToFragment modified its input")
	}
	if !reflect.DeepEqual(fragment.Data[0].Processes, trace.Data[0].Processes) {
		t.Errorf("processes = %v, want all of them", fragment.Data[0].Processes)
	}
}

func TestFragmentWithoutAMatchKeepsOnlyOrphans(t *testing.T) {
	fragExpect(t, fragSpans(fragTrace(), "none"), "O<-missing")
}

func TestFragmentReferencesToRemovedSpansAreDropped(t *testing.T) {
	spans := []jaeger.Span{
		mkSpan("X", "opX", "pX", 0, 1000, nil),
		mkSpan("A", "opA", "pA", 200, 500, fragChildOf("X")),
		mkSpan("U", "opU", "pZ", 10, 100, fragChildOf("X")),
		// K's parent is A. Its earlier reference to the removed U goes away.
		mkSpan("K", "opK", "pB", 250, 100, fragChildOf("U", "A")),
		// F follows from the removed U, from A, and from a span of another trace.
		mkSpan("F", "opF", "pB", 260, 10, append(fragChildOf("A"), fragRefs("FOLLOWS_FROM", "U", "A", "elsewhere")...)),
	}
	fragExpect(t, fragSpans(spans, "opA"), "X<-", "A<-X", "K<-A", "F<-A,A,elsewhere")
}

// ParseTrace reads the last CHILD_OF reference as the parent. When that
// parent is removed, an earlier reference must not become the parent.
func TestFragmentKeepsATestTraceMarker(t *testing.T) {
	old := TestTraceOpPrefixes
	TestTraceOpPrefixes = []string{"test-"}
	defer func() { TestTraceOpPrefixes = old }()
	spans := []jaeger.Span{
		mkSpan("X", "opX", "pX", 0, 1000, nil),
		mkSpan("A", "opA", "pA", 200, 500, fragChildOf("X")),
		mkSpan("U", "opU", "pZ", 10, 100, fragChildOf("X")),
		// T marks the trace as a test trace. Its parent is U, which is removed.
		mkSpan("T", "test-probe", "pZ", 600, 90, fragChildOf("A", "U")),
		mkSpan("T2", "test-probe", "pZ", 30, 50, fragChildOf("U")),
	}
	fragExpect(t, fragSpans(spans, "opA"), "X<-", "A<-X", "T<-")
	// As a child of A, T would be on A's critical path.
	fragExpectOutputs(t, spans, LightConfig{MergeAllRoots: true})
	fragExpectOutputs(t, spans, LightConfig{MergeAllRoots: true, IgnoreTestTraces: true})
}

func TestFragmentAncestorWalkEndsOnACycle(t *testing.T) {
	spans := []jaeger.Span{
		mkSpan("P", "opP", "pX", 0, 1000, fragChildOf("Q")),
		mkSpan("Q", "opQ", "pX", 0, 1000, fragChildOf("P")),
		mkSpan("A", "opA", "pA", 200, 500, fragChildOf("P")),
		mkSpan("U", "opU", "pZ", 10, 100, nil),
	}
	fragExpect(t, fragSpans(spans, "opA"), "P<-Q", "Q<-P", "A<-P")
}

// --- Fragment in the light pipeline ------------------------------------------

func TestFragmentRejectsOptionsThatNeedTheWholeTrace(t *testing.T) {
	for _, tt := range []struct {
		name    string
		config  LightConfig
		message string
	}{
		{name: "rootTrace", config: LightConfig{RootTrace: true}, message: "--rootTrace"},
		{name: "trace-root error breakdown", config: LightConfig{ErrorBreakdown: &ErrorBreakdownOptions{Mode: ErrorBreakdownOrigins, Root: ErrorBreakdownTraceRoot}}, message: "--errorBreakdownRoot trace"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.config
			c.ServiceName, c.OperationName, c.Fragment, c.OutputDir = "svcA", "opA", true, t.TempDir()
			for call, err := range map[string]error{
				"CheckFragmentConfig":    CheckFragmentConfig(&c),
				"LightProcess":           LightProcess(&c),
				"ProcessSingleTraceData": ProcessSingleTraceData([]byte(`{"data":[]}`), "T1", &c),
			} {
				if err == nil || !strings.Contains(err.Error(), tt.message) {
					t.Errorf("%s: err = %v, want one about %s", call, err, tt.message)
				}
			}
			// Without Fragment the same options are fine.
			c.Fragment = false
			if err := CheckFragmentConfig(&c); err != nil {
				t.Errorf("without Fragment: %v", err)
			}
		})
	}
	ok := LightConfig{Fragment: true, ErrorBreakdown: &ErrorBreakdownOptions{Mode: ErrorBreakdownOrigins, Root: ErrorBreakdownAnalysisRoot}}
	if err := CheckFragmentConfig(&ok); err != nil {
		t.Errorf("analysis-root error breakdown: %v", err)
	}
}

func TestFragmentDragReportCoversOnlyTheAnalysisSubtree(t *testing.T) {
	single := fragTrace()[:5] // one analysis root, A
	data, err := json.Marshal(mkTrace(fragProcs(), single...))
	if err != nil {
		t.Fatal(err)
	}
	dragPaths := func(fragment bool) []string {
		results, err := processTraceRoots(data, "T1", "T1.json", &LightConfig{ServiceName: "svcA", OperationName: "opA", Fragment: fragment})
		if err != nil || len(results) != 1 {
			t.Fatalf("processTraceRoots = %v, %v; want one root", results, err)
		}
		paths := []string{}
		for path := range results[0].slackDrag.ByPath {
			paths = append(paths, path)
		}
		sort.Strings(paths)
		return paths
	}
	if got, want := dragPaths(true), []string{"[svcA] opA", "[svcA] opA->[svcB] opB"}; !reflect.DeepEqual(got, want) {
		t.Errorf("with Fragment: drag paths = %v, want %v", got, want)
	}
	// Without Fragment a single root reports every span of the trace.
	if got := dragPaths(false); len(got) != 5 {
		t.Errorf("without Fragment: drag paths = %v, want one per span", got)
	}
}

// --- a fragment gives the outputs of its trace -------------------------------

func fragFixtureFiles(t *testing.T) []string {
	t.Helper()
	files := discoverFixtures(t)
	more, err := filepath.Glob(filepath.Join(fixturesDir(t), "error_breakdown", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	return append(files, more...)
}

// fragTargets returns every (service, operation) of the trace, in span order.
func fragTargets(trace *jaeger.Trace) [][2]string {
	names := map[string]string{}
	for _, item := range trace.Data {
		for pid, p := range item.Processes {
			names[pid] = p.ServiceName
		}
	}
	seen := map[[2]string]bool{}
	var targets [][2]string
	for _, item := range trace.Data {
		for _, s := range item.Spans {
			target := [2]string{names[s.ProcessID], s.OperationName}
			if !seen[target] {
				seen[target] = true
				targets = append(targets, target)
			}
		}
	}
	return targets
}

// fragLightOutputs runs the light pipeline on one trace and returns its
// output files.
func fragLightOutputs(t *testing.T, data []byte, traceID string, c LightConfig) map[string]string {
	t.Helper()
	c.OutputDir = t.TempDir()
	c.Conformance = true
	c.MaxExemplars = 3
	if err := ProcessSingleTraceData(data, traceID, &c); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(c.OutputDir)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, e := range entries {
		b, err := os.ReadFile(filepath.Join(c.OutputDir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		out[e.Name()] = string(b)
	}
	return out
}

// fragExpectOutputs checks that the light outputs for the fragment of a trace
// are those for the trace, and returns them.
func fragExpectOutputs(t *testing.T, spans []jaeger.Span, c LightConfig) map[string]string {
	t.Helper()
	trace := mkTrace(fragProcs(), spans...)
	data, err := json.Marshal(trace)
	if err != nil {
		t.Fatal(err)
	}
	fragment, err := json.Marshal(PruneToFragment(trace, "svcA", "opA"))
	if err != nil {
		t.Fatal(err)
	}
	c.ServiceName, c.OperationName, c.Fragment = "svcA", "opA", true
	full := fragLightOutputs(t, data, "T1", c)
	if pruned := fragLightOutputs(t, fragment, "T1", c); !reflect.DeepEqual(pruned, full) {
		t.Errorf("the fragment's outputs differ from the trace's")
	}
	return full
}

// The orphan Q is in svcB, which also appears under the root A: A's subtree
// is incomplete. The other root, M, is complete.
func TestFragmentKeepsTheOrphansThatMakeASubtreeIncomplete(t *testing.T) {
	spans := append(fragTrace(), mkSpan("Q", "opQ", "pB", 4000, 10, fragChildOf("gone")))
	rejected := fragExpectOutputs(t, spans, LightConfig{MergeAllRoots: true, RejectSubtreeIncomplete: true})
	if got, want := rejected[LightCCTFile], "[svcA] opA 400 <<1>>\n"; got != want {
		t.Errorf("cct = %q, want %q", got, want)
	}
	if kept := fragExpectOutputs(t, spans, LightConfig{MergeAllRoots: true}); kept[LightCCTFile] == rejected[LightCCTFile] {
		t.Errorf("without RejectSubtreeIncomplete: cct = %q, want both roots", kept[LightCCTFile])
	}
}

// With FilterProxy the children of a proxy span attach to the proxy's parent.
// X calls A through the proxy P1, next to the unrelated U. A calls B through
// the proxy P2. M matches under the proxy P3, whose parent is not in the trace.
func TestFragmentGivesTheOutputsOfItsTraceThroughProxies(t *testing.T) {
	saved := ProxyServiceOpPairs
	ProxyServiceOpPairs = append(append([][2]string{}, saved...), [2]string{"svcProxy", "opProxy"})
	t.Cleanup(func() { ProxyServiceOpPairs = saved })

	spans := []jaeger.Span{
		mkSpan("X", "opX", "pX", 0, 2000, nil),
		mkSpan("P1", "opProxy", "pP", 10, 900, fragChildOf("X")),
		mkSpan("U", "opU", "pZ", 20, 100, fragChildOf("P1")),
		mkSpan("A", "opA", "pA", 200, 500, fragChildOf("P1")),
		mkSpan("P2", "opProxy", "pP", 250, 300, fragChildOf("A")),
		mkSpan("B", "opB", "pB", 260, 200, fragChildOf("P2")),
		mkSpan("P3", "opProxy", "pP", 3000, 500, fragChildOf("missing")),
		mkSpan("M", "opA", "pA", 3010, 400, fragChildOf("P3")),
	}
	fragExpect(t, fragSpans(spans, "opA"), "X<-", "P1<-X", "A<-P1", "P2<-A", "B<-P2", "P3<-missing", "M<-P3")

	trace := mkTrace(fragProcs(), spans...)
	data, err := json.Marshal(trace)
	if err != nil {
		t.Fatal(err)
	}
	fragment, err := json.Marshal(PruneToFragment(trace, "svcA", "opA"))
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name   string
		config LightConfig
	}{
		{name: "filterProxy", config: LightConfig{FilterProxy: true, MergeAllRoots: true}},
		{name: "filterProxy, reject incomplete", config: LightConfig{FilterProxy: true, MergeAllRoots: true, RejectSubtreeIncomplete: true}},
		{name: "filterProxy, no merge", config: LightConfig{FilterProxy: true}},
		{name: "no filterProxy", config: LightConfig{MergeAllRoots: true}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := tt.config
			c.ServiceName, c.OperationName, c.Fragment = "svcA", "opA", true
			full := fragLightOutputs(t, data, "T1", c)
			if pruned := fragLightOutputs(t, fragment, "T1", c); !reflect.DeepEqual(pruned, full) {
				t.Errorf("the fragment's outputs differ from the trace's")
			}
			if full[LightCCTFile] == "" {
				t.Errorf("no critical path in %s", LightCCTFile)
			}
		})
	}
}

func TestFragmentGivesTheOutputsOfItsTrace(t *testing.T) {
	us := func(v int64) *int64 { return &v }
	optionSets := []struct {
		name   string
		config LightConfig
	}{
		{name: "default", config: LightConfig{MergeAllRoots: true}},
		{name: "no merge", config: LightConfig{}},
		{name: "cohort filters", config: LightConfig{MergeAllRoots: true, MinDuration: us(20), MaxDuration: us(5_000_000), RejectSubtreeIncomplete: true}},
		{name: "error origins", config: LightConfig{MergeAllRoots: true, ErrorBreakdown: &ErrorBreakdownOptions{Mode: ErrorBreakdownOrigins, Root: ErrorBreakdownAnalysisRoot}}},
		{name: "error propToRoot", config: LightConfig{MergeAllRoots: true, FilterProxy: true, ErrorBreakdown: &ErrorBreakdownOptions{Mode: ErrorBreakdownPropToRoot, Root: ErrorBreakdownAnalysisRoot}}},
		{name: "ignore test traces", config: LightConfig{MergeAllRoots: true, IgnoreTestTraces: true}},
	}
	for _, path := range fragFixtureFiles(t) {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		trace, err := jaeger.Decode(data)
		if err != nil {
			t.Fatal(err)
		}
		traceID := TraceIDFromFilePath(path)
		for _, target := range fragTargets(trace) {
			fragment, err := json.Marshal(PruneToFragment(trace, target[0], target[1]))
			if err != nil {
				t.Fatal(err)
			}
			for _, options := range optionSets {
				where := fmt.Sprintf("%s: %s/%s, %s", filepath.Base(path), target[0], target[1], options.name)
				c := options.config
				c.ServiceName, c.OperationName, c.Fragment = target[0], target[1], true
				full := fragLightOutputs(t, data, traceID, c)
				if pruned := fragLightOutputs(t, fragment, traceID, c); !reflect.DeepEqual(pruned, full) {
					t.Errorf("%s: the fragment's outputs differ from the trace's", where)
				}
				// Fragment only changes the drag report.
				c.Fragment = false
				plain := fragLightOutputs(t, data, traceID, c)
				delete(plain, SlackDragCSV)
				delete(full, SlackDragCSV)
				if !reflect.DeepEqual(plain, full) {
					t.Errorf("%s: Fragment changed an output other than %s", where, SlackDragCSV)
				}
			}
		}
	}
}
