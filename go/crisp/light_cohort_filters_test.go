package crisp

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/uber-research/CRISP/go/crisp/jaeger"
)

// Light-mode cohort filters: structural orphan detection, incomplete analysis
// subtrees, RejectSubtreeIncomplete, and MinDuration/MaxDuration. Mirrors
// tests/test_light_cohort_filters.py on the same traces.

// cfSpan is one span of a test trace. parent "" means no reference.
type cfSpan struct {
	id, op, pid, parent, refType string
	start, dur                   int64
}

func cfTrace(t *testing.T, spans ...cfSpan) []byte {
	t.Helper()
	services := map[string]string{"pA": "svcA", "pB": "svcB", "pC": "svcC", "pX": "svcX", "pZ": "svcZ", "pP": "svcProxy"}
	type ref struct {
		TraceID string `json:"traceID"`
		SpanID  string `json:"spanID"`
		RefType string `json:"refType"`
	}
	type span struct {
		TraceID       string `json:"traceID"`
		SpanID        string `json:"spanID"`
		OperationName string `json:"operationName"`
		References    []ref  `json:"references"`
		StartTime     int64  `json:"startTime"`
		Duration      int64  `json:"duration"`
		ProcessID     string `json:"processID"`
	}
	processes := map[string]map[string]string{}
	out := make([]span, 0, len(spans))
	for _, s := range spans {
		refs := []ref{}
		if s.parent != "" {
			rt := s.refType
			if rt == "" {
				rt = "CHILD_OF"
			}
			refs = append(refs, ref{TraceID: "T1", SpanID: s.parent, RefType: rt})
		}
		start, dur := s.start, s.dur
		if start == 0 {
			start = 1000
		}
		if dur == 0 {
			dur = 100
		}
		out = append(out, span{TraceID: "T1", SpanID: s.id, OperationName: s.op, References: refs, StartTime: start, Duration: dur, ProcessID: s.pid})
		processes[s.pid] = map[string]string{"serviceName": services[s.pid]}
	}
	b, err := json.Marshal(map[string]any{"data": []any{map[string]any{"traceID": "T1", "spans": out, "processes": processes}}})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// cfChain is svcA/opA (the analysis root, 1000us) -> svcB/opB -> svcC/opC.
var cfChain = []cfSpan{
	{id: "1", op: "opA", pid: "pA", start: 1000, dur: 1000},
	{id: "2", op: "opB", pid: "pB", parent: "1", start: 1100, dur: 600},
	{id: "3", op: "opC", pid: "pC", parent: "2", start: 1200, dur: 300},
}

// cfUnderX is the same chain under a trace root svcX/opX.
var cfUnderX = []cfSpan{
	{id: "0", op: "opX", pid: "pX", start: 900, dur: 1200},
	{id: "1", op: "opA", pid: "pA", parent: "0", start: 1000, dur: 1000},
	cfChain[1], cfChain[2],
}

// cfOrphan is a span whose CHILD_OF parent is not in the trace.
func cfOrphan(id, pid string) cfSpan {
	return cfSpan{id: id, op: "lost", pid: pid, parent: "missing-" + id, start: 1300, dur: 50}
}

func with(base []cfSpan, extra ...cfSpan) []cfSpan {
	return append(append([]cfSpan{}, base...), extra...)
}

func registerTestProxy(t *testing.T) {
	t.Helper()
	saved := ProxyServiceOpPairs
	ProxyServiceOpPairs = append(append([][2]string{}, saved...), [2]string{"svcProxy", "opProxy"})
	t.Cleanup(func() { ProxyServiceOpPairs = saved })
}

func cfGraph(t *testing.T, data []byte, filterProxy bool) *Graph {
	t.Helper()
	trace, err := jaeger.Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	rootTrace := false
	g, err := NewGraph(trace, "svcA", "opA", &GraphOptions{RootTrace: &rootTrace, FilterProxy: filterProxy})
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func TestOrphanSpans(t *testing.T) {
	registerTestProxy(t)
	for _, tt := range []struct {
		name         string
		spans        []cfSpan
		filterProxy  bool
		wantIDs      map[string]bool
		wantServices map[string]bool
	}{
		{name: "complete", spans: cfChain},
		{name: "orphan", spans: with(cfChain, cfOrphan("9", "pZ")), wantIDs: map[string]bool{"9": true}, wantServices: map[string]bool{"svcZ": true}},
		{name: "dangling FOLLOWS_FROM is not an orphan", spans: with(cfChain, cfSpan{id: "9", op: "async", pid: "pB", parent: "missing", refType: "FOLLOWS_FROM"})},
		{
			name:        "child of a short-wired root proxy",
			spans:       []cfSpan{{id: "1", op: "opProxy", pid: "pP", dur: 1000}, {id: "2", op: "opA", pid: "pA", parent: "1", start: 1010, dur: 900}},
			filterProxy: true,
		},
		{
			name:         "proxy with a dropped parent",
			spans:        []cfSpan{{id: "1", op: "opProxy", pid: "pP", parent: "dropped", dur: 1000}, {id: "2", op: "opA", pid: "pA", parent: "1", start: 1010, dur: 900}},
			filterProxy:  true,
			wantIDs:      map[string]bool{"1": true},
			wantServices: map[string]bool{"svcProxy": true},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			g := cfGraph(t, cfTrace(t, tt.spans...), tt.filterProxy)
			if len(g.OrphanSpanIDs) != len(tt.wantIDs) || (len(tt.wantIDs) > 0 && !reflect.DeepEqual(g.OrphanSpanIDs, tt.wantIDs)) {
				t.Errorf("OrphanSpanIDs = %v, want %v", g.OrphanSpanIDs, tt.wantIDs)
			}
			if got := g.OrphanServices(); len(got) != len(tt.wantServices) || (len(got) > 0 && !reflect.DeepEqual(got, tt.wantServices)) {
				t.Errorf("OrphanServices = %v, want %v", got, tt.wantServices)
			}
		})
	}
}

func TestSubtreeIncomplete(t *testing.T) {
	for _, tt := range []struct {
		name  string
		spans []cfSpan
		want  bool
	}{
		{name: "complete", spans: cfChain},
		{name: "orphan in a child's service", spans: with(cfChain, cfOrphan("9", "pB")), want: true},
		{name: "orphan in a deep descendant's service", spans: with(cfChain, cfOrphan("9", "pC")), want: true},
		{name: "orphan in the analysis root's service", spans: with(cfChain, cfOrphan("9", "pA")), want: true},
		{name: "orphan in an unrelated service", spans: with(cfChain, cfOrphan("9", "pZ"))},
		{name: "orphan service only above the analysis root", spans: with(cfUnderX, cfOrphan("9", "pX"))},
		{name: "children of an orphan are not orphans", spans: with(cfChain, cfOrphan("9", "pZ"), cfSpan{id: "10", op: "opB", pid: "pB", parent: "9", start: 1310, dur: 20})},
	} {
		t.Run(tt.name, func(t *testing.T) {
			res, err := processTraceData(cfTrace(t, tt.spans...), "T1", "T1.json", &LightConfig{ServiceName: "svcA", OperationName: "opA"})
			if err != nil || res == nil {
				t.Fatalf("processTraceData = %v, %v", res, err)
			}
			if res.subtreeIncomplete != tt.want {
				t.Errorf("subtreeIncomplete = %v, want %v", res.subtreeIncomplete, tt.want)
			}
		})
	}
}

func TestLightDurationBounds(t *testing.T) {
	// The trace root svcX/opX is 200us; the analysis root svcA/opA is 100us.
	nested := cfTrace(t,
		cfSpan{id: "0", op: "opX", pid: "pX", start: 1000, dur: 200},
		cfSpan{id: "1", op: "opA", pid: "pA", parent: "0", start: 1010, dur: 100},
		cfSpan{id: "2", op: "opB", pid: "pB", parent: "1", start: 1020, dur: 50},
	)
	us := func(v int64) *int64 { return &v }
	for _, tt := range []struct {
		name     string
		min, max *int64
		kept     bool
	}{
		{name: "no bounds", kept: true},
		{name: "min equal to duration", min: us(100), kept: true},
		{name: "min above duration", min: us(101)},
		{name: "max equal to duration", max: us(100), kept: true},
		{name: "max below duration", max: us(99)},
		{name: "max zero is a bound", max: us(0)},
		{name: "min applies to the analysis root, not the trace root", min: us(150)},
		{name: "max applies to the analysis root, not the trace root", max: us(150), kept: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			res, err := processTraceData(nested, "T1", "T1.json", &LightConfig{ServiceName: "svcA", OperationName: "opA", MinDuration: tt.min, MaxDuration: tt.max})
			if err != nil {
				t.Fatal(err)
			}
			if (res != nil) != tt.kept {
				t.Errorf("kept = %v, want %v", res != nil, tt.kept)
			}
		})
	}
}

func cfRunLight(t *testing.T, files map[string][]byte, cfg LightConfig) []byte {
	t.Helper()
	dir := t.TempDir()
	for name, data := range files {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, data, 0o644); err != nil {
			t.Fatal(err)
		}
		cfg.TraceFiles = append(cfg.TraceFiles, path)
	}
	cfg.ServiceName, cfg.OperationName, cfg.OutputDir = "svcA", "opA", dir
	if err := LightProcess(&cfg); err != nil {
		t.Fatalf("LightProcess: %v", err)
	}
	out, err := os.ReadFile(filepath.Join(dir, LightCCTFile))
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestLightRejectSubtreeIncomplete(t *testing.T) {
	tripled := make([]cfSpan, len(cfChain))
	for i, s := range cfChain {
		s.dur *= 3
		tripled[i] = s
	}
	complete := cfTrace(t, cfChain...)
	upstream := cfTrace(t, with(cfChain, cfOrphan("9", "pZ"))...) // incomplete, but not under the analysis root
	downstream := cfTrace(t, with(tripled, cfOrphan("9", "pB"))...)
	files := map[string][]byte{"a.json": complete, "b.json": upstream, "c.json": downstream}

	rejected := cfRunLight(t, files, LightConfig{RejectSubtreeIncomplete: true})
	expected := cfRunLight(t, map[string][]byte{"a.json": complete, "b.json": upstream}, LightConfig{})
	kept := cfRunLight(t, files, LightConfig{})
	if !bytes.Equal(rejected, expected) {
		t.Errorf("the rejected trace reached the outputs:\ngot:\n%s\nwant:\n%s", rejected, expected)
	}
	if bytes.Equal(kept, expected) {
		t.Error("without the flag the incomplete trace should be analyzed")
	}

	// ProcessSingleTraceData applies the same filter.
	dir := t.TempDir()
	if err := ProcessSingleTraceData(downstream, "c", &LightConfig{ServiceName: "svcA", OperationName: "opA", RejectSubtreeIncomplete: true, OutputDir: dir}); err != nil {
		t.Fatal(err)
	}
	empty := cfRunLight(t, map[string][]byte{"x.json": cfTrace(t, cfSpan{id: "1", op: "opX", pid: "pX"})}, LightConfig{})
	if got, err := os.ReadFile(filepath.Join(dir, LightCCTFile)); err != nil || !bytes.Equal(got, empty) {
		t.Errorf("ProcessSingleTraceData kept a rejected trace: %q (err %v)", got, err)
	}
}

func TestLightShortWiredRootProxyIsComplete(t *testing.T) {
	registerTestProxy(t)
	data := cfTrace(t,
		cfSpan{id: "1", op: "opProxy", pid: "pP", dur: 1000},
		cfSpan{id: "2", op: "opA", pid: "pA", parent: "1", start: 1010, dur: 900},
		cfSpan{id: "3", op: "opB", pid: "pB", parent: "2", start: 1020, dur: 500},
	)
	res, err := processTraceData(data, "T1", "T1.json", &LightConfig{ServiceName: "svcA", OperationName: "opA", FilterProxy: true})
	if err != nil || res == nil {
		t.Fatalf("processTraceData = %v, %v", res, err)
	}
	if res.subtreeIncomplete {
		t.Error("a trace rooted at a short-wired proxy is complete")
	}
}
