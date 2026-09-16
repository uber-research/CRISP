package crisp

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/uber-research/CRISP/go/crisp/jaeger"
)

// repoRoot walks up from the test's working directory to find test_cases/,
// which works under both `go test` (cwd = package dir) and `bazel test`
// (cwd = runfiles root, where test_cases is a data dependency).
func fixturesDir(t *testing.T) string {
	t.Helper()
	candidates := []string{
		"../../test_cases",    // go test, run from go/crisp
		"../../../test_cases", // extra nesting safety net
		"test_cases",          // bazel test, data glob at runfiles root
	}
	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c
		}
	}
	t.Fatalf("could not locate test_cases/ from any of %v (cwd=%s)", candidates, mustGetwd(t))
	return ""
}

func mustGetwd(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return wd
}

func discoverFixtures(t *testing.T) []string {
	t.Helper()
	dir := fixturesDir(t)
	var files []string
	top, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	files = append(files, top...)
	nested, err := filepath.Glob(filepath.Join(dir, "err_pattern*", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	files = append(files, nested...)
	if len(files) == 0 {
		t.Fatalf("no fixtures found under %s", dir)
	}
	return files
}

func parseFixture(t *testing.T, path string) *ParsedTrace {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	trace, err := jaeger.Decode(data)
	if err != nil {
		t.Fatalf("decoding %s: %v", path, err)
	}
	pt, err := ParseTrace(trace)
	if err != nil {
		t.Fatalf("ParseTrace(%s): %v", path, err)
	}
	return pt
}

// TestParseTrace_AllFixtures is the lightweight verification slice pulled
// forward from Phase 1: every committed fixture must decode without error
// and produce a non-empty, structurally sane result.
func TestParseTrace_AllFixtures(t *testing.T) {
	for _, path := range discoverFixtures(t) {
		path := path
		t.Run(filepath.Base(path), func(t *testing.T) {
			pt := parseFixture(t, path)
			if len(pt.Nodes) == 0 {
				t.Fatalf("no nodes parsed from %s", path)
			}
			if len(pt.ProcessName) == 0 {
				t.Fatalf("no processes parsed from %s", path)
			}
			for _, n := range pt.Nodes {
				if n.SID == "" {
					t.Errorf("node with empty SID in %s", path)
				}
				if n.EndTime != n.StartTime+n.Duration {
					t.Errorf("node %s: EndTime %d != StartTime %d + Duration %d", n.SID, n.EndTime, n.StartTime, n.Duration)
				}
				if _, ok := pt.ProcessName[n.ProcessID]; !ok {
					t.Errorf("node %s: ProcessID %q not found in ProcessName map", n.SID, n.ProcessID)
				}
			}
		})
	}
}

// TestParseTrace_Fixture18 pins the exact decode of test_cases/18.json (9
// linear-ish spans, a mix of bool/string error tags, one span with an
// unrelated log entry) against hand-verified expected values.
func TestParseTrace_Fixture18(t *testing.T) {
	pt := parseFixture(t, filepath.Join(fixturesDir(t), "18.json"))

	wantServices := map[string]string{
		"S1": "S1", "S2": "S2", "S3": "S3", "S4": "S4", "S5": "S5",
		"S6": "S6", "S7": "S7", "S8": "S8", "S9": "S9",
	}
	if len(pt.ProcessName) != len(wantServices) {
		t.Fatalf("ProcessName: got %d entries, want %d", len(pt.ProcessName), len(wantServices))
	}
	for pid, svc := range wantServices {
		if got := pt.ProcessName[pid]; got != svc {
			t.Errorf("ProcessName[%q] = %q, want %q", pid, got, svc)
		}
	}

	if len(pt.Nodes) != 9 {
		t.Fatalf("got %d nodes, want 9", len(pt.Nodes))
	}

	byID := map[string]Node{}
	for _, n := range pt.Nodes {
		byID[n.SID] = n
	}

	// Span A: root, no parent, no error, no tags.
	a := byID["A"]
	if a.ParentSpanID != nil {
		t.Errorf("span A: ParentSpanID = %v, want nil (root)", *a.ParentSpanID)
	}
	if a.StartTime != 0 || a.Duration != 250 || a.EndTime != 250 {
		t.Errorf("span A: StartTime=%d Duration=%d EndTime=%d, want 0/250/250", a.StartTime, a.Duration, a.EndTime)
	}
	if a.ReturnError {
		t.Error("span A: ReturnError = true, want false")
	}

	// Span B: parent A, tag {error, bool, true} -> hasError via truthy value.
	b := byID["B"]
	if b.ParentSpanID == nil || *b.ParentSpanID != "A" {
		t.Errorf("span B: ParentSpanID = %v, want \"A\"", b.ParentSpanID)
	}
	if !b.ReturnError {
		t.Error("span B: ReturnError = false, want true (bool tag value=true)")
	}

	// Span C: parent B, no tags, no error.
	c := byID["C"]
	if c.ParentSpanID == nil || *c.ParentSpanID != "B" {
		t.Errorf("span C: ParentSpanID = %v, want \"B\"", c.ParentSpanID)
	}
	if c.ReturnError {
		t.Error("span C: ReturnError = true, want false")
	}

	// Span D: parent B, tag {error, string, "ClientSideError"} -> hasError via type=="string".
	d := byID["D"]
	if !d.ReturnError {
		t.Error("span D: ReturnError = false, want true (type=string tag)")
	}

	// Span F: parent A, no tags, but a log entry with event=error -- the
	// reference's logs loop flags it (graph.py parseForErrorReturn).
	f := byID["F"]
	if !f.ReturnError {
		t.Error("span F: ReturnError = false, want true (log field event=error)")
	}

	if pt.NumErrors != 6 {
		t.Errorf("NumErrors = %d, want 6 (B,D,E,F,H,I)", pt.NumErrors)
	}
}

func TestGetSpanKind(t *testing.T) {
	cases := []struct {
		name string
		tags []jaeger.Tag
		want SpanKind
	}{
		{"no tags", nil, SpanKindUnknown},
		{"server", []jaeger.Tag{{Key: "span.kind", Value: "server"}}, SpanKindServer},
		{"client", []jaeger.Tag{{Key: "span.kind", Value: "CLIENT"}}, SpanKindClient},
		{"case-insensitive key", []jaeger.Tag{{Key: "Span.Kind", Value: "server"}}, SpanKindServer},
		{"unrecognized value", []jaeger.Tag{{Key: "span.kind", Value: "producer"}}, SpanKindUnknown},
		{
			"first match wins even if unrecognized",
			[]jaeger.Tag{
				{Key: "span.kind", Value: "producer"},
				{Key: "span.kind", Value: "server"},
			},
			SpanKindUnknown,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := getSpanKind(tc.tags)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("getSpanKind() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestGetSpanKind_NonStringValueErrors(t *testing.T) {
	_, err := getSpanKind([]jaeger.Tag{{Key: "span.kind", Value: true}})
	if err == nil {
		t.Fatal("expected an error for a non-string span.kind value")
	}
}

func TestParseForErrorReturn(t *testing.T) {
	cases := []struct {
		name    string
		tags    []jaeger.Tag
		logs    []jaeger.Log
		want    bool
		wantErr bool
	}{
		{name: "no tags", want: false},
		{name: "string type, empty value", tags: []jaeger.Tag{{Key: "error", Type: "string", Value: ""}}, want: true},
		{name: "bool true", tags: []jaeger.Tag{{Key: "error", Type: "bool", Value: true}}, want: true},
		{name: "bool false", tags: []jaeger.Tag{{Key: "error", Type: "bool", Value: false}}, want: false},
		{name: "non-empty string value, non-string type", tags: []jaeger.Tag{{Key: "error", Type: "object", Value: "x"}}, want: true},
		{name: "zero value, non-string type", tags: []jaeger.Tag{{Key: "error", Type: "int64", Value: float64(0)}}, want: false},
		// Numeric tag values decode as json.Number under UseNumber: zero must
		// be falsy exactly like Python's 0.
		{name: "json.Number zero", tags: []jaeger.Tag{{Key: "error", Type: "int64", Value: json.Number("0")}}, want: false},
		{name: "json.Number non-zero", tags: []jaeger.Tag{{Key: "error", Type: "int64", Value: json.Number("1")}}, want: true},
		{name: "wrong key", tags: []jaeger.Tag{{Key: "not-error", Type: "bool", Value: true}}, want: false},
		{
			name: "continues scanning past a non-matching error tag",
			tags: []jaeger.Tag{
				{Key: "error", Type: "int64", Value: json.Number("0")},
				{Key: "error", Type: "string", Value: "boom"},
			},
			want: true,
		},
		// http.status_code: int(value) >= 400, mirroring Python int() on
		// JSON numbers, strings, and bools.
		{name: "http 500 numeric", tags: []jaeger.Tag{{Key: "http.status_code", Type: "int64", Value: json.Number("500")}}, want: true},
		{name: "http 399 numeric", tags: []jaeger.Tag{{Key: "http.status_code", Type: "int64", Value: json.Number("399")}}, want: false},
		{name: "http 500 string", tags: []jaeger.Tag{{Key: "http.status_code", Type: "string", Value: "500"}}, want: true},
		{name: "http float truncates", tags: []jaeger.Tag{{Key: "http.status_code", Type: "float64", Value: json.Number("499.9")}}, want: true},
		{name: "http bool true is 1", tags: []jaeger.Tag{{Key: "http.status_code", Type: "bool", Value: true}}, want: false},
		{name: "http non-numeric string aborts", tags: []jaeger.Tag{{Key: "http.status_code", Type: "string", Value: "abc"}}, wantErr: true},
		// grpc.status: value != "OK" (case-sensitive) and truthy.
		{name: "grpc unavailable", tags: []jaeger.Tag{{Key: "grpc.status", Type: "string", Value: "UNAVAILABLE"}}, want: true},
		{name: "grpc OK", tags: []jaeger.Tag{{Key: "grpc.status", Type: "string", Value: "OK"}}, want: false},
		{name: "grpc lowercase ok still errors", tags: []jaeger.Tag{{Key: "grpc.status", Type: "string", Value: "ok"}}, want: true},
		{name: "grpc zero code is falsy", tags: []jaeger.Tag{{Key: "grpc.status", Type: "int64", Value: json.Number("0")}}, want: false},
		// Logs loop: error.object, error with string type, event=error.
		{name: "log error.object", logs: []jaeger.Log{{Fields: []jaeger.Tag{{Key: "error.object", Type: "string", Value: "x"}}}}, want: true},
		{name: "log error string type", logs: []jaeger.Log{{Fields: []jaeger.Tag{{Key: "error", Type: "string", Value: ""}}}}, want: true},
		{name: "log event error", logs: []jaeger.Log{{Fields: []jaeger.Tag{{Key: "event", Type: "string", Value: "error"}}}}, want: true},
		{name: "log event Error case-insensitive", logs: []jaeger.Log{{Fields: []jaeger.Tag{{Key: "event", Type: "string", Value: "Error"}}}}, want: true},
		{name: "log event ok", logs: []jaeger.Log{{Fields: []jaeger.Tag{{Key: "event", Type: "string", Value: "ok"}}}}, want: false},
		{name: "log event non-string aborts", logs: []jaeger.Log{{Fields: []jaeger.Tag{{Key: "event", Type: "bool", Value: true}}}}, wantErr: true},
		{name: "log event null treated as absent", logs: []jaeger.Log{{Fields: []jaeger.Tag{{Key: "event", Type: "string", Value: nil}}}}, want: false},
		{name: "log unrelated field", logs: []jaeger.Log{{Fields: []jaeger.Tag{{Key: "msg", Type: "string", Value: "error"}}}}, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseForErrorReturn(tc.tags, tc.logs)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected an error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("parseForErrorReturn() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestGetPeerService(t *testing.T) {
	if got := getPeerService(nil); got != nil {
		t.Errorf("getPeerService(nil) = %v, want nil", got)
	}
	tags := []jaeger.Tag{{Key: "peer.service", Value: "downstream-svc"}}
	got := getPeerService(tags)
	if got == nil || *got != "downstream-svc" {
		t.Errorf("getPeerService() = %v, want \"downstream-svc\"", got)
	}
}

func TestParentSpanID_LastChildOfWins(t *testing.T) {
	trace := &jaeger.Trace{Data: []jaeger.TraceData{{
		Processes: map[string]jaeger.Process{"p1": {ServiceName: "svc"}},
		Spans: []jaeger.Span{
			{
				SpanID:    "child",
				ProcessID: "p1",
				StartTime: "0",
				Duration:  "1",
				References: []jaeger.Reference{
					{RefType: "CHILD_OF", SpanID: "first"},
					{RefType: "FOLLOWS_FROM", SpanID: "ignored"},
					{RefType: "CHILD_OF", SpanID: "second"},
				},
			},
		},
	}}}
	pt, err := ParseTrace(trace)
	if err != nil {
		t.Fatal(err)
	}
	if pt.Nodes[0].ParentSpanID == nil || *pt.Nodes[0].ParentSpanID != "second" {
		t.Errorf("ParentSpanID = %v, want \"second\" (last CHILD_OF ref wins)", pt.Nodes[0].ParentSpanID)
	}
}
