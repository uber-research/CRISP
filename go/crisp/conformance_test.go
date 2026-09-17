package crisp

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestCanonicalCCT(t *testing.T) {
	raw := "b 2 <<1>>\na 1 <<1>>\n"
	if got := CanonicalCCT(raw); got != "a 1 <<1>>\nb 2 <<1>>\n" {
		t.Errorf("got %q", got)
	}
	// Empty inputs produce empty output (no trailing newline).
	if got := CanonicalCCT(""); got != "" {
		t.Errorf("empty: got %q", got)
	}
	if got := CanonicalCCT("\n\n"); got != "" {
		t.Errorf("blank lines: got %q", got)
	}
	// Blank lines interleaved are dropped.
	if got := CanonicalCCT("b 2 <<1>>\n\na 1 <<1>>\n\n"); got != "a 1 <<1>>\nb 2 <<1>>\n" {
		t.Errorf("interleaved blanks: got %q", got)
	}
}

func TestParseCCTLine(t *testing.T) {
	s := ParseCCTLine("[S1] O1;[S2] O2 70 <<2>>")
	if s == nil {
		t.Fatal("nil summary")
	}
	if s.Duration != 70 || s.Frequency != 2 {
		t.Errorf("got dur=%d freq=%d, want 70/2", s.Duration, s.Frequency)
	}
	want := []CCTCallPath{{"S1", "O1"}, {"S2", "O2"}}
	if !reflect.DeepEqual(s.CallPath, want) {
		t.Errorf("call path = %v, want %v", s.CallPath, want)
	}

	// Extra whitespace before the timing is fine (\s* and strip).
	s = ParseCCTLine("[S1] O1 5   <<3>>")
	if s == nil || s.Duration != 5 || s.Frequency != 3 {
		t.Errorf("whitespace variant: %+v", s)
	}

	// No timing suffix -> nil (Python returns {}).
	if ParseCCTLine("[S1] O1") != nil {
		t.Error("no timing: want nil")
	}
	// Empty / whitespace-only -> nil.
	if ParseCCTLine("") != nil || ParseCCTLine("   ") != nil {
		t.Error("empty: want nil")
	}
	// A line with a timing but no parseable call path -> nil.
	if ParseCCTLine("junk 5 <<1>>") != nil {
		t.Error("no call path: want nil")
	}
	// Malformed middle parts are skipped, not fatal.
	s = ParseCCTLine("garbage;[S2] O2 5 <<1>>")
	if s == nil || len(s.CallPath) != 1 || s.CallPath[0].Service != "S2" {
		t.Errorf("malformed middle: %+v", s)
	}
}

func TestParseCallPathPart(t *testing.T) {
	cp, ok := ParseCallPathPart("  [svc] op name  ")
	if !ok || cp.Service != "svc" || cp.Operation != "op name" {
		t.Errorf("got %+v ok=%v", cp, ok)
	}
	if _, ok := ParseCallPathPart("svc] op"); ok {
		t.Error("missing [ should fail")
	}
	if _, ok := ParseCallPathPart("[svc op"); ok {
		t.Error("missing ] should fail")
	}
	// Empty service is parseable (elided later at JSON build time).
	cp, ok = ParseCallPathPart("[] op")
	if !ok || cp.Service != "" || cp.Operation != "op" {
		t.Errorf("empty service: got %+v ok=%v", cp, ok)
	}
}

func TestDurationJSON(t *testing.T) {
	// Cross-checked against google.protobuf json_format.MessageToDict.
	cases := []struct {
		micros int64
		want   string
	}{
		{0, "0s"},
		{5, "0.000005s"},
		{50, "0.000050s"},
		{1000, "0.001s"},
		{1500, "0.001500s"},
		{1000000, "1s"},
		{1234567, "1.234567s"},
		{123456789, "123.456789s"},
	}
	for _, c := range cases {
		if got := durationJSON(c.micros); got != c.want {
			t.Errorf("durationJSON(%d) = %q, want %q", c.micros, got, c.want)
		}
	}
}

func TestBuildConformanceResponse(t *testing.T) {
	cct := "[S1] O1 50 <<1>>\n[S1] O1;[S2] O2 70 <<2>>\n"
	merged := NewCallPathProfile(0, "")
	leaf := NewMetricVals(70, 70, 2, "spanB")
	leaf.Exemplars = [][2]string{{"t1", "spanB"}}
	merged.Upsert("[S1] O1->[S2] O2", leaf)
	merged.Upsert("[S1] O1", NewMetricVals(50, 50, 1, "spanA"))

	resp := BuildConformanceResponse(cct, merged, 3)
	entries, ok := resp["report_window_1"].([]any)
	if !ok || len(entries) != 2 {
		t.Fatalf("report_window_1 = %v", resp["report_window_1"])
	}

	// First entry (sorted CCT order): [S1] O1, no exemplars on it (only the
	// leaf of the second entry has them).
	e0 := entries[0].(map[string]any)
	cp0 := e0["call_path"].([]any)
	if len(cp0) != 1 {
		t.Fatalf("entry 0 call_path = %v", cp0)
	}
	m0 := cp0[0].(map[string]any)
	if m0["service"] != "S1" || m0["operation_name"] != "O1" {
		t.Errorf("entry 0 = %v", m0)
	}
	if _, hasExemplars := m0["exemplars"]; hasExemplars {
		t.Errorf("entry 0 should have no exemplars: %v", m0)
	}
	base0 := e0["base"].(map[string]any)
	if base0["duration"] != "0.000050s" || base0["frequency"] != int64(1) {
		t.Errorf("entry 0 base = %v", base0)
	}

	// Second entry: leaf gets the exemplars.
	e1 := entries[1].(map[string]any)
	cp1 := e1["call_path"].([]any)
	leafMap := cp1[1].(map[string]any)
	exs, ok := leafMap["exemplars"].([]any)
	if !ok || len(exs) != 1 {
		t.Fatalf("leaf exemplars = %v", leafMap)
	}
	ex := exs[0].(map[string]any)
	if ex["trace_id"] != "t1" || ex["span_id"] != "spanB" {
		t.Errorf("exemplar = %v", ex)
	}
	// Non-leaf entry has no exemplars.
	if _, hasExemplars := cp1[0].(map[string]any)["exemplars"]; hasExemplars {
		t.Error("non-leaf should have no exemplars")
	}
}

func TestBuildConformanceResponseElisions(t *testing.T) {
	// Empty input -> no report_window_1 key at all (MessageToDict elides
	// empty repeated fields).
	resp := BuildConformanceResponse("", nil, 3)
	if len(resp) != 0 {
		t.Errorf("empty: got %v", resp)
	}

	// frequency 0 is elided; empty service is elided.
	resp = BuildConformanceResponse("[] op 5 <<0>>\n", nil, 3)
	entries := resp["report_window_1"].([]any)
	e := entries[0].(map[string]any)
	base := e["base"].(map[string]any)
	if _, has := base["frequency"]; has {
		t.Errorf("frequency 0 should be elided: %v", base)
	}
	cp := e["call_path"].([]any)[0].(map[string]any)
	if _, has := cp["service"]; has {
		t.Errorf("empty service should be elided: %v", cp)
	}
	if cp["operation_name"] != "op" {
		t.Errorf("operation_name = %v", cp)
	}
}

func TestBuildConformanceResponseExemplarsDisabled(t *testing.T) {
	// maxExemplars=0: no exemplars field even when the merged profile
	// carries exemplars (Python _build_exemplar_lookup returns {} early).
	cct := "[S1] O1 50 <<1>>\n"
	merged := NewCallPathProfile(0, "")
	leaf := NewMetricVals(50, 50, 1, "spanA")
	leaf.Exemplars = [][2]string{{"t1", "spanA"}}
	merged.Upsert("[S1] O1", leaf)

	resp := BuildConformanceResponse(cct, merged, 0)
	entries := resp["report_window_1"].([]any)
	m := entries[0].(map[string]any)["call_path"].([]any)[0].(map[string]any)
	if _, has := m["exemplars"]; has {
		t.Errorf("exemplars should be elided with maxExemplars=0: %v", m)
	}
}

func TestCanonicalResponseJSON(t *testing.T) {
	// Sorted keys, 2-space indent, trailing newline, no HTML escaping.
	resp := map[string]any{
		"b": "x<y>&z",
		"a": []any{map[string]any{"k": 1}},
	}
	got, err := CanonicalResponseJSON(resp)
	if err != nil {
		t.Fatal(err)
	}
	want := "{\n  \"a\": [\n    {\n      \"k\": 1\n    }\n  ],\n  \"b\": \"x<y>&z\"\n}\n"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestWriteConformanceOutputs(t *testing.T) {
	dir := t.TempDir()
	cctPath, jsonPath, err := WriteConformanceOutputs(dir, "b 2 <<1>>\na 1 <<1>>\n", nil, 3)
	if err != nil {
		t.Fatal(err)
	}
	if cctPath != filepath.Join(dir, ConformanceCCTFile) || jsonPath != filepath.Join(dir, ConformanceJSONFile) {
		t.Errorf("paths = (%s, %s)", cctPath, jsonPath)
	}
	cct, _ := os.ReadFile(cctPath)
	if string(cct) != "a 1 <<1>>\nb 2 <<1>>\n" {
		t.Errorf("cct = %q", cct)
	}
	js, _ := os.ReadFile(jsonPath)
	// Lines "a 1 <<1>>" / "b 2 <<1>>" have no parseable call paths, so the
	// response is empty and renders as "{}\n".
	if string(js) != "{}\n" {
		t.Errorf("json = %q", js)
	}
}
