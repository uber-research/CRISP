package crisp

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/uber-research/CRISP/go/crisp/proto/analyzer"
)

func parseLines(t *testing.T, lines ...string) []*CCTSummary {
	t.Helper()
	var summaries []*CCTSummary
	for _, line := range lines {
		if s := ParseCCTLine(line); s != nil {
			summaries = append(summaries, s)
		}
	}
	return summaries
}

func TestCreateProtobufResponseWithExemplars(t *testing.T) {
	summaries := parseLines(t, "[S1] O1 50 <<1>>", "[S1] O1;[S2] O2 70 <<2>>")
	merged := NewCallPathProfile(0, "")
	leaf := NewMetricVals(70, 70, 2, "spanB")
	leaf.Exemplars = [][2]string{{"t1", "spanB"}}
	merged.Upsert("[S1] O1->[S2] O2", leaf)

	resp := CreateProtobufResponseWithExemplars(summaries, merged, 3)
	if len(resp.GetReportWindow_1()) != 2 {
		t.Fatalf("entries = %d, want 2", len(resp.GetReportWindow_1()))
	}
	e1 := resp.GetReportWindow_1()[1]
	if got := e1.GetBase().GetDuration().AsDuration().Microseconds(); got != 70 {
		t.Errorf("duration = %d, want 70", got)
	}
	if e1.GetBase().GetFrequency() != 2 {
		t.Errorf("frequency = %d, want 2", e1.GetBase().GetFrequency())
	}
	exs := e1.GetCallPath()[1].GetExemplars()
	if len(exs) != 1 || exs[0].GetTraceId() != "t1" || exs[0].GetSpanId() != "spanB" {
		t.Errorf("exemplars = %v", exs)
	}
	// Non-leaf has no exemplars.
	if len(e1.GetCallPath()[0].GetExemplars()) != 0 {
		t.Error("non-leaf should have no exemplars")
	}

	// Byte-level parity with Python's SerializeToString (generated from
	// crisp/cct_utils.py create_protobuf_response_with_exemplars).
	got, err := proto.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	wantHex := "0a140a080a02533112024f3122080a0410d0860310010a2b0a080a02533112024f310a150a02533212024f322a0b0a02743112057370616e4222080a0410f0a2041002"
	if hex.EncodeToString(got) != wantHex {
		t.Errorf("pb bytes = %x, want %s", got, wantHex)
	}

	// Without a merged profile: no exemplars anywhere.
	resp2 := CreateProtobufResponseWithExemplars(summaries, nil, 3)
	got2, _ := proto.Marshal(resp2)
	wantHex2 := "0a140a080a02533112024f3122080a0410d0860310010a1e0a080a02533112024f310a080a02533212024f3222080a0410f0a2041002"
	if hex.EncodeToString(got2) != wantHex2 {
		t.Errorf("pb (no exemplars) = %x, want %s", got2, wantHex2)
	}
}

func TestWriteCCTOutputs(t *testing.T) {
	dir := t.TempDir()
	cctFile := filepath.Join(dir, "light-flame-graph-P100.cct")
	flame := "[S1] O1 50 <<1>>\n[S1] O1;[S2] O2 70 <<2>>\n"
	if err := WriteCCTOutputs(cctFile, flame, nil, 3); err != nil {
		t.Fatal(err)
	}

	cct, err := os.ReadFile(cctFile)
	if err != nil {
		t.Fatal(err)
	}
	if string(cct) != flame {
		t.Errorf("cct = %q", cct)
	}

	dot, err := os.ReadFile(filepath.Join(dir, "light-flame-graph-P100.dot"))
	if err != nil {
		t.Fatal(err)
	}
	wantDot := `digraph CCT {
    rankdir=TB;
    node [shape=box, style=filled, fillcolor=lightyellow, fontname="Helvetica", fontsize=10];
    edge [fontname="Helvetica", fontsize=8];

    n0 [label="[S1] O1\nincl: 120µs\nexcl: 50µs\nfreq: 1"];
    n1 [label="[S2] O2\nincl: 70µs\nexcl: 70µs\nfreq: 2"];

    n0 -> n1;
}
`
	if string(dot) != wantDot {
		t.Errorf("dot =:\n%s\nwant:\n%s", dot, wantDot)
	}

	pbData, err := os.ReadFile(filepath.Join(dir, "light-flame-graph-P100.pb"))
	if err != nil {
		t.Fatal(err)
	}
	var resp analyzer.AnalyzeResponse
	if err := proto.Unmarshal(pbData, &resp); err != nil {
		t.Fatalf("pb does not decode: %v", err)
	}
	if len(resp.GetReportWindow_1()) != 2 {
		t.Errorf("pb entries = %d, want 2", len(resp.GetReportWindow_1()))
	}
}

func TestWriteCCTOutputsEmpty(t *testing.T) {
	// No valid traces: Python writes an empty .cct, an empty digraph .dot,
	// and a zero-byte .pb.
	dir := t.TempDir()
	cctFile := filepath.Join(dir, "light-flame-graph-P100.cct")
	if err := WriteCCTOutputs(cctFile, "", nil, 3); err != nil {
		t.Fatal(err)
	}
	dot, _ := os.ReadFile(filepath.Join(dir, "light-flame-graph-P100.dot"))
	if string(dot) != "digraph CCT {\n}\n" {
		t.Errorf("dot = %q", dot)
	}
	pbData, _ := os.ReadFile(filepath.Join(dir, "light-flame-graph-P100.pb"))
	if len(pbData) != 0 {
		t.Errorf("pb = %x, want empty", pbData)
	}
}
