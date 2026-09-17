package crisp

import (
	"strings"
	"testing"
)

func TestCCTToDot(t *testing.T) {
	var summaries []*CCTSummary
	for _, line := range []string{
		"[S1] O1 50 <<1>>",
		"[S1] O1;[S2] O2 70 <<2>>",
		"[S1] O1;[S2] O2;[S3] O3 10 <<1>>",
	} {
		s := ParseCCTLine(line)
		if s == nil {
			t.Fatalf("unparseable line %q", line)
		}
		summaries = append(summaries, s)
	}

	// Cross-checked against Python cct_to_dot.
	want := `digraph CCT {
    rankdir=TB;
    node [shape=box, style=filled, fillcolor=lightyellow, fontname="Helvetica", fontsize=10];
    edge [fontname="Helvetica", fontsize=8];

    n0 [label="[S1] O1\nincl: 130µs\nexcl: 50µs\nfreq: 1"];
    n1 [label="[S2] O2\nincl: 80µs\nexcl: 70µs\nfreq: 2"];
    n2 [label="[S3] O3\nincl: 10µs\nexcl: 10µs\nfreq: 1"];

    n0 -> n1;
    n1 -> n2;
}
`
	if got := CCTToDot(summaries); got != want {
		t.Errorf("got:\n%s\nwant:\n%s", got, want)
	}
}

func TestCCTToDotEmpty(t *testing.T) {
	if got := CCTToDot(nil); got != "digraph CCT {\n}\n" {
		t.Errorf("got %q", got)
	}
}

func TestCCTToDotEscapesLabels(t *testing.T) {
	s := ParseCCTLine(`[S"1] o\p 5 <<1>>`)
	if s == nil {
		t.Fatal("nil summary")
	}
	got := CCTToDot([]*CCTSummary{s})
	// Backslash doubled, quote escaped (in that order).
	want := `    n0 [label="[S\"1] o\\p\nincl: 5µs\nexcl: 5µs\nfreq: 1"];`
	if !strings.Contains(got, want) {
		t.Errorf("got:\n%s\nwant substring:\n%s", got, want)
	}
}
