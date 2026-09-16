package crisp

import (
	"encoding/json"
	"testing"

	"github.com/uber-research/CRISP/go/crisp/jaeger"
)

func span(id, processID, opName, start string, refs ...jaeger.Reference) jaeger.Span {
	return jaeger.Span{
		SpanID:        id,
		ProcessID:     processID,
		OperationName: opName,
		StartTime:     json.Number(start),
		Duration:      json.Number("1"),
		References:    refs,
	}
}

func childOf(spanID string) jaeger.Reference {
	return jaeger.Reference{RefType: "CHILD_OF", SpanID: spanID}
}

func TestDeriveRootSpan(t *testing.T) {
	procs := map[string]jaeger.Process{
		"p1": {ServiceName: "svc-a"},
		"p2": {ServiceName: "svc-b"},
	}

	t.Run("single root with no references", func(t *testing.T) {
		trace := &jaeger.Trace{Data: []jaeger.TraceData{{
			Processes: procs,
			Spans: []jaeger.Span{
				span("root", "p1", "GET /", "100"),
				span("child", "p2", "GET /db", "150", childOf("root")),
			},
		}}}
		svc, op, err := DeriveRootSpan(trace)
		if err != nil {
			t.Fatal(err)
		}
		if svc != "svc-a" || op != "GET /" {
			t.Errorf("got (%q, %q), want (%q, %q)", svc, op, "svc-a", "GET /")
		}
	})

	t.Run("orphan CHILD_OF is a root candidate", func(t *testing.T) {
		trace := &jaeger.Trace{Data: []jaeger.TraceData{{
			Processes: procs,
			Spans: []jaeger.Span{
				span("orphan", "p2", "GET /resumed", "200", childOf("not-in-trace")),
			},
		}}}
		svc, _, err := DeriveRootSpan(trace)
		if err != nil {
			t.Fatal(err)
		}
		if svc != "svc-b" {
			t.Errorf("got service %q, want %q", svc, "svc-b")
		}
	})

	t.Run("startTime tie broken by spanID", func(t *testing.T) {
		trace := &jaeger.Trace{Data: []jaeger.TraceData{{
			Processes: procs,
			Spans: []jaeger.Span{
				span("b-span", "p1", "op-b", "100"),
				span("a-span", "p2", "op-a", "100"),
			},
		}}}
		_, op, err := DeriveRootSpan(trace)
		if err != nil {
			t.Fatal(err)
		}
		if op != "op-a" {
			t.Errorf("got operation %q, want %q (spanID tie-break)", op, "op-a")
		}
	})

	t.Run("earliest startTime wins over span order", func(t *testing.T) {
		trace := &jaeger.Trace{Data: []jaeger.TraceData{{
			Processes: procs,
			Spans: []jaeger.Span{
				span("late", "p1", "op-late", "300"),
				span("early", "p2", "op-early", "50"),
			},
		}}}
		_, op, err := DeriveRootSpan(trace)
		if err != nil {
			t.Fatal(err)
		}
		if op != "op-early" {
			t.Errorf("got operation %q, want %q", op, "op-early")
		}
	})

	t.Run("only first data element is considered", func(t *testing.T) {
		trace := &jaeger.Trace{Data: []jaeger.TraceData{
			{
				Processes: procs,
				Spans:     []jaeger.Span{span("first", "p1", "op-first", "100")},
			},
			{
				Processes: procs,
				Spans:     []jaeger.Span{span("second", "p2", "op-second", "10")},
			},
		}}
		_, op, err := DeriveRootSpan(trace)
		if err != nil {
			t.Fatal(err)
		}
		if op != "op-first" {
			t.Errorf("got operation %q, want %q (data[1] must be ignored)", op, "op-first")
		}
	})

	t.Run("error when every span has an in-trace parent", func(t *testing.T) {
		trace := &jaeger.Trace{Data: []jaeger.TraceData{{
			Processes: procs,
			Spans: []jaeger.Span{
				span("a", "p1", "op-a", "100", childOf("b")),
				span("b", "p2", "op-b", "200", childOf("a")),
			},
		}}}
		if _, _, err := DeriveRootSpan(trace); err == nil {
			t.Fatal("expected an error for a trace with no root candidate")
		}
	})

	t.Run("error on empty data", func(t *testing.T) {
		if _, _, err := DeriveRootSpan(&jaeger.Trace{}); err == nil {
			t.Fatal("expected an error for an empty trace")
		}
	})
}
