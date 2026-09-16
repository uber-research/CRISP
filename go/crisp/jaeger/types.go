// Package jaeger decodes Jaeger's HTTP-API trace JSON format. Field names and
// shapes mirror crisp/constants.py and the "data[].processes/spans" wire
// format consumed by crisp/graph.py's parseNode.
package jaeger

import (
	"bytes"
	"encoding/json"
)

// Trace is the top-level Jaeger JSON document: {"data": [...]}.
type Trace struct {
	Data []TraceData `json:"data"`
}

// TraceData is one element of the "data" array. In practice CRISP traces have
// exactly one element, but the format allows more.
type TraceData struct {
	TraceID   string             `json:"traceID"`
	Processes map[string]Process `json:"processes"`
	Spans     []Span             `json:"spans"`
}

// Process is a Jaeger process/service descriptor.
type Process struct {
	ServiceName string `json:"serviceName"`
	Tags        []Tag  `json:"tags"`
}

// Span is a single Jaeger span.
//
// StartTime and Duration are microsecond integers and MUST decode as exact
// int64 values, never float64 (see CONFORMANCE.md determinism rule 4) --
// json.Number is used here specifically to defer that decision to the caller.
type Span struct {
	TraceID       string      `json:"traceID"`
	SpanID        string      `json:"spanID"`
	OperationName string      `json:"operationName"`
	References    []Reference `json:"references"`
	StartTime     json.Number `json:"startTime"`
	Duration      json.Number `json:"duration"`
	ProcessID     string      `json:"processID"`
	Tags          []Tag       `json:"tags"`
	Logs          []Log       `json:"logs"`
}

// Reference links a span to another span in the same trace.
type Reference struct {
	RefType string `json:"refType"`
	TraceID string `json:"traceID"`
	SpanID  string `json:"spanID"`
}

// Tag is a Jaeger key/value tag. Value is left as the raw decoded JSON value
// (string, float64, bool, nil, []any, or map[string]any) because Python's
// dynamically-typed tag values are compared and tested for truthiness without
// a fixed type -- see IsTruthy and the span.kind / error classification
// helpers in classify.go.
type Tag struct {
	Key   string `json:"key"`
	Type  string `json:"type"`
	Value any    `json:"value"`
}

// Log is a Jaeger span log entry. parseForErrorReturn scans Fields for
// error.object / error / event=error markers (see classify.go), so fields are
// decoded and consumed, not merely structural.
type Log struct {
	Timestamp json.Number `json:"timestamp"`
	Fields    []Tag       `json:"fields"`
}

// StartTimeMicros returns the span's start time as an exact int64 microsecond
// value. Decoding via json.Number and ParseInt (rather than float64) avoids
// precision loss for large epoch-microsecond timestamps.
func (s Span) StartTimeMicros() (int64, error) {
	return s.StartTime.Int64()
}

// DurationMicros returns the span's duration as an exact int64 microsecond
// value.
func (s Span) DurationMicros() (int64, error) {
	return s.Duration.Int64()
}

// Decode parses raw Jaeger trace JSON. Numbers are decoded via json.Number so
// callers can convert to int64 exactly instead of through float64.
func Decode(data []byte) (*Trace, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var t Trace
	if err := dec.Decode(&t); err != nil {
		return nil, err
	}
	return &t, nil
}
