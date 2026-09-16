package crisp

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/uber-research/CRISP/go/crisp/jaeger"
)

// SpanKind mirrors crisp.shared.models.SpanKind.
type SpanKind int

const (
	SpanKindUnknown SpanKind = iota
	SpanKindServer
	SpanKindClient
)

const (
	tagKeySpanKind       = "span.kind"
	tagKeyPeerService    = "peer.service"
	tagKeyError          = "error"
	tagKeyHTTPStatusCode = "http.status_code"
	tagKeyGRPCStatus     = "grpc.status"
	tagTypeString        = "string"
	tagValueServer       = "server"
	tagValueClient       = "client"
	tagValueOK           = "OK"
	logKeyErrorObject    = "error.object"
	logKeyEvent          = "event"
	logValueError        = "error"
)

// tagString returns (s, true) if v decoded as a JSON string, else ("", false).
func tagString(v any) (string, bool) {
	s, ok := v.(string)
	return s, ok
}

// getSpanKind mirrors graph.py Graph.getSpanKind: it returns on the FIRST tag
// whose key case-insensitively equals "span.kind", even if that tag's value
// doesn't match "server" or "client" (in which case it returns Unknown
// without considering any later span.kind tag).
//
// If that first matching tag's value is not a JSON string, this returns an
// error: Python calls v.lower() unconditionally, which raises for a
// non-string value and aborts the whole trace's parse (caught and logged by
// Graph.__init__). Callers should treat this the same way.
func getSpanKind(tags []jaeger.Tag) (SpanKind, error) {
	for _, t := range tags {
		if !strings.EqualFold(t.Key, tagKeySpanKind) {
			continue
		}
		v, ok := tagString(t.Value)
		if !ok {
			return SpanKindUnknown, &nonStringTagValueError{key: t.Key}
		}
		switch {
		case strings.EqualFold(v, tagValueServer):
			return SpanKindServer, nil
		case strings.EqualFold(v, tagValueClient):
			return SpanKindClient, nil
		default:
			return SpanKindUnknown, nil
		}
	}
	return SpanKindUnknown, nil
}

// getPeerService mirrors graph.py Graph.getPeerService: returns the value of
// the FIRST tag whose key case-insensitively equals "peer.service", or nil if
// none is found. Unlike getSpanKind, Python never calls .lower() on this
// value, so a non-string value doesn't crash Python -- but peerService is not
// read anywhere downstream in crisp/graph.py today, so a non-string value
// here is treated as absent rather than plumbing through an `any`.
func getPeerService(tags []jaeger.Tag) *string {
	for _, t := range tags {
		if !strings.EqualFold(t.Key, tagKeyPeerService) {
			continue
		}
		if v, ok := tagString(t.Value); ok {
			return &v
		}
		return nil
	}
	return nil
}

// isTruthy mirrors Python truthiness for a JSON-decoded value: None/nil,
// false, zero numbers, empty strings, and empty arrays/objects are falsy;
// everything else is truthy.
func isTruthy(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case string:
		return x != ""
	case json.Number:
		f, err := x.Float64()
		if err != nil {
			// Unreachable from json.Decode; treat a hand-built json.Number
			// like the string it wraps (non-empty string is truthy).
			return x.String() != ""
		}
		return f != 0
	case float64:
		return x != 0
	case []any:
		return len(x) > 0
	case map[string]any:
		return len(x) > 0
	default:
		return true
	}
}

// pyInt mirrors Python's int(v) for the JSON-decoded values that can reach it:
// bools (int(True) == 1), numbers (truncating toward zero, as int() does for
// floats), and strings (optional surrounding whitespace and sign, base 10 --
// anything else is a ValueError in Python, surfaced here as an error). A nil
// value behaves like int(False) == 0, matching Python's missing-value default
// in parseForErrorReturn; see its doc comment for the explicit-null caveat.
func pyInt(v any) (int64, error) {
	switch x := v.(type) {
	case nil:
		return 0, nil
	case bool:
		if x {
			return 1, nil
		}
		return 0, nil
	case json.Number:
		if i, err := x.Int64(); err == nil {
			return i, nil
		}
		f, err := x.Float64()
		if err != nil {
			return 0, err
		}
		return int64(f), nil
	case float64:
		return int64(x), nil
	case string:
		i, err := strconv.ParseInt(strings.TrimSpace(x), 10, 64)
		if err != nil {
			return 0, fmt.Errorf("int(%q): %v", x, err)
		}
		return i, nil
	default:
		return 0, fmt.Errorf("int() of %T value", v)
	}
}

// pyStringNE mirrors Python's v != s: true when v is a different string, and
// always true when v is not a string at all (Python values of different types
// are never equal). Note the comparison is case-SENSITIVE, as in Python.
func pyStringNE(v any, s string) bool {
	vs, ok := v.(string)
	return !ok || vs != s
}

// parseForErrorReturn mirrors graph.py Graph.parseForErrorReturn: it scans ALL
// span tags and ALL log fields (it does not stop at the first key match) and
// returns true as soon as any of these hold:
//
//	tags: key "error" with (type "string" OR truthy value)
//	tags: key "http.status_code" with int(value) >= 400
//	tags: key "grpc.status" with value != "OK" and a truthy value
//	logs: field key "error.object" (value not read)
//	logs: field key "error" with type "string"
//	logs: field key "event" with value.lower() == "error"
//
// Keys and the compared types/values are matched case-insensitively unless
// noted (Python lowercases them first); the grpc.status comparison is
// case-sensitive, as in Python. Following Python, a non-numeric
// http.status_code value or a non-string event value raises there and aborts
// the whole trace's parse (caught by Graph.__init__); an error return here
// should be treated the same way.
//
// Known micro-divergence: after JSON decoding, an explicit null value is
// indistinguishable from a missing value key, so null is treated as missing.
// Python crashes on null in two of these checks (int(None), None.lower()) and
// discards the whole trace. Real Jaeger tags and log fields always carry a
// value, so this only affects hand-crafted inputs.
func parseForErrorReturn(tags []jaeger.Tag, logs []jaeger.Log) (bool, error) {
	for _, t := range tags {
		switch {
		case strings.EqualFold(t.Key, tagKeyError):
			if strings.EqualFold(t.Type, tagTypeString) || isTruthy(t.Value) {
				return true, nil
			}
		case strings.EqualFold(t.Key, tagKeyHTTPStatusCode):
			code, err := pyInt(t.Value)
			if err != nil {
				return false, fmt.Errorf("http.status_code tag: %w", err)
			}
			if code >= 400 {
				return true, nil
			}
		case strings.EqualFold(t.Key, tagKeyGRPCStatus):
			if pyStringNE(t.Value, tagValueOK) && isTruthy(t.Value) {
				return true, nil
			}
		}
	}
	for _, entry := range logs {
		for _, f := range entry.Fields {
			switch {
			case strings.EqualFold(f.Key, logKeyErrorObject):
				return true, nil
			case strings.EqualFold(f.Key, tagKeyError):
				if strings.EqualFold(f.Type, tagTypeString) {
					return true, nil
				}
			case strings.EqualFold(f.Key, logKeyEvent):
				// Python reads value.lower() unconditionally when the key
				// exists, so a non-string value aborts the trace's parse.
				if f.Value == nil {
					continue // treated as absent; see doc comment
				}
				v, ok := f.Value.(string)
				if !ok {
					return false, &nonStringTagValueError{key: f.Key}
				}
				if strings.EqualFold(v, logValueError) {
					return true, nil
				}
			}
		}
	}
	return false, nil
}

type nonStringTagValueError struct {
	key string
}

func (e *nonStringTagValueError) Error() string {
	return "tag " + e.key + " has a non-string value"
}
