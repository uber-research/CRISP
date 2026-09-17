package crisp

// conformance.go ports crisp/conformance.py (canonical_cct,
// canonical_response_json, build_conformance_response,
// write_conformance_outputs) and the parsing/response helpers from
// crisp/cct_utils.py (parse_cct_line, parse_call_path_part,
// create_protobuf_response_with_exemplars, _build_exemplar_lookup,
// _cct_key_to_profile_key).
//
// Parity notes:
//   - canonical_response_json renders json_format.MessageToDict(response,
//     preserving_proto_field_name=True) via json.dumps(sort_keys=True,
//     indent=2, ensure_ascii=False) + "\n". Instead of generated protobuf
//     stubs, the response is built directly as the map MessageToDict would
//     produce: snake_case field names, default-valued scalar fields omitted
//     ("", 0, empty repeated), message fields present when set. The proto
//     schema is crisp/proto/analyzer.proto.
//   - google.protobuf.Duration renders as a JSON string per the proto3
//     JSON spec: "<seconds>s" with 0/3/6/9 fractional digits chosen by
//     nanosecond divisibility (see durationJSON).
//   - Go's json.Encoder with SetEscapeHTML(false) matches Python's
//     ensure_ascii=False except that Go still escapes U+2028/U+2029 in
//     strings; those cannot appear in practice (they would have to survive
//     Jaeger span/operation names into call paths).

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Conformance output file names (crisp/conformance.py).
const (
	ConformanceCCTFile  = "conformance.cct"
	ConformanceJSONFile = "conformance.json"
)

// CanonicalCCT mirrors conformance.py canonical_cct: drop empty lines,
// sort by code point (identical to UTF-8 byte order), terminate with "\n".
func CanonicalCCT(flameGraphStr string) string {
	var lines []string
	for _, line := range strings.Split(flameGraphStr, "\n") {
		if line != "" {
			lines = append(lines, line)
		}
	}
	sort.Strings(lines)
	if len(lines) == 0 {
		return ""
	}
	return strings.Join(lines, "\n") + "\n"
}

// timingPattern mirrors cct_utils.py TIMING_PATTERN.
var timingPattern = regexp.MustCompile(`(\d+)\s*<<(\d+)>>$`)

// CCTCallPath is one parsed "[service] operation" call-path segment.
type CCTCallPath struct {
	Service   string
	Operation string
}

// CCTSummary mirrors the dict returned by cct_utils.py parse_cct_line.
type CCTSummary struct {
	CallPath  []CCTCallPath
	Duration  int64
	Frequency int64
}

// ParseCallPathPart mirrors cct_utils.py parse_call_path_part. The second
// return value is false when the part is not a "[service] operation" span.
func ParseCallPathPart(part string) (CCTCallPath, bool) {
	part = strings.TrimSpace(part)
	if !strings.HasPrefix(part, "[") {
		return CCTCallPath{}, false
	}
	serviceEnd := strings.Index(part, "]")
	if serviceEnd == -1 {
		return CCTCallPath{}, false
	}
	service := part[1:serviceEnd]
	operation := strings.TrimSpace(part[serviceEnd+1:])
	return CCTCallPath{Service: service, Operation: operation}, true
}

// ParseCCTLine mirrors cct_utils.py parse_cct_line: parse a folded-stack
// line into call path, duration, and frequency. Returns nil for empty or
// malformed lines (Python returns {}).
func ParseCCTLine(line string) *CCTSummary {
	line = strings.TrimSpace(line)
	if line == "" {
		return nil
	}
	parts := strings.Split(line, ";")
	lastPart := parts[len(parts)-1]
	loc := timingPattern.FindStringSubmatchIndex(lastPart)
	if loc == nil {
		return nil
	}
	// The pattern is \d+, so these cannot fail.
	duration, _ := strconv.ParseInt(lastPart[loc[2]:loc[3]], 10, 64)
	frequency, _ := strconv.ParseInt(lastPart[loc[4]:loc[5]], 10, 64)
	lastPart = strings.TrimSpace(lastPart[:loc[0]])

	var callPath []CCTCallPath
	for _, part := range parts[:len(parts)-1] {
		if cp, ok := ParseCallPathPart(part); ok {
			callPath = append(callPath, cp)
		}
	}
	if cp, ok := ParseCallPathPart(lastPart); ok {
		callPath = append(callPath, cp)
	}
	if len(callPath) == 0 {
		return nil
	}
	return &CCTSummary{CallPath: callPath, Duration: duration, Frequency: frequency}
}

// cctKeyToProfileKey mirrors cct_utils.py _cct_key_to_profile_key.
func cctKeyToProfileKey(callPath []CCTCallPath) string {
	parts := make([]string, 0, len(callPath))
	for _, cp := range callPath {
		parts = append(parts, "["+cp.Service+"] "+cp.Operation)
	}
	return strings.Join(parts, "->")
}

// buildExemplarLookup mirrors cct_utils.py _build_exemplar_lookup,
// including its guard: no lookup when mergedCpp is nil or maxExemplars
// is 0 (or negative) — so no exemplars field is emitted at all.
func buildExemplarLookup(mergedCpp *CallPathProfile, maxExemplars int) map[string][][2]string {
	lookup := make(map[string][][2]string)
	if mergedCpp == nil || maxExemplars <= 0 {
		return lookup
	}
	for _, path := range mergedCpp.Order {
		exemplars := mergedCpp.Profile[path].Exemplars
		if len(exemplars) == 0 {
			continue
		}
		if len(exemplars) > maxExemplars {
			exemplars = exemplars[:maxExemplars]
		}
		lookup[path] = exemplars
	}
	return lookup
}

// durationJSON renders a microsecond count the way
// json_format.MessageToDict renders a google.protobuf.Duration built via
// FromMicroseconds: seconds + 0/3/6/9 fractional digits by nanosecond
// divisibility, e.g. 50µs -> "0.000050s". Microsecond inputs never need 9
// digits (nanos is always a multiple of 1000).
func durationJSON(micros int64) string {
	seconds := micros / 1_000_000
	nanos := (micros % 1_000_000) * 1000
	switch {
	case nanos == 0:
		return strconv.FormatInt(seconds, 10) + "s"
	case nanos%1_000_000 == 0:
		return strconv.FormatInt(seconds, 10) + "." + padLeft(strconv.FormatInt(nanos/1_000_000, 10), 3) + "s"
	default: // nanos%1_000 == 0 always holds for microsecond inputs
		return strconv.FormatInt(seconds, 10) + "." + padLeft(strconv.FormatInt(nanos/1_000, 10), 6) + "s"
	}
}

// padLeft left-pads s with zeros to width (Python's f"{v:03d}" style).
func padLeft(s string, width int) string {
	for len(s) < width {
		s = "0" + s
	}
	return s
}

// BuildConformanceResponse mirrors conformance.py build_conformance_response
// plus cct_utils.py create_protobuf_response_with_exemplars: parse the
// sorted CCT lines and build the AnalyzeResponse as the map that
// json_format.MessageToDict(preserving_proto_field_name=True) would
// produce. Fields with proto3 default values are omitted, matching
// MessageToDict's default elision.
func BuildConformanceResponse(canonicalCCTStr string, mergedCpp *CallPathProfile, maxExemplars int) map[string]any {
	var summaries []*CCTSummary
	for _, line := range strings.Split(canonicalCCTStr, "\n") {
		if summary := ParseCCTLine(line); summary != nil {
			summaries = append(summaries, summary)
		}
	}
	exemplarLookup := buildExemplarLookup(mergedCpp, maxExemplars)

	entries := make([]any, 0, len(summaries))
	for _, summary := range summaries {
		callPath := make([]any, 0, len(summary.CallPath))
		for i, cp := range summary.CallPath {
			m := make(map[string]any)
			if cp.Service != "" {
				m["service"] = cp.Service
			}
			if cp.Operation != "" {
				m["operation_name"] = cp.Operation
			}
			if i == len(summary.CallPath)-1 {
				key := cctKeyToProfileKey(summary.CallPath)
				if exemplars, ok := exemplarLookup[key]; ok {
					exList := make([]any, 0, len(exemplars))
					for _, ex := range exemplars {
						em := make(map[string]any)
						if ex[0] != "" {
							em["trace_id"] = ex[0]
						}
						if ex[1] != "" {
							em["span_id"] = ex[1]
						}
						exList = append(exList, em)
					}
					m["exemplars"] = exList
				}
			}
			callPath = append(callPath, m)
		}
		base := map[string]any{"duration": durationJSON(summary.Duration)}
		if summary.Frequency != 0 {
			base["frequency"] = summary.Frequency
		}
		entries = append(entries, map[string]any{"call_path": callPath, "base": base})
	}

	response := make(map[string]any)
	if len(entries) > 0 {
		// An empty repeated field is elided by MessageToDict.
		response["report_window_1"] = entries
	}
	return response
}

// CanonicalResponseJSON mirrors conformance.py canonical_response_json:
// json.dumps(d, sort_keys=True, indent=2, ensure_ascii=False) + "\n".
// Go's json.Encoder sorts map keys byte-wise (identical to code-point
// order for UTF-8) and appends exactly one newline.
func CanonicalResponseJSON(response map[string]any) (string, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(response); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// WriteConformanceOutputs mirrors conformance.py write_conformance_outputs:
// write conformance.cct and conformance.json into outputDir and return
// their paths.
func WriteConformanceOutputs(outputDir, flameGraphStr string, mergedCpp *CallPathProfile, maxExemplars int) (string, string, error) {
	cctStr := CanonicalCCT(flameGraphStr)
	cctPath := filepath.Join(outputDir, ConformanceCCTFile)
	if err := os.WriteFile(cctPath, []byte(cctStr), 0o644); err != nil {
		return "", "", err
	}

	response := BuildConformanceResponse(cctStr, mergedCpp, maxExemplars)
	jsonStr, err := CanonicalResponseJSON(response)
	if err != nil {
		return "", "", err
	}
	jsonPath := filepath.Join(outputDir, ConformanceJSONFile)
	if err := os.WriteFile(jsonPath, []byte(jsonStr), 0o644); err != nil {
		return "", "", err
	}
	return cctPath, jsonPath, nil
}
