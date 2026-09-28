package crisp

// critical_path_segments.go ports crisp/critical_path_segments.py: each
// critical-path span of one trace with the time windows it spends on the
// critical path. The rule and the cp-segments.json format are specified in
// CONFORMANCE.md ("Critical-path segments").
//
// Parity notes:
//   - Times are the nodes' sanitized StartTime/EndTime. EndTime is never
//     recomputed from Duration: sanitization can shorten a server span's
//     Duration to its client's without moving its EndTime.
//   - Exclusive is AccumeCPMetrics' per-span map, rooted at cp[0]. As a
//     time.Duration it saturates at math.MaxInt64 nanoseconds (~292 years);
//     Python's integers do not, so the JSON matches only below that.
//   - canonical_segments_json is json.dumps(sort_keys=True, indent=2,
//     ensure_ascii=False) + "\n"; struct fields below are declared in sorted
//     key order. As in conformance.go, Go escapes U+2028/U+2029, which
//     Python writes verbatim; here span IDs, services, and operations are
//     written as-is, so a name containing either would differ.

import (
	"bytes"
	"cmp"
	"encoding/json"
	"math"
	"slices"
	"time"
)

// CPSegmentsFile mirrors critical_path_segments.CP_SEGMENTS_FILE.
const CPSegmentsFile = "cp-segments.json"

// Segment is a window [Start, End) of a span's time on the critical path.
type Segment struct {
	Start, End time.Time
}

// CriticalPathSpan is one span on a trace's critical path.
type CriticalPathSpan struct {
	SpanID       string
	ParentSpanID string // "" for the analysis root
	Service      string
	Operation    string
	Start, End   time.Time
	// Exclusive is the span's duration minus the durations of its
	// critical-path children, clamped at zero.
	Exclusive time.Duration
	// Segments are the time-ordered windows of the span's time, within its
	// parent's, that no critical-path child covers.
	Segments []Segment
}

// CriticalPathSegments mirrors Graph.criticalPathSegments: a
// CriticalPathSpan for each node of cp, in cp order. cp is a critical path
// as returned by FindCriticalPath: its first node is the analysis root,
// every later node's parent appears earlier in cp, and no node repeats. The
// result is unspecified for any other cp.
func (g *Graph) CriticalPathSegments(cp []*Node) []CriticalPathSpan {
	if len(cp) == 0 {
		return []CriticalPathSpan{}
	}
	root := cp[0]
	_, exclusive := g.AccumeCPMetrics(cp, "", root)

	cpChildren := make(map[string][]*Node, len(cp))
	for _, node := range cp[1:] {
		cpChildren[node.Parent.SID] = append(cpChildren[node.Parent.SID], node)
	}

	type window struct{ lo, hi int64 }
	windows := map[string]window{root.SID: {root.StartTime, root.EndTime}}
	spans := make([]CriticalPathSpan, 0, len(cp))
	for _, node := range cp {
		w := windows[node.SID]
		segments := []Segment{}
		cursor := w.lo
		children := cpChildren[node.SID]
		slices.SortStableFunc(children, func(a, b *Node) int { return cmp.Compare(a.StartTime, b.StartTime) })
		for _, child := range children {
			childLo := min(max(child.StartTime, w.lo), w.hi)
			windows[child.SID] = window{childLo, max(min(child.EndTime, w.hi), childLo)}
			segments = appendClipped(segments, cursor, child.StartTime, w.lo, w.hi)
			cursor = max(cursor, child.EndTime)
		}
		segments = appendClipped(segments, cursor, w.hi, w.lo, w.hi)

		parent := ""
		if node != root {
			parent = node.Parent.SID
		}
		spans = append(spans, CriticalPathSpan{
			SpanID:       node.SID,
			ParentSpanID: parent,
			Service:      g.ProcessName[node.ProcessID],
			Operation:    node.OpName,
			Start:        microTime(node.StartTime),
			End:          microTime(node.EndTime),
			Exclusive:    microDuration(exclusive[node.SID]),
			Segments:     segments,
		})
	}
	return spans
}

func appendClipped(segments []Segment, start, end, lo, hi int64) []Segment {
	start, end = max(start, lo), min(end, hi)
	if end > start {
		segments = append(segments, Segment{microTime(start), microTime(end)})
	}
	return segments
}

func microTime(us int64) time.Time {
	return time.UnixMicro(us).UTC()
}

// microDuration converts non-negative microseconds, saturating instead of
// overflowing.
func microDuration(us int64) time.Duration {
	if us > math.MaxInt64/int64(time.Microsecond) {
		return math.MaxInt64
	}
	return time.Duration(us) * time.Microsecond
}

type cpSegmentsDoc struct {
	Spans []cpSegmentsSpan `json:"spans"`
}

type cpSegmentsSpan struct {
	EndTime      int64      `json:"endTime"`
	Exclusive    int64      `json:"exclusive"`
	Operation    string     `json:"operation"`
	ParentSpanID *string    `json:"parentSpanID"`
	Segments     [][2]int64 `json:"segments"`
	Service      string     `json:"service"`
	SpanID       string     `json:"spanID"`
	StartTime    int64      `json:"startTime"`
}

// CanonicalCPSegmentsJSON mirrors
// critical_path_segments.canonical_segments_json.
func CanonicalCPSegmentsJSON(spans []CriticalPathSpan) (string, error) {
	doc := cpSegmentsDoc{Spans: make([]cpSegmentsSpan, 0, len(spans))}
	for _, s := range spans {
		var parent *string
		if s.ParentSpanID != "" {
			p := s.ParentSpanID
			parent = &p
		}
		segments := make([][2]int64, 0, len(s.Segments))
		for _, seg := range s.Segments {
			segments = append(segments, [2]int64{seg.Start.UnixMicro(), seg.End.UnixMicro()})
		}
		doc.Spans = append(doc.Spans, cpSegmentsSpan{
			EndTime:      s.End.UnixMicro(),
			Exclusive:    s.Exclusive.Microseconds(),
			Operation:    s.Operation,
			ParentSpanID: parent,
			Segments:     segments,
			Service:      s.Service,
			SpanID:       s.SpanID,
			StartTime:    s.Start.UnixMicro(),
		})
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(doc); err != nil {
		return "", err
	}
	return buf.String(), nil
}
