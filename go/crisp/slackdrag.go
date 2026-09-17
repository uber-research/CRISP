package crisp

// slackdrag.go ports the drag half of crisp/slack_drag.py (calculate_drag,
// _exclusive_cp_time, aggregate_drag_slack_by_callpath,
// merge_per_method_slack_drag) and the pandas-based writer
// crisp/output/csv_generators.py genSlackDragCSVFile.
//
// Scope: slack (calculate_slack) is NOT ported — it requires the
// DependencyGraph machinery and is only consumed by this informational CSV
// behind --computeSlackDrag. Slack columns are always 0.0 here, matching
// Python when the flag is off.
//
// Parity notes:
//   - PerMethodSlackDrag totals accumulate from Python's [0, 0.0, 0.0]
//     seed, so total/avg columns are always floats; spanCount is always an
//     int. Go uses float64 throughout (all inputs are microsecond int64s,
//     exactly representable).
//   - CSV floats render with CPython repr semantics (shortest round-trip,
//     ".0" for integral values, scientific outside [1e-4, 1e16)); see
//     pyFloatRepr.
//   - Python sorts rows with pandas sort_values(kind='quicksort'), whose
//     tie order is not stable for larger inputs; Go uses a stable sort.
//     Row order can therefore diverge from Python when many call paths
//     share the exact same avgDrag. Row contents are always identical, and
//     this file is informational only (not part of the conformance
//     contract).

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// SlackDragCSV mirrors common.SLACK_DRAG_CSV.
const SlackDragCSV = "slackDrag.csv"

// Drag mirrors slack_drag.Drag: per-span drag for critical-path nodes only.
type Drag struct {
	// PerSpan maps span ID to drag; only nodes on the critical path get an
	// entry (nodes off the CP conceptually have zero drag).
	PerSpan map[string]float64
	Total   float64
}

// exclusiveCPTime mirrors slack_drag._exclusive_cp_time: each CP node's
// duration minus the durations of its CP children, negatives clamped to 0.
func exclusiveCPTime(cp []*Node) map[string]float64 {
	exclusiveTime := make(map[string]float64)
	for i := len(cp) - 1; i >= 0; i-- {
		node := cp[i]
		exclusiveTime[node.SID] += float64(node.Duration)
		if node.Parent != nil {
			exclusiveTime[node.Parent.SID] += float64(-node.Duration)
		}
	}
	for sid, v := range exclusiveTime {
		if v < 0 {
			exclusiveTime[sid] = 0.0
		}
	}
	return exclusiveTime
}

// CalculateDrag mirrors slack_drag.calculate_drag (inclusive mode when
// exclusive is false). See the Python docstring for the definition; in
// short, a CP node's drag is how much its duration could shrink before a
// sibling takes over as the critical child, capped at its own duration.
func (g *Graph) CalculateDrag(cp []*Node, exclusive bool) *Drag {
	var exclusiveCP map[string]float64
	if exclusive {
		exclusiveCP = exclusiveCPTime(cp)
	}

	dragPerSpan := make(map[string]float64)
	// sortedCache memoizes each parent's end-time-descending child order so
	// CP nodes sharing a parent sort its children once, not once per node
	// (Python re-sorts per node; the memoized result is identical because
	// the stable-sort-then-reverse idiom is deterministic for a fixed input
	// order). On traces whose critical path threads through a very wide
	// parent this avoids an O(|cp| x fanout log fanout) blowup.
	sortedCache := map[*Node][]*Node{}
	sortedChildren := func(n *Node) []*Node {
		if s, ok := sortedCache[n]; ok {
			return s
		}
		s := sortedByEndTimeDesc(n.Children)
		sortedCache[n] = s
		return s
	}
	for _, node := range cp {
		ownMetric := float64(node.Duration)
		if exclusive {
			ownMetric = exclusiveCP[node.SID]
		}

		parent := node.Parent
		if parent == nil {
			dragPerSpan[node.SID] = ownMetric
			continue
		}

		if len(parent.Children) == 1 {
			dragPerSpan[node.SID] = ownMetric
			continue
		}

		// Same idiom as computeCriticalPath: stable ascending sort by
		// endTime, then reverse, so ties break identically.
		sortedSiblings := sortedChildren(parent)
		nodeIdx := -1
		for idx, sibling := range sortedSiblings {
			if sibling == node {
				nodeIdx = idx
				break
			}
		}
		if nodeIdx == -1 || nodeIdx == len(sortedSiblings)-1 {
			// Not found (defensive) or earliest-ending sibling: uncapped.
			dragPerSpan[node.SID] = ownMetric
			continue
		}

		nextSibling := sortedSiblings[nodeIdx+1]
		var dragVal float64
		if !exclusive || len(node.Children) == 0 {
			dragVal = float64(node.EndTime - nextSibling.EndTime)
		} else {
			ownCPChild := sortedChildren(node)[0]
			dragVal = float64(node.EndTime - max64(ownCPChild.EndTime, nextSibling.EndTime))
			if ownCPChild.StartTime > nextSibling.EndTime {
				dragVal += float64(ownCPChild.StartTime - nextSibling.EndTime)
			}
		}
		dragPerSpan[node.SID] = math.Max(0.0, math.Min(ownMetric, dragVal))
	}

	total := 0.0
	for _, v := range dragPerSpan {
		total += v
	}
	return &Drag{PerSpan: dragPerSpan, Total: total}
}

// sortedByEndTimeDesc mirrors Python's sorted(nodes, key=endTime)[::-1]:
// stable ascending sort, then full reversal (ties end in reverse document
// order).
func sortedByEndTimeDesc(nodes []*Node) []*Node {
	out := make([]*Node, len(nodes))
	copy(out, nodes)
	sort.SliceStable(out, func(i, j int) bool { return out[i].EndTime < out[j].EndTime })
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// PerMethodSlackDrag mirrors slack_drag.PerMethodSlackDrag.
type PerMethodSlackDrag struct {
	CallPath   string
	SpanCount  int64
	TotalDrag  float64
	AvgDrag    float64
	TotalSlack float64
	AvgSlack   float64
}

// SlackDragByCallpath is an insertion-ordered set of per-call-path
// aggregates (Python dict order matters for CSV row order).
type SlackDragByCallpath struct {
	ByPath map[string]*PerMethodSlackDrag
	Order  []string
}

func newSlackDragByCallpath() *SlackDragByCallpath {
	return &SlackDragByCallpath{ByPath: make(map[string]*PerMethodSlackDrag)}
}

// sumEntry mirrors _sum_slack_drag_by_callpath's accumulation of one
// (call_path, span_count, drag, slack) entry.
func (s *SlackDragByCallpath) sumEntry(callPath string, spanCount int64, drag, slack float64) {
	agg, ok := s.ByPath[callPath]
	if !ok {
		agg = &PerMethodSlackDrag{CallPath: callPath}
		s.ByPath[callPath] = agg
		s.Order = append(s.Order, callPath)
	}
	agg.SpanCount += spanCount
	agg.TotalDrag += drag
	agg.TotalSlack += slack
}

// deriveAverages mirrors _sum_slack_drag_by_callpath's final averaging.
func (s *SlackDragByCallpath) deriveAverages() {
	for _, k := range s.Order {
		agg := s.ByPath[k]
		if agg.SpanCount != 0 {
			agg.AvgDrag = agg.TotalDrag / float64(agg.SpanCount)
			agg.AvgSlack = agg.TotalSlack / float64(agg.SpanCount)
		}
	}
}

// AggregateDragSlackByCallpath mirrors slack_drag.aggregate_drag_slack_by_
// callpath: group every node in the graph by call path and sum drag/slack.
// Slack is always 0 (calculate_slack is not ported; see file docstring).
// Iteration follows nodeHT insertion order (Python dict order).
func (g *Graph) AggregateDragSlackByCallpath(drag *Drag) *SlackDragByCallpath {
	result := newSlackDragByCallpath()
	for _, sid := range g.nodeOrder {
		node := g.NodeHT[sid]
		result.sumEntry(g.getCallPath(node), 1, drag.PerSpan[node.SID], 0.0)
	}
	result.deriveAverages()
	return result
}

// MergePerMethodSlackDrag mirrors slack_drag.merge_per_method_slack_drag:
// sum per-trace aggregates per call path, then re-derive averages.
func MergePerMethodSlackDrag(perTrace []*SlackDragByCallpath) *SlackDragByCallpath {
	merged := newSlackDragByCallpath()
	for _, per := range perTrace {
		for _, callPath := range per.Order {
			agg := per.ByPath[callPath]
			merged.sumEntry(callPath, agg.SpanCount, agg.TotalDrag, agg.TotalSlack)
		}
	}
	merged.deriveAverages()
	return merged
}

// pyFloatRepr renders f the way CPython's repr(float) does (which is what
// pandas to_csv writes): shortest round-trip digits, fixed notation for
// decimal point positions in (-4, 16], scientific otherwise, ".0" for
// integral values, exponent with sign and at least two digits.
func pyFloatRepr(f float64) string {
	if math.IsInf(f, 1) {
		return "inf"
	}
	if math.IsInf(f, -1) {
		return "-inf"
	}
	if math.IsNaN(f) {
		return "nan"
	}
	if f == 0 {
		if math.Signbit(f) {
			return "-0.0"
		}
		return "0.0"
	}

	// Shortest round-trip digits in scientific form: [-]d[.ddd]e±XX.
	s := strconv.FormatFloat(f, 'e', -1, 64)
	neg := false
	if s[0] == '-' {
		neg = true
		s = s[1:]
	}
	eIdx := strings.IndexByte(s, 'e')
	mant := s[:eIdx]
	exp, _ := strconv.Atoi(s[eIdx+1:])
	digits := strings.Replace(mant, ".", "", 1)
	// decpt: value == 0.d1d2... * 10^decpt (CPython _Py_dg_dtoa convention).
	decpt := exp + 1

	var out string
	if decpt > -4 && decpt <= 16 {
		switch {
		case decpt <= 0:
			out = "0." + strings.Repeat("0", -decpt) + digits
		case decpt >= len(digits):
			out = digits + strings.Repeat("0", decpt-len(digits)) + ".0"
		default:
			out = digits[:decpt] + "." + digits[decpt:]
		}
	} else {
		m := digits[:1]
		if len(digits) > 1 {
			m += "." + digits[1:]
		}
		e := decpt - 1
		sign := "+"
		if e < 0 {
			sign = "-"
			e = -e
		}
		out = fmt.Sprintf("%se%s%02d", m, sign, e)
	}
	if neg {
		return "-" + out
	}
	return out
}

// GenSlackDragCSV mirrors csv_generators.py genSlackDragCSVFile: write the
// per-call-path drag/slack CSV sorted by descending avgDrag. Returns the
// file path, or "" without writing when there is no data (Python returns
// None). Rows are sorted stably; see the file docstring for the pandas
// quicksort tie-order caveat.
func GenSlackDragCSV(perMethod *SlackDragByCallpath, outputDir, filename string) (string, error) {
	if filename == "" {
		filename = SlackDragCSV
	}
	if perMethod == nil || len(perMethod.Order) == 0 {
		return "", nil
	}

	rows := make([]*PerMethodSlackDrag, 0, len(perMethod.Order))
	for _, k := range perMethod.Order {
		rows = append(rows, perMethod.ByPath[k])
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].AvgDrag > rows[j].AvgDrag })

	var buf bytes.Buffer
	writeCSVRecord(&buf, []string{"callPath", "spanCount", "avgDrag", "totalDrag", "avgSlack", "totalSlack"})
	for _, agg := range rows {
		writeCSVRecord(&buf, []string{
			agg.CallPath,
			strconv.FormatInt(agg.SpanCount, 10),
			pyFloatRepr(agg.AvgDrag),
			pyFloatRepr(agg.TotalDrag),
			pyFloatRepr(agg.AvgSlack),
			pyFloatRepr(agg.TotalSlack),
		})
	}

	path := filepath.Join(outputDir, filename)
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		return "", err
	}
	return path, nil
}

// writeCSVRecord writes one CSV record with Python csv.QUOTE_MINIMAL
// semantics: a field is quoted only when it contains a comma, double
// quote, \n, or \r; quotes inside are doubled; records end with \n.
// (Go's encoding/csv additionally quotes fields with a leading space,
// which Python does not.)
func writeCSVRecord(buf *bytes.Buffer, fields []string) {
	for i, field := range fields {
		if i > 0 {
			buf.WriteByte(',')
		}
		if strings.ContainsAny(field, ",\"\n\r") {
			buf.WriteByte('"')
			buf.WriteString(strings.ReplaceAll(field, "\"", "\"\""))
			buf.WriteByte('"')
		} else {
			buf.WriteString(field)
		}
	}
	buf.WriteByte('\n')
}
