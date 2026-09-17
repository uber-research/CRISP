package crisp

// aggregate.go ports the cross-trace aggregation from crisp/flamegraph.py
// (aggregateCallPathProfiles, getParentCallPath, MIN_TIME_METRIC_VALUE) and
// crisp/metrics/aggregators.py (MergeCallPathProfilesWithExemplars,
// MergeMetricValsWithTrace).
//
// Parity notes:
//   - The exemplar top-N selection uses Python's heapq with tuple
//     comparison; the final list is sorted by exclusive value descending
//     with a STABLE sort over the heap array. The heap array order is not
//     arrival order, so the heap operations are replicated exactly
//     (siftdown/siftup as in CPython's heapq.py) rather than substituted
//     with a simpler top-N algorithm.

import (
	"errors"
	"sort"
	"strconv"
	"strings"
)

// minTimeMetricValue mirrors flamegraph.py MIN_TIME_METRIC_VALUE: leaf
// call paths with zero exclusive time are bumped to this value.
const minTimeMetricValue = 1

// GetParentCallPath mirrors flamegraph.py getParentCallPath: everything
// before the last "->", or "" if there is none.
func GetParentCallPath(callpath string) string {
	idx := strings.LastIndex(callpath, "->")
	if idx < 0 {
		return ""
	}
	return callpath[:idx]
}

// AggregateCallPathProfiles mirrors flamegraph.py aggregateCallPathProfiles
// (the CallPathProfile branch of aggregateCCTs): merges the per-trace
// profiles, averages exclusive times by the merged count, bumps zero-value
// leaf call paths to minTimeMetricValue, and renders the folded-stack CCT
// text ("path;to;leaf <excl> <<freq>>" per line, insertion order).
func AggregateCallPathProfiles(cpps []*CallPathProfile) (string, error) {
	cpp := NewCallPathProfile(0, "")
	for _, c := range cpps {
		cpp.Add(c)
	}
	if cpp.Count == 0 {
		// Python's NormalizeField raises Exception("NormalizeField called
		// with zero count.").
		return "", errors.New("NormalizeField called with zero count")
	}
	cpp.NormalizeExcl()

	parentCallpaths := make(map[string]bool)
	for _, k := range cpp.Order {
		if p := GetParentCallPath(k); p != "" {
			parentCallpaths[p] = true
		}
	}
	for _, k := range cpp.Order {
		v := cpp.Profile[k]
		if !parentCallpaths[k] && v.Excl == 0 {
			v.Excl = minTimeMetricValue
		}
	}

	var sb strings.Builder
	for _, k := range cpp.Order {
		v := cpp.Profile[k]
		// Python: k.replace(";", "_").replace("->", ";") — replacement
		// order matters (";" introduced by the second replace must not be
		// re-replaced).
		newKey := strings.ReplaceAll(strings.ReplaceAll(k, ";", "_"), "->", ";")
		sb.WriteString(newKey + " " + strconv.FormatInt(v.Excl, 10) + " <<" + strconv.FormatInt(v.Freq, 10) + ">>\n")
	}
	return sb.String(), nil
}

// TraceMetrics carries the subset of crisp/shared/models.py Metrics that
// MergeCallPathProfilesWithExemplars consumes.
type TraceMetrics struct {
	TraceID   string
	CPMetrics *CallPathProfile
}

// exemplarEntry mirrors the (exclExVal, str(traceID), str(exclEx)) tuples
// pushed onto the Python heapq min-heap.
type exemplarEntry struct {
	val     int64
	traceID string
	spanID  string
}

// exemplarLess mirrors Python tuple comparison for exemplarEntry.
func exemplarLess(a, b exemplarEntry) bool {
	if a.val != b.val {
		return a.val < b.val
	}
	if a.traceID != b.traceID {
		return a.traceID < b.traceID
	}
	return a.spanID < b.spanID
}

// heapPush mirrors heapq.heappush.
func heapPush(h []exemplarEntry, item exemplarEntry) []exemplarEntry {
	h = append(h, item)
	siftDown(h, 0, len(h)-1)
	return h
}

// heapReplace mirrors heapq.heapreplace (the popped minimum is unused).
func heapReplace(h []exemplarEntry, item exemplarEntry) {
	h[0] = item
	siftUp(h, 0)
}

// siftDown mirrors heapq._siftdown.
func siftDown(h []exemplarEntry, startpos, pos int) {
	newitem := h[pos]
	for pos > startpos {
		parentpos := (pos - 1) >> 1
		parent := h[parentpos]
		if exemplarLess(newitem, parent) {
			h[pos] = parent
			pos = parentpos
			continue
		}
		break
	}
	h[pos] = newitem
}

// siftUp mirrors heapq._siftup: bubbles the smaller child all the way to a
// leaf (no early break, as in CPython), then sifts the displaced item down.
// Note `not heap[childpos] < heap[rightpos]` picks the RIGHT child on ties.
func siftUp(h []exemplarEntry, pos int) {
	endpos := len(h)
	startpos := pos
	newitem := h[pos]
	childpos := 2*pos + 1
	for childpos < endpos {
		rightpos := childpos + 1
		if rightpos < endpos && !exemplarLess(h[childpos], h[rightpos]) {
			childpos = rightpos
		}
		h[pos] = h[childpos]
		pos = childpos
		childpos = 2*pos + 1
	}
	h[pos] = newitem
	siftDown(h, startpos, pos)
}

// mergeMetricValsWithTrace mirrors aggregators.py MergeMetricValsWithTrace.
func mergeMetricValsWithTrace(a, b *MetricVals, bTrace string) {
	a.Inc += b.Inc
	a.Excl += b.Excl
	a.Freq += b.Freq
	if b.IncExVal > a.IncExVal {
		a.IncEx = b.IncEx
		a.IncTrace = bTrace
		a.IncExVal = b.IncExVal
	}
	if b.ExclExVal > a.ExclExVal {
		a.ExclEx = b.ExclEx
		a.ExclTrace = bTrace
		a.ExclExVal = b.ExclExVal
	}
}

// MergeCallPathProfilesWithExemplars mirrors aggregators.py
// MergeCallPathProfilesWithExemplars: merges all per-trace profiles into
// one, and when maxExemplars > 0 collects the top-N (traceID, spanID)
// exemplars per call path ranked by exclusive time.
func MergeCallPathProfilesWithExemplars(metrics []*TraceMetrics, maxExemplars int) *CallPathProfile {
	result := NewCallPathProfile(0, "")
	var heaps map[string][]exemplarEntry
	if maxExemplars > 0 {
		heaps = make(map[string][]exemplarEntry)
	}

	for _, m := range metrics {
		for _, callPath := range m.CPMetrics.Order {
			metric := m.CPMetrics.Profile[callPath]
			if existing, ok := result.Profile[callPath]; ok {
				mergeMetricValsWithTrace(existing, metric, m.TraceID)
			} else {
				// copy.copy(metric), then attach the trace IDs.
				cp := *metric
				cp.ExclTrace = m.TraceID
				cp.IncTrace = m.TraceID
				result.Profile[callPath] = &cp
				result.Order = append(result.Order, callPath)
				if heaps != nil {
					heaps[callPath] = nil
				}
			}

			if heaps != nil {
				entry := exemplarEntry{metric.ExclExVal, m.TraceID, metric.ExclEx}
				heap := heaps[callPath]
				if len(heap) < maxExemplars {
					heap = heapPush(heap, entry)
				} else if entry.val > heap[0].val {
					heapReplace(heap, entry)
				}
				heaps[callPath] = heap
			}
		}
		result.Count += m.CPMetrics.Count
	}

	if heaps != nil {
		for callPath, heap := range heaps {
			// sorted(heap, key=val, reverse=True): stable, so ties keep
			// heap-array order.
			sort.SliceStable(heap, func(i, j int) bool { return heap[i].val > heap[j].val })
			exemplars := make([][2]string, 0, len(heap))
			for _, e := range heap {
				exemplars = append(exemplars, [2]string{e.traceID, e.spanID})
			}
			result.Profile[callPath].Exemplars = exemplars
		}
	}

	return result
}
