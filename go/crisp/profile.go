package crisp

// profile.go ports the call-path profile data model from
// crisp/shared/models.py (MetricVals, CallPathProfile) and the per-trace
// accumulation from crisp/graph.py (accumeCPMetrics, getCallPath and
// helpers). These are the inputs to the folded-CCT output stage.
//
// Parity notes:
//   - Python dicts iterate in insertion order; CallPathProfile keeps an
//     explicit order slice so folded-CCT emission matches byte-for-byte.
//   - Python's // is floor division (rounds toward -inf); Go's / truncates
//     toward zero. floorDiv mirrors Python exactly (matters only if a
//     negative value slips through before Sanitize).
//   - Python uses the int -1 as the "no span" sentinel for sids; str(-1)
//     renders as "-1" in exemplar output, so Go uses the string "-1".

// noSIDSentinel mirrors Python's sid=-1 sentinel in MetricVals (str(-1)).
const noSIDSentinel = "-1"

// floorDiv mirrors Python's // operator on ints (floor toward -inf).
func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}

// MetricVals mirrors crisp/shared/models.py MetricVals: inclusive and
// exclusive times, frequency, and the worst-case exemplar span IDs.
type MetricVals struct {
	Inc  int64
	Excl int64
	Freq int64
	// IncEx/ExclEx are the span IDs of the worst inclusive/exclusive
	// measurements; IncExVal/ExclExVal are their values.
	IncEx     string
	ExclEx    string
	IncExVal  int64
	ExclExVal int64
	// IncTrace/ExclTrace are set only by MergeCallPathProfilesWithExemplars
	// (Python adds them as dynamic attributes in MergeMetricValsWithTrace).
	IncTrace  string
	ExclTrace string
	// Exemplars holds (traceID, spanID) pairs, ranked by exclusive time.
	Exemplars [][2]string
}

// NewMetricVals mirrors MetricVals(inc, excl, freq, sid): the exemplar
// values start at the given inc/excl and the exemplar sids at sid.
func NewMetricVals(inc, excl, freq int64, sid string) *MetricVals {
	return &MetricVals{
		Inc:       inc,
		Excl:      excl,
		Freq:      freq,
		IncEx:     sid,
		ExclEx:    sid,
		IncExVal:  inc,
		ExclExVal: excl,
	}
}

// Add mirrors MetricVals.__iadd__: sums the values and keeps the
// worst-case exemplars (strict >, so the first sid wins ties).
func (m *MetricVals) Add(o *MetricVals) {
	m.Inc += o.Inc
	m.Excl += o.Excl
	m.Freq += o.Freq
	if o.IncExVal > m.IncExVal {
		m.IncEx = o.IncEx
		m.IncExVal = o.IncExVal
	}
	if o.ExclExVal > m.ExclExVal {
		m.ExclEx = o.ExclEx
		m.ExclExVal = o.ExclExVal
	}
}

// FloorDiv mirrors MetricVals.__ifloordiv__ (Python floor division).
func (m *MetricVals) FloorDiv(val int64) {
	m.Inc = floorDiv(m.Inc, val)
	m.Excl = floorDiv(m.Excl, val)
	m.Freq = floorDiv(m.Freq, val)
}

// CallPathProfile mirrors crisp/shared/models.py CallPathProfile: metrics
// per call path with insertion order preserved (Python dict order).
type CallPathProfile struct {
	Profile map[string]*MetricVals
	// Order holds the Profile keys in first-insertion order.
	Order []string
	Count int64
	// TraceID mirrors the traceId constructor argument (informational).
	TraceID string
}

// NewCallPathProfile mirrors CallPathProfile({}, count, traceId).
func NewCallPathProfile(count int64, traceID string) *CallPathProfile {
	return &CallPathProfile{
		Profile: make(map[string]*MetricVals),
		Count:   count,
		TraceID: traceID,
	}
}

// Upsert mirrors CallPathProfile.Upsert: adds into an existing entry or
// inserts a (shallow) copy of metric without changing the count. The copy
// mirrors Python's copy.copy: the Exemplars slice header is shared.
func (c *CallPathProfile) Upsert(path string, metric *MetricVals) {
	if existing, ok := c.Profile[path]; ok {
		existing.Add(metric)
		return
	}
	cp := *metric
	c.Profile[path] = &cp
	c.Order = append(c.Order, path)
}

// Add mirrors CallPathProfile.__iadd__: upserts every entry of other (in
// other's insertion order) and adds other's count.
func (c *CallPathProfile) Add(other *CallPathProfile) {
	for _, path := range other.Order {
		c.Upsert(path, other.Profile[path])
	}
	c.Count += other.Count
}

// NormalizeExcl mirrors CallPathProfile.NormalizeField("excl"): floor-
// divides every entry's Excl by the profile count. Only "excl" is ever
// normalized in the Python codebase.
func (c *CallPathProfile) NormalizeExcl() {
	for _, k := range c.Order {
		v := c.Profile[k]
		v.Excl = floorDiv(v.Excl, c.Count)
	}
}

// SanitizeExcl mirrors CallPathProfile.Sanitize("excl", ...): clamps
// negative exclusive times to zero. Python's debug branch references an
// unset attribute and would raise AttributeError if reached with
// debug_on=True; that dead branch is intentionally not ported.
func (c *CallPathProfile) SanitizeExcl() {
	for _, k := range c.Order {
		if v := c.Profile[k]; v.Excl < 0 {
			v.Excl = 0
		}
	}
}

// canonicalOpName mirrors crisp/graph.py canonicalOpName.
func canonicalOpName(processName map[string]string, node *Node) string {
	return "[" + processName[node.ProcessID] + "] " + node.OpName
}

// appendCallPath mirrors crisp/graph.py appendCallPath.
func appendCallPath(callPath, opName string) string {
	if callPath == "" {
		return opName
	}
	return callPath + "->" + opName
}

// getCallPath mirrors crisp/graph.py getCallPath: the canonical op names
// from the root down to node, joined by "->".
func (g *Graph) getCallPath(node *Node) string {
	if node.Parent == nil {
		return canonicalOpName(g.ProcessName, node)
	}
	return appendCallPath(g.getCallPath(node.Parent), canonicalOpName(g.ProcessName, node))
}

// AccumeCPMetrics mirrors crisp/graph.py accumeCPMetrics: walks the
// critical path in reverse, accumulating inclusive/exclusive times per
// call path. Each node contributes its full duration as inclusive and
// exclusive to its own call path, then subtracts its duration from the
// parent call path's exclusive time. Returns the profile (count 1) and
// the per-span exclusive-time map.
func (g *Graph) AccumeCPMetrics(criticalPath []*Node, traceID string, rootNode *Node) (*CallPathProfile, map[string]int64) {
	if rootNode == nil {
		rootNode = g.RootNode
	}
	cpp := NewCallPathProfile(1, traceID)
	sidTimeExclusive := make(map[string]int64)
	for i := len(criticalPath) - 1; i >= 0; i-- {
		n := criticalPath[i]
		sid := n.SID
		opCallpath := g.getCallPath(n)
		cpp.Upsert(opCallpath, NewMetricVals(n.Duration, n.Duration, 1, sid))
		sidTimeExclusive[sid] += n.Duration
		if n == rootNode {
			continue
		}
		parentCC := g.getCallPath(n.Parent)
		cpp.Upsert(parentCC, NewMetricVals(0, -n.Duration, 0, noSIDSentinel))
		sidTimeExclusive[n.Parent.SID] += -n.Duration
	}
	cpp.SanitizeExcl()
	// sanitizeExclusiveTime: clamp negative exclusive times to zero.
	for k, v := range sidTimeExclusive {
		if v < 0 {
			sidTimeExclusive[k] = 0
		}
	}
	return cpp, sidTimeExclusive
}
