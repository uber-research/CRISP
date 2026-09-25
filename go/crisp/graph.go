package crisp

import (
	"fmt"

	"github.com/uber-research/CRISP/go/crisp/jaeger"
)

// ServiceOp identifies an operation within a service; used as the
// ExclusionSet key (Python uses (serviceName, opName) tuples).
type ServiceOp struct {
	Service string
	Op      string
}

// TagFilter mirrors one entry of the CLI/config tag filter list:
// {name, value, search_depth} in Python (see crisp/common.py and
// crisp/shared/constants.py). Value is dynamically typed and compared with
// Python == semantics (pyEqual).
type TagFilter struct {
	Name        string
	Value       any
	SearchDepth int
}

// GraphOptions carries Graph constructor knobs that are keyword arguments in
// Python. A nil *GraphOptions is equivalent to all defaults.
type GraphOptions struct {
	// Filename is informational only (used in diagnostic messages), as in
	// Python. Python also records the file size; that is only consumed by
	// the not-yet-ported output stage and is omitted here.
	Filename string
	// RootTrace mirrors the rootTrace argument: when true (the Python
	// default, applied when this field is nil), the trace must have exactly
	// one root and it must match ServiceName/OperationName; when false, the
	// first matching node in a DFS from each potential root becomes the
	// root.
	RootTrace *bool
	// FilterProxy enables proxy short-wiring and error-propagation-node
	// handling (see span_utils.go's configurable lists).
	FilterProxy bool
	// Tags is the tag filter list matched against the tree after
	// construction (GetMatchingTagsInTree).
	Tags []TagFilter
	// ExclusionSet lists (service, operation) subtrees to drop from the
	// graph after sanitization.
	ExclusionSet []ServiceOp
	// Config overrides analysis tunables; nil applies Python defaults.
	Config *AnalysisConfig
	// ErrorBreakdown, when set, computes Graph.ErrorBreakdown.
	ErrorBreakdown *ErrorBreakdownOptions
	// RootSpanID mirrors the rootSpanId argument: when set, that span
	// becomes the root and its service and operation replace
	// ServiceName/OperationName, so a different span with the same names
	// cannot be chosen instead. It implies RootTrace=false; an unknown span
	// ID leaves RootNode nil. An ErrorBreakdownTraceRoot breakdown still
	// covers the whole trace; ErrorBreakdownAnalysisRoot uses this span.
	RootSpanID string
}

// Graph mirrors the construction-time state of crisp.graph.Graph: a Jaeger
// trace parsed into a parent/child node tree, sanitized so children fit
// within their parents' timelines. Analysis passes (critical path, error
// stats, outputs) are later port stages and their fields are not here yet.
type Graph struct {
	// ParsedTrace holds the pass-1/pass-2 results: ProcessName, HostMap,
	// RegionMap, Nodes (span document order), NumErrors, IsTestTrace.
	*ParsedTrace

	ServiceName   string
	OperationName string
	Filename      string
	RootNode      *Node

	// NodeHT mirrors nodeHT (spanID -> node). nodeOrder records its
	// insertion order -- span document order -- because Python dict
	// iteration order is semantically significant in
	// buildParentChildRelationships (it determines potentialRoots order,
	// which rootTrace=false root selection depends on) and Go maps are
	// unordered.
	NodeHT    map[string]*Node
	nodeOrder []string
	// TagHT mirrors tagHT: spanID -> the span's raw tag list, used by
	// GetMatchingTagsInNode.
	TagHT map[string][]jaeger.Tag

	// Sanitization counters, as in Python. TotalDrop is initialized and
	// reported by Python but never incremented there; it is kept for
	// parity.
	TotalShrink   int64
	TotalDrop     int64
	ShrinkCounter int64

	// ProxyNodes mirrors proxyNodes (proxy spanID -> short-wired child
	// count); only populated when FilterProxy is set.
	ProxyNodes    map[string]int
	FilterProxy   bool
	NumProxyRoots int

	exclusionSet map[ServiceOp]bool
	config       AnalysisConfig

	// MatchedTags is the result of matching Options.Tags against the tree
	// (Graph.tags in Python).
	MatchedTags []TagFilter

	// ErrorBreakdown holds this trace's error paths when
	// GraphOptions.ErrorBreakdown is set; nil if its root could not be
	// chosen (analysis root only).
	ErrorBreakdown TraceErrorBreakdown
}

// NewGraph mirrors Graph.__init__ for the JSON (non-Parquet) path: parse,
// link, select the root, sanitize overflowing children, remove excluded
// operations, and match tags.
//
// Python signals construction failure by logging and leaving rootNode None
// on a half-initialized Graph; NewGraph mirrors that by returning the Graph
// with RootNode == nil (and a nil error) for no roots, multiple roots under
// rootTrace, root service/operation mismatch, or no matching node under
// rootTrace=false. Callers skip such traces, like Python's process(). A
// non-nil error is reserved for structurally undecodable trace data
// (ParseTrace failure), which Python's json.load/parseNode would also
// reject.
func NewGraph(trace *jaeger.Trace, serviceName, operationName string, opts *GraphOptions) (*Graph, error) {
	o := GraphOptions{}
	if opts != nil {
		o = *opts
	}
	rootTrace := true
	if o.RootTrace != nil {
		rootTrace = *o.RootTrace
	}
	config := DefaultAnalysisConfig()
	if o.Config != nil {
		config = *o.Config
	}

	pt, err := ParseTrace(trace)
	if err != nil {
		return nil, fmt.Errorf("parseNode failed: %w", err)
	}

	g := &Graph{
		ParsedTrace:   pt,
		ServiceName:   serviceName,
		OperationName: operationName,
		Filename:      o.Filename,
		NodeHT:        make(map[string]*Node, len(pt.Nodes)),
		TagHT:         make(map[string][]jaeger.Tag, len(pt.Nodes)),
		ProxyNodes:    map[string]int{},
		FilterProxy:   o.FilterProxy,
		config:        config,
	}
	if len(o.ExclusionSet) > 0 {
		g.exclusionSet = make(map[ServiceOp]bool, len(o.ExclusionSet))
		for _, so := range o.ExclusionSet {
			g.exclusionSet[so] = true
		}
	}

	// storeNodeData: index nodes and raw span tags in span document order,
	// and classify proxy / error-propagation nodes when filtering proxies.
	// Python interleaves this with node creation in parseNode pass 2;
	// ParseTrace already created the nodes, so this is a parallel walk.
	errPropNodes := map[string]int{}
	i := 0
	for _, item := range trace.Data {
		for _, span := range item.Spans {
			node := &pt.Nodes[i]
			i++
			// Python evaluates processName[pid] eagerly here, so a span
			// referencing an unknown process aborts construction.
			svcName, ok := pt.ProcessName[node.ProcessID]
			if !ok {
				return nil, fmt.Errorf("parseNode failed: span %s references unknown processID %q",
					node.SID, node.ProcessID)
			}
			// Duplicate spanIDs follow Python dict semantics: the last
			// assignment wins the value, but the key keeps its first
			// insertion position (and pass 3 visits each unique ID once).
			if _, exists := g.NodeHT[node.SID]; !exists {
				g.nodeOrder = append(g.nodeOrder, node.SID)
			}
			g.NodeHT[node.SID] = node
			g.TagHT[node.SID] = span.Tags
			if g.FilterProxy {
				if isProxyNode(svcName, node.OpName) {
					g.ProxyNodes[node.SID] = 0
				}
				if isErrPropNode(svcName, node.OpName) {
					errPropNodes[node.SID] = 0
				}
			}
		}
	}

	// pass 3: build parent-child relationships.
	potentialRoots := g.buildParentChildRelationships()

	// pass 4: propagate errors for nodes identified as errorProps.
	for _, root := range potentialRoots {
		g.propagateErrors(root, errPropNodes)
	}

	// (Python pass 5 records the fixture's expected test results; the Go
	// port verifies against goldens via the difftest harness instead.)

	// Before root selection and sanitization, which detach or drop spans.
	if o.ErrorBreakdown != nil && o.ErrorBreakdown.Root == ErrorBreakdownTraceRoot {
		root, virtual := g.selectTraceRoot(potentialRoots)
		g.ErrorBreakdown = g.computeTraceBreakdown(root, virtual, o.ErrorBreakdown.Mode)
	}

	if o.RootSpanID != "" {
		potentialRoots = nil
		if root, ok := g.NodeHT[o.RootSpanID]; ok {
			potentialRoots = []*Node{root}
			g.ServiceName, g.OperationName = g.ProcessName[root.ProcessID], root.OpName
		}
		rootTrace = false
	}

	if len(potentialRoots) == 0 {
		// Python: logging.warning("no root node in file ...") and return.
		return g, nil
	}

	if rootTrace {
		if len(potentialRoots) != 1 {
			// Python: logging.warning("%d roots node in file ...") and return.
			return g, nil
		}
		if !g.checkRootAndWarn(potentialRoots[0], rootTrace) {
			return g, nil
		}
		g.RootNode = potentialRoots[0]
	} else {
		for _, candidate := range potentialRoots {
			someRoot := g.findARoot(candidate)
			if someRoot == nil || !g.checkRootAndWarn(someRoot, rootTrace) {
				continue
			}
			// Detach someRoot from its parent. Python does not remove it
			// from the former parent's children dict; the old parent simply
			// becomes unreachable from the chosen root.
			someRoot.Parent = nil
			someRoot.ParentSpanID = nil
			g.RootNode = someRoot
			break
		}
		// Python: logging.warning("rootTrace == false but no matching node
		// found ...") and return with rootNode still None.
	}

	// Python's early returns skip the rest of __init__ when no root was
	// selected.
	if g.RootNode == nil {
		return g, nil
	}

	if o.ErrorBreakdown != nil && o.ErrorBreakdown.Root == ErrorBreakdownAnalysisRoot {
		g.ErrorBreakdown = g.computeTraceBreakdown(g.RootNode, false, o.ErrorBreakdown.Mode)
	}

	g.sanitizeOverflowingChildren(g.RootNode)
	g.removeExcludedOps(g.RootNode)
	g.MatchedTags = g.getMatchingTagsInTree(o.Tags, g.RootNode, 1)
	g.TagHT = nil // no longer needed, as in Python
	return g, nil
}

// buildParentChildRelationships mirrors graph.py's pass 3. Iteration follows
// nodeHT insertion order (span document order), which Python gets from dict
// ordering. Nodes with no parent -- or a parent absent from nodeHT -- become
// potential roots. Children of proxy nodes are short-wired to the proxy's
// parent; proxy nodes themselves are never added as children.
func (g *Graph) buildParentChildRelationships() []*Node {
	var potentialRoots []*Node
	for _, spanID := range g.nodeOrder {
		me := g.NodeHT[spanID]
		parentID := me.ParentSpanID
		if parentID == nil {
			potentialRoots = append(potentialRoots, me)
			continue
		}
		if _, ok := g.NodeHT[*parentID]; !ok {
			potentialRoots = append(potentialRoots, me)
			continue
		}

		// Parent is a proxy node; short wire it.
		if _, isProxy := g.ProxyNodes[*parentID]; isProxy {
			g.ProxyNodes[*parentID]++
			proxyNode := g.NodeHT[*parentID]
			parentID = proxyNode.ParentSpanID
			if parentID == nil {
				g.NumProxyRoots++
				potentialRoots = append(potentialRoots, me)
				continue
			}
			if _, ok := g.NodeHT[*parentID]; !ok {
				g.NumProxyRoots++
				potentialRoots = append(potentialRoots, me)
				continue
			}
		}

		parent := g.NodeHT[*parentID]
		me.setParent(parent)
		if _, isProxy := g.ProxyNodes[spanID]; !isProxy {
			parent.addChild(me)
		}
	}
	return potentialRoots
}

// propagateErrors mirrors graph.py's post-order propagateErrors: nodes
// identified as error-propagation nodes with exactly one child adopt the
// child's error state when they have none of their own.
func (g *Graph) propagateErrors(me *Node, errPropNodes map[string]int) {
	if len(me.Children) == 0 {
		return
	}
	for _, c := range me.Children {
		g.propagateErrors(c, errPropNodes)
	}

	if _, ok := errPropNodes[me.SID]; !ok {
		return
	}
	if len(me.Children) != 1 {
		return
	}
	child := me.Children[0]
	if !me.ReturnError && child.ReturnError {
		me.ReturnError = child.ReturnError
	}
}

// findARoot mirrors graph.py's DFS for the first node matching the required
// service and operation name.
func (g *Graph) findARoot(node *Node) *Node {
	if g.ProcessName[node.ProcessID] == g.ServiceName && node.OpName == g.OperationName {
		return node
	}
	for _, c := range node.Children {
		if found := g.findARoot(c); found != nil {
			return found
		}
	}
	return nil
}

// checkRootAndWarn mirrors graph.py: the root's service and operation must
// match the requested ones (Python logs a warning and returns False). A
// process ID missing from the process table yields an empty service name
// here, where Python would raise KeyError out of Graph.__init__; both
// prevent the node from becoming the root.
func (g *Graph) checkRootAndWarn(node *Node, rootTrace bool) bool {
	return g.ProcessName[node.ProcessID] == g.ServiceName && node.OpName == g.OperationName
}

// removeExcludedOps mirrors graph.py: subtrees whose (service, operation) is
// in the exclusion set are dropped without recursion; everything else is
// recursed into.
func (g *Graph) removeExcludedOps(curNode *Node) {
	var removeList []*Node
	for _, c := range curNode.Children {
		so := ServiceOp{Service: g.ProcessName[c.ProcessID], Op: c.OpName}
		if g.exclusionSet[so] {
			removeList = append(removeList, c)
		} else {
			g.removeExcludedOps(c)
		}
	}
	for _, r := range removeList {
		curNode.removeChild(r)
	}
}

// isAcceptableParentChildDuration mirrors graph.py: a server span may exceed
// its client's duration by at most the configured lengthening factor.
// Python compares int durations against a float product; Go does the same
// float64 comparison.
func (g *Graph) isAcceptableParentChildDuration(parent, me *Node) bool {
	return !(float64(me.Duration) > g.config.ServerLengtheningFactor*float64(parent.Duration))
}

// isFuzzyClientServerCall mirrors graph.py: detects
// server1 -> [^server] -> server2, where the middle node has exactly one
// child (server2).
func (g *Graph) isFuzzyClientServerCall(parent, me *Node) bool {
	if me.SpanKind != SpanKindServer {
		return false
	}
	if parent.SpanKind == SpanKindServer {
		return false
	}
	if len(parent.Children) != 1 {
		return false
	}
	grandParent := parent.Parent
	if grandParent == nil || grandParent.SpanKind != SpanKindServer {
		return false
	}
	return g.isAcceptableParentChildDuration(parent, me)
}

// isCleanClientServerCall mirrors graph.py: detects client1 -> server2,
// where client1 has exactly one child (server2).
func (g *Graph) isCleanClientServerCall(parent, me *Node) bool {
	if me.SpanKind != SpanKindServer {
		return false
	}
	if parent.SpanKind != SpanKindClient {
		return false
	}
	if len(parent.Children) != 1 {
		return false
	}
	return g.isAcceptableParentChildDuration(parent, me)
}

// onDifferentHosts mirrors graph.py: parent and child run on different
// hosts (per the process-tag host map), and the parent has exactly one
// child. Python's `if not parentHost or not meHost` treats a missing OR
// empty hostname as "no host", so both are checked here.
func (g *Graph) onDifferentHosts(parent, me *Node) bool {
	parentHost := g.HostMap[parent.ProcessID]
	meHost := g.HostMap[me.ProcessID]
	if parentHost == "" || meHost == "" {
		return false
	}
	if parentHost == meHost {
		return false
	}
	if len(parent.Children) != 1 {
		return false
	}
	return g.isAcceptableParentChildDuration(parent, me)
}

// adjustChildDuration mirrors graph.py: cap the child's duration at the
// parent's so downstream math stays non-negative. Bug-for-bug: Python does
// not update endTime here, so EndTime can go stale relative to
// StartTime+Duration after this adjustment.
func adjustChildDuration(parent, me *Node) {
	if me.Duration > parent.Duration {
		me.Duration = parent.Duration
	}
}

// isClientServerCall mirrors graph.py: parent is client(ish) and me is the
// server, with an acceptable duration ratio; the child's duration is capped
// as a side effect when adjustChild is set.
func (g *Graph) isClientServerCall(parent, me *Node, adjustChild bool) bool {
	if g.isCleanClientServerCall(parent, me) ||
		g.isFuzzyClientServerCall(parent, me) ||
		g.onDifferentHosts(parent, me) {
		if g.isAcceptableParentChildDuration(parent, me) {
			if adjustChild {
				adjustChildDuration(parent, me)
			}
			return true
		}
	}
	return false
}

// sanitizeOverflowingChildren mirrors graph.py's Case 0-4 logic: children
// that overflow or underflow their parent's timeline are truncated or
// dropped so the tree adheres to parent timelines.
//
// Bug-for-bug notes: the case-0 client-server check runs first and caps the
// child's duration as a side effect even when the child would otherwise be
// case 1; and case 2 updates startTime/duration without touching endTime
// (which stays consistent), while case 3 updates duration/endTime.
func (g *Graph) sanitizeOverflowingChildren(curNode *Node) {
	parentStart := curNode.StartTime
	parentEnd := curNode.EndTime

	var removeList []*Node
	for _, c := range curNode.Children {
		childStart := c.StartTime
		childEnd := c.EndTime

		if g.isClientServerCall(curNode, c, true) {
			// Case 0: curNode is client and c is a server. Don't chop c to
			// adjust to curNode's times; continue recursion.
			g.sanitizeOverflowingChildren(c)
		} else if childStart >= parentStart && childEnd <= parentEnd {
			// Case 1: everything looks good.
			g.sanitizeOverflowingChildren(c)
		} else if childStart < parentStart && childEnd <= parentEnd && childEnd > parentStart {
			// Case 2: child starts before parent; truncate the left edge.
			shrunk := parentStart - childStart
			g.TotalShrink += shrunk
			g.ShrinkCounter++
			c.StartTime = parentStart
			c.Duration -= shrunk
			g.sanitizeOverflowingChildren(c)
		} else if childStart >= parentStart && childEnd > parentEnd && childStart < parentEnd {
			// Case 3: child ends after parent; truncate the right edge.
			shrunk := childEnd - parentEnd
			g.TotalShrink += shrunk
			g.ShrinkCounter++
			c.Duration -= shrunk
			c.EndTime -= shrunk
			g.sanitizeOverflowingChildren(c)
		} else {
			// Case 4: child lies entirely outside the parent's range; drop
			// it. No recursion: all its descendants become unreachable.
			removeList = append(removeList, c)
		}
	}

	for _, r := range removeList {
		curNode.removeChild(r)
	}
}

// getMatchingTagsInNode mirrors graph.py GetMatchingTagsInNode: the subset
// of the filter list whose (name, value) appears among the node's raw span
// tags. Tag values are compared with Python == semantics (jaeger.ValueEqual).
func (g *Graph) getMatchingTagsInNode(tags []TagFilter, node *Node) []TagFilter {
	if node == nil || len(tags) == 0 {
		return nil
	}
	spanTags, ok := g.TagHT[node.SID]
	if !ok {
		return nil
	}
	var matching []TagFilter
	for _, tag := range spanTags {
		for _, f := range tags {
			if tag.Key == f.Name && jaeger.ValueEqual(tag.Value, f.Value) {
				matching = append(matching, f)
			}
		}
	}
	return matching
}

// getMatchingTagsInTree mirrors graph.py GetMatchingTagsInTree: a DFS
// accumulating matched filters, descending only while unmatched filters
// whose search depth permits remain.
func (g *Graph) getMatchingTagsInTree(tags []TagFilter, rootNode *Node, curSearchDepth int) []TagFilter {
	matchingTags := g.getMatchingTagsInNode(tags, rootNode)
	if len(matchingTags) == len(tags) {
		return matchingTags
	}

	newDepth := curSearchDepth + 1
	remainingTags := getRemainingTags(matchingTags, tags, newDepth)
	for _, child := range rootNode.Children {
		moreMatches := g.getMatchingTagsInTree(remainingTags, child, newDepth)
		if len(moreMatches) > 0 {
			matchingTags = append(matchingTags, moreMatches...)
			remainingTags = getRemainingTags(moreMatches, remainingTags, newDepth)
		}
	}
	return matchingTags
}

// getRemainingTags mirrors models.GetRemainingTags: filters not yet found
// whose search depth still allows matching at curSearchDepth. Membership in
// foundTags is by value equality, as with Python dicts.
func getRemainingTags(foundTags, allTags []TagFilter, curSearchDepth int) []TagFilter {
	var remaining []TagFilter
	for _, m := range allTags {
		if curSearchDepth > m.SearchDepth {
			continue
		}
		found := false
		for _, f := range foundTags {
			if tagFilterEqual(m, f) {
				found = true
				break
			}
		}
		if !found {
			remaining = append(remaining, m)
		}
	}
	return remaining
}

func tagFilterEqual(a, b TagFilter) bool {
	return a.Name == b.Name && a.SearchDepth == b.SearchDepth && jaeger.ValueEqual(a.Value, b.Value)
}
