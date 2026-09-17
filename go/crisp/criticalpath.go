package crisp

import (
	"fmt"
	"sort"
)

// ZeroDurationError mirrors the ZeroDivisionError Python raises in
// happensBefore when the overlap allowance is computed against a
// zero-duration parent. It is unreachable on sanitized well-formed traces
// (children of a zero-duration parent are zero-duration or dropped, so the
// overlap preconditions can never hold), but happensBefore is also called
// on arbitrary span sets by the time-saved analysis, so the error is
// threaded through rather than assumed away.
type ZeroDurationError struct {
	ParentSID string
}

func (e *ZeroDurationError) Error() string {
	return fmt.Sprintf("happensBefore: parent span %s has zero duration", e.ParentSID)
}

// ComputeCriticalPath mirrors graph.py's computeCriticalPath: recursively
// find the critical path for curNode.
//
// The node itself is on the path; then, walking its children in descending
// end-time order, each child that happens-before the previously added child
// (and its own critical sub-path) is appended. Python's
// sorted(children, key=endTime)[::-1] is a stable ascending sort followed
// by a reversal, so children with equal end times are visited in REVERSE
// document order -- replicated here with SliceStable + reverse.
//
// Python tracks lastStartTime for debug logging only; it has no effect on
// the result and is not ported.
func (g *Graph) ComputeCriticalPath(curNode *Node) ([]*Node, error) {
	criticalPath := []*Node{curNode}

	if len(curNode.Children) == 0 {
		return criticalPath, nil
	}

	// Step 1: reverse-sort children by end time (stable, then reversed).
	sortedChildren := make([]*Node, len(curNode.Children))
	copy(sortedChildren, curNode.Children)
	sort.SliceStable(sortedChildren, func(i, j int) bool {
		return sortedChildren[i].EndTime < sortedChildren[j].EndTime
	})
	for i, j := 0, len(sortedChildren)-1; i < j; i, j = i+1, j-1 {
		sortedChildren[i], sortedChildren[j] = sortedChildren[j], sortedChildren[i]
	}

	// Step 2: begin with the child who finishes last.
	lrc := sortedChildren[0]
	subPath, err := g.ComputeCriticalPath(lrc)
	if err != nil {
		return nil, err
	}
	criticalPath = append(criticalPath, subPath...)

	// Step 3: walk the remaining children; a child that happens before the
	// last-added one (and its critical sub-path) joins the path.
	for _, cn := range sortedChildren[1:] {
		hb, err := g.happensBefore(curNode, sortedChildren, cn, lrc)
		if err != nil {
			return nil, err
		}
		if hb {
			subPath, err := g.ComputeCriticalPath(cn)
			if err != nil {
				return nil, err
			}
			criticalPath = append(criticalPath, subPath...)
			lrc = cn
		}
	}
	return criticalPath, nil
}

// FindCriticalPath mirrors graph.py's findCriticalPath: the critical path
// starting from rootNode, or from the graph's root when rootNode is nil.
func (g *Graph) FindCriticalPath(rootNode *Node) ([]*Node, error) {
	node := rootNode
	if node == nil {
		node = g.RootNode
	}
	if node == nil {
		return nil, fmt.Errorf("FindCriticalPath: no root node")
	}
	return g.ComputeCriticalPath(node)
}

// happensBeforeSimple mirrors graph.py: true if the end of childBefore
// happens before (or exactly at) the start of childLater.
func (g *Graph) happensBeforeSimple(childBefore, childLater *Node) bool {
	return childBefore.EndTime <= childLater.StartTime
}

// happensBefore mirrors graph.py: true if the end of childBefore happens
// before the start of childLater, with a heuristic to accommodate clock
// skew. A small overlap (below the configured allowance fraction of the
// parent's duration) is tolerated provided exactly two sync events (the two
// spans' boundary events) fall in the overlap window -- a third event means
// another sibling interleaves, so the spans are concurrent.
func (g *Graph) happensBefore(parent *Node, children []*Node, childBefore, childLater *Node) (bool, error) {
	if childBefore.EndTime < childLater.StartTime {
		return true, nil
	}

	if childBefore.EndTime < childLater.EndTime &&
		childBefore.StartTime < childLater.StartTime {
		if parent.Duration == 0 {
			// Python raises ZeroDivisionError here.
			return false, &ZeroDurationError{ParentSID: parent.SID}
		}
		overlap := float64(childBefore.EndTime-childLater.StartTime) / float64(parent.Duration)
		if overlap < g.config.OverlapAllowance {
			nEvt := g.numSyncEventsInWindowInclusive(children, childLater.StartTime, childBefore.EndTime)
			if nEvt == 2 {
				return true, nil
			}
		}
	}
	return false, nil
}

// numSyncEventsInWindowInclusive mirrors graph.py: the count of the
// children's start and end events falling within [startTime, endTime].
func (g *Graph) numSyncEventsInWindowInclusive(children []*Node, startTime, endTime int64) int {
	numEvents := 0
	for _, c := range children {
		if c.StartTime >= startTime && c.StartTime <= endTime {
			numEvents++
		}
		if c.EndTime >= startTime && c.EndTime <= endTime {
			numEvents++
		}
	}
	return numEvents
}

// isRPCNode mirrors graph.py: true for server or client nodes, as opposed
// to user-defined spans.
func (g *Graph) isRPCNode(node *Node) bool {
	return node.SpanKind == SpanKindServer || node.SpanKind == SpanKindClient
}
