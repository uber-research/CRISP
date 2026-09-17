package crisp

// dot.go ports crisp/cct_utils.py cct_to_dot (plus _escape_dot_label and
// _make_node_label): render parsed CCT summaries as a Graphviz DOT digraph.
//
// Parity notes:
//   - Node IDs are assigned in first-seen order while walking the
//     summaries (Python's next_id counter); node lines emit in ascending
//     ID order and edges in sorted (parent, child) order, so the output is
//     deterministic.
//   - The path key joins "[service]operation" segments (no space, unlike
//     the node label's "[service] operation") exactly like Python's tuple
//     key; Go encodes the segment list with %q to keep the map key
//     injective for arbitrary names.

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// escapeDotLabel mirrors cct_utils.py _escape_dot_label.
func escapeDotLabel(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, `\`, `\\`), `"`, `\"`)
}

// makeNodeLabel mirrors cct_utils.py _make_node_label: a multi-line DOT
// label (literal "\n" joins) with optional incl/excl/freq lines.
func makeNodeLabel(label string, exclTime, inclTime, freq int64) string {
	parts := []string{escapeDotLabel(label)}
	if inclTime > 0 {
		parts = append(parts, "incl: "+strconv.FormatInt(inclTime, 10)+"µs")
	}
	if exclTime > 0 {
		parts = append(parts, "excl: "+strconv.FormatInt(exclTime, 10)+"µs")
	}
	if freq > 0 {
		parts = append(parts, "freq: "+strconv.FormatInt(freq, 10))
	}
	return strings.Join(parts, `\n`)
}

// CCTToDot mirrors cct_utils.py cct_to_dot.
func CCTToDot(summaries []*CCTSummary) string {
	if len(summaries) == 0 {
		return "digraph CCT {\n}\n"
	}

	pathToID := make(map[string]int)
	nodeLabel := make(map[int]string)
	nodeExcl := make(map[int]int64)
	nodeFreq := make(map[int]int64)
	children := make(map[int][]int)
	type edge struct{ parent, child int }
	edgeSet := make(map[edge]bool)
	nextID := 0

	getOrCreate := func(pathKey, label string) int {
		if nid, ok := pathToID[pathKey]; ok {
			return nid
		}
		nid := nextID
		nextID++
		pathToID[pathKey] = nid
		nodeLabel[nid] = label
		nodeExcl[nid] = 0
		nodeFreq[nid] = 0
		children[nid] = nil
		return nid
	}

	for _, summary := range summaries {
		prevID := -1
		for i, part := range summary.CallPath {
			// Python key: tuple(f"[{p['service']}]{p['operation_name']}")
			segments := make([]string, 0, i+1)
			for _, p := range summary.CallPath[:i+1] {
				segments = append(segments, "["+p.Service+"]"+p.Operation)
			}
			pathKey := fmt.Sprintf("%q", segments)
			label := "[" + part.Service + "] " + part.Operation
			nid := getOrCreate(pathKey, label)

			if i == len(summary.CallPath)-1 {
				nodeExcl[nid] += summary.Duration
				nodeFreq[nid] += summary.Frequency
			}

			if prevID >= 0 && !edgeSet[edge{prevID, nid}] {
				children[prevID] = append(children[prevID], nid)
				edgeSet[edge{prevID, nid}] = true
			}
			prevID = nid
		}
	}

	// Inclusive times bottom-up. The graph is a tree (a node's key is its
	// full path), so every node is reached exactly once from the roots.
	nodeIncl := make(map[int]int64)
	var computeInclusive func(nid int) int64
	computeInclusive = func(nid int) int64 {
		total := nodeExcl[nid]
		for _, cid := range children[nid] {
			total += computeInclusive(cid)
		}
		nodeIncl[nid] = total
		return total
	}
	isChild := make(map[int]bool)
	for _, ch := range children {
		for _, nid := range ch {
			isChild[nid] = true
		}
	}
	for nid := range nodeLabel {
		if !isChild[nid] {
			computeInclusive(nid)
		}
	}

	lines := []string{
		"digraph CCT {",
		"    rankdir=TB;",
		`    node [shape=box, style=filled, fillcolor=lightyellow, fontname="Helvetica", fontsize=10];`,
		`    edge [fontname="Helvetica", fontsize=8];`,
		"",
	}
	for nid := 0; nid < nextID; nid++ {
		label := makeNodeLabel(nodeLabel[nid], nodeExcl[nid], nodeIncl[nid], nodeFreq[nid])
		lines = append(lines, "    n"+strconv.Itoa(nid)+` [label="`+label+`"];`)
	}
	lines = append(lines, "")
	// sorted(edge_set): ascending by (parent, child).
	sortedEdges := make([]edge, 0, len(edgeSet))
	for e := range edgeSet {
		sortedEdges = append(sortedEdges, e)
	}
	sort.Slice(sortedEdges, func(i, j int) bool {
		a, b := sortedEdges[i], sortedEdges[j]
		return a.parent < b.parent || (a.parent == b.parent && a.child < b.child)
	})
	for _, e := range sortedEdges {
		lines = append(lines, "    n"+strconv.Itoa(e.parent)+" -> n"+strconv.Itoa(e.child)+";")
	}
	lines = append(lines, "}")
	return strings.Join(lines, "\n") + "\n"
}
