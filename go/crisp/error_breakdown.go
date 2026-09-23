package crisp

// error_breakdown.go ports crisp/error_breakdown.py: per-trace error call
// paths keyed by RPC protocol and status code, merged across traces with
// exemplars. See that module's docstring and CONFORMANCE.md for the
// semantics; this file must stay byte-for-byte equivalent in output.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/uber-research/CRISP/go/crisp/jaeger"
)

// ErrorBreakdownFile mirrors error_breakdown.ERROR_BREAKDOWN_FILE.
const ErrorBreakdownFile = "error-breakdown.json"

// ErrorBreakdownMode selects which error paths are reported.
type ErrorBreakdownMode string

const (
	// ErrorBreakdownOrigins reports every erroring span none of whose
	// children error, traversing non-erroring spans too.
	ErrorBreakdownOrigins ErrorBreakdownMode = "origins"
	// ErrorBreakdownPropToRoot reports only errors that propagate to the
	// root: a non-erroring RPC span stops the walk, and a non-erroring
	// user-defined span only descends into children of its own service.
	ErrorBreakdownPropToRoot ErrorBreakdownMode = "propToRoot"
)

// ErrorBreakdownRoot selects where the walk starts.
type ErrorBreakdownRoot string

const (
	// ErrorBreakdownTraceRoot starts at the trace's own root span, or a
	// virtual root over orphaned spans when there is none.
	ErrorBreakdownTraceRoot ErrorBreakdownRoot = "trace"
	// ErrorBreakdownAnalysisRoot starts at the root NewGraph selected for
	// the requested service/operation.
	ErrorBreakdownAnalysisRoot ErrorBreakdownRoot = "analysis"
)

const (
	virtualRootService   = "INCOMPLETE_TRACE"
	virtualRootOperation = "virtual_root"
)

// RPC protocols derived by extractRPCStatus; "" means unknown.
const (
	ProtocolHTTP          = "http"
	ProtocolGRPC          = "grpc"
	ProtocolTChannel      = "tchannel"
	ProtocolYARPCHTTP     = "yarpc_http"
	ProtocolYARPCGRPC     = "yarpc_grpc"
	ProtocolYARPCTChannel = "yarpc_tchannel"
)

const (
	tagKeyYARPCStatusCode = "rpc.yarpc.status_code"
	tagKeyRPCTransport    = "rpc.transport"
	tagKeyComponent       = "component"
	tagTypeInt64          = "int64"
)

var (
	grpcStatusKeys = []string{"grpc.status", "grpc.status_code", "rpc.grpc.status_code"}
	httpStatusKeys = []string{"http.response.status_code", "http.status_code"}

	// transport -> {plain protocol, YARPC protocol}
	transportProtocols = map[string][2]string{
		"tchannel": {ProtocolTChannel, ProtocolYARPCTChannel},
		"http":     {ProtocolHTTP, ProtocolYARPCHTTP},
		"grpc":     {ProtocolGRPC, ProtocolYARPCGRPC},
	}
)

// ErrorBreakdownOptions mirrors error_breakdown.ErrorBreakdownOptions.
type ErrorBreakdownOptions struct {
	Mode ErrorBreakdownMode
	Root ErrorBreakdownRoot
}

// NewErrorBreakdownOptions validates a mode and root; an empty root means
// ErrorBreakdownTraceRoot.
func NewErrorBreakdownOptions(mode, root string) (*ErrorBreakdownOptions, error) {
	o := &ErrorBreakdownOptions{Mode: ErrorBreakdownMode(mode), Root: ErrorBreakdownRoot(root)}
	if o.Root == "" {
		o.Root = ErrorBreakdownTraceRoot
	}
	switch o.Mode {
	case ErrorBreakdownOrigins, ErrorBreakdownPropToRoot:
	default:
		return nil, fmt.Errorf("unknown error breakdown mode %q; want %q or %q",
			mode, ErrorBreakdownOrigins, ErrorBreakdownPropToRoot)
	}
	switch o.Root {
	case ErrorBreakdownTraceRoot, ErrorBreakdownAnalysisRoot:
	default:
		return nil, fmt.Errorf("unknown error breakdown root %q; want %q or %q",
			root, ErrorBreakdownTraceRoot, ErrorBreakdownAnalysisRoot)
	}
	return o, nil
}

// ErrorPathNode is one span along a per-trace error path. Protocol is ""
// and StatusCode nil for non-RPC spans. Virtual marks the virtual root,
// which has no span and so contributes no exemplar.
type ErrorPathNode struct {
	Service    string
	Operation  string
	Protocol   string
	StatusCode *int64
	SpanID     string
	Virtual    bool
}

// TraceErrorPath is one error path within a trace: how often its key
// occurred and the spans of its first occurrence.
type TraceErrorPath struct {
	Count int
	Nodes []ErrorPathNode
}

// TraceErrorBreakdown maps path key to path for one trace (Python's
// key -> [count, nodes] dict).
type TraceErrorBreakdown map[string]*TraceErrorPath

// TraceErrorBreakdownEntry pairs a trace's breakdown with the trace ID its
// exemplars carry.
type TraceErrorBreakdownEntry struct {
	TraceID   string
	Breakdown TraceErrorBreakdown
}

// ErrorBreakdown is the merged error-breakdown.json document. Fields are
// declared in JSON key order so encoding matches Python's sort_keys.
type ErrorBreakdown struct {
	Mode   ErrorBreakdownMode `json:"mode"`
	Paths  []ErrorPath        `json:"paths"`
	Root   ErrorBreakdownRoot `json:"root"`
	Traces int                `json:"traces"`
}

// ErrorPath is one merged error path.
type ErrorPath struct {
	Count int                   `json:"count"`
	Key   string                `json:"key"`
	Nodes []ErrorPathOutputNode `json:"nodes"`
}

// ErrorPathOutputNode is one span of a merged error path.
type ErrorPathOutputNode struct {
	Exemplars  []ErrorExemplar `json:"exemplars"`
	Operation  string          `json:"operation"`
	Protocol   *string         `json:"protocol"`
	Service    string          `json:"service"`
	StatusCode *int64          `json:"statusCode"`
}

// ErrorExemplar identifies one span instance of an error path node.
type ErrorExemplar struct {
	SpanID  string `json:"spanID"`
	TraceID string `json:"traceID"`
}

// parseStatusCode mirrors error_breakdown._parse_code: a numeric string
// value (strconv.Atoi syntax, int64 range) or an int64-typed integer value.
func parseStatusCode(t jaeger.Tag) *int64 {
	if s, ok := t.Value.(string); ok && s != "" {
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return &n
		}
	}
	if t.Type == tagTypeInt64 {
		if num, ok := t.Value.(json.Number); ok {
			if n, err := num.Int64(); err == nil {
				return &n
			}
		}
	}
	return nil
}

func protocolFromTransport(transport string, yarpc bool) string {
	pair, ok := transportProtocols[transport]
	if !ok {
		return ""
	}
	if yarpc {
		return pair[1]
	}
	return pair[0]
}

func containsString(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// extractRPCStatus mirrors error_breakdown.extract_rpc_status: derive
// (protocol, statusCode) from a span's tags. Keys match exactly; a later
// status tag overrides an earlier one. TChannelStatusTags and
// YARPCStatusTags extend the standard status keys.
func extractRPCStatus(tags []jaeger.Tag) (string, *int64) {
	protocol := ""
	var code, yarpcCode *int64
	transport := ""
	componentHTTP := false
	for _, t := range tags {
		switch {
		case containsString(grpcStatusKeys, t.Key):
			protocol, code = ProtocolGRPC, parseStatusCode(t)
		case containsString(httpStatusKeys, t.Key):
			protocol, code = ProtocolHTTP, parseStatusCode(t)
		case t.Key == tagKeyYARPCStatusCode:
			yarpcCode = parseStatusCode(t)
		case t.Key == tagKeyRPCTransport:
			transport, _ = tagString(t.Value)
		case t.Key == tagKeyComponent:
			v, ok := tagString(t.Value)
			componentHTTP = ok && containsString(HTTPComponents, v)
		case containsString(TChannelStatusTags, t.Key):
			protocol, code = ProtocolTChannel, parseStatusCode(t)
		case containsString(YARPCStatusTags, t.Key):
			yarpcCode = parseStatusCode(t)
		case containsString(TChannelMarkerTags, t.Key):
			protocol = ProtocolTChannel
		}
	}

	if protocol == "" {
		switch {
		case yarpcCode != nil:
			protocol, code = protocolFromTransport(transport, true), yarpcCode
		case componentHTTP:
			protocol = ProtocolHTTP
		case transport != "":
			protocol = protocolFromTransport(transport, false)
		}
	}
	return protocol, code
}

// isStatusError mirrors error_breakdown.is_status_error: HTTP errors are
// codes >= 400; for every other protocol, any non-zero code.
func isStatusError(protocol string, code *int64) bool {
	if code == nil {
		return false
	}
	if protocol == ProtocolHTTP {
		return *code >= 400
	}
	return *code != 0
}

// selectTraceRoot mirrors error_breakdown.select_trace_root: the first
// potential root without a CHILD_OF reference that is neither a proxy nor
// an ignored op; else a virtual root over the orphans (virtual == true);
// else nil.
func (g *Graph) selectTraceRoot(potentialRoots []*Node) (root *Node, virtual bool) {
	var orphans []*Node
	for _, n := range potentialRoots {
		if n.ParentSpanID == nil {
			if _, isProxy := g.ProxyNodes[n.SID]; isProxy || containsString(IgnoredRootOps, n.OpName) {
				continue
			}
			return n, false
		}
		orphans = append(orphans, n)
	}
	if len(orphans) == 0 {
		return nil, false
	}
	// Children is set directly: the orphans' Parent links stay untouched.
	return &Node{OpName: virtualRootOperation, SpanKind: SpanKindUnknown, Children: orphans}, true
}

// errorBreakdownWalk carries the state of one computeTraceBreakdown call.
type errorBreakdownWalk struct {
	g           *Graph
	mode        ErrorBreakdownMode
	virtualRoot *Node
	paths       TraceErrorBreakdown
}

func (w *errorBreakdownWalk) service(n *Node) string {
	if n == w.virtualRoot {
		return virtualRootService
	}
	return w.g.ProcessName[n.ProcessID]
}

func isRPCNode(n *Node) bool {
	return n.SpanKind == SpanKindServer || n.SpanKind == SpanKindClient
}

func (w *errorBreakdownWalk) element(n *Node) (string, ErrorPathNode) {
	e := ErrorPathNode{Service: w.service(n), Operation: n.OpName, SpanID: n.SID, Virtual: n == w.virtualRoot}
	var label strings.Builder
	label.WriteString("[")
	label.WriteString(e.Service)
	label.WriteString("]")
	label.WriteString(e.Operation)
	if isRPCNode(n) {
		if n.RPCProtocol != "" {
			e.Protocol = n.RPCProtocol
			label.WriteString(":")
			label.WriteString(e.Protocol)
		}
		if n.RPCStatusCode != nil {
			e.StatusCode = n.RPCStatusCode
			label.WriteString(":")
			label.WriteString(strconv.FormatInt(*e.StatusCode, 10))
		}
	}
	return label.String(), e
}

type pathElement struct {
	label string
	node  ErrorPathNode
}

func (w *errorBreakdownWalk) record(path []pathElement) {
	labels := make([]string, len(path))
	for i, p := range path {
		labels[i] = p.label
	}
	key := strings.Join(labels, ";")
	if existing, ok := w.paths[key]; ok {
		existing.Count++
		return
	}
	nodes := make([]ErrorPathNode, len(path))
	for i, p := range path {
		nodes[i] = p.node
	}
	w.paths[key] = &TraceErrorPath{Count: 1, Nodes: nodes}
}

// visitAll visits every child (no short-circuit: each may record paths) and
// reports whether any of them reached an error.
func (w *errorBreakdownWalk) visitAll(children []*Node, path []pathElement, keep func(*Node) bool) bool {
	found := false
	for _, c := range children {
		if keep != nil && !keep(c) {
			continue
		}
		if w.visit(c, path) {
			found = true
		}
	}
	return found
}

func (w *errorBreakdownWalk) visit(n *Node, parentPath []pathElement) bool {
	label, e := w.element(n)
	path := make([]pathElement, len(parentPath), len(parentPath)+1)
	copy(path, parentPath)
	path = append(path, pathElement{label: label, node: e})

	if n == w.virtualRoot {
		return w.visitAll(n.Children, path, nil)
	}
	if n.ReturnError || isStatusError(n.RPCProtocol, n.RPCStatusCode) {
		if !w.visitAll(n.Children, path, nil) {
			w.record(path)
		}
		return true
	}
	if w.mode == ErrorBreakdownOrigins {
		return w.visitAll(n.Children, path, nil)
	}
	if isRPCNode(n) {
		return false
	}
	own := w.service(n)
	return w.visitAll(n.Children, path, func(c *Node) bool { return w.service(c) == own })
}

// computeTraceBreakdown mirrors error_breakdown.compute_trace_breakdown;
// virtual marks root as selectTraceRoot's virtual root. It always returns a
// non-nil map (empty when root is nil).
func (g *Graph) computeTraceBreakdown(root *Node, virtual bool, mode ErrorBreakdownMode) TraceErrorBreakdown {
	w := &errorBreakdownWalk{g: g, mode: mode, paths: TraceErrorBreakdown{}}
	if root == nil {
		return w.paths
	}
	if virtual {
		w.virtualRoot = root
	}
	w.visit(root, nil)
	return w.paths
}

// MergeErrorBreakdowns mirrors error_breakdown.merge_error_breakdowns:
// counts are summed per key, and each node keeps up to maxExemplars
// distinct (traceID, spanID) exemplars in trace order.
func MergeErrorBreakdowns(perTrace []TraceErrorBreakdownEntry, opts ErrorBreakdownOptions, maxExemplars int) *ErrorBreakdown {
	merged := map[string]*ErrorPath{}
	for _, entry := range perTrace {
		for key, tp := range entry.Breakdown {
			out, ok := merged[key]
			if !ok {
				out = &ErrorPath{Key: key, Nodes: make([]ErrorPathOutputNode, len(tp.Nodes))}
				for i, n := range tp.Nodes {
					on := ErrorPathOutputNode{
						Exemplars:  []ErrorExemplar{},
						Operation:  n.Operation,
						Service:    n.Service,
						StatusCode: n.StatusCode,
					}
					if n.Protocol != "" {
						p := n.Protocol
						on.Protocol = &p
					}
					out.Nodes[i] = on
				}
				merged[key] = out
			}
			out.Count += tp.Count
			for i, n := range tp.Nodes {
				if n.Virtual {
					continue
				}
				ex := ErrorExemplar{SpanID: n.SpanID, TraceID: entry.TraceID}
				on := &out.Nodes[i]
				if len(on.Exemplars) < maxExemplars && !containsExemplar(on.Exemplars, ex) {
					on.Exemplars = append(on.Exemplars, ex)
				}
			}
		}
	}

	keys := make([]string, 0, len(merged))
	for k := range merged {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	doc := &ErrorBreakdown{Mode: opts.Mode, Paths: make([]ErrorPath, 0, len(keys)), Root: opts.Root, Traces: len(perTrace)}
	for _, k := range keys {
		doc.Paths = append(doc.Paths, *merged[k])
	}
	return doc
}

func containsExemplar(list []ErrorExemplar, ex ErrorExemplar) bool {
	for _, e := range list {
		if e == ex {
			return true
		}
	}
	return false
}

// CanonicalErrorBreakdownJSON mirrors
// error_breakdown.canonical_error_breakdown_json (see CanonicalResponseJSON).
func CanonicalErrorBreakdownJSON(doc *ErrorBreakdown) (string, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(doc); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// WriteErrorBreakdown mirrors error_breakdown.write_error_breakdown.
func WriteErrorBreakdown(outputDir string, doc *ErrorBreakdown) (string, error) {
	s, err := CanonicalErrorBreakdownJSON(doc)
	if err != nil {
		return "", err
	}
	path := filepath.Join(outputDir, ErrorBreakdownFile)
	if err := os.WriteFile(path, []byte(s), 0o644); err != nil {
		return "", err
	}
	return path, nil
}
