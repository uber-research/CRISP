package crisp

// light.go ports the light-mode orchestration from crisp/process_trace.py:
// lightProcess, seqProcess, and the per-trace process() pipeline.
//
// Parity notes:
//   - Python's process() constructs Graph with (data, serviceName,
//     operationName, filename, rootTrace, filterProxy): tags/exclusionSet
//     are never forwarded in light mode, so the corresponding CLI flags
//     are accepted by the Go CLI but do not affect the analysis, exactly
//     like Python. filterProxy is forwarded (LightConfig.FilterProxy); it
//     only changes the analysis when the proxy/err-prop lists in
//     span_utils are populated, and they ship empty.
//   - minDuration/maxDuration and rejectSubtreeIncomplete filter exactly as
//     in Python's processRoots() and lightProcess(), and MergeAllRoots
//     selects analysis roots exactly as Graph.analysisRoots does. Python's logging of them
//     (the "Rejected N/M" warning and the incomplete-trace stats) is not
//     ported; outputs are unaffected.
//   - With no valid traces, Python's aggregateCCTs([]) takes the
//     non-CallPathProfile branch and returns ""; mirrored here.
//   - Python's getOutputDir() (the Config method) resolves to the --file
//     parent directory or inputDir; the -o/--outputDir value stored on
//     the Config is never read in light mode. The caller of LightProcess
//     is expected to resolve OutputDir the same way (the CLI does).

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/uber-research/CRISP/go/crisp/jaeger"
)

// LightCCTFile mirrors the lightProcess output file name.
const LightCCTFile = "light-flame-graph-P100.cct"

// LightConfig mirrors the subset of common.Config that lightProcess reads.
type LightConfig struct {
	ServiceName   string
	OperationName string
	// RootTrace mirrors config.rootTrace (CLI default false, unlike the
	// Graph constructor default).
	RootTrace        bool
	Conformance      bool
	MaxExemplars     int
	IgnoreTestTraces bool
	// FilterProxy mirrors config.filterProxy: short-wire proxy spans and
	// enable error-propagation nodes. It has no effect unless the
	// proxy/err-prop lists in span_utils.go are populated.
	FilterProxy bool
	// MinDuration and MaxDuration mirror config.minDuration/maxDuration:
	// skip a trace whose analysis root is shorter or longer than this many
	// microseconds. Both bounds are inclusive; nil is no bound.
	MinDuration, MaxDuration *int64
	// RejectSubtreeIncomplete mirrors config.rejectSubtreeIncomplete: leave
	// out traces whose analysis subtree is incomplete (a service with an
	// orphan span also appears under the analysis root).
	RejectSubtreeIncomplete bool
	// MergeAllRoots mirrors config.mergeAllRoots: without RootTrace, every
	// parentless span that matches the service and operation is an analysis
	// root next to the first match, and each root contributes one profile
	// (see Graph.AnalysisRoots). The zero value keeps the first match only;
	// Python and the CLI default to on.
	MergeAllRoots bool
	// ErrorBreakdown, when set, also writes error-breakdown.json.
	ErrorBreakdown *ErrorBreakdownOptions
	// Context, when non-nil, is checked before each trace file in
	// LightProcess so a canceled caller stops the run promptly instead of
	// processing the whole cohort. Nil means no cancellation checks.
	Context context.Context
	// TraceFiles mirrors c.jaegerTraceFiles (already resolved).
	TraceFiles []string
	// OutputDir mirrors c.getOutputDir() (resolved by the caller).
	OutputDir string
}

// lightTraceResult carries the contribution of one analysis root of a trace
// to the light outputs (Python's Metrics). A trace has one error breakdown,
// carried by the result of its first root.
type lightTraceResult struct {
	traceID string
	// rootSpanID mirrors metrics.rootSpanID: the analysis root of this result.
	rootSpanID     string
	cpp            *CallPathProfile
	slackDrag      *SlackDragByCallpath
	errorBreakdown TraceErrorBreakdown
	// subtreeIncomplete mirrors metrics.isSubtreeIncomplete.
	subtreeIncomplete bool
}

// TraceIDFromFilePath mirrors process_trace.py getTraceIdFromFilePath:
// basename, then everything before the first ".".
func TraceIDFromFilePath(traceFile string) string {
	return strings.Split(filepath.Base(traceFile), ".")[0]
}

// processTraceFile mirrors process_trace.py processRoots(): build the
// graph and analyze each analysis root. See processTraceRoots.
func processTraceFile(filename string, c *LightConfig) ([]*lightTraceResult, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	return processTraceRoots(data, TraceIDFromFilePath(filename), filename, c)
}

// processTraceData mirrors process_trace.py process(): the result of the
// trace's first analysis root, or nil when the trace is skipped.
func processTraceData(data []byte, traceID, filename string, c *LightConfig) (*lightTraceResult, error) {
	results, err := processTraceRoots(data, traceID, filename, c)
	if err != nil || len(results) == 0 {
		return nil, err
	}
	return results[0], nil
}

// mergeAllRoots mirrors process_trace.py _mergeAllRoots: whether a trace may
// contribute several analysis roots.
func mergeAllRoots(c *LightConfig) bool {
	return c.MergeAllRoots && !c.RootTrace
}

// processTraceRoots mirrors process_trace.py processRoots() with the trace
// bytes already in memory: one result per analysis root. traceID and
// filename are used exactly as Python uses getTraceIdFromFilePath(filename)
// and the filename (exemplars, messages).
//
// A trace has one analysis root unless MergeAllRoots applies (RootTrace
// off): then every parentless span matching the service and operation is a
// root next to the first match (see Graph.AnalysisRoots), and each root is
// analyzed on its own. An empty result means the trace is skipped (no root,
// a test trace with ignoreTestTraces set, or no analysis root inside the
// duration bounds).
func processTraceRoots(data []byte, traceID, filename string, c *LightConfig) ([]*lightTraceResult, error) {
	trace, err := jaeger.Decode(data)
	if err != nil {
		return nil, err
	}
	// Duration bounds apply to the analysis root, not the trace root.
	g, err := NewGraph(trace, c.ServiceName, c.OperationName, &GraphOptions{
		Filename:        filename,
		RootTrace:       &c.RootTrace,
		FilterProxy:     c.FilterProxy,
		ErrorBreakdown:  c.ErrorBreakdown,
		MinRootDuration: c.MinDuration,
		MaxRootDuration: c.MaxDuration,
	})
	if err != nil {
		// Python swallows parseNode failures (warning + rootNode None ->
		// the trace is skipped); mirror that, keeping the reason visible.
		fmt.Fprintf(os.Stderr, "skipping %s: %v\n", filename, err)
		return nil, nil
	}
	if g.RootNode == nil {
		return nil, nil
	}
	if c.IgnoreTestTraces && g.IsTestTrace {
		return nil, nil
	}

	// An out-of-range root produces no result. Only a RootTrace root can
	// still be out of range here.
	var roots []*Node
	for _, root := range g.AnalysisRoots(mergeAllRoots(c)) {
		if g.rootDurationInRange(root) {
			roots = append(roots, root)
		}
	}

	var results []*lightTraceResult
	var analysisBreakdowns []TraceErrorBreakdown
	for _, root := range roots {
		// Before the root is sanitized, as NewGraph does for its first root.
		analysisBreakdowns = append(analysisBreakdowns, g.AnalysisRootErrorBreakdown(root))
		g.PrepareAnalysisRoot(root)
		// With several roots, each root's drag report covers its own
		// subtree, so that the rest of the trace is not counted once per
		// root.
		var dragRoot *Node
		if len(roots) > 1 {
			dragRoot = root
		}
		res, err := processRoot(g, root, traceID, dragRoot)
		if err != nil {
			return nil, err
		}
		results = append(results, res)
	}

	if len(results) > 0 {
		// One error breakdown per trace, carried by its first result: the
		// trace-root breakdown as is, or the sum of the per-root breakdowns.
		if c.ErrorBreakdown != nil && c.ErrorBreakdown.Root == ErrorBreakdownTraceRoot {
			results[0].errorBreakdown = g.ErrorBreakdown
		} else {
			results[0].errorBreakdown = mergeTraceErrorBreakdowns(analysisBreakdowns)
		}
	}
	return results, nil
}

// processRoot mirrors process_trace.py _processRoot(): compute the critical
// path of one prepared analysis root, accumulate the call-path profile, and
// compute drag.
func processRoot(g *Graph, root *Node, traceID string, dragRoot *Node) (*lightTraceResult, error) {
	cp, err := g.FindCriticalPath(root)
	if err != nil {
		return nil, err
	}
	cpp, _ := g.AccumeCPMetrics(cp, traceID, root)
	// Drag is always computed in Python's process(); slack stays behind
	// --computeSlackDrag, which this port does not support (slack columns
	// are 0.0).
	drag := g.CalculateDrag(cp, false)
	return &lightTraceResult{
		traceID:    traceID,
		rootSpanID: root.SID,
		cpp:        cpp,
		slackDrag:  g.AggregateDragSlackByCallpath(drag, dragRoot),
		// The analysis subtree is incomplete when a service with an orphan
		// span also appears under the analysis root: the dropped spans were
		// likely its descendants.
		subtreeIncomplete: anyServiceInSubtree(g, root, g.OrphanServices()),
	}, nil
}

// mergeTraceErrorBreakdowns mirrors process_trace.py
// _mergeTraceErrorBreakdowns: sum the per-root analysis-root error
// breakdowns of one trace. The same key keeps the nodes of the first root
// that has it and adds the counts. Nil when no root has a breakdown.
func mergeTraceErrorBreakdowns(breakdowns []TraceErrorBreakdown) TraceErrorBreakdown {
	var merged TraceErrorBreakdown
	for _, breakdown := range breakdowns {
		if breakdown == nil {
			continue
		}
		if merged == nil {
			merged = TraceErrorBreakdown{}
		}
		for key, path := range breakdown {
			if have, ok := merged[key]; ok {
				merged[key] = &TraceErrorPath{Count: have.Count + path.Count, Nodes: have.Nodes}
			} else {
				merged[key] = &TraceErrorPath{Count: path.Count, Nodes: path.Nodes}
			}
		}
	}
	return merged
}

// anyServiceInSubtree mirrors the set intersection of process_trace.py's
// get_subtree_services with the orphan services: whether any node under
// root, root included, belongs to one of services.
func anyServiceInSubtree(g *Graph, root *Node, services map[string]bool) bool {
	if len(services) == 0 {
		return false
	}
	stack := []*Node{root}
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if services[g.ProcessName[n.ProcessID]] {
			return true
		}
		stack = append(stack, n.Children...)
	}
	return false
}

// keep mirrors lightProcess's rejectSubtreeIncomplete filter.
func keep(c *LightConfig, res *lightTraceResult) bool {
	return res != nil && !(c.RejectSubtreeIncomplete && res.subtreeIncomplete)
}

// LightProcess mirrors process_trace.py lightProcess: process all traces,
// write light-flame-graph-P100.{cct,dot,pb}, conformance outputs when
// enabled, and slackDrag.csv.
func LightProcess(c *LightConfig) error {
	ctx := c.Context
	if ctx == nil {
		ctx = context.Background()
	}
	var valid []*lightTraceResult
	for _, traceFile := range c.TraceFiles {
		if err := ctx.Err(); err != nil {
			return err
		}
		results, err := processTraceFile(traceFile, c)
		if err != nil {
			return err
		}
		for _, res := range results {
			if keep(c, res) {
				valid = append(valid, res)
			}
		}
	}
	return writeLightOutputs(c, valid)
}

// ProcessSingleTraceData runs the light-mode pipeline on one in-memory
// Jaeger JSON trace and writes the same outputs a CLI single-file run
// would into c.OutputDir (including the empty outputs Python writes for a
// skipped trace). traceID plays the role of getTraceIdFromFilePath. It is
// the library entry point for callers that already hold trace bytes (no
// disk read, no subprocess); it spawns no goroutines, so callers control
// parallelism entirely.
func ProcessSingleTraceData(data []byte, traceID string, c *LightConfig) error {
	results, err := processTraceRoots(data, traceID, traceID, c)
	if err != nil {
		return err
	}
	var valid []*lightTraceResult
	for _, res := range results {
		if keep(c, res) {
			valid = append(valid, res)
		}
	}
	return writeLightOutputs(c, valid)
}

// writeLightOutputs is the LightProcess tail: merge the per-trace results
// and write all output files.
func writeLightOutputs(c *LightConfig, valid []*lightTraceResult) error {
	metrics := make([]*TraceMetrics, 0, len(valid))
	cpps := make([]*CallPathProfile, 0, len(valid))
	perTraceSlackDrag := make([]*SlackDragByCallpath, 0, len(valid))
	for _, res := range valid {
		metrics = append(metrics, &TraceMetrics{TraceID: res.traceID, CPMetrics: res.cpp})
		cpps = append(cpps, res.cpp)
		perTraceSlackDrag = append(perTraceSlackDrag, res.slackDrag)
	}

	merged := MergeCallPathProfilesWithExemplars(metrics, c.MaxExemplars)
	flameGraphStr := ""
	if len(cpps) > 0 {
		var err error
		flameGraphStr, err = AggregateCallPathProfiles(cpps)
		if err != nil {
			return err
		}
	}

	cctFile := filepath.Join(c.OutputDir, LightCCTFile)
	if err := WriteCCTOutputs(cctFile, flameGraphStr, merged, c.MaxExemplars); err != nil {
		return err
	}

	if c.Conformance {
		if _, _, err := WriteConformanceOutputs(c.OutputDir, flameGraphStr, merged, c.MaxExemplars); err != nil {
			return err
		}
	}

	if c.ErrorBreakdown != nil {
		var perTrace []TraceErrorBreakdownEntry
		for _, res := range valid {
			if res.errorBreakdown != nil {
				perTrace = append(perTrace, TraceErrorBreakdownEntry{TraceID: res.traceID, Breakdown: res.errorBreakdown})
			}
		}
		if _, err := WriteErrorBreakdown(c.OutputDir, MergeErrorBreakdowns(perTrace, *c.ErrorBreakdown, c.MaxExemplars)); err != nil {
			return err
		}
	}

	// Unconditional in lightProcess, like the Python heavy path.
	if _, err := GenSlackDragCSV(MergePerMethodSlackDrag(perTraceSlackDrag), c.OutputDir, SlackDragCSV); err != nil {
		return err
	}
	return nil
}
