package crisp

// light.go ports the light-mode orchestration from crisp/process_trace.py:
// lightProcess, seqProcess, and the per-trace process() pipeline.
//
// Parity notes:
//   - Python's process() constructs Graph with only (data, serviceName,
//     operationName, filename, rootTrace): tags/exclusionSet/filterProxy
//     are never forwarded in light mode, so the corresponding CLI flags
//     are accepted by the Go CLI but do not affect the analysis, exactly
//     like Python.
//   - With no valid traces, Python's aggregateCCTs([]) takes the
//     non-CallPathProfile branch and returns ""; mirrored here.
//   - Python's getOutputDir() (the Config method) resolves to the --file
//     parent directory or inputDir; the -o/--outputDir value stored on
//     the Config is never read in light mode. The caller of LightProcess
//     is expected to resolve OutputDir the same way (the CLI does).

import (
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
	// TraceFiles mirrors c.jaegerTraceFiles (already resolved).
	TraceFiles []string
	// OutputDir mirrors c.getOutputDir() (resolved by the caller).
	OutputDir string
}

// lightTraceResult carries one trace's contribution to the light outputs.
type lightTraceResult struct {
	traceID   string
	cpp       *CallPathProfile
	slackDrag *SlackDragByCallpath
}

// TraceIDFromFilePath mirrors process_trace.py getTraceIdFromFilePath:
// basename, then everything before the first ".".
func TraceIDFromFilePath(traceFile string) string {
	return strings.Split(filepath.Base(traceFile), ".")[0]
}

// processTraceFile mirrors process_trace.py process(): build the graph,
// compute the critical path, accumulate the call-path profile, and compute
// drag. Returns (nil, nil) when the trace is skipped (no root, or a test
// trace with ignoreTestTraces set).
func processTraceFile(filename string, c *LightConfig) (*lightTraceResult, error) {
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	trace, err := jaeger.Decode(data)
	if err != nil {
		return nil, err
	}
	g, err := NewGraph(trace, c.ServiceName, c.OperationName, &GraphOptions{
		Filename:  filename,
		RootTrace: &c.RootTrace,
	})
	if err != nil {
		return nil, err
	}
	if g.RootNode == nil {
		return nil, nil
	}
	if c.IgnoreTestTraces && g.IsTestTrace {
		return nil, nil
	}

	traceID := TraceIDFromFilePath(filename)
	cp, err := g.FindCriticalPath(nil)
	if err != nil {
		return nil, err
	}
	cpp, _ := g.AccumeCPMetrics(cp, traceID, nil)
	// Drag is always computed in Python's process(); slack stays behind
	// --computeSlackDrag, which this port does not support (slack columns
	// are 0.0).
	drag := g.CalculateDrag(cp, false)
	return &lightTraceResult{
		traceID:   traceID,
		cpp:       cpp,
		slackDrag: g.AggregateDragSlackByCallpath(drag),
	}, nil
}

// LightProcess mirrors process_trace.py lightProcess: process all traces,
// write light-flame-graph-P100.{cct,dot,pb}, conformance outputs when
// enabled, and slackDrag.csv.
func LightProcess(c *LightConfig) error {
	var valid []*lightTraceResult
	for _, traceFile := range c.TraceFiles {
		res, err := processTraceFile(traceFile, c)
		if err != nil {
			return err
		}
		if res != nil {
			valid = append(valid, res)
		}
	}

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

	// Unconditional in lightProcess, like the Python heavy path.
	if _, err := GenSlackDragCSV(MergePerMethodSlackDrag(perTraceSlackDrag), c.OutputDir, SlackDragCSV); err != nil {
		return err
	}
	return nil
}
