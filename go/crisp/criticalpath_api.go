package crisp

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/uber-research/CRISP/go/crisp/jaeger"
)

// CriticalPathContributor is one span on the critical path.
type CriticalPathContributor struct {
	SpanID    string
	Service   string
	Operation string
	// Duration is the span duration minus the durations of its critical-path
	// children, clamped at zero.
	Duration time.Duration
}

// ErrRootNotFound means that the root span ID is empty or that the trace has
// no span with it.
var ErrRootNotFound = errors.New("root span not found")

// AnalyzeOptions configures AnalyzeTrace. It has no fields yet; nil and the
// zero value select the defaults.
type AnalyzeOptions struct{}

// TraceAnalysis is the single-trace analysis of one critical path.
type TraceAnalysis struct {
	// Spans are the critical-path spans in critical-path order; Spans[0] is
	// the root. Their Segments together cover the root span exactly.
	Spans []CriticalPathSpan
}

// AnalyzeTrace returns the critical path under the span with ID rootSpanID,
// with each span's timestamps, exclusive time, and the time windows it is on
// the critical path (Graph.CriticalPathSegments). Unlike the light-mode entry
// points it takes an already decoded, non-nil trace and does no file I/O. The
// graph is built with default options plus GraphOptions.RootSpanID. ctx is
// checked once, before any work.
func AnalyzeTrace(ctx context.Context, trace *jaeger.Trace, rootSpanID string, opts *AnalyzeOptions) (*TraceAnalysis, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if rootSpanID == "" {
		return nil, fmt.Errorf("%w: empty span ID", ErrRootNotFound)
	}
	g, err := NewGraph(trace, "", "", &GraphOptions{RootSpanID: rootSpanID})
	if err != nil {
		return nil, err
	}
	if g.RootNode == nil {
		return nil, fmt.Errorf("%w: %q", ErrRootNotFound, rootSpanID)
	}
	cp, err := g.FindCriticalPath(nil)
	if err != nil {
		return nil, err
	}
	return &TraceAnalysis{Spans: g.CriticalPathSegments(cp)}, nil
}

// CriticalPath returns the critical path under the span with ID rootSpanID,
// sorted by Duration descending, then by SpanID. It is AnalyzeTrace reduced
// to each span's exclusive time. The light-mode flame graph sums the same
// times per call path, but clamps negative sums per call path rather than
// per span.
func CriticalPath(ctx context.Context, trace *jaeger.Trace, rootSpanID string) ([]CriticalPathContributor, error) {
	analysis, err := AnalyzeTrace(ctx, trace, rootSpanID, nil)
	if err != nil {
		return nil, err
	}
	contributors := make([]CriticalPathContributor, len(analysis.Spans))
	for i, s := range analysis.Spans {
		contributors[i] = CriticalPathContributor{
			SpanID:    s.SpanID,
			Service:   s.Service,
			Operation: s.Operation,
			Duration:  s.Exclusive,
		}
	}
	slices.SortFunc(contributors, func(a, b CriticalPathContributor) int {
		return cmp.Or(cmp.Compare(b.Duration, a.Duration), strings.Compare(a.SpanID, b.SpanID))
	})
	return contributors, nil
}
