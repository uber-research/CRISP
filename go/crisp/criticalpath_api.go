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

// ErrRootNotFound means that the trace has no span with the requested ID.
var ErrRootNotFound = errors.New("root span not found")

// CriticalPath returns the critical path under the span with ID rootSpanID,
// sorted by Duration descending, then by SpanID. Unlike the light-mode entry
// points it takes an already decoded, non-nil trace and does no file I/O. The
// graph is built with default options plus GraphOptions.RootSpanID. Durations
// are the per-span exclusive times from AccumeCPMetrics; the light-mode flame
// graph sums the same times per call path, but clamps negative sums per call
// path rather than per span. ctx is checked once, before any work.
func CriticalPath(ctx context.Context, trace *jaeger.Trace, rootSpanID string) ([]CriticalPathContributor, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
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
	_, exclusive := g.AccumeCPMetrics(cp, "", nil)

	contributors := make([]CriticalPathContributor, len(cp))
	for i, node := range cp {
		contributors[i] = CriticalPathContributor{
			SpanID:    node.SID,
			Service:   g.ProcessName[node.ProcessID],
			Operation: node.OpName,
			Duration:  time.Duration(exclusive[node.SID]) * time.Microsecond,
		}
	}
	slices.SortFunc(contributors, func(a, b CriticalPathContributor) int {
		return cmp.Or(cmp.Compare(b.Duration, a.Duration), strings.Compare(a.SpanID, b.SpanID))
	})
	return contributors, nil
}
