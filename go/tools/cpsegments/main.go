// Command cpsegments prints one trace's canonical critical-path segments JSON
// (cp-segments.json, see CONFORMANCE.md) to stdout. It takes the flags of
// `python -m crisp.critical_path_segments`, so the two can be compared byte
// for byte; -rootSpanId instead selects the root by span ID through
// crisp.AnalyzeTrace.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/uber-research/CRISP/go/crisp"
	"github.com/uber-research/CRISP/go/crisp/jaeger"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var file, serviceName, operationName, rootSpanID string
	var rootTrace bool
	flag.StringVar(&file, "file", "", "Jaeger JSON trace file")
	flag.StringVar(&serviceName, "s", "", "name of the service")
	flag.StringVar(&serviceName, "serviceName", "", "name of the service")
	flag.StringVar(&operationName, "a", "", "operation name")
	flag.StringVar(&operationName, "operationName", "", "operation name")
	flag.BoolVar(&rootTrace, "rootTrace", false, "the root span must be serviceName/operationName")
	flag.StringVar(&rootSpanID, "rootSpanId", "", "select the root by span ID (ignores -s, -a and -rootTrace)")
	flag.Parse()
	if file == "" {
		return fmt.Errorf("-file is required")
	}
	if rootSpanID == "" && (serviceName == "" || operationName == "") {
		return fmt.Errorf("-s and -a are required unless -rootSpanId is set")
	}

	data, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	trace, err := jaeger.Decode(data)
	if err != nil {
		return err
	}

	var spans []crisp.CriticalPathSpan
	if rootSpanID != "" {
		analysis, err := crisp.AnalyzeTrace(context.Background(), trace, rootSpanID, nil)
		if err != nil {
			return err
		}
		spans = analysis.Spans
	} else {
		g, err := crisp.NewGraph(trace, serviceName, operationName, &crisp.GraphOptions{Filename: file, RootTrace: &rootTrace})
		if err != nil {
			return err
		}
		if g.RootNode == nil {
			return fmt.Errorf("no analysis root in %s", file)
		}
		cp, err := g.FindCriticalPath(nil)
		if err != nil {
			return err
		}
		spans = g.CriticalPathSegments(cp)
	}

	out, err := crisp.CanonicalCPSegmentsJSON(spans)
	if err != nil {
		return err
	}
	_, err = os.Stdout.WriteString(out)
	return err
}
