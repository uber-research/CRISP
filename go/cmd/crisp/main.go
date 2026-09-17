// Command crisp is the Go port of the CRISP critical-path analyzer CLI
// (crisp/process_trace.py). It supports the light and conformance modes
// (lightProcess); the heavy analysis mode (HTML report, percentile
// flamegraphs, error analysis) is not ported.
//
// Flag surface mirrors initArgs for the light/conformance path:
//   - --file wins over --inputDir when both are given (the help text says
//     "mutually exclusive" but Python never enforces it).
//   - -o/--outputDir is accepted for compatibility but, exactly like
//     Python's light mode, never read: outputs go to the --file parent
//     directory or inputDir (Config.getOutputDir).
//   - --tags and --exclude-from-cp are accepted for compatibility but do
//     not affect light mode (Python's process() never forwards them to the
//     Graph constructor).
//   - Python's glob.glob returns traces in directory (os.scandir) order;
//     globJSONDirOrder mirrors that with Readdirnames so multi-file runs
//     produce byte-identical .cct/.pb on the same filesystem. Conformance
//     outputs are canonicalized (sorted) regardless.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/uber-research/CRISP/go/crisp"
)

func main() {
	var (
		serviceName, operationName         string
		inputDir, fileArg, outputDir       string
		rootTrace, lightMode, conformance  bool
		ignoreTestTraces, computeSlackDrag bool
		maxExemplars, parallelism          int
		tags, excludeFromCP                string
	)

	flag.StringVar(&serviceName, "s", "", "name of the service")
	flag.StringVar(&serviceName, "serviceName", "", "name of the service")
	flag.StringVar(&operationName, "a", "", "operation name")
	flag.StringVar(&operationName, "operationName", "", "operation name")
	flag.BoolVar(&rootTrace, "rootTrace", false, "Should the service and operation be the root span of the trace (default:false).")
	flag.BoolVar(&lightMode, "lightMode", false, "Light mode: generate only the P0-100 call-path CCT outputs.")
	flag.BoolVar(&conformance, "conformance", false, "Write deterministic conformance outputs (conformance.cct, conformance.json); runs the light-mode pipeline.")
	flag.IntVar(&maxExemplars, "maxExemplars", 3, "Maximum number of exemplars (trace/span pairs) to keep per call path in protobuf output (default=3).")
	flag.StringVar(&inputDir, "i", "traces", "input path of the trace directory")
	flag.StringVar(&inputDir, "inputDir", "traces", "input path of the trace directory")
	flag.StringVar(&fileArg, "file", "", "input path of the trace file")
	flag.StringVar(&outputDir, "o", "", "accepted for compatibility; unused in light mode (mirrors Python)")
	flag.StringVar(&outputDir, "outputDir", "", "accepted for compatibility; unused in light mode (mirrors Python)")
	flag.BoolVar(&ignoreTestTraces, "ignoreTestTraces", false, "Ignore traces marked as synthetic test traces.")
	flag.BoolVar(&computeSlackDrag, "computeSlackDrag", false, "Not supported by the Go port (slack computation is not ported).")
	flag.IntVar(&parallelism, "parallelism", 1, "accepted for compatibility; analysis runs sequentially (outputs are identical)")
	flag.StringVar(&tags, "tags", "", "accepted for compatibility; unused in light mode (mirrors Python)")
	flag.StringVar(&excludeFromCP, "exclude-from-cp", "", "accepted for compatibility; unused in light mode (mirrors Python)")
	_ = outputDir
	_ = parallelism
	_ = tags
	_ = excludeFromCP

	flag.Parse()

	if computeSlackDrag {
		fmt.Fprintln(os.Stderr, "crisp: --computeSlackDrag is not supported by the Go port (slack computation requires the unported DependencyGraph); drag is always computed")
		os.Exit(2)
	}
	if !lightMode && !conformance {
		fmt.Fprintln(os.Stderr, "crisp: the Go port supports --lightMode and --conformance only; the heavy analysis mode is not ported")
		os.Exit(2)
	}

	// initArgs trace-file resolution: --file wins over --inputDir.
	var traceFiles []string
	if fileArg != "" {
		traceFiles = []string{fileArg}
	} else {
		var err error
		traceFiles, err = globJSONDirOrder(inputDir)
		if err != nil {
			fmt.Fprintln(os.Stderr, "crisp:", err)
			os.Exit(2)
		}
	}

	// Config.getOutputDir(): --file's parent directory, else inputDir.
	outDir := inputDir
	if fileArg != "" {
		outDir = filepath.Dir(fileArg)
	}

	cfg := &crisp.LightConfig{
		ServiceName:      serviceName,
		OperationName:    operationName,
		RootTrace:        rootTrace,
		Conformance:      conformance,
		MaxExemplars:     maxExemplars,
		IgnoreTestTraces: ignoreTestTraces,
		TraceFiles:       traceFiles,
		OutputDir:        outDir,
	}
	if err := crisp.LightProcess(cfg); err != nil {
		fmt.Fprintln(os.Stderr, "crisp:", err)
		os.Exit(1)
	}
}

// globJSONDirOrder mirrors glob.glob(inputDir/*.json): directory entries
// in readdir order (like Python's os.scandir-backed glob), filtered to
// "*.json".
func globJSONDirOrder(dir string) ([]string, error) {
	f, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	names, err := f.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, name := range names {
		if strings.HasSuffix(name, ".json") {
			out = append(out, filepath.Join(dir, name))
		}
	}
	return out, nil
}
