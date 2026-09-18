// Command prodbench times three production-style ways of running the
// CRISP light/conformance pipeline over a sample of traces:
//
//	A: Go wrapper shelling out to the Python CLI (python -m crisp.process_trace)
//	B: Go wrapper shelling out to the Go CLI (crisp-go)
//	C: traces preloaded into memory, calling the Go library in-process
//	   (crisp.ProcessSingleTraceData) — no subprocess, no per-trace disk read
//
// All modes use a worker pool of -jobs and record per-trace wall times.
// Modes A/B symlink each sampled trace into a per-trace scratch dir so the
// CLI's "outputs next to the trace file" behavior does not collide across
// traces sharing one corpus directory. Mode C's preload time is reported
// separately and excluded from the timed section.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/uber-research/CRISP/go/crisp"
	"github.com/uber-research/CRISP/go/crisp/jaeger"
)

type task struct {
	traceID   string
	realPath  string
	linkPath  string // modes A/B: symlink inside outDir
	outDir    string
	data      []byte // mode C only
	service   string
	operation string
}

type outcome struct {
	traceID string
	dur     time.Duration
	err     error
}

func main() {
	var (
		mode      = flag.String("mode", "", "A (Go->Python CLI), B (Go->Go CLI), or C (in-memory Go library)")
		dir       = flag.String("dir", "", "directory of Jaeger .json traces")
		n         = flag.Int("n", 100, "number of traces to sample")
		seed      = flag.Int64("seed", 42, "sampling RNG seed")
		jobs      = flag.Int("jobs", 8, "worker parallelism")
		service   = flag.String("s", "", "root service name")
		operation = flag.String("a", "", "root operation name")
		pythonBin = flag.String("python", "", "python binary for mode A")
		repoRoot  = flag.String("repo", "", "CRISP repo root (python cwd) for mode A")
		goBin     = flag.String("gobin", "", "crisp-go binary for mode B")
		workDir   = flag.String("workdir", "", "scratch dir for per-trace outputs")
		derive    = flag.Bool("derive", false, "derive per-trace root service/operation (untimed setup) instead of using -s/-a for all")
		rootTrace = flag.Bool("roottrace", false, "pass --rootTrace to the CLIs / set RootTrace in mode C")
	)
	flag.Parse()
	if *mode == "" || *dir == "" || *workDir == "" || (!*derive && (*service == "" || *operation == "")) {
		fmt.Fprintln(os.Stderr, "required: -mode -dir -workdir, and -s -a unless -derive")
		os.Exit(2)
	}
	runtime.GOMAXPROCS(*jobs)

	// Deterministic sample: sort names, then seeded shuffle, take n.
	names, err := jsonNames(*dir)
	if err != nil {
		fmt.Fprintln(os.Stderr, "prodbench:", err)
		os.Exit(1)
	}
	rng := rand.New(rand.NewSource(*seed))
	rng.Shuffle(len(names), func(i, j int) { names[i], names[j] = names[j], names[i] })
	if *n > len(names) {
		*n = len(names)
	}
	picked := names[:*n]
	sort.Strings(picked) // stable processing order for reproducible logs

	tasks := make([]task, 0, *n)
	for _, name := range picked {
		traceID := strings.Split(filepath.Base(name), ".")[0]
		outDir := filepath.Join(*workDir, *mode, traceID)
		if err := os.MkdirAll(outDir, 0o755); err != nil {
			fmt.Fprintln(os.Stderr, "prodbench:", err)
			os.Exit(1)
		}
		t := task{traceID: traceID, realPath: filepath.Join(*dir, name), outDir: outDir,
			service: *service, operation: *operation}
		if *derive {
			raw, err := os.ReadFile(t.realPath)
			if err != nil {
				fmt.Fprintln(os.Stderr, "prodbench: derive read:", err)
				os.Exit(1)
			}
			tr, err := jaeger.Decode(raw)
			if err != nil {
				fmt.Fprintln(os.Stderr, "prodbench: derive decode:", err)
				os.Exit(1)
			}
			svc, op, err := crisp.DeriveRootSpan(tr)
			if err != nil {
				fmt.Fprintf(os.Stderr, "prodbench: derive %s: %v (trace will skip)\n", traceID, err)
			}
			t.service, t.operation = svc, op
		}
		if *mode == "A" || *mode == "B" {
			link := filepath.Join(outDir, name)
			if err := os.Symlink(t.realPath, link); err != nil {
				fmt.Fprintln(os.Stderr, "prodbench: symlink:", err)
				os.Exit(1)
			}
			t.linkPath = link
		}
		tasks = append(tasks, t)
	}

	// Mode C preload: read all trace bytes before the timed section.
	var preloadDur time.Duration
	if *mode == "C" {
		start := time.Now()
		for i := range tasks {
			data, err := os.ReadFile(tasks[i].realPath)
			if err != nil {
				fmt.Fprintln(os.Stderr, "prodbench: preload:", err)
				os.Exit(1)
			}
			tasks[i].data = data
		}
		preloadDur = time.Since(start)
	}

	threadCaps := append(os.Environ(),
		"OMP_NUM_THREADS=1", "OPENBLAS_NUM_THREADS=1", "MKL_NUM_THREADS=1", "VECLIB_MAXIMUM_THREADS=1")

	cliArgs := func(t task) []string {
		args := []string{"--file", t.linkPath, "-s", t.service, "-a", t.operation, "--conformance"}
		if *rootTrace {
			args = append(args, "--rootTrace")
		}
		return args
	}
	runOne := func(t task) error {
		switch *mode {
		case "A":
			args := append([]string{"-m", "crisp.process_trace"}, cliArgs(t)...)
			cmd := exec.Command(*pythonBin, args...)
			cmd.Dir = *repoRoot
			cmd.Env = threadCaps
			return cmd.Run()
		case "B":
			cmd := exec.Command(*goBin, cliArgs(t)...)
			cmd.Env = threadCaps
			return cmd.Run()
		case "C":
			cfg := &crisp.LightConfig{
				ServiceName:   t.service,
				OperationName: t.operation,
				RootTrace:     *rootTrace,
				Conformance:   true,
				MaxExemplars:  3,
				OutputDir:     t.outDir,
			}
			return crisp.ProcessSingleTraceData(t.data, t.traceID, cfg)
		}
		return fmt.Errorf("unknown mode %q", *mode)
	}

	taskCh := make(chan task)
	var outcomes []outcome
	var mu sync.Mutex
	var wg sync.WaitGroup
	for w := 0; w < *jobs; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range taskCh {
				start := time.Now()
				err := runOne(t)
				rec := outcome{traceID: t.traceID, dur: time.Since(start), err: err}
				mu.Lock()
				outcomes = append(outcomes, rec)
				mu.Unlock()
			}
		}()
	}

	wallStart := time.Now()
	for _, t := range tasks {
		taskCh <- t
	}
	close(taskCh)
	wg.Wait()
	wall := time.Since(wallStart)

	durs := make([]time.Duration, 0, len(outcomes))
	failures := 0
	var sum time.Duration
	for _, o := range outcomes {
		if o.err != nil {
			failures++
			fmt.Fprintf(os.Stderr, "FAIL %s: %v\n", o.traceID, o.err)
			continue
		}
		durs = append(durs, o.dur)
		sum += o.dur
	}
	sort.Slice(durs, func(i, j int) bool { return durs[i] < durs[j] })
	pct := func(p float64) time.Duration {
		if len(durs) == 0 {
			return 0
		}
		idx := int(p * float64(len(durs)-1))
		return durs[idx]
	}

	mean := int64(0)
	if len(durs) > 0 {
		mean = sum.Milliseconds() / int64(len(durs))
	}
	res := map[string]any{
		"mode":       *mode,
		"traces":     len(tasks),
		"jobs":       *jobs,
		"failures":   failures,
		"wall_ms":    wall.Milliseconds(),
		"preload_ms": preloadDur.Milliseconds(),
		"sum_ms":     sum.Milliseconds(),
		"mean_ms":    mean,
		"p50_ms":     pct(0.50).Milliseconds(),
		"p95_ms":     pct(0.95).Milliseconds(),
		"min_ms":     pct(0).Milliseconds(),
		"max_ms":     pct(1).Milliseconds(),
	}
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(res); err != nil {
		fmt.Fprintln(os.Stderr, "prodbench:", err)
		os.Exit(1)
	}
	if failures > 0 {
		os.Exit(1)
	}
}

func jsonNames(dir string) ([]string, error) {
	f, err := os.Open(dir)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	names, err := f.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	out := names[:0]
	for _, name := range names {
		if strings.HasSuffix(name, ".json") {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out, nil
}
