// Command difftest is the differential test harness for the CRISP Go port.
// It runs a reference and a candidate implementation over a corpus of Jaeger
// trace JSONs and compares their conformance outputs (conformance.cct, and
// structurally conformance.json).
//
// Modes:
//
//	golden: byte-compare the candidate against the committed goldens under
//	test_cases/golden/. Strict: no re-sorting, so output ordering bugs are
//	caught too. This is the per-PR tier.
//
//	corpus: compare the candidate against a local reference cache, sorting
//	both sides' lines first (content comparison for large corpora whose
//	references are cached, not committed -- e.g. Zenodo shards). Populate or
//	refresh the cache with -refresh, which runs the reference side.
//
//	-strict upgrades corpus mode to byte-compare every light-mode output
//	(conformance.cct/json, light-flame-graph-P100.{cct,dot,pb}, and
//	error-breakdown.json when both templates pass --errorBreakdown)
//	between reference and candidate. slackDrag.csv is compared with data
//	rows sorted: pandas sorts it with unstable quicksort, so tied avgDrag
//	rows are legitimately ordered differently.
//
// The (service, operation) root is derived per trace via crisp.DeriveRootSpan,
// mirroring scripts/generate_goldens.py -- no manifest is needed.
//
// Phase 1 self-check: with -candidate defaulting to the Python reference
// itself, both modes must be 100% green (Python-vs-Python).
//
// Command templates are split on whitespace and then {file}, {service} and
// {operation} are substituted per trace, so substituted values may contain
// spaces. Templates run with cwd = repo root and must write conformance.cct
// (and conformance.json) next to the trace file they are given.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/uber-research/CRISP/go/crisp"
	"github.com/uber-research/CRISP/go/crisp/jaeger"
)

const (
	statusPass  = "PASS"
	statusFail  = "FAIL"
	statusSkip  = "SKIP"
	statusStale = "STALE"
)

var (
	mode      = flag.String("mode", "golden", "golden (compare against committed goldens) | corpus (compare against reference cache)")
	corpus    = flag.String("corpus", "test_cases", "directory of trace JSONs (walked recursively; dirs named 'golden' or starting with '.' are skipped)")
	goldenDir = flag.String("golden", "test_cases/golden", "directory of committed goldens (golden mode)")
	cacheDir  = flag.String("cache", ".difftest-cache", "reference output cache (corpus mode)")
	refresh   = flag.Bool("refresh", false, "corpus mode: re-run the reference side and rewrite the cache before checking")
	strict    = flag.Bool("strict", false, "corpus mode: byte-compare all light-mode outputs, not just canonicalized conformance.cct")
	reference = flag.String("reference", "", "reference command template; default: the Python conformance CLI (auto-detected interpreter)")
	candidate = flag.String("candidate", "", "candidate command template; default: same as reference (Python-vs-Python self-check)")
	repoRoot  = flag.String("repo-root", "", "repo root used as cwd for command templates; default: walk up from cwd looking for crisp/process_trace.py")
	jobs      = flag.Int("jobs", max(1, runtime.NumCPU()/2), "parallel trace workers")
	timeout   = flag.Duration("timeout", 120*time.Second, "per-trace command timeout")
	verbose   = flag.Bool("v", false, "print per-trace results as they complete")
)

type traceCase struct {
	name string // fixture name: path relative to corpus, sans .json, "/" -> "_"
	path string
	rel  string // path relative to corpus
}

type result struct {
	name   string
	status string
	detail string
}

func main() {
	flag.Parse()
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "difftest: %v\n", err)
		os.Exit(2)
	}
}

func run() error {
	root, err := resolveRepoRoot(*repoRoot)
	if err != nil {
		return err
	}
	refTmpl, err := resolveTemplate(*reference, root)
	if err != nil {
		return fmt.Errorf("reference: %w", err)
	}
	candTmpl := *candidate
	if candTmpl == "" {
		candTmpl = refTmpl
	}

	traces, err := discoverTraces(*corpus)
	if err != nil {
		return err
	}
	if len(traces) == 0 {
		return fmt.Errorf("no trace JSONs found under %s", *corpus)
	}

	switch *mode {
	case "golden":
		traces = conformanceFixtures(traces)
	case "corpus":
	default:
		return fmt.Errorf("unknown -mode %q", *mode)
	}
	if *strict && *mode != "corpus" {
		return errors.New("-strict only applies to corpus mode")
	}
	if *mode == "corpus" && *refresh {
		if err := refreshCache(traces, root, refTmpl); err != nil {
			return err
		}
	}

	results := make(chan result, len(traces))
	work := make(chan traceCase)
	var wg sync.WaitGroup
	for i := 0; i < *jobs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for tc := range work {
				results <- checkTrace(tc, root, candTmpl)
			}
		}()
	}
	go func() {
		for _, tc := range traces {
			work <- tc
		}
		close(work)
		wg.Wait()
		close(results)
	}()

	var rs []result
	for r := range results {
		rs = append(rs, r)
		if *verbose && r.status != statusPass {
			fmt.Printf("%-5s %s: %s\n", r.status, r.name, r.detail)
		}
	}
	sort.Slice(rs, func(i, j int) bool { return rs[i].name < rs[j].name })

	counts := map[string]int{}
	for _, r := range rs {
		counts[r.status]++
		if r.status == statusPass {
			continue
		}
		fmt.Printf("%-5s %s: %s\n", r.status, r.name, r.detail)
	}
	fmt.Printf("\n%d/%d passed", counts[statusPass], len(rs))
	if counts[statusSkip] > 0 {
		fmt.Printf(" (%d skipped)", counts[statusSkip])
	}
	fmt.Println()
	if counts[statusFail] > 0 || counts[statusStale] > 0 {
		return errors.New("mismatches found")
	}
	return nil
}

// checkTrace runs the candidate on one trace and compares its output against
// the golden (golden mode) or the reference cache (corpus mode).
func checkTrace(tc traceCase, root, candTmpl string) result {
	data, err := os.ReadFile(tc.path)
	if err != nil {
		return result{tc.name, statusFail, err.Error()}
	}
	trace, err := jaeger.Decode(data)
	if err != nil {
		return result{tc.name, statusSkip, fmt.Sprintf("cannot decode: %v", err)}
	}
	service, operation, err := crisp.DeriveRootSpan(trace)
	if err != nil {
		return result{tc.name, statusSkip, fmt.Sprintf("cannot derive root span: %v", err)}
	}

	tmp, err := os.MkdirTemp("", "crisp-difftest-")
	if err != nil {
		return result{tc.name, statusFail, err.Error()}
	}
	defer os.RemoveAll(tmp)
	tmpTrace := filepath.Join(tmp, filepath.Base(tc.path))
	if err := os.WriteFile(tmpTrace, data, 0o644); err != nil {
		return result{tc.name, statusFail, err.Error()}
	}

	if *strict {
		outputs, err := runSideAll(candTmpl, root, tmpTrace, service, operation)
		if err != nil {
			return result{tc.name, statusFail, err.Error()}
		}
		return compareCacheStrict(tc.name, data, outputs)
	}

	cct, jsonOut, err := runSide(candTmpl, root, tmpTrace, service, operation)
	if err != nil {
		return result{tc.name, statusFail, err.Error()}
	}

	if *mode == "golden" {
		return compareGolden(tc.name, cct, jsonOut)
	}
	return compareCache(tc.name, data, cct)
}

// compareGolden compares candidate output byte-wise against the committed
// golden (no re-sorting: ordering bugs must fail), plus a structural
// comparison of conformance.json.
func compareGolden(name string, cct, jsonOut []byte) result {
	goldenCCT, err := os.ReadFile(filepath.Join(*goldenDir, name, "conformance.cct"))
	if err != nil {
		return result{name, statusFail, fmt.Sprintf("no committed golden: %v", err)}
	}
	if !bytes.Equal(cct, goldenCCT) {
		return result{name, statusFail, diffReport("conformance.cct", goldenCCT, cct)}
	}
	goldenJSON, err := os.ReadFile(filepath.Join(*goldenDir, name, "conformance.json"))
	if err != nil {
		return result{name, statusFail, fmt.Sprintf("no committed golden: %v", err)}
	}
	if !jsonEqual(goldenJSON, jsonOut) {
		return result{name, statusFail, "conformance.json differs structurally (parse both sides and field-compare to locate)"}
	}
	return result{name, statusPass, ""}
}

// compareCache compares candidate output against the cached reference,
// canonicalizing both sides (line sort) first -- corpus mode checks content,
// leaving ordering enforcement to the golden tier.
func compareCache(name string, traceData, cct []byte) result {
	cachePath := filepath.Join(*cacheDir, name+".cct")
	ref, err := os.ReadFile(cachePath)
	if err != nil {
		return result{name, statusStale, "no cached reference; re-run with -refresh"}
	}
	hashPath := filepath.Join(*cacheDir, name+".sha256")
	wantHash, err := os.ReadFile(hashPath)
	if err != nil {
		return result{name, statusStale, "no cached trace hash; re-run with -refresh"}
	}
	if got := hashHex(traceData); got != strings.TrimSpace(string(wantHash)) {
		return result{name, statusStale, "trace changed since reference was cached; re-run with -refresh"}
	}
	got := canonicalize(cct)
	want := canonicalize(ref)
	if !bytes.Equal(got, want) {
		return result{name, statusFail, diffReport("conformance.cct (canonicalized)", want, got)}
	}
	return result{name, statusPass, ""}
}

// refreshCache runs the reference side over the whole corpus in parallel,
// writing a trace-content hash per trace plus either the canonicalized
// conformance.cct (default) or every light-mode output file (-strict).
func refreshCache(traces []traceCase, root, refTmpl string) error {
	fmt.Printf("refreshing reference cache in %s (%d traces)\n", *cacheDir, len(traces))
	if err := os.MkdirAll(*cacheDir, 0o755); err != nil {
		return err
	}
	type failure struct {
		name string
		err  error
	}
	work := make(chan traceCase)
	failures := make(chan failure, len(traces))
	var wg sync.WaitGroup
	for i := 0; i < *jobs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for tc := range work {
				if err := refreshOne(tc, root, refTmpl); err != nil {
					failures <- failure{tc.name, err}
				}
			}
		}()
	}
	go func() {
		for _, tc := range traces {
			work <- tc
		}
		close(work)
		wg.Wait()
		close(failures)
	}()
	var fails []failure
	for f := range failures {
		fails = append(fails, f)
	}
	if len(fails) > 0 {
		sort.Slice(fails, func(i, j int) bool { return fails[i].name < fails[j].name })
		for _, f := range fails {
			fmt.Printf("REFRESH-FAIL %s: %v\n", f.name, f.err)
		}
		return fmt.Errorf("%d reference failures", len(fails))
	}
	return nil
}

func refreshOne(tc traceCase, root, refTmpl string) error {
	data, err := os.ReadFile(tc.path)
	if err != nil {
		return err
	}
	// Resume: a cache entry whose trace hash still matches is reused, so an
	// interrupted (or transferred) refresh does not redo finished traces.
	hashPath := filepath.Join(*cacheDir, tc.name+".sha256")
	if wantHash, err := os.ReadFile(hashPath); err == nil &&
		hashHex(data) == strings.TrimSpace(string(wantHash)) {
		entry := filepath.Join(*cacheDir, tc.name)
		if !*strict {
			entry += ".cct"
		}
		if _, err := os.Stat(entry); err == nil {
			return nil
		}
	}
	trace, err := jaeger.Decode(data)
	if err != nil {
		fmt.Printf("SKIP %s: cannot decode: %v\n", tc.name, err)
		return nil
	}
	service, operation, err := crisp.DeriveRootSpan(trace)
	if err != nil {
		fmt.Printf("SKIP %s: cannot derive root span: %v\n", tc.name, err)
		return nil
	}
	tmp, err := os.MkdirTemp("", "crisp-difftest-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	tmpTrace := filepath.Join(tmp, filepath.Base(tc.path))
	if err := os.WriteFile(tmpTrace, data, 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(*cacheDir, tc.name+".sha256"), []byte(hashHex(data)+"\n"), 0o644); err != nil {
		return err
	}
	if *strict {
		outputs, err := runSideAll(refTmpl, root, tmpTrace, service, operation)
		if err != nil {
			return fmt.Errorf("reference failed: %w", err)
		}
		dir := filepath.Join(*cacheDir, tc.name)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
		for fname, content := range outputs {
			if err := os.WriteFile(filepath.Join(dir, fname), content, 0o644); err != nil {
				return err
			}
		}
		return nil
	}
	cct, _, err := runSide(refTmpl, root, tmpTrace, service, operation)
	if err != nil {
		return fmt.Errorf("reference failed: %w", err)
	}
	return os.WriteFile(filepath.Join(*cacheDir, tc.name+".cct"), canonicalize(cct), 0o644)
}

// runSide executes a command template against a trace copy and returns the
// conformance outputs written next to it.
func runSide(tmpl, root, tracePath, service, operation string) (cct, jsonOut []byte, err error) {
	repl := strings.NewReplacer("{file}", tracePath, "{service}", service, "{operation}", operation)
	parts := strings.Fields(tmpl)
	argv := make([]string, 0, len(parts))
	for _, p := range parts {
		argv = append(argv, repl.Replace(p))
	}
	if len(argv) == 0 {
		return nil, nil, errors.New("empty command template")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Dir = root
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if runErr := cmd.Run(); runErr != nil {
		snip := stderr.String()
		if len(snip) > 2000 {
			snip = snip[:2000] + "... (truncated)"
		}
		return nil, nil, fmt.Errorf("command failed: %v\nstderr: %s", runErr, snip)
	}
	outDir := filepath.Dir(tracePath)
	cct, err = os.ReadFile(filepath.Join(outDir, "conformance.cct"))
	if err != nil {
		return nil, nil, fmt.Errorf("candidate did not produce conformance.cct: %v", err)
	}
	jsonOut, _ = os.ReadFile(filepath.Join(outDir, "conformance.json")) // optional in corpus mode
	return cct, jsonOut, nil
}

// lightOutputFiles are the files lightProcess writes next to the trace,
// compared byte-wise in strict mode.
var lightOutputFiles = []string{
	"conformance.cct",
	"conformance.json",
	"light-flame-graph-P100.cct",
	"light-flame-graph-P100.dot",
	"light-flame-graph-P100.pb",
	"slackDrag.csv",
	crisp.ErrorBreakdownFile,
}

// runSideAll is runSide but captures every light-mode output file. A file
// absent on disk is absent from the map (slackDrag.csv is legitimately not
// written when there is no drag data).
func runSideAll(tmpl, root, tracePath, service, operation string) (map[string][]byte, error) {
	repl := strings.NewReplacer("{file}", tracePath, "{service}", service, "{operation}", operation)
	parts := strings.Fields(tmpl)
	argv := make([]string, 0, len(parts))
	for _, p := range parts {
		argv = append(argv, repl.Replace(p))
	}
	if len(argv) == 0 {
		return nil, errors.New("empty command template")
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Dir = root
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if runErr := cmd.Run(); runErr != nil {
		snip := stderr.String()
		if len(snip) > 2000 {
			snip = snip[:2000] + "... (truncated)"
		}
		return nil, fmt.Errorf("command failed: %v\nstderr: %s", runErr, snip)
	}
	outDir := filepath.Dir(tracePath)
	outputs := make(map[string][]byte, len(lightOutputFiles))
	for _, name := range lightOutputFiles {
		data, err := os.ReadFile(filepath.Join(outDir, name))
		if err != nil {
			continue
		}
		outputs[name] = data
	}
	// An empty map means the implementation skipped the trace (Python's
	// lightProcess writes nothing when the trace has no usable root).
	return outputs, nil
}

// sortCSVRows sorts a slackDrag.csv's data rows (header stays first) so the
// comparison is insensitive to pandas' unstable quicksort tie order.
func sortCSVRows(b []byte) []byte {
	lines := bytes.Split(bytes.TrimRight(b, "\n"), []byte("\n"))
	if len(lines) <= 2 {
		return b
	}
	rows := lines[1:]
	sort.Slice(rows, func(i, j int) bool { return bytes.Compare(rows[i], rows[j]) < 0 })
	return append(bytes.Join(append(lines[:1], rows...), []byte("\n")), '\n')
}

// compareCacheStrict byte-compares every light-mode output against the
// cached reference, except slackDrag.csv, which is compared with data rows
// sorted (pandas' quicksort orders tied avgDrag rows nondeterministically).
func compareCacheStrict(name string, traceData []byte, outputs map[string][]byte) result {
	dir := filepath.Join(*cacheDir, name)
	hashPath := filepath.Join(*cacheDir, name+".sha256")
	wantHash, err := os.ReadFile(hashPath)
	if err != nil {
		return result{name, statusStale, "no cached trace hash; re-run with -refresh"}
	}
	if got := hashHex(traceData); got != strings.TrimSpace(string(wantHash)) {
		return result{name, statusStale, "trace changed since reference was cached; re-run with -refresh"}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return result{name, statusStale, "no cached reference; re-run with -refresh"}
	}
	if len(entries) == 0 {
		// The reference skipped this trace (no usable root); the candidate
		// must skip it too.
		if len(outputs) == 0 {
			return result{name, statusPass, ""}
		}
		return result{name, statusFail, "reference produced no outputs (trace skipped), candidate produced some"}
	}
	if len(outputs) == 0 {
		return result{name, statusFail, "reference produced outputs, candidate produced none (skipped?)"}
	}
	for _, fname := range lightOutputFiles {
		ref, refErr := os.ReadFile(filepath.Join(dir, fname))
		got, ok := outputs[fname]
		if refErr != nil {
			if ok {
				return result{name, statusFail, fname + ": candidate produced output, reference did not"}
			}
			continue // absent on both sides (e.g. empty slackDrag data)
		}
		if !ok {
			return result{name, statusFail, fname + ": reference produced output, candidate did not"}
		}
		if fname == "slackDrag.csv" {
			ref, got = sortCSVRows(ref), sortCSVRows(got)
		}
		if !bytes.Equal(got, ref) {
			return result{name, statusFail, diffReport(fname, ref, got)}
		}
	}
	return result{name, statusPass, ""}
}

// canonicalize mirrors crisp/conformance.py canonical_cct: drop empty lines,
// sort by code point (identical to UTF-8 byte order), exactly one trailing
// newline.
func canonicalize(b []byte) []byte {
	var lines [][]byte
	for _, line := range bytes.Split(b, []byte("\n")) {
		if len(line) > 0 {
			lines = append(lines, line)
		}
	}
	sort.Slice(lines, func(i, j int) bool { return bytes.Compare(lines[i], lines[j]) < 0 })
	if len(lines) == 0 {
		return nil
	}
	return append(bytes.Join(lines, []byte("\n")), '\n')
}

// diffReport describes the first differing line between want and got, with a
// two-line context window from each side.
func diffReport(label string, want, got []byte) string {
	wantLines := bytes.Split(want, []byte("\n"))
	gotLines := bytes.Split(got, []byte("\n"))
	i := 0
	for i < len(wantLines) && i < len(gotLines) && bytes.Equal(wantLines[i], gotLines[i]) {
		i++
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s first differs at line %d", label, i+1)
	window := func(tag string, lines [][]byte) {
		lo := max(0, i-2)
		hi := min(len(lines), i+3)
		for j := lo; j < hi; j++ {
			marker := "  "
			if j == i {
				marker = "> "
			}
			fmt.Fprintf(&b, "\n%s %s%d| %s", tag, marker, j+1, lines[j])
		}
	}
	window("ref", wantLines)
	window("got", gotLines)
	return b.String()
}

// jsonEqual reports whether two JSON documents are structurally equal
// (numbers compared exactly via json.Number).
func jsonEqual(a, b []byte) bool {
	decode := func(data []byte) (any, error) {
		dec := json.NewDecoder(bytes.NewReader(data))
		dec.UseNumber()
		var v any
		return v, dec.Decode(&v)
	}
	va, err := decode(a)
	if err != nil {
		return false
	}
	vb, err := decode(b)
	if err != nil {
		return false
	}
	return reflect.DeepEqual(va, vb)
}

func hashHex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// discoverTraces walks corpus for *.json files, skipping the golden output
// directory and hidden directories.
func discoverTraces(corpus string) ([]traceCase, error) {
	var traces []traceCase
	err := filepath.WalkDir(corpus, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != corpus && (d.Name() == "golden" || strings.HasPrefix(d.Name(), ".")) {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(d.Name(), ".json") {
			return nil
		}
		rel, err := filepath.Rel(corpus, path)
		if err != nil {
			return err
		}
		name := strings.TrimSuffix(rel, ".json")
		name = strings.ReplaceAll(name, string(filepath.Separator), "_")
		traces = append(traces, traceCase{name: name, path: path, rel: rel})
		return nil
	})
	return traces, err
}

// conformanceFixtures keeps the traces that have conformance goldens, as laid
// out by scripts/generate_goldens.py: top-level *.json and err_pattern*/*.json.
// Other fixture directories (e.g. error_breakdown/) carry their own goldens.
func conformanceFixtures(traces []traceCase) []traceCase {
	var kept []traceCase
	for _, tc := range traces {
		dir := filepath.Dir(tc.rel)
		if dir == "." || (!strings.ContainsRune(dir, filepath.Separator) && strings.HasPrefix(dir, "err_pattern")) {
			kept = append(kept, tc)
		}
	}
	return kept
}

// resolveRepoRoot finds the repo root (the directory the Python CLI must run
// from) by walking up from cwd looking for crisp/process_trace.py.
func resolveRepoRoot(flagVal string) (string, error) {
	if flagVal != "" {
		return filepath.Abs(flagVal)
	}
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "crisp", "process_trace.py")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", errors.New("could not find repo root (crisp/process_trace.py); pass -repo-root")
		}
		dir = parent
	}
}

// resolveTemplate returns the reference command template, defaulting to the
// Python conformance CLI with the repo's .venv interpreter when present.
func resolveTemplate(flagVal, root string) (string, error) {
	if flagVal != "" {
		return flagVal, nil
	}
	python := "python3"
	if _, err := os.Stat(filepath.Join(root, ".venv", "bin", "python")); err == nil {
		python = filepath.Join(root, ".venv", "bin", "python")
	}
	return python + " -m crisp.process_trace --file {file} -s {service} -a {operation} --rootTrace --conformance", nil
}
