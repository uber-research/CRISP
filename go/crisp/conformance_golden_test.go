package crisp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/uber-research/CRISP/go/crisp/jaeger"
)

// goldenName maps a fixture path to its golden directory name, mirroring
// tests/test_conformance.py discover_fixtures: the path relative to
// test_cases/ without extension, path components joined by "_".
func goldenName(t *testing.T, dir, fixture string) string {
	t.Helper()
	rel, err := filepath.Rel(dir, fixture)
	if err != nil {
		t.Fatal(err)
	}
	stem := strings.TrimSuffix(rel, ".json")
	return strings.ReplaceAll(filepath.ToSlash(stem), "/", "_")
}

// runConformancePipeline mirrors the light+conformance pipeline of
// crisp/process_trace.py lightProcess for a single trace file: Graph ->
// findCriticalPath -> accumeCPMetrics -> aggregateCallPathProfiles ->
// MergeCallPathProfilesWithExemplars -> conformance outputs. The
// findErrorsOnCriticalPath/computeTimeSaved/getMetrics steps in between do
// not mutate timings (see graph.py mutation sites: only sanitize and the
// restore-guarded projection path), so CPMetrics is unaffected by them.
func runConformancePipeline(t *testing.T, fixture, traceID string) (string, string) {
	t.Helper()
	data, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatal(err)
	}
	trace, err := jaeger.Decode(data)
	if err != nil {
		t.Fatalf("decode %s: %v", fixture, err)
	}
	service, operation, err := DeriveRootSpan(trace)
	if err != nil {
		t.Fatalf("derive root span %s: %v", fixture, err)
	}
	// Config(rootTrace=True, lightMode=True, conformance=True) with defaults.
	g, err := NewGraph(trace, service, operation, &GraphOptions{Filename: fixture})
	if err != nil {
		t.Fatalf("graph %s: %v", fixture, err)
	}
	if g.RootNode == nil {
		t.Fatalf("no root node for %s (golden fixtures all root)", fixture)
	}
	cp, err := g.FindCriticalPath(nil)
	if err != nil {
		t.Fatalf("critical path %s: %v", fixture, err)
	}
	cpp, _ := g.AccumeCPMetrics(cp, traceID, nil)
	flameGraphStr, err := AggregateCallPathProfiles([]*CallPathProfile{cpp})
	if err != nil {
		t.Fatalf("aggregate %s: %v", fixture, err)
	}
	merged := MergeCallPathProfilesWithExemplars([]*TraceMetrics{{TraceID: traceID, CPMetrics: cpp}}, 3)

	cctStr := CanonicalCCT(flameGraphStr)
	jsonStr, err := CanonicalResponseJSON(BuildConformanceResponse(cctStr, merged, 3))
	if err != nil {
		t.Fatalf("canonical json %s: %v", fixture, err)
	}
	return cctStr, jsonStr
}

// TestConformanceMatchesGolden is the Go port of
// tests/test_conformance.py::test_conformance_matches_golden: the full
// pipeline output must be byte-identical to the Python-generated goldens.
func TestConformanceMatchesGolden(t *testing.T) {
	dir := fixturesDir(t)
	for _, fixture := range discoverFixtures(t) {
		name := goldenName(t, dir, fixture)
		goldenDir := filepath.Join(dir, "golden", name)
		if _, err := os.Stat(goldenDir); err != nil {
			t.Fatalf("fixture %s has no golden dir %s", fixture, goldenDir)
		}
		t.Run(name, func(t *testing.T) {
			traceID := strings.TrimSuffix(filepath.Base(fixture), ".json")
			cctStr, jsonStr := runConformancePipeline(t, fixture, traceID)

			goldenCCT, err := os.ReadFile(filepath.Join(goldenDir, ConformanceCCTFile))
			if err != nil {
				t.Fatal(err)
			}
			if cctStr != string(goldenCCT) {
				t.Errorf("conformance.cct diverged from golden\n--- go ---\n%s\n--- golden ---\n%s", cctStr, goldenCCT)
			}

			goldenJSON, err := os.ReadFile(filepath.Join(goldenDir, ConformanceJSONFile))
			if err != nil {
				t.Fatal(err)
			}
			if jsonStr != string(goldenJSON) {
				t.Errorf("conformance.json diverged from golden\n--- go ---\n%s\n--- golden ---\n%s", jsonStr, goldenJSON)
			}
		})
	}
}
