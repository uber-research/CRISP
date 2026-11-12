package crisp

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/uber-research/CRISP/go/crisp/jaeger"
)

// Tag filters in the light pipeline: per-root matching and the per-tag flame
// graphs (see Graph.MatchingTags, writeLightOutputs and CONFORMANCE.md,
// "Per-tag outputs"). Mirrors tests/test_light_tags.py on the same traces.

func ltTag(key, value string) jaeger.Tag {
	return jaeger.Tag{Key: key, Type: "string", Value: value}
}

func ltProcs() map[string]jaeger.Process {
	return map[string]jaeger.Process{"pA": mkProc("svcA"), "pB": mkProc("svcB"), "pC": mkProc("svcC")}
}

var (
	ltEnvProd        = TagFilter{Name: "env", Value: "prod", SearchDepth: 1}
	ltRegionWest     = TagFilter{Name: "region", Value: "west", SearchDepth: 2}
	ltRegionWestRoot = TagFilter{Name: "region", Value: "west", SearchDepth: 1}
	ltTierOne        = TagFilter{Name: "tier", Value: "1", SearchDepth: 3}
)

// Three analysis roots, each a trace root matching svcA/opA.
//
//	A (100us) has env=prod. Its child A1 has region=west, and A1's child A2 has tier=1.
//	B (200us) has region=west itself.
//	C (300us) has no tag. Its child C1 starts after C ended, so sanitization
//	drops it: its region=west does not count.
func ltRoots() []jaeger.Span {
	return []jaeger.Span{
		mkSpan("A", "opA", "pA", 0, 100, nil, ltTag("env", "prod")),
		mkSpan("A1", "opB", "pB", 10, 80, []jaeger.Reference{childOf("A")}, ltTag("region", "west")),
		mkSpan("A2", "opC", "pC", 20, 50, []jaeger.Reference{childOf("A1")}, ltTag("tier", "1")),
		mkSpan("B", "opA", "pA", 1000, 200, nil, ltTag("region", "west")),
		mkSpan("C", "opA", "pA", 2000, 300, nil),
		mkSpan("C1", "opB", "pB", 3000, 50, []jaeger.Reference{childOf("C")}, ltTag("region", "west")),
	}
}

func ltConfig(tags ...TagFilter) LightConfig {
	return LightConfig{ServiceName: "svcA", OperationName: "opA", MergeAllRoots: true, Tags: tags}
}

// ltMatched returns "root: name=value/depth,..." for every analysis root.
func ltMatched(t *testing.T, c LightConfig) []string {
	t.Helper()
	data, err := json.Marshal(mkTrace(ltProcs(), ltRoots()...))
	if err != nil {
		t.Fatal(err)
	}
	results, err := processTraceRoots(data, "T1", "T1.json", &c)
	if err != nil {
		t.Fatal(err)
	}
	out := []string{}
	for _, res := range results {
		tags := []string{}
		for _, f := range res.tags {
			tags = append(tags, TagOutputPrefix(f)+string(rune('0'+f.SearchDepth)))
		}
		out = append(out, res.rootSpanID+": "+strings.Join(tags, ","))
	}
	return out
}

func ltExpect(t *testing.T, got []string, want ...string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("matched = %q, want %q", got, want)
	}
}

// --- which filters match under a root ----------------------------------------

func TestTagsAreMatchedUnderEachRoot(t *testing.T) {
	ltExpect(t, ltMatched(t, ltConfig(ltEnvProd, ltRegionWest)), "A: env_prod_1,region_west_2", "B: region_west_2", "C: ")
}

// A1 is at level 2 and A2 at level 3.
func TestTagSearchDepthCountsLevelsFromTheRoot(t *testing.T) {
	c := ltConfig(ltRegionWestRoot, TagFilter{Name: "tier", Value: "1", SearchDepth: 2}, ltTierOne)
	ltExpect(t, ltMatched(t, c), "A: tier_1_3", "B: region_west_1", "C: ")
}

func TestTagsWithoutMergeAllRootsMatchTheFirstRoot(t *testing.T) {
	c := ltConfig(ltEnvProd, ltRegionWest)
	c.MergeAllRoots = false
	ltExpect(t, ltMatched(t, c), "A: env_prod_1,region_west_2")
}

func TestTagValueMustBeEqual(t *testing.T) {
	c := ltConfig(TagFilter{Name: "env", Value: "pro", SearchDepth: 1}, TagFilter{Name: "ENV", Value: "prod", SearchDepth: 1},
		TagFilter{Name: "tier", Value: json.Number("1"), SearchDepth: 3})
	ltExpect(t, ltMatched(t, c), "A: ", "B: ", "C: ")
}

// --- per-tag outputs ---------------------------------------------------------

func ltRunLight(t *testing.T, trace *jaeger.Trace, c LightConfig) map[string]string {
	t.Helper()
	data, err := json.Marshal(trace)
	if err != nil {
		t.Fatal(err)
	}
	c.OutputDir = t.TempDir()
	c.MaxExemplars = 3
	if err := ProcessSingleTraceData(data, "T1", &c); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(c.OutputDir)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, e := range entries {
		b, err := os.ReadFile(filepath.Join(c.OutputDir, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		out[e.Name()] = string(b)
	}
	return out
}

func ltLines(outputs map[string]string, prefix string) []string {
	lines := strings.Split(strings.TrimSuffix(outputs[prefix+LightCCTFile], "\n"), "\n")
	sort.Strings(lines)
	return lines
}

func TestEachTagFilterGetsItsOwnFlameGraph(t *testing.T) {
	envTest := TagFilter{Name: "env", Value: "test", SearchDepth: 1}
	outputs := ltRunLight(t, mkTrace(ltProcs(), ltRoots()...), ltConfig(ltEnvProd, ltRegionWest, envTest))
	for _, tt := range []struct {
		prefix string
		want   []string
	}{
		// All three roots. Times are averaged over the roots: (20 + 200 + 300) / 3.
		{"", []string{"[svcA] opA 173 <<3>>", "[svcA] opA;[svcB] opB 10 <<1>>", "[svcA] opA;[svcB] opB;[svcC] opC 16 <<1>>"}},
		// env=prod: root A only.
		{"env_prod_", []string{"[svcA] opA 20 <<1>>", "[svcA] opA;[svcB] opB 30 <<1>>", "[svcA] opA;[svcB] opB;[svcC] opC 50 <<1>>"}},
		// region=west within two levels: roots A and B.
		{"region_west_", []string{"[svcA] opA 110 <<2>>", "[svcA] opA;[svcB] opB 15 <<1>>", "[svcA] opA;[svcB] opB;[svcC] opC 25 <<1>>"}},
	} {
		if got := ltLines(outputs, tt.prefix); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("%scct = %q, want %q", tt.prefix, got, tt.want)
		}
	}
	// A filter that matches no root still writes its files.
	if got, ok := outputs["env_test_"+LightCCTFile]; !ok || got != "" {
		t.Errorf("env_test_ cct = %q, %v; want an empty file", got, ok)
	}
	for _, prefix := range []string{"", "env_prod_", "region_west_", "env_test_"} {
		for _, name := range []string{LightCCTFile, "light-flame-graph-P100.dot", "light-flame-graph-P100.pb"} {
			if _, ok := outputs[prefix+name]; !ok {
				t.Errorf("missing %s", prefix+name)
			}
		}
	}
}

func TestTagFiltersDoNotChangeTheOtherOutputs(t *testing.T) {
	c := ltConfig(ltEnvProd, ltRegionWest)
	c.Conformance = true
	tagged := ltRunLight(t, mkTrace(ltProcs(), ltRoots()...), c)
	c.Tags = nil
	plain := ltRunLight(t, mkTrace(ltProcs(), ltRoots()...), c)
	var extra []string
	for name, body := range tagged {
		if want, ok := plain[name]; !ok {
			extra = append(extra, name)
		} else if body != want {
			t.Errorf("%s changed with tag filters", name)
		}
	}
	var want []string
	for _, prefix := range []string{"env_prod_", "region_west_"} {
		for _, name := range []string{LightCCTFile, "light-flame-graph-P100.dot", "light-flame-graph-P100.pb", ConformanceCCTFile, ConformanceJSONFile} {
			want = append(want, prefix+name)
		}
	}
	sort.Strings(extra)
	sort.Strings(want)
	if !reflect.DeepEqual(extra, want) || len(tagged) != len(plain)+len(want) {
		t.Errorf("extra files = %q, want %q", extra, want)
	}
}

// Only B (200us) is inside the window.
func TestPerTagOutputsFollowTheCohortFilters(t *testing.T) {
	us := func(v int64) *int64 { return &v }
	c := ltConfig(ltRegionWest)
	c.MinDuration, c.MaxDuration = us(150), us(250)
	outputs := ltRunLight(t, mkTrace(ltProcs(), ltRoots()...), c)
	if got, want := outputs["region_west_"+LightCCTFile], "[svcA] opA 200 <<1>>\n"; got != want || outputs[LightCCTFile] != want {
		t.Errorf("region_west_ cct = %q, cct = %q; want %q for both", got, outputs[LightCCTFile], want)
	}
}

func TestPerTagOutputsOfAFragmentAreThoseOfItsTrace(t *testing.T) {
	spans := []jaeger.Span{
		mkSpan("X", "opX", "pC", 0, 5000, nil),
		mkSpan("U", "opU", "pC", 10, 10, []jaeger.Reference{childOf("X")}, ltTag("env", "prod")),
	}
	for _, s := range ltRoots()[:4] {
		if len(s.References) == 0 {
			s.References = []jaeger.Reference{childOf("X")}
		}
		spans = append(spans, s)
	}
	trace := mkTrace(ltProcs(), spans...)
	c := ltConfig(ltEnvProd, ltRegionWest, ltTierOne)
	c.Fragment, c.Conformance = true, true
	full := ltRunLight(t, trace, c)
	if pruned := ltRunLight(t, PruneToFragment(trace, "svcA", "opA"), c); !reflect.DeepEqual(pruned, full) {
		t.Errorf("the fragment's outputs differ from the trace's")
	}
	if full["env_prod_"+LightCCTFile] == "" {
		t.Errorf("no critical path in env_prod_%s", LightCCTFile)
	}
}

// --- file names and the --tags value -----------------------------------------

func TestTagOutputPrefix(t *testing.T) {
	for _, tt := range []struct{ name, value, want string }{
		{"env", "prod", "env_prod_"},
		{"http.status_code", "200", "http_status_code_200_"},
		{"a b", "c/d", "a_b_c_d_"},
	} {
		if got := TagOutputPrefix(TagFilter{Name: tt.name, Value: tt.value}); got != tt.want {
			t.Errorf("TagOutputPrefix(%s, %s) = %q, want %q", tt.name, tt.value, got, tt.want)
		}
	}
}

func TestTagFiltersWithTheSamePrefixAreRejected(t *testing.T) {
	c := ltConfig(TagFilter{Name: "a.b", Value: "c"}, TagFilter{Name: "a", Value: "b.c"})
	c.OutputDir = t.TempDir()
	for call, err := range map[string]error{
		"CheckTagFilters":        CheckTagFilters(&c),
		"LightProcess":           LightProcess(&c),
		"ProcessSingleTraceData": ProcessSingleTraceData([]byte(`{"data":[]}`), "T1", &c),
	} {
		if err == nil || !strings.Contains(err.Error(), "same output file prefix") {
			t.Errorf("%s: err = %v, want one about the same output file prefix", call, err)
		}
	}
	ok := ltConfig(TagFilter{Name: "a.b", Value: "c"}, TagFilter{Name: "a", Value: "c"})
	if err := CheckTagFilters(&ok); err != nil {
		t.Errorf("different prefixes: %v", err)
	}
}

func TestParseTagFilters(t *testing.T) {
	got, err := ParseTagFilters(`[{"name": "env", "value": "prod", "search_depth": 1}, {"name": "region", "value": "west", "search_depth": 2}]`)
	if want := []TagFilter{ltEnvProd, ltRegionWest}; err != nil || !reflect.DeepEqual(got, want) {
		t.Errorf("ParseTagFilters = %v, %v; want %v", got, err, want)
	}
	if got, err := ParseTagFilters(`[]`); err != nil || len(got) != 0 {
		t.Errorf("empty list: %v, %v", got, err)
	}
	for _, bad := range []string{
		`[{name: env, value: prod, search_depth: 1}]`, // YAML that is not JSON
		`{"name": "env", "value": "prod", "search_depth": 1}`,
		`[{"name": "env", "value": "prod"}]`,
		`[{"name": "env", "value": "prod", "search_depth": 1, "extra": 1}]`,
		`[{"name": "env", "value": 200, "search_depth": 1}]`,
		`[{"name": 1, "value": "prod", "search_depth": 1}]`,
		`[{"name": "env", "value": "prod", "search_depth": "1"}]`,
		`[{"name": "env", "value": "prod", "search_depth": 1.5}]`,
		`[] []`,
	} {
		if got, err := ParseTagFilters(bad); err == nil {
			t.Errorf("ParseTagFilters(%s) = %v, want an error", bad, got)
		}
	}
}
