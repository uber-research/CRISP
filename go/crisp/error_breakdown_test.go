package crisp

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/uber-research/CRISP/go/crisp/jaeger"
)

var (
	errorBreakdownModes = []ErrorBreakdownMode{ErrorBreakdownOrigins, ErrorBreakdownPropToRoot}
	errorBreakdownRoots = []ErrorBreakdownRoot{ErrorBreakdownTraceRoot, ErrorBreakdownAnalysisRoot}
)

// runLightErrorBreakdown mirrors tests/test_error_breakdown.py _run_light:
// one fixture through LightProcess, returning error-breakdown.json.
func runLightErrorBreakdown(t *testing.T, fixture string, opts ErrorBreakdownOptions, rootTrace bool) []byte {
	t.Helper()
	data, err := os.ReadFile(fixture)
	if err != nil {
		t.Fatal(err)
	}
	trace, err := jaeger.Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	service, operation, err := DeriveRootSpan(trace)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	tracePath := filepath.Join(dir, filepath.Base(fixture))
	if err := os.WriteFile(tracePath, data, 0o644); err != nil {
		t.Fatal(err)
	}
	c := &LightConfig{
		ServiceName:    service,
		OperationName:  operation,
		RootTrace:      rootTrace,
		MaxExemplars:   3,
		ErrorBreakdown: &opts,
		TraceFiles:     []string{tracePath},
		OutputDir:      dir,
	}
	if err := LightProcess(c); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(dir, ErrorBreakdownFile))
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func TestErrorBreakdownConformanceFixturesMatchGolden(t *testing.T) {
	dir := fixturesDir(t)
	for _, fixture := range discoverFixtures(t) {
		name := goldenName(t, dir, fixture)
		for _, mode := range errorBreakdownModes {
			t.Run(name+"/"+string(mode), func(t *testing.T) {
				got := runLightErrorBreakdown(t, fixture, ErrorBreakdownOptions{Mode: mode, Root: ErrorBreakdownTraceRoot}, true)
				want, err := os.ReadFile(filepath.Join(dir, "golden", name, "error-breakdown-"+string(mode)+".json"))
				if err != nil {
					t.Fatal(err)
				}
				if string(got) != string(want) {
					t.Errorf("diverged from Python golden\n got: %s\nwant: %s", got, want)
				}
			})
		}
	}
}

func errorBreakdownFixtures(t *testing.T) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(fixturesDir(t), "error_breakdown", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("no test_cases/error_breakdown fixtures")
	}
	return files
}

func TestErrorBreakdownFixturesMatchGolden(t *testing.T) {
	for _, fixture := range errorBreakdownFixtures(t) {
		name := strings.TrimSuffix(filepath.Base(fixture), ".json")
		for _, mode := range errorBreakdownModes {
			for _, root := range errorBreakdownRoots {
				t.Run(name+"/"+string(mode)+"-"+string(root), func(t *testing.T) {
					got := runLightErrorBreakdown(t, fixture, ErrorBreakdownOptions{Mode: mode, Root: root}, false)
					want, err := os.ReadFile(filepath.Join(filepath.Dir(fixture), "golden", name, string(mode)+"-"+string(root)+".json"))
					if err != nil {
						t.Fatal(err)
					}
					if string(got) != string(want) {
						t.Errorf("diverged from Python golden\n got: %s\nwant: %s", got, want)
					}
				})
			}
		}
	}
}

func errorBreakdownGraph(t *testing.T, fixture, service, operation string, opts *GraphOptions) *Graph {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(fixturesDir(t), "error_breakdown", fixture+".json"))
	if err != nil {
		t.Fatal(err)
	}
	trace, err := jaeger.Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	g, err := NewGraph(trace, service, operation, opts)
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func sortedKeys(b TraceErrorBreakdown) []string {
	keys := make([]string, 0, len(b))
	for k := range b {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// leafOperations returns the operation of the last node of every path.
func leafOperations(b TraceErrorBreakdown) []string {
	var leaves []string
	for _, p := range b {
		leaves = append(leaves, p.Nodes[len(p.Nodes)-1].Operation)
	}
	sort.Strings(leaves)
	return leaves
}

func TestErrorBreakdownCountingModes(t *testing.T) {
	notRoot := false
	tests := []struct {
		fixture, service, operation string
		mode                        ErrorBreakdownMode
		want                        []string
	}{
		// The computePropToRootGraph reference tree (see test_compute_prop_to_root_graph).
		{"prop_to_root", "testService", "root", ErrorBreakdownPropToRoot, []string{"B1", "B5", "B6", "B7", "B8"}},
		{"prop_to_root", "testService", "root", ErrorBreakdownOrigins, []string{"B1", "B3", "B4", "B5", "B6", "B7", "B8"}},
		// A successful RPC between a failed root and a deeper error.
		{"success_rpc_between", "svc", "root", ErrorBreakdownPropToRoot, []string{"root"}},
		{"success_rpc_between", "svc", "root", ErrorBreakdownOrigins, []string{"B"}},
		// The request succeeded despite a downstream failure.
		{"root_succeeded", "svc", "root", ErrorBreakdownPropToRoot, nil},
		{"root_succeeded", "svc", "root", ErrorBreakdownOrigins, []string{"B"}},
		// Sanitization would drop "late"; the breakdown runs before it.
		{"sanitized_child", "s", "r", ErrorBreakdownOrigins, []string{"late"}},
	}
	for _, tt := range tests {
		t.Run(tt.fixture+"/"+string(tt.mode), func(t *testing.T) {
			g := errorBreakdownGraph(t, tt.fixture, tt.service, tt.operation, &GraphOptions{
				RootTrace:      &notRoot,
				ErrorBreakdown: &ErrorBreakdownOptions{Mode: tt.mode, Root: ErrorBreakdownTraceRoot},
			})
			if got := leafOperations(g.ErrorBreakdown); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("leaves = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestErrorBreakdownSanitizedChildLeavesCriticalPath(t *testing.T) {
	g := errorBreakdownGraph(t, "sanitized_child", "s", "r", &GraphOptions{
		ErrorBreakdown: &ErrorBreakdownOptions{Mode: ErrorBreakdownOrigins, Root: ErrorBreakdownAnalysisRoot},
	})
	if len(g.RootNode.Children) != 0 {
		t.Fatalf("sanitization kept %d children, want the overflowing child dropped", len(g.RootNode.Children))
	}
	if got, want := sortedKeys(g.ErrorBreakdown), []string{"[s]r;[s]late:grpc:13"}; !reflect.DeepEqual(got, want) {
		t.Errorf("keys = %v, want %v", got, want)
	}
}

func TestErrorBreakdownTraceRootSkipsIgnoredOpsAndProxies(t *testing.T) {
	savedIgnored, savedProxy := IgnoredRootOps, ProxyOnlyOps
	defer func() { IgnoredRootOps, ProxyOnlyOps = savedIgnored, savedProxy }()
	notRoot := false
	opts := func(filterProxy bool) *GraphOptions {
		return &GraphOptions{
			RootTrace:      &notRoot,
			FilterProxy:    filterProxy,
			ErrorBreakdown: &ErrorBreakdownOptions{Mode: ErrorBreakdownOrigins, Root: ErrorBreakdownTraceRoot},
		}
	}

	g := errorBreakdownGraph(t, "multi_root", "two", "second", opts(false))
	if got, want := leafOperations(g.ErrorBreakdown), []string{"first"}; !reflect.DeepEqual(got, want) {
		t.Errorf("default: leaves = %v, want %v", got, want)
	}

	IgnoredRootOps = []string{"first"}
	g = errorBreakdownGraph(t, "multi_root", "two", "second", opts(false))
	if got, want := leafOperations(g.ErrorBreakdown), []string{"fetch"}; !reflect.DeepEqual(got, want) {
		t.Errorf("ignored root op: leaves = %v, want %v", got, want)
	}

	IgnoredRootOps, ProxyOnlyOps = nil, []string{"first"}
	g = errorBreakdownGraph(t, "multi_root", "two", "second", opts(true))
	if got, want := sortedKeys(g.ErrorBreakdown), []string{"[two]second;[two]fetch:http:500"}; !reflect.DeepEqual(got, want) {
		t.Errorf("proxy root: keys = %v, want %v", got, want)
	}
}

func TestErrorBreakdownWithoutSpans(t *testing.T) {
	g, err := NewGraph(&jaeger.Trace{Data: []jaeger.TraceData{{TraceID: "t"}}}, "s", "o", &GraphOptions{
		ErrorBreakdown: &ErrorBreakdownOptions{Mode: ErrorBreakdownOrigins, Root: ErrorBreakdownTraceRoot},
	})
	if err != nil {
		t.Fatal(err)
	}
	if g.ErrorBreakdown == nil || len(g.ErrorBreakdown) != 0 {
		t.Errorf("ErrorBreakdown = %#v, want empty non-nil", g.ErrorBreakdown)
	}
	if g.RootNode != nil {
		t.Errorf("RootNode = %v, want nil", g.RootNode)
	}
}

func TestErrorBreakdownWithoutOption(t *testing.T) {
	g := errorBreakdownGraph(t, "prop_to_root", "testService", "root", nil)
	if g.ErrorBreakdown != nil {
		t.Errorf("ErrorBreakdown = %v, want nil", g.ErrorBreakdown)
	}
}

func TestErrorBreakdownRepeatedKeyInOneTrace(t *testing.T) {
	data, err := os.ReadFile(filepath.Join(fixturesDir(t), "error_breakdown", "prop_to_root.json"))
	if err != nil {
		t.Fatal(err)
	}
	trace, err := jaeger.Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	var b5SpanID string
	for i := range trace.Data[0].Spans {
		s := &trace.Data[0].Spans[i]
		switch s.OperationName {
		case "B5":
			b5SpanID = s.SpanID
		case "B6":
			s.OperationName = "B5"
		}
	}
	opts := ErrorBreakdownOptions{Mode: ErrorBreakdownOrigins, Root: ErrorBreakdownTraceRoot}
	g, err := NewGraph(trace, "testService", "root", &GraphOptions{ErrorBreakdown: &opts})
	if err != nil {
		t.Fatal(err)
	}
	doc := MergeErrorBreakdowns([]TraceErrorBreakdownEntry{{TraceID: "t", Breakdown: g.ErrorBreakdown}}, opts, 3)
	for _, p := range doc.Paths {
		if !strings.HasSuffix(p.Key, "[testService]B5") {
			continue
		}
		if p.Count != 2 {
			t.Errorf("count = %d, want 2", p.Count)
		}
		want := []ErrorExemplar{{SpanID: b5SpanID, TraceID: "t"}}
		if got := p.Nodes[len(p.Nodes)-1].Exemplars; !reflect.DeepEqual(got, want) {
			t.Errorf("exemplars = %v, want %v", got, want)
		}
		return
	}
	t.Fatal("no B5 path")
}

func tag(key string, value any, typ string) jaeger.Tag {
	return jaeger.Tag{Key: key, Type: typ, Value: value}
}

func int64Ptr(n int64) *int64 { return &n }

// TestExtractRPCStatus mirrors tests/test_error_breakdown.py
// test_extract_rpc_status.
func TestExtractRPCStatus(t *testing.T) {
	tests := []struct {
		name         string
		tags         []jaeger.Tag
		wantProtocol string
		wantCode     *int64
	}{
		{"none", nil, "", nil},
		{"http int64", []jaeger.Tag{tag("http.response.status_code", json.Number("503"), "int64")}, "http", int64Ptr(503)},
		{"http string", []jaeger.Tag{tag("http.response.status_code", "404", "string")}, "http", int64Ptr(404)},
		{"http legacy key", []jaeger.Tag{tag("http.status_code", json.Number("502"), "int64")}, "http", int64Ptr(502)},
		{"grpc int64", []jaeger.Tag{tag("grpc.status_code", json.Number("14"), "int64")}, "grpc", int64Ptr(14)},
		{"grpc signed string", []jaeger.Tag{tag("rpc.grpc.status_code", "+7", "string")}, "grpc", int64Ptr(7)},
		{"grpc name", []jaeger.Tag{tag("grpc.status", "OK", "string")}, "grpc", nil},
		{"last status wins", []jaeger.Tag{
			tag("grpc.status_code", json.Number("2"), "int64"),
			tag("http.response.status_code", json.Number("500"), "int64"),
		}, "http", int64Ptr(500)},
		{"yarpc grpc", []jaeger.Tag{
			tag("rpc.yarpc.status_code", json.Number("5"), "int64"),
			tag("rpc.transport", "grpc", "string"),
		}, "yarpc_grpc", int64Ptr(5)},
		{"yarpc tchannel", []jaeger.Tag{
			tag("rpc.transport", "tchannel", "string"),
			tag("rpc.yarpc.status_code", "3", "string"),
		}, "yarpc_tchannel", int64Ptr(3)},
		{"yarpc no transport", []jaeger.Tag{tag("rpc.yarpc.status_code", json.Number("5"), "int64")}, "", int64Ptr(5)},
		{"direct over yarpc", []jaeger.Tag{
			tag("rpc.yarpc.status_code", json.Number("5"), "int64"),
			tag("grpc.status_code", json.Number("1"), "int64"),
		}, "grpc", int64Ptr(1)},
		{"transport only", []jaeger.Tag{tag("rpc.transport", "http", "string")}, "http", nil},
		{"unknown transport", []jaeger.Tag{tag("rpc.transport", "quic", "string")}, "", nil},
		{"non-string transport", []jaeger.Tag{tag("rpc.transport", json.Number("1"), "int64")}, "", nil},
		{"float code", []jaeger.Tag{tag("http.response.status_code", json.Number("404.0"), "float64")}, "http", nil},
		{"bool code", []jaeger.Tag{tag("http.response.status_code", true, "bool")}, "http", nil},
		{"padded string", []jaeger.Tag{tag("http.response.status_code", " 404", "string")}, "http", nil},
		{"empty string", []jaeger.Tag{tag("http.response.status_code", "", "string")}, "http", nil},
		{"out of range", []jaeger.Tag{tag("http.response.status_code", json.Number("9223372036854775808"), "int64")}, "http", nil},
		{"number typed string", []jaeger.Tag{tag("http.response.status_code", json.Number("404"), "string")}, "http", nil},
		{"key case", []jaeger.Tag{tag("HTTP.response.status_code", json.Number("404"), "int64")}, "", nil},
		{"component list empty", []jaeger.Tag{tag("component", "my-http", "string")}, "", nil},
		{"marker list empty", []jaeger.Tag{tag("as", "thrift", "string")}, "", nil},
		{"tchannel status list empty", []jaeger.Tag{tag("my.tchannel.status", json.Number("1"), "int64")}, "", nil},
		{"yarpc status list empty", []jaeger.Tag{tag("my.yarpc.status", json.Number("1"), "int64")}, "", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			protocol, code := extractRPCStatus(tt.tags)
			if protocol != tt.wantProtocol || !reflect.DeepEqual(code, tt.wantCode) {
				t.Errorf("got (%q, %v), want (%q, %v)", protocol, ptrString(code), tt.wantProtocol, ptrString(tt.wantCode))
			}
		})
	}
}

func ptrString(p *int64) string {
	if p == nil {
		return "nil"
	}
	return strconv.FormatInt(*p, 10)
}

func TestExtractRPCStatusDeploymentLists(t *testing.T) {
	savedComponents, savedMarkers := HTTPComponents, TChannelMarkerTags
	defer func() { HTTPComponents, TChannelMarkerTags = savedComponents, savedMarkers }()
	HTTPComponents, TChannelMarkerTags = []string{"my-http"}, []string{"as"}

	tests := []struct {
		name         string
		tags         []jaeger.Tag
		wantProtocol string
		wantCode     *int64
	}{
		{"component", []jaeger.Tag{tag("component", "my-http", "string")}, "http", nil},
		{"component before transport", []jaeger.Tag{
			tag("component", "my-http", "string"),
			tag("rpc.transport", "grpc", "string"),
		}, "http", nil},
		{"component keeps direct protocol", []jaeger.Tag{
			tag("grpc.status_code", json.Number("2"), "int64"),
			tag("component", "my-http", "string"),
		}, "grpc", int64Ptr(2)},
		{"marker keeps code", []jaeger.Tag{
			tag("http.response.status_code", json.Number("500"), "int64"),
			tag("as", "thrift", "string"),
		}, "tchannel", int64Ptr(500)},
		{"marker blocks yarpc", []jaeger.Tag{
			tag("as", "json", "string"),
			tag("rpc.yarpc.status_code", json.Number("5"), "int64"),
		}, "tchannel", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			protocol, code := extractRPCStatus(tt.tags)
			if protocol != tt.wantProtocol || !reflect.DeepEqual(code, tt.wantCode) {
				t.Errorf("got (%q, %v), want (%q, %v)", protocol, ptrString(code), tt.wantProtocol, ptrString(tt.wantCode))
			}
		})
	}
}

// TestExtractRPCStatusStatusTagLists mirrors tests/test_error_breakdown.py
// test_extract_rpc_status_status_tag_lists.
func TestExtractRPCStatusStatusTagLists(t *testing.T) {
	savedTChannel, savedYARPC := TChannelStatusTags, YARPCStatusTags
	defer func() { TChannelStatusTags, YARPCStatusTags = savedTChannel, savedYARPC }()
	TChannelStatusTags, YARPCStatusTags = []string{"my.tchannel.status"}, []string{"my.yarpc.status"}

	tests := []struct {
		name         string
		tags         []jaeger.Tag
		wantProtocol string
		wantCode     *int64
	}{
		{"tchannel", []jaeger.Tag{tag("my.tchannel.status", json.Number("0"), "int64")}, "tchannel", int64Ptr(0)},
		{"yarpc", []jaeger.Tag{
			tag("rpc.transport", "tchannel", "string"),
			tag("my.yarpc.status", "3", "string"),
		}, "yarpc_tchannel", int64Ptr(3)},
		{"last status wins", []jaeger.Tag{
			tag("my.tchannel.status", json.Number("1"), "int64"),
			tag("grpc.status_code", json.Number("2"), "int64"),
		}, "grpc", int64Ptr(2)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			protocol, code := extractRPCStatus(tt.tags)
			if protocol != tt.wantProtocol || !reflect.DeepEqual(code, tt.wantCode) {
				t.Errorf("got (%q, %v), want (%q, %v)", protocol, ptrString(code), tt.wantProtocol, ptrString(tt.wantCode))
			}
		})
	}
}

func TestIsStatusError(t *testing.T) {
	tests := []struct {
		protocol string
		code     *int64
		want     bool
	}{
		{"http", nil, false},
		{"http", int64Ptr(399), false},
		{"http", int64Ptr(400), true},
		{"grpc", int64Ptr(0), false},
		{"grpc", int64Ptr(14), true},
		{"tchannel", int64Ptr(1), true},
		{"yarpc_http", int64Ptr(400), true},
		{"", int64Ptr(0), false},
		{"", int64Ptr(3), true},
	}
	for _, tt := range tests {
		if got := isStatusError(tt.protocol, tt.code); got != tt.want {
			t.Errorf("isStatusError(%q, %s) = %v, want %v", tt.protocol, ptrString(tt.code), got, tt.want)
		}
	}
}

// TestMergeErrorBreakdowns mirrors tests/test_error_breakdown.py
// test_merge_error_breakdowns.
func TestMergeErrorBreakdowns(t *testing.T) {
	node := func(op, spanID string) []ErrorPathNode {
		return []ErrorPathNode{{Service: "s", Operation: op, Protocol: "grpc", StatusCode: int64Ptr(2), SpanID: spanID}}
	}
	perTrace := []TraceErrorBreakdownEntry{
		{"t1", TraceErrorBreakdown{"k": {Count: 2, Nodes: node("op", "a1")}}},
		{"t2", TraceErrorBreakdown{
			"k": {Count: 1, Nodes: node("op", "a1")},
			"j": {Count: 1, Nodes: []ErrorPathNode{{Service: "s", Operation: "j", Virtual: true}}},
		}},
		{"t1", TraceErrorBreakdown{"k": {Count: 1, Nodes: node("op", "a1")}}}, // duplicate exemplar
		{"t3", TraceErrorBreakdown{"k": {Count: 1, Nodes: node("op", "a3")}}},
		{"t4", TraceErrorBreakdown{"k": {Count: 1, Nodes: node("op", "a4")}}},
	}
	opts := ErrorBreakdownOptions{Mode: ErrorBreakdownOrigins, Root: ErrorBreakdownTraceRoot}
	doc := MergeErrorBreakdowns(perTrace, opts, 3)
	if doc.Traces != 5 {
		t.Errorf("traces = %d, want 5", doc.Traces)
	}
	if len(doc.Paths) != 2 || doc.Paths[0].Key != "j" || doc.Paths[1].Key != "k" {
		t.Fatalf("paths = %+v, want [j k]", doc.Paths)
	}
	j, k := doc.Paths[0], doc.Paths[1]
	if len(j.Nodes[0].Exemplars) != 0 || j.Nodes[0].Exemplars == nil {
		t.Errorf("virtual node exemplars = %#v, want empty non-nil", j.Nodes[0].Exemplars)
	}
	if k.Count != 6 {
		t.Errorf("k count = %d, want 6", k.Count)
	}
	want := []ErrorExemplar{{"a1", "t1"}, {"a1", "t2"}, {"a3", "t3"}}
	if got := k.Nodes[0].Exemplars; !reflect.DeepEqual(got, want) {
		t.Errorf("k exemplars = %v, want %v", got, want)
	}
	if k.Nodes[0].Protocol == nil || *k.Nodes[0].Protocol != "grpc" || *k.Nodes[0].StatusCode != 2 {
		t.Errorf("k node = %+v, want grpc/2", k.Nodes[0])
	}
}

func TestNewErrorBreakdownOptions(t *testing.T) {
	o, err := NewErrorBreakdownOptions("propToRoot", "")
	if err != nil || o.Mode != ErrorBreakdownPropToRoot || o.Root != ErrorBreakdownTraceRoot {
		t.Errorf("got (%+v, %v), want propToRoot/trace", o, err)
	}
	if _, err := NewErrorBreakdownOptions("everything", "trace"); err == nil {
		t.Error("unknown mode: want error")
	}
	if _, err := NewErrorBreakdownOptions("origins", "somewhere"); err == nil {
		t.Error("unknown root: want error")
	}
}

func TestLightProcessWithoutErrorBreakdownWritesNothing(t *testing.T) {
	src := filepath.Join(fixturesDir(t), "error_breakdown", "prop_to_root.json")
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	tracePath := filepath.Join(dir, "t.json")
	if err := os.WriteFile(tracePath, data, 0o644); err != nil {
		t.Fatal(err)
	}
	c := &LightConfig{
		ServiceName: "testService", OperationName: "root", RootTrace: true, MaxExemplars: 3,
		TraceFiles: []string{tracePath}, OutputDir: dir,
	}
	if err := LightProcess(c); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, ErrorBreakdownFile)); !os.IsNotExist(err) {
		t.Errorf("%s written without ErrorBreakdown (stat err %v)", ErrorBreakdownFile, err)
	}
}
