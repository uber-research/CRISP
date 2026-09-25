package crisp

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/uber-research/CRISP/go/crisp/jaeger"
	"github.com/uber-research/CRISP/go/crisp/proto/analyzer"
	"google.golang.org/protobuf/proto"
)

// fanOutJSON: A[0,100) -> B[10,60) -> E[20,50); A -> C[60,90); A -> D[15,20)
// in ms. D overlaps B, so D is not on the critical path. extra spans come
// first.
func fanOutJSON(extra string) []byte {
	return []byte(fmt.Sprintf(`{"data": [{
  "spans": [%s
    {"spanID": "A", "operationName": "opA", "startTime": 0, "duration": 100000, "processID": "pa"},
    {"spanID": "B", "operationName": "opB", "references": [{"refType": "CHILD_OF", "spanID": "A"}], "startTime": 10000, "duration": 50000, "processID": "pb"},
    {"spanID": "C", "operationName": "opC", "references": [{"refType": "CHILD_OF", "spanID": "A"}], "startTime": 60000, "duration": 30000, "processID": "pc"},
    {"spanID": "D", "operationName": "opD", "references": [{"refType": "CHILD_OF", "spanID": "A"}], "startTime": 15000, "duration": 5000, "processID": "pd"},
    {"spanID": "E", "operationName": "opE", "references": [{"refType": "CHILD_OF", "spanID": "B"}], "startTime": 20000, "duration": 30000, "processID": "pe"}
  ],
  "processes": {
    "pa": {"serviceName": "svc-a"}, "pb": {"serviceName": "svc-b"}, "pc": {"serviceName": "svc-c"},
    "pd": {"serviceName": "svc-d"}, "pe": {"serviceName": "svc-e"}, "px": {"serviceName": "other-svc"}
  }
}]}`, extra))
}

// hijackSpans adds X2, which has A's service and operation and comes first
// in DFS order, so name-based root selection picks it instead of A.
const hijackSpans = `
    {"spanID": "X", "operationName": "other-op", "startTime": 0, "duration": 1000, "processID": "px"},
    {"spanID": "X2", "operationName": "opA", "references": [{"refType": "CHILD_OF", "spanID": "X"}], "startTime": 0, "duration": 1000, "processID": "pa"},`

func decodeTrace(t *testing.T, data []byte) *jaeger.Trace {
	t.Helper()
	trace, err := jaeger.Decode(data)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	return trace
}

func TestNewGraphRootSpanID(t *testing.T) {
	trace := decodeTrace(t, fanOutJSON(hijackSpans))
	rootTrace := false

	byName, err := NewGraph(trace, "svc-a", "opA", &GraphOptions{RootTrace: &rootTrace})
	if err != nil {
		t.Fatal(err)
	}
	if byName.RootNode == nil || byName.RootNode.SID != "X2" {
		t.Fatalf("name-based root = %v, want X2", byName.RootNode)
	}

	byID, err := NewGraph(trace, "ignored", "ignored", &GraphOptions{RootSpanID: "A"})
	if err != nil {
		t.Fatal(err)
	}
	if byID.RootNode == nil || byID.RootNode.SID != "A" {
		t.Fatalf("RootSpanID root = %v, want A", byID.RootNode)
	}
	if byID.ServiceName != "svc-a" || byID.OperationName != "opA" {
		t.Errorf("names = %s/%s, want svc-a/opA", byID.ServiceName, byID.OperationName)
	}

	nonRoot, err := NewGraph(trace, "", "", &GraphOptions{RootSpanID: "B"})
	if err != nil {
		t.Fatal(err)
	}
	if nonRoot.RootNode == nil || nonRoot.RootNode.SID != "B" || nonRoot.RootNode.Parent != nil || nonRoot.RootNode.ParentSpanID != nil {
		t.Errorf("RootSpanID=B root = %+v, want B detached from its parent", nonRoot.RootNode)
	}

	unknown, err := NewGraph(trace, "svc-a", "opA", &GraphOptions{RootSpanID: "nope"})
	if err != nil {
		t.Fatal(err)
	}
	if unknown.RootNode != nil {
		t.Errorf("unknown RootSpanID root = %v, want nil", unknown.RootNode.SID)
	}
}

func TestCriticalPath(t *testing.T) {
	want := []CriticalPathContributor{
		{SpanID: "C", Service: "svc-c", Operation: "opC", Duration: 30 * time.Millisecond},
		{SpanID: "E", Service: "svc-e", Operation: "opE", Duration: 30 * time.Millisecond},
		{SpanID: "A", Service: "svc-a", Operation: "opA", Duration: 20 * time.Millisecond},
		{SpanID: "B", Service: "svc-b", Operation: "opB", Duration: 20 * time.Millisecond},
	}
	for name, extra := range map[string]string{"basic": "", "root hijack": hijackSpans} {
		t.Run(name, func(t *testing.T) {
			got, err := CriticalPath(context.Background(), decodeTrace(t, fanOutJSON(extra)), "A")
			if err != nil {
				t.Fatalf("CriticalPath: %v", err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("got %+v\nwant %+v", got, want)
			}
		})
	}
}

func TestCriticalPathErrors(t *testing.T) {
	trace := decodeTrace(t, fanOutJSON(""))
	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := CriticalPath(context.Background(), trace, "nope"); !errors.Is(err, ErrRootNotFound) {
		t.Errorf("unknown root: got %v, want ErrRootNotFound", err)
	}
	if _, err := CriticalPath(canceled, trace, "A"); !errors.Is(err, context.Canceled) {
		t.Errorf("canceled ctx: got %v, want context.Canceled", err)
	}
}

// The light-mode flame graph folds the same exclusive times by call path.
// Here every call path has one span and no children overlap, so the sums per
// leaf service and operation must be equal.
func TestCriticalPathParityWithLightMode(t *testing.T) {
	data := fanOutJSON("")
	contributors, err := CriticalPath(context.Background(), decodeTrace(t, data), "A")
	if err != nil {
		t.Fatalf("CriticalPath: %v", err)
	}
	dir := t.TempDir()
	c := &LightConfig{ServiceName: "svc-a", OperationName: "opA", OutputDir: dir}
	if err := ProcessSingleTraceData(data, "FANOUT", c); err != nil {
		t.Fatalf("ProcessSingleTraceData: %v", err)
	}
	pb, err := os.ReadFile(filepath.Join(dir, splitExt(LightCCTFile)+".pb"))
	if err != nil {
		t.Fatal(err)
	}
	var resp analyzer.AnalyzeResponse
	if err := proto.Unmarshal(pb, &resp); err != nil {
		t.Fatalf("flame graph pb: %v", err)
	}

	got := map[string]time.Duration{}
	for _, c := range contributors {
		got[c.Service+"/"+c.Operation] += c.Duration
	}
	want := map[string]time.Duration{}
	for _, entry := range resp.GetReportWindow_1() {
		leaf := entry.GetCallPath()[len(entry.GetCallPath())-1]
		want[leaf.GetService()+"/"+leaf.GetOperationName()] += entry.GetBase().GetDuration().AsDuration()
	}
	if len(want) == 0 {
		t.Fatal("light-mode flame graph is empty")
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("CriticalPath %v, light mode %v", got, want)
	}
}
