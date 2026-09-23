package crisp

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestLightProcessContextCancellation pins LightConfig.Context: a canceled
// context stops the run before any trace file is processed, and no outputs
// are written.
func TestLightProcessContextCancellation(t *testing.T) {
	dir := t.TempDir()
	trace := `{"data":[{"traceID":"T1","spans":[` +
		`{"traceID":"T1","spanID":"111","operationName":"opA1","references":[],"startTime":1000000,"duration":100,"processID":"p0","warnings":null}` +
		`],"processes":{"p0":{"serviceName":"svcA"}},"warnings":null}]}`
	traceFile := filepath.Join(dir, "T1.json")
	if err := os.WriteFile(traceFile, []byte(trace), 0o644); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cfg := &LightConfig{
		ServiceName:   "svcA",
		OperationName: "opA1",
		TraceFiles:    []string{traceFile},
		OutputDir:     dir,
		Context:       ctx,
	}
	if err := LightProcess(cfg); !errors.Is(err, context.Canceled) {
		t.Fatalf("LightProcess with canceled Context = %v, want context.Canceled", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "light-flame-graph-P100.dot")); !os.IsNotExist(err) {
		t.Errorf("no outputs should be written after cancellation, stat err = %v", err)
	}
}

// TestLightProcessNilContext pins that a nil Context disables cancellation
// checks and the run completes normally.
func TestLightProcessNilContext(t *testing.T) {
	dir := t.TempDir()
	trace := `{"data":[{"traceID":"T1","spans":[` +
		`{"traceID":"T1","spanID":"111","operationName":"opA1","references":[],"startTime":1000000,"duration":100,"processID":"p0","warnings":null}` +
		`],"processes":{"p0":{"serviceName":"svcA"}},"warnings":null}]}`
	traceFile := filepath.Join(dir, "T1.json")
	if err := os.WriteFile(traceFile, []byte(trace), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := &LightConfig{
		ServiceName:   "svcA",
		OperationName: "opA1",
		TraceFiles:    []string{traceFile},
		OutputDir:     dir,
	}
	if err := LightProcess(cfg); err != nil {
		t.Fatalf("LightProcess with nil Context: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "light-flame-graph-P100.dot")); err != nil {
		t.Errorf("expected flame graph output: %v", err)
	}
}

// TestLightProcessFilterProxy pins that LightConfig.FilterProxy is
// forwarded to the Graph: with a registered proxy pair, the proxy span is
// short-wired out of the call tree (its child is re-parented to the
// proxy's parent); without the flag the proxy span is kept.
func TestLightProcessFilterProxy(t *testing.T) {
	ProxyServiceOpPairs = append(ProxyServiceOpPairs, [2]string{"svcProxy", "opProxy"})
	defer func() { ProxyServiceOpPairs = ProxyServiceOpPairs[:len(ProxyServiceOpPairs)-1] }()

	// svcA/opA (100us) -> svcProxy/opProxy (80us) -> svcC/opC (50us).
	trace := `{"data":[{"traceID":"T1","spans":[` +
		`{"traceID":"T1","spanID":"111","operationName":"opA","references":[],"startTime":1000000,"duration":100,"processID":"p0","warnings":null},` +
		`{"traceID":"T1","spanID":"222","operationName":"opProxy","references":[{"traceID":"T1","spanID":"111","refType":"CHILD_OF"}],"startTime":1000010,"duration":80,"processID":"p1","warnings":null},` +
		`{"traceID":"T1","spanID":"333","operationName":"opC","references":[{"traceID":"T1","spanID":"222","refType":"CHILD_OF"}],"startTime":1000020,"duration":50,"processID":"p2","warnings":null}` +
		`],"processes":{"p0":{"serviceName":"svcA"},"p1":{"serviceName":"svcProxy"},"p2":{"serviceName":"svcC"}},"warnings":null}]}`

	for _, tt := range []struct {
		name        string
		filterProxy bool
		wantProxy   bool
	}{
		{name: "filter off keeps proxy span", filterProxy: false, wantProxy: true},
		{name: "filter on short-wires proxy span", filterProxy: true, wantProxy: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			traceFile := filepath.Join(dir, "T1.json")
			if err := os.WriteFile(traceFile, []byte(trace), 0o644); err != nil {
				t.Fatal(err)
			}
			cfg := &LightConfig{
				ServiceName:   "svcA",
				OperationName: "opA",
				FilterProxy:   tt.filterProxy,
				TraceFiles:    []string{traceFile},
				OutputDir:     dir,
			}
			if err := LightProcess(cfg); err != nil {
				t.Fatalf("LightProcess: %v", err)
			}
			dot, err := os.ReadFile(filepath.Join(dir, "light-flame-graph-P100.dot"))
			if err != nil {
				t.Fatal(err)
			}
			if got := strings.Contains(string(dot), "opProxy"); got != tt.wantProxy {
				t.Errorf("DOT contains opProxy = %v, want %v:\n%s", got, tt.wantProxy, dot)
			}
			if !strings.Contains(string(dot), "[svcC] opC") {
				t.Errorf("DOT should contain [svcC] opC:\n%s", dot)
			}
		})
	}
}
