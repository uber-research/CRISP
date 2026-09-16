package crisp

import "strings"

// Test-trace heuristics, mirroring crisp/utils/span_utils.py. Both lists ship
// empty by default; downstream users can append to these package-level
// variables to exclude their own test-framework traffic from analysis.
//
//	crisp.TestTraceServices = append(crisp.TestTraceServices, "my-test-service")
//	crisp.TestTraceOpPrefixes = append(crisp.TestTraceOpPrefixes, "[mytests.")
var (
	TestTraceServices   []string
	TestTraceOpPrefixes []string

	// Proxy-node configuration, mirroring crisp/utils/span_utils.py. A
	// "proxy node" forwards work to a child but does not faithfully reflect
	// the child's timing or error state; both lists ship empty by default
	// and are intended to be populated per deployment.
	ProxyServiceOpPairs [][2]string // (serviceName, opName) pairs
	ProxyOnlyOps        []string    // opNames treated as proxies regardless of service

	// ErrPropServiceOpPairs marks (serviceName, opName) pairs where errors
	// are reported on the caller side, so analysis propagates the child's
	// error upward through the proxy's span. Empty by default.
	ErrPropServiceOpPairs [][2]string
)

// isProxyNode mirrors span_utils.isProxyNode.
func isProxyNode(serviceName, opName string) bool {
	for _, so := range ProxyServiceOpPairs {
		if serviceName == so[0] && opName == so[1] {
			return true
		}
	}
	for _, o := range ProxyOnlyOps {
		if opName == o {
			return true
		}
	}
	return false
}

// isErrPropNode mirrors span_utils.isErrPropNode.
func isErrPropNode(serviceName, opName string) bool {
	for _, so := range ErrPropServiceOpPairs {
		if serviceName == so[0] && opName == so[1] {
			return true
		}
	}
	return false
}

// isTestTraceByServiceName mirrors span_utils.isTestTraceByServiceName:
// exact match against TestTraceServices.
func isTestTraceByServiceName(serviceName string) bool {
	for _, s := range TestTraceServices {
		if serviceName == s {
			return true
		}
	}
	return false
}

// isTestTraceByOpName mirrors span_utils.isTestTraceByOpName: prefix match
// against TestTraceOpPrefixes.
func isTestTraceByOpName(operationName string) bool {
	for _, prefix := range TestTraceOpPrefixes {
		if strings.HasPrefix(operationName, prefix) {
			return true
		}
	}
	return false
}
