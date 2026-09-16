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
)

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
