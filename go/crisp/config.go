package crisp

// AnalysisConfig mirrors the tunables of crisp/configuration.py that graph
// construction depends on. Python reads these from a process-global
// singleton; the Go port carries them per-Graph so concurrent analyses
// cannot interfere.
type AnalysisConfig struct {
	// ServerLengtheningFactor is how much a server span may exceed its
	// client span's duration before the pairing is rejected
	// (isAcceptableParentChildDuration). Python default: 1.01.
	ServerLengtheningFactor float64
	// OverlapAllowance is how much sibling spans may overlap, as a fraction
	// of their common parent's duration, before the earlier one is
	// considered concurrent rather than happens-before (happensBefore).
	// Python default: 0.01.
	OverlapAllowance float64
}

// DefaultAnalysisConfig mirrors AnalysisConfig's Python defaults.
func DefaultAnalysisConfig() AnalysisConfig {
	return AnalysisConfig{
		ServerLengtheningFactor: 1.01,
		OverlapAllowance:        0.01,
	}
}
