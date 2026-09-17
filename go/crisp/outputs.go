package crisp

// outputs.go ports crisp/process_trace.py _writeCCTOutputs and
// crisp/cct_utils.py create_protobuf_response_with_exemplars: the
// light-mode .cct/.dot/.pb file triple.
//
// Parity notes:
//   - The .pb is serialized with the generated Go stubs for
//     crisp/proto/analyzer.proto (checked in, mirroring the checked-in
//     analyzer_pb2.py). AnalyzeResponse has no map fields, so Go's
//     proto.Marshal is byte-identical to Python's SerializeToString.
//   - proto3 implicit presence means zero frequencies and empty strings
//     never hit the wire, matching Python.

import (
	"os"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	"github.com/uber-research/CRISP/go/crisp/proto/analyzer"
)

// CreateProtobufResponseWithExemplars mirrors cct_utils.py
// create_protobuf_response_with_exemplars: build an AnalyzeResponse from
// parsed CCT summaries, grafting exemplars from the merged profile onto
// each entry's leaf call path.
func CreateProtobufResponseWithExemplars(summaries []*CCTSummary, mergedCpp *CallPathProfile, maxExemplars int) *analyzer.AnalyzeResponse {
	exemplarLookup := buildExemplarLookup(mergedCpp, maxExemplars)

	response := &analyzer.AnalyzeResponse{}
	for _, summary := range summaries {
		entry := &analyzer.CallChainSummary{}
		for i, path := range summary.CallPath {
			cp := &analyzer.CallPath{
				Service:       path.Service,
				OperationName: path.Operation,
			}
			if i == len(summary.CallPath)-1 && len(exemplarLookup) > 0 {
				profileKey := cctKeyToProfileKey(summary.CallPath)
				for _, ex := range exemplarLookup[profileKey] {
					cp.Exemplars = append(cp.Exemplars, &analyzer.Exemplar{
						TraceId: ex[0],
						SpanId:  ex[1],
					})
				}
			}
			entry.CallPath = append(entry.CallPath, cp)
		}
		entry.Base = &analyzer.PathStats{
			// FromMicroseconds equivalent.
			Duration:  durationpb.New(time.Duration(summary.Duration) * time.Microsecond),
			Frequency: int32(summary.Frequency),
		}
		response.ReportWindow_1 = append(response.ReportWindow_1, entry)
	}
	return response
}

// parseCCTString mirrors cct_utils.py parse_cct_file, reading from the
// in-memory folded-stack string instead of the file just written (same
// content).
func parseCCTString(cct string) []*CCTSummary {
	var summaries []*CCTSummary
	for _, line := range strings.Split(cct, "\n") {
		if summary := ParseCCTLine(line); summary != nil {
			summaries = append(summaries, summary)
		}
	}
	return summaries
}

// splitExt mirrors os.path.splitext: strip the final extension.
func splitExt(path string) string {
	return strings.TrimSuffix(path, filepath.Ext(path))
}

// WriteCCTOutputs mirrors process_trace.py _writeCCTOutputs: write the
// folded-stack .cct, the Graphviz .dot, and the protobuf .pb next to
// cctFile.
func WriteCCTOutputs(cctFile, flameGraphStr string, mergedCpp *CallPathProfile, maxExemplars int) error {
	if err := os.WriteFile(cctFile, []byte(flameGraphStr), 0o644); err != nil {
		return err
	}

	summaries := parseCCTString(flameGraphStr)
	dotStr := CCTToDot(summaries)
	if err := os.WriteFile(splitExt(cctFile)+".dot", []byte(dotStr), 0o644); err != nil {
		return err
	}

	pbResponse := CreateProtobufResponseWithExemplars(summaries, mergedCpp, maxExemplars)
	data, err := proto.Marshal(pbResponse)
	if err != nil {
		return err
	}
	if err := os.WriteFile(splitExt(cctFile)+".pb", data, 0o644); err != nil {
		return err
	}
	return nil
}
