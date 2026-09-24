# Changelog

All notable changes to CRISP are documented here.  
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [Unreleased]

### Added
- Full critical-path analysis pipeline: error analysis helpers, flamegraph
  tag filters, and orchestration (`performCriticalPathAnalysis`,
  `performErrorAnalysis`, `processReal`, `lightProcess`, `main()`).
- Output generators: `genCriticalPathFiles`, `genTimeSavedSummary`,
  `genTraceCSVFile`.
- Error CSV generators: `genPercentErrorFile`, `genSavingPotential`,
  `genErrStatsFiles`, `genMaxErrDepthPropToRootToNumTracesFiles`,
  `genSelfErrDepthToNumTracesFiles`.
- Call-chain tree output (`_writeCCTOutputs`): writes `.cct` and `.dot` files.
- Flamegraph tag filters: `GetFilteredMetrics`, `TagToStr`,
  `ProduceFlameGraphsForEachFilter`, `GetAllFlameGraphFiles`,
  `GetAllErrorFlameGraphFiles`, `genTagYAML`.
- `pyproject.toml` runtime dependencies: `boto3`, `numpy`, `pandas`,
  `python-dateutil`, `PyYAML`, `ratelimit`, `requests`, `tenacity`.
- Go port: `LightConfig.Context` — when non-nil, `LightProcess` checks it
  before each trace file so canceled callers stop promptly. Nil keeps the
  previous behavior.
- Go port: `LightConfig.FilterProxy` and a `--filterProxy` CLI flag,
  matching the Python CLI.
- Error breakdown (`crisp/error_breakdown.py`, Go port
  `go/crisp/error_breakdown.go`): `--errorBreakdown {origins,propToRoot}`
  and `--errorBreakdownRoot {trace,analysis}` write `error-breakdown.json`
  in light mode. It lists the call paths ending in an error, keyed by RPC
  protocol and status code (derived from standard span tags), with counts
  and exemplars. Errors are spans flagged by `parseForErrorReturn` or with a
  failing status code. Spec in CONFORMANCE.md; goldens for every fixture
  plus new `test_cases/error_breakdown/` fixtures. Go library:
  `GraphOptions.ErrorBreakdown` / `Graph.ErrorBreakdown`,
  `LightConfig.ErrorBreakdown`, `MergeErrorBreakdowns`.
- `span_utils` deployment lists for the error breakdown, empty by default:
  `HTTP_COMPONENTS`, `TCHANNEL_MARKER_TAGS`, `TCHANNEL_STATUS_TAGS`,
  `YARPC_STATUS_TAGS`, `IGNORED_ROOT_OPS` (Go: `HTTPComponents`,
  `TChannelMarkerTags`, `TChannelStatusTags`, `YARPCStatusTags`,
  `IgnoredRootOps`).
- Tests for `Graph.computePropToRootGraph`.

### Fixed
- `--errorAnalysis` flame graphs of errors propagated to the root were always
  empty: `process()` passed `{}` to `getMetrics` instead of
  `computePropToRootGraph()`'s result. Heavy `--errorAnalysis` runs now
  write a non-empty `errorsPropToRoot-flame-graph-P100` output.

### Changed
- `difftest -mode golden` checks only the conformance fixtures
  (`test_cases/*.json`, `test_cases/err_pattern*/*.json`), like
  `scripts/generate_goldens.py`; `-strict` corpus mode also compares
  `error-breakdown.json` when both templates pass `--errorBreakdown`.
- `go/crisp:crisp_test` now includes `light_test.go`.
- Light mode now honors `--filterProxy` (Python `process()` forwards it to
  `Graph`; the Go port matches). Previously the flag was accepted but
  ignored in light mode. No output change unless the proxy/err-prop lists
  in `span_utils` are populated; they ship empty.
- `mergeCallChains`, `mergeExampleID`, `makeClickable`, `renameSortableIcon`
  are now imported from `crisp.metrics.aggregators` /
  `crisp.output.formatters` instead of being redefined in `process_trace.py`.

---

## [0.1.0-dev] — initial release

CRISP (**C**ritical-path **I**nsights into **S**ervice **P**erformance) is a
Python library and CLI tool for extracting and visualising the critical path
from [Jaeger](https://www.jaegertracing.io/) distributed traces.

### Features

- **Critical-path extraction** — identifies the chain of spans that determines
  end-to-end latency for each trace.
- **Error analysis** — classifies errors by whether they appear on the critical
  path and computes per-operation saving potential.
- **Flame graphs** — per-percentile SVG flame graphs via
  [Brendan Gregg's FlameGraph](https://github.com/brendangregg/FlameGraph)
  scripts; gracefully skipped if `flamegraph.pl` is not on `PATH`.
- **HTML report** — interactive heatmap of per-operation critical-path time
  across all traces.
- **CSV outputs** — per-trace stats, latency percentiles, error depth,
  propagation length, resiliency, saving potential, cycles, cross-region calls.
- **Tag-based filtering** — separate flame graph outputs per tag value (e.g. by
  deployment region or build version).
- **Call-chain tree (CCT)** — `.cct` and `.dot` file output for call-chain
  tree analysis.
- **Light mode** — fast single-pass CCT output without full HTML/CSV
  generation (`--lightMode`).
- **Parallel processing** — `--computeParallelism N` to spread trace
  processing across worker processes.
- **Jaeger downloader** — `crisp/get_trace.py` fetches traces directly from a
  Jaeger HTTP endpoint with rate limiting and retries.
- **S3 storage client** — `crisp/tb_client.py` for uploading output files to
  S3-compatible object storage.

### Package layout

```
crisp/
  process_trace.py           — CLI entry point and analysis pipeline
  graph.py                   — critical-path graph algorithm
  flamegraph.py              — flame graph generation
  common.py                  — Config, constants, shared utilities
  cct_utils.py               — call-chain tree parsing and DOT export
  get_trace.py               — Jaeger HTTP trace downloader
  tb_client.py               — S3-compatible object storage client
  metrics/
    aggregators.py            — call-path profile aggregation
    percentile_calculator.py  — percentile DataFrame builders
  output/
    csv_generators.py         — summary / latency / cycles CSVs
    formatters.py             — HTML DataFrame formatters
  shared/
    models.py     — core dataclasses (Metrics, LatencyData, SavingData, …)
    constants.py  — shared string constants
    utils.py      — shared utility functions
  utils/
    dict_utils.py, singleton_wrapper.py, span_utils.py
```

### Known limitations

- **Protobuf output** — `_writeCCTOutputs` writes `.cct` and `.dot` but not
  `.pb`; protobuf serialization is deferred to a future release.
- **Storage upload** — `storage.py` exposes the upload interface but the
  implementation is a stub; bring your own upload logic or use `tb_client.py`
  directly.
