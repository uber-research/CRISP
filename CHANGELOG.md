# Changelog

All notable changes to CRISP are documented here.  
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

---

## [Unreleased]

---

## [0.1.0] — 2026-06-19

### Added
- Full critical-path analysis pipeline: error analysis helpers, flamegraph
  tag filters, and orchestration (`performCriticalPathAnalysis`,
  `performErrorAnalysis`, `processReal`, `lightProcess`, `main()`).
- Output generators: `genCriticalPathFiles`, `genTimeSavedSummary`,
  `genTraceCSVFile`.
- Error CSV generators: `genPercentErrorFile`, `genSavingPotential`,
  `genErrStatsFiles`, `genMaxErrDepthPropToRootToNumTracesFiles`,
  `genSelfErrDepthToNumTracesFiles`.
- Call-chain tree output (`_writeCCTOutputs`): writes `.cct`, `.dot`, and
  `.pb` (Protocol Buffers) files.
- Protobuf schema (`crisp/proto/analyzer.proto`) and generated bindings
  (`analyzer_pb2.py`) for `CallPath`, `Exemplar`, `PathStats`,
  `CallChainSummary`, and `AnalyzeResponse`.
- Flamegraph tag filters: `GetFilteredMetrics`, `TagToStr`,
  `ProduceFlameGraphsForEachFilter`, `GetAllFlameGraphFiles`,
  `GetAllErrorFlameGraphFiles`, `genTagYAML`.
- `pyproject.toml` runtime dependencies: `boto3`, `numpy`, `pandas`,
  `protobuf`, `python-dateutil`, `PyYAML`, `ratelimit`, `requests`,
  `tenacity`.
- End-to-end integration tests (`tests/test_e2e.py`) covering 25 trace
  fixtures and 4 error-pattern scenarios, including a protobuf/CCT parity
  regression test.
- GitHub Actions publish workflow (`.github/workflows/publish.yml`):
  automatic build → TestPyPI → PyPI on `v*` tags using OIDC trusted
  publishers (no API token required).

### Changed
- `mergeCallChains`, `mergeExampleID`, `makeClickable`, `renameSortableIcon`
  are now imported from `crisp.metrics.aggregators` /
  `crisp.output.formatters` instead of being redefined in `process_trace.py`.
- `main()` in `process_trace.py` returns an integer exit code (`0` on
  success, `1` on failure) instead of `None`.

---

## [0.1.0-dev] — initial OSS snapshot

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

- **Storage upload** — `storage.py` exposes the upload interface but the
  implementation is a stub; bring your own upload logic or use `tb_client.py`
  directly.
