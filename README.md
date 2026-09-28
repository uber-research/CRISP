# CRISP: Critical Path Analysis of Microservice Traces

[![CI](https://github.com/uber-research/CRISP/actions/workflows/ci.yml/badge.svg)](https://github.com/uber-research/CRISP/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)](https://www.python.org/downloads/)

CRISP identifies **which spans are on the critical path** of a distributed trace and tells you exactly where latency comes from.
Given a directory of [Jaeger](https://www.jaegertracing.io/) traces for a single service/operation, it produces:

- **HTML report** — interactive per-operation heatmap of critical-path time across all traces
- **Flame graphs** — per-percentile SVG flame graphs (P50, P75, P95, …) via [Brendan Gregg's FlameGraph](https://github.com/brendangregg/FlameGraph)
- **Calling-context tree (CCT)** — `.cct` and `.dot` files for downstream graph analysis
- **Protobuf output** — `.pb` binary using the bundled `analyzer.proto` schema
- **CSVs** — per-trace stats, latency percentiles, saving potential, cross-region calls, error depth

The original paper: **[CRISP: Critical Path Analysis of Large-Scale Microservice Architectures](https://www.usenix.org/conference/atc22/presentation/zhang-zhizhou)**, USENIX ATC '22.

> A **Go port** of the light/conformance pipeline — byte-identical outputs, single static binary, embeddable as a library — is in beta. See [Go port](#go-port-beta).

---

## Installation

> **Coming soon** — the `crisp-trace` PyPI package is not yet published.
> Install from source in the meantime (see [Development](#development)).

To also run the HTTP streaming service, install the optional `[server]` extras:

```bash
pip install -e ".[server]"   # adds fastapi, uvicorn[standard], aiofiles, httpx
```

---

## Quick start

**1. Collect Jaeger traces** for a single service + operation into a directory — each trace is a `.json` file from the [Jaeger HTTP API](https://www.jaegertracing.io/docs/latest/apis/).

**2. Run the analyzer:**

```bash
crisp-trace \
  -a checkout \
  -s frontend \
  -i traces/ \
  -o output/ \
  --parallelism 8
```

**3. Open the report:**

```bash
open output/criticalPaths.html   # macOS
xdg-open output/criticalPaths.html  # Linux
```

---

## CLI reference

```
crisp-trace [-h] -a OPERATIONNAME -s SERVICENAME [-i INPUTDIR] [--file FILE]
            [-o OUTPUTDIR] [--parallelism PARALLELISM]
            [--topN TOPN] [--numHMTrace NUMHMTRACE] [--numOperation NUMOPERATION]
            [--lightMode] [--errorAnalysis] [--doRanges]
            [--mergeAllRoots | --no-mergeAllRoots] [--rootTrace] [--anonymize]
            [--tags TAGS] [--exclude-from-cp EXCLUDEFROMCP]
            [--maxExemplars MAXEXEMPLARS]
            [--errorBreakdown {origins,propToRoot}]
            [--errorBreakdownRoot {trace,analysis}]
            [--deltaMicroSec DELTAMICROSEC]
            [--deltaTargetService DELTATARGETSERVICE]
            [--deltaTargetOperation DELTATARGETOPERATION]
            [--jaegerQueryUrl JAEGERQUERYURL]
```

### Core options

| Flag | Default | Description |
|---|---|---|
| `-a`, `--operationName` | *(required)* | Jaeger operation name to analyze |
| `-s`, `--serviceName` | *(required)* | Jaeger service name |
| `-i`, `--inputDir` | *(required)* | Directory of Jaeger trace `.json` files (mutually exclusive with `--file`) |
| `--file` | — | Single Jaeger trace file (mutually exclusive with `--inputDir`) |
| `-o`, `--outputDir` | same as `--inputDir` | Directory where output files are written |
| `--parallelism` | 1 | Number of parallel worker processes |

### Analysis options

| Flag | Default | Description |
|---|---|---|
| `--lightMode` | off | Fast single-pass CCT + protobuf output; skips HTML/CSV generation |
| `--errorAnalysis` | off | Run error-path analysis in addition to critical-path analysis |
| `--errorBreakdown MODE` | off | Light mode: also write `error-breakdown.json`, the error call paths keyed by RPC protocol and status code. `origins` reports every erroring span with no erroring child; `propToRoot` only errors that propagate to the root. See [CONFORMANCE.md](CONFORMANCE.md#error-breakdown) |
| `--errorBreakdownRoot ROOT` | `trace` | Where the error breakdown starts: `trace` (the trace's root span, or a virtual root over orphaned spans) or `analysis` (the root chosen for `-s`/`-a`) |
| `--doRanges` | off | Produce flame graphs for every 20-percentile window (P0–P20, P20–P40, …) |
| `--topN` | 20 | Max services shown in the summary |
| `--numHMTrace` | 200 | Max traces shown in the heatmap |
| `--numOperation` | 20 | Max operations shown in the heatmap |
| `--mergeAllRoots` / `--no-mergeAllRoots` | on | Merge metrics from every matching root span vs. only the first |
| `--rootTrace` | off | Require the service/operation to be the root span of the trace. Note: production traces are rarely single-rooted (orphan spans from sampling/truncation), and such traces are skipped — for endpoint analysis on production data, leave this off |
| `--anonymize` | off | Anonymize service and operation names in output |
| `--maxExemplars` | 3 | Max exemplar (trace/span) pairs kept per call path in `.pb` output |

### Filtering

| Flag | Description |
|---|---|
| `--tags YAML` | YAML list of `{name, value, search_depth}` tag filters to apply before analysis |
| `--exclude-from-cp FILE` | YAML file listing operations to exclude from the critical path |
| `--ignoreTestTraces` | Skip traces marked as synthetic test traces |

### Latency projection

| Flag | Description |
|---|---|
| `--deltaMicroSec N` | Simulate adding/removing N µs from the target service/operation |
| `--deltaTargetService SVC` | Service to target for latency projection (use with `--deltaMicroSec`) |
| `--deltaTargetOperation OP` | Operation to target for latency projection (use with `--deltaMicroSec`) |

### Jaeger API

| Flag | Default | Description |
|---|---|---|
| `--jaegerQueryUrl URL` | — | Base URL for the Jaeger query HTTP API (used by `crisp.get_trace`) |

---

## Output files

| File | Description |
|---|---|
| `criticalPaths.html` | Interactive HTML report with per-operation heatmap |
| `flame-graph-P{N}.svg` | SVG flame graph at percentile N (requires `perl` on `PATH`) |
| `*.cct` | calling-context tree in folded-stack format |
| `*.dot` | GraphViz DOT representation of the call-chain tree |
| `*.pb` | Protobuf binary (`AnalyzeResponse` message from `crisp/proto/analyzer.proto`) |
| `criticalPath*.csv` | Per-trace latency breakdown |
| `timeSaved*.csv` | Per-operation saving potential |
| `error*.csv` | Error depth / propagation stats (requires `--errorAnalysis`) |
| `error-breakdown.json` | Error call paths with counts and exemplars (requires `--errorBreakdown`) |

---

## HTTP service

CRISP ships an optional [FastAPI](https://fastapi.tiangolo.com/) server that exposes the same critical-path analysis over HTTP with a streaming protobuf wire protocol, suitable for programmatic integration.

### Starting the server

```bash
pip install -e ".[server]"
uvicorn crisp.server:app --host 0.0.0.0 --port 8080
```

### Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/health` | Liveness check — returns `{"status":"ok"}` |
| `POST` | `/v2/trace/analysis/stream` | Analyze one window of traces; returns a streaming protobuf response |
| `POST` | `/v2/trace/analysis/compare` | Analyze two windows and diff them; returns a streaming protobuf response |

### Wire protocol

Both `POST` endpoints accept a binary request body consisting of length-prefixed protobuf messages (`StreamAnalyzeRequest` / `CompareAnalyzeRequest` defined in [`crisp/proto/analyzer.proto`](crisp/proto/analyzer.proto)):

```
[varint length][StreamAnalyzeRequest bytes] [varint length][trace JSON bytes] ...
```

The response is a stream of length-prefixed `AnalyzeResponse` protobuf messages written as they become available.

---

## Go port (beta)

A Go implementation of the light/conformance pipeline lives in [`go/`](go/). It produces **byte-identical outputs** to the Python reference — `conformance.cct/json`, `light-flame-graph-P100.{cct,dot,pb}`, `slackDrag.csv`, `error-breakdown.json` — as a single static binary or an embeddable library, with no interpreter or pandas startup cost.

Not ported: the heavy analysis mode (HTML report, percentile flame graphs, `--errorAnalysis` outputs) and slack computation (`--computeSlackDrag`; drag is always computed, slack columns are `0.0`).

### CLI

```bash
go build -o crisp-go ./go/cmd/crisp
./crisp-go --file trace.json -s frontend -a checkout --conformance
./crisp-go -i traces/ -s frontend -a checkout --lightMode   # directory mode
```

Flags mirror the Python CLI for the light/conformance path; like Python's light mode, outputs are written next to `--file` or into `-i`.

### Library

```go
import "github.com/uber-research/CRISP/go/crisp"

cfg := &crisp.LightConfig{
    ServiceName: "frontend", OperationName: "checkout",
    Conformance: true, MaxExemplars: 3, OutputDir: outDir,
}
err := crisp.ProcessSingleTraceData(traceJSON, traceID, cfg) // one in-memory trace
```

`ProcessSingleTraceData` runs the full pipeline on trace bytes already in memory — no disk read, no subprocess. The library spawns no goroutines and keeps no mutable global state, so callers parallelize simply by calling it from their own goroutines and own the parallelism budget entirely.

For a single trace where only the critical path is needed, `crisp.CriticalPath(ctx, trace, rootSpanID)` takes a decoded `jaeger.Trace` and returns each critical-path span with its exclusive time, writing no files. It selects the root by span ID (`GraphOptions.RootSpanID`; Python: `Graph(..., rootSpanId=...)`), so another span with the same service and operation cannot be chosen instead.

### Validation

Byte-parity against the Python reference is enforced by a [difftest harness](go/tools/difftest) that compares all six light-mode outputs per trace (`slackDrag.csv` is compared row-sorted; pandas' sort is not stable across tied values):

- **Zenodo artifact corpus** — 170,993/170,993 traces byte-identical across all three datasets (`bottom-up-trace`, `Service43-Operation159`, `ml-service3`), including skip-for-skip agreement on multi-root/no-root traces and a 119,680-span trace. Fully reproducible: download the artifact and run `difftest -mode corpus -strict`.
- **The Tale of Errors in Microservices corpus** ([Zenodo part 1](https://zenodo.org/records/13947828) + [part 2](https://zenodo.org/records/13952897)) — 1,388,527/1,388,527 traces byte-identical across both artifact parts (`trace1`, `trace2`). Combined with the artifact corpus above, that's 1,559,520/1,559,520 public traces with zero divergence.

Performance is trace-size dependent, so we report both ends of the spectrum. On a random 100-trace sample of the public `bottom-up-trace` dataset (median 24 spans, 8 workers): shelling out to the Python CLI averages 380 ms/trace — almost entirely fixed interpreter + pandas startup — while the Go CLI as a subprocess takes 7 ms/trace and in-process library calls 3 ms/trace (48× and 89× wall-clock respectively). At the other end, the corpus' largest trace (119,680 spans) runs the identical algorithm in 17m05s (Python) vs 10m37s (Go), a 1.6× analysis-time speedup where startup cost is negligible. Between these regimes the ratio interpolates: startup elimination dominates small traces, analysis speed dominates large ones. Separately, a synthetic 957k-span trace completes in 13 s at 2.0 GB RSS (Jaeger deployments can see up to ~1M spans per trace).

A full write-up of the validation journey — methodology, the skip-semantics divergence the corpus surfaced, the 277× drag-sort fix, and the invocation-mode benchmark — is in [docs/go-port-validation.html](docs/go-port-validation.html).

---

## Development

### Requirements

- **Python 3.11** (what CI and Bazel use)
- **Perl** — only needed to generate SVG flame graphs; the rest works without it

### First-time setup (macOS + Homebrew)

```bash
brew install python@3.11 bazelisk

cd /path/to/CRISP
python3.11 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements_lock.txt

bash scripts/ci-local.sh      # pytest + smoke tests
bazel test //...               # Bazel build + test
```

### Without Bazel

```bash
python3.11 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -U pip
pip install -r requirements_lock.txt

bash scripts/ci-local.sh         # same checks as the CI "Python 3.11" job
```

One-liner (no venv management; uses whatever `python3` is first on `PATH`):

```bash
bash scripts/ci-local.sh --install
```

Set `PYTHON=/path/to/python3.11` if your default interpreter is not 3.11.

### With Bazel ([Bazelisk](https://github.com/bazelbuild/bazelisk))

```bash
bazel test //...    # Bazelisk reads .bazelversion and downloads the right Bazel
```

Third-party packages come from `requirements_lock.txt` via `rules_python` in `MODULE.bazel`.

### Updating dependencies

1. Edit [`requirements.in`](requirements.in).
2. Regenerate the lockfile (requires [`pip-tools`](https://pypi.org/project/pip-tools/)):

   ```bash
   PIP_INDEX_URL=https://pypi.org/simple \
     pip-compile requirements.in -o requirements_lock.txt \
     --strip-extras --no-emit-index-url
   ```

3. Re-run `bash scripts/ci-local.sh` and `bazel test //...`.

### Troubleshooting

| Problem | Fix |
|---|---|
| `python3.11: command not found` | **macOS:** `brew install python@3.11`, add `/opt/homebrew/bin` to `PATH`. **Linux:** `apt install python3.11-venv`. **Windows:** install [Python 3.11](https://www.python.org/downloads/). |
| `bazel: command not found` | **macOS:** `brew install bazelisk`. **Other:** download from [Bazelisk releases](https://github.com/bazelbuild/bazelisk/releases). |
| Wrong Python in `.venv` | `rm -rf .venv && python3.11 -m venv .venv` |
| `ModuleNotFoundError` / missing `pytest` | `source .venv/bin/activate && pip install -r requirements_lock.txt` |
| `pip-compile: command not found` | `pip install pip-tools` |
| Flame graph SVGs not generated | Install Perl (`perl -v`). The `.pl` scripts live in `crisp/` and are invoked automatically; no manual `chmod` needed. |
| Bazel downloads wrong version | Use **Bazelisk**, not a manually installed Bazel — it reads `.bazelversion`. |

### Continuous integration

| Job | What it runs |
|---|---|
| **Python 3.11** | `pip install -r requirements_lock.txt`, then [`scripts/ci-local.sh`](scripts/ci-local.sh) |
| **Bazel** | `bazel test //...` (skipped if no `BUILD.bazel` files exist) |

Unit tests for the HTTP service live in `tests/service/` and `tests/test_server.py`; end-to-end integration tests are in `tests/test_e2e_server.py`.

---

## Dataset

- Artifact from the original CRISP paper: <https://zenodo.org/records/13956078>
- ~1.4 million production traces from [The Tale of Errors in Microservices](https://doi.org/10.1145/3700436): <https://zenodo.org/records/13947828>

Please cite our papers if you use the datasets in your research.

## Related projects

- **[Calligator](https://github.com/google/calligator)** — a critical-path analysis and resource-optimization tool for microservices that vendors CRISP (`third_party/CRISP`) for graph-based critical-path summaries and flamegraph generation, building trace retiming and resource-reallocation recommendations on top.

## Citation

```bibtex
@inproceedings{zhang2022crisp,
  title={$\{$CRISP$\}$: Critical path analysis of $\{$Large-Scale$\}$ microservice architectures},
  author={Zhang, Zhizhou and Ramanathan, Murali Krishna and Raj, Prithvi and Parwal, Abhishek and Sherwood, Timothy and Chabbi, Milind},
  booktitle={2022 USENIX Annual Technical Conference (USENIX ATC 22)},
  pages={655--672},
  year={2022}
}
```
