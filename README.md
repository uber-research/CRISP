# CRISP: Critical Path Analysis of Microservice Traces

[![CI](https://github.com/uber-research/CRISP/actions/workflows/ci.yml/badge.svg)](https://github.com/uber-research/CRISP/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](LICENSE)
[![Python 3.11+](https://img.shields.io/badge/python-3.11%2B-blue)](https://www.python.org/downloads/)

CRISP identifies **which spans are on the critical path** of a distributed trace and tells you exactly where latency comes from.
Given a directory of [Jaeger](https://www.jaegertracing.io/) traces for a single service/operation, it produces:

- **HTML report** — interactive per-operation heatmap of critical-path time across all traces
- **Flame graphs** — per-percentile SVG flame graphs (P50, P75, P95, …) via [Brendan Gregg's FlameGraph](https://github.com/brendangregg/FlameGraph)
- **Call-chain tree (CCT)** — `.cct` and `.dot` files for downstream graph analysis
- **Protobuf output** — `.pb` binary using the bundled `analyzer.proto` schema
- **CSVs** — per-trace stats, latency percentiles, saving potential, cross-region calls, error depth

The original paper: **[CRISP: Critical Path Analysis of Large-Scale Microservice Architectures](https://www.usenix.org/conference/atc22/presentation/zhang-zhizhou)**, USENIX ATC '22.

---

## Installation

> **Coming soon** — the `crisp-trace` PyPI package is not yet published.
> Install from source in the meantime (see [Development](#development)).

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
| `--doRanges` | off | Produce flame graphs for every 20-percentile window (P0–P20, P20–P40, …) |
| `--topN` | 20 | Max services shown in the summary |
| `--numHMTrace` | 200 | Max traces shown in the heatmap |
| `--numOperation` | 20 | Max operations shown in the heatmap |
| `--mergeAllRoots` / `--no-mergeAllRoots` | on | Merge metrics from every matching root span vs. only the first |
| `--rootTrace` | off | Require the service/operation to be the root span of the trace |
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
| `*.cct` | Call-chain tree in folded-stack format |
| `*.dot` | GraphViz DOT representation of the call-chain tree |
| `*.pb` | Protobuf binary (`AnalyzeResponse` message from `crisp/proto/analyzer.proto`) |
| `criticalPath*.csv` | Per-trace latency breakdown |
| `timeSaved*.csv` | Per-operation saving potential |
| `error*.csv` | Error depth / propagation stats (requires `--errorAnalysis`) |

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

---

## Dataset

- Artifact from the original CRISP paper: <https://zenodo.org/records/13956078>
- ~1.4 million production traces from [The Tale of Errors in Microservices](https://doi.org/10.1145/3700436): <https://zenodo.org/records/13947828>

Please cite our papers if you use the datasets in your research.

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
