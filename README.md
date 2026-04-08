# CRISP: Critical Path Analysis of Microservice Traces

This repo contains code to compute and present critical path summary from [Jaeger](https://github.com/jaegertracing/jaeger) microservice traces.
To use first collect the microservice traces of a specific endpoint in a directory (say `traces`).
Let the traces be for `OP` operation and `SVC` service (these are Jaeger termonologies).
`python3 process.py --operationName OP --serviceName SVC -t <path to trace> -o . --parallelism 8` will produce the critical path summary using 8 concurrent processes. 
The summary will be output in the current directory as an HTML file with a heatmap, flamegraph, and summary text in `criticalPaths.html`.
It will also produce three flamegraphs `flame-graph-*.svg` for three different percentile values.

The script accepts the following options:

```
python3 process.py --help
usage: process.py [-h] -a OPERATIONNAME -s SERVICENAME [-t TRACEDIR] [--file FILE] -o OUTPUTDIR
                  [--parallelism PARALLELISM] [--topN TOPN] [--numTrace NUMTRACE] [--numOperation NUMOPERATION]

optional arguments:
  -h, --help            show this help message and exit
  -a OPERATIONNAME, --operationName OPERATIONNAME
                        operation name
  -s SERVICENAME, --serviceName SERVICENAME
                        name of the service
  -t TRACEDIR, --traceDir TRACEDIR
                        path of the trace directory (mutually exclusive with --file)
  --file FILE           input path of the trace file (mutually exclusivbe with --traceDir)
  -o OUTPUTDIR, --outputDir OUTPUTDIR
                        directory where output will be produced
  --parallelism PARALLELISM
                        number of concurrent python processes.
  --topN TOPN           number of services to show in the summary
  --numTrace NUMTRACE   number of traces to show in the heatmap
  --numOperation NUMOPERATION
                        number of operations to show in the heatmap
```

## Development

**Recommendation:** treat **Bazel + `requirements_lock.txt` as canonical** (aligned with a Bazel-first internal repo), and keep a **plain-Python path** so external contributors never have to install Bazel. Both use the same lockfile; CI runs the Python workflow on every PR and **also** runs `bazel test //...` whenever `BUILD.bazel` files are present.

### Requirements

- **Python 3.11** is what **CI and Bazel** use (`MODULE.bazel`, GitHub Actions).
- **Locally**, use **3.11** for the closest match to CI, or **3.11+** if you accept small version skew. Put the interpreter you want on `PATH`, or set `PYTHON` when calling `scripts/ci-local.sh`.

### First-time setup (macOS + Homebrew)

Install tooling and create a **3.11** virtualenv in the repo (adjust paths if you are not using Homebrew’s prefix):

```bash
brew install python@3.11 bazelisk
# Ensures brew’s binaries are on PATH in new shells (Apple Silicon default):
#   export PATH="/opt/homebrew/bin:$PATH"

cd /path/to/CRISP
rm -rf .venv && python3.11 -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -r requirements_lock.txt

bash scripts/ci-local.sh
bazel test //...
```

After `source .venv/bin/activate`, `python3` / `python` point at 3.11, so you usually do **not** need `PYTHON=...` for `scripts/ci-local.sh`.

### Without Bazel

```bash
python3.11 -m venv .venv    # or: python3 -m venv .venv  (if python3 is 3.11+)
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -U pip
pip install -r requirements_lock.txt

# Same checks as the CI “Python 3.11” job (pytest + trace smoke tests)
bash scripts/ci-local.sh
```

One step (creates no venv for you; uses whatever `python3` is first on `PATH`):

```bash
bash scripts/ci-local.sh --install
```

Set `PYTHON=/path/to/python` if the interpreter you want is not `python3` (for example the venv’s `python`).

### With Bazel ([Bazelisk](https://github.com/bazelbuild/bazelisk))

Bazelisk installs the Bazel version from [`.bazelversion`](.bazelversion) automatically.

- **macOS:** `brew install bazelisk` — run `bazel test //...` or `bazelisk test //...` depending on your install (some setups only put `bazelisk` on `PATH`).
- **Other platforms:** see the [Bazelisk releases](https://github.com/bazelbuild/bazelisk/releases) page.

```bash
bazel test //...    # or: bazelisk test //...
```

Third-party Python packages are loaded from `requirements_lock.txt` via `rules_python` in `MODULE.bazel`.

### Updating dependencies

1. Edit [`requirements.in`](requirements.in).
2. Regenerate the lockfile (use a public index so the file stays OSS-safe). Requires [pip-tools](https://pypi.org/project/pip-tools/) (`pip install pip-tools`):

   ```bash
   PIP_INDEX_URL=https://pypi.org/simple pip-compile requirements.in -o requirements_lock.txt --strip-extras --no-emit-index-url
   ```

3. Re-run `bash scripts/ci-local.sh` and `bazel test //...` (or `bazelisk test //...`).

### Troubleshooting

| Problem | What to do |
|--------|------------|
| `python3.11: command not found` | **macOS:** `brew install python@3.11`, then ensure `/opt/homebrew/bin` is on your `PATH` (restart the terminal or add `export PATH="/opt/homebrew/bin:$PATH"` to `~/.zshrc`). **Linux:** install `python3.11` from your distro (e.g. `apt install python3.11-venv`) and use the full path to `python3.11` if it is not default. **Windows:** install [Python 3.11](https://www.python.org/downloads/) and use `py -3.11` or the **Python 3.11** app in the installer. |
| `bazel: command not found` | **macOS:** `brew install bazelisk` — you should get both `bazel` and `bazelisk` under `/opt/homebrew/bin`. If only `bazelisk` exists, run `bazelisk test //...`. **Else:** download a binary from [Bazelisk releases](https://github.com/bazelbuild/bazelisk/releases) and put it on your `PATH`. |
| Wrong Python inside `.venv` (e.g. still 3.14 after installing 3.11) | Remove and recreate the venv so it is not reused from an older interpreter: `rm -rf .venv && python3.11 -m venv .venv` (use the full path to 3.11 if needed, e.g. `/opt/homebrew/bin/python3.11`). |
| `ModuleNotFoundError` / `No module named pytest` | Activate the venv (`source .venv/bin/activate`) and run `pip install -r requirements_lock.txt`, or one shot: `bash scripts/ci-local.sh --install`. |
| `pip-compile: command not found` | `pip install pip-tools` (in your active venv or user site). |
| `test.sh` / `flamegraph.pl` errors | Install **Perl** if missing (`perl -v`). Ensure scripts are executable: `chmod +x flamegraph.pl difffolded.pl`. |
| `bazel` downloads forever / wrong Bazel version | The repo pins Bazel via [`.bazelversion`](.bazelversion). Use **Bazelisk** (not a hand-installed Bazel) so that file is respected. |

### Continuous integration

| Job | What it runs |
|-----|----------------|
| **Python 3.11** | `pip install -r requirements_lock.txt`, then [`scripts/ci-local.sh`](scripts/ci-local.sh) |
| **Bazel** | `bazel test //...` (only if any `BUILD.bazel` exists in the tree) |

## Dataset
- We released the artifact of the original [CRISP](https://www.usenix.org/conference/atc22/presentation/zhang-zhizhou) paper at https://zenodo.org/records/13956078.
- We released ~1.4 million production traces along with our paper [The Tale of Errors in Microservices](https://doi.org/10.1145/3700436) at https://zenodo.org/records/13947828.

Please cite our paper if you use the dataset in your research.
