#!/usr/bin/env bash
# Run the same checks as the GitHub Actions "Python 3.11" job (pytest + test.sh).
# Bazel is optional: install Bazelisk (https://github.com/bazelbuild/bazelisk) and pass --with-bazel.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

export MPLBACKEND="${MPLBACKEND:-Agg}"
PY="${PYTHON:-python3}"

usage() {
  echo "Usage: $0 [--install] [--with-bazel]" >&2
  echo "  --install     pip install -r requirements_lock.txt first (uses \$PYTHON, default python3)" >&2
  echo "  --with-bazel  run bazel test //... after (requires bazel or bazelisk on PATH)" >&2
}

INSTALL=0
WITH_BAZEL=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --install) INSTALL=1; shift ;;
    --with-bazel) WITH_BAZEL=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown option: $1" >&2; usage; exit 1 ;;
  esac
done

if [[ "$INSTALL" == 1 ]]; then
  "$PY" -m pip install -U pip
  "$PY" -m pip install -r requirements_lock.txt
fi

"$PY" -m pytest -q
chmod +x flamegraph.pl difffolded.pl
bash test.sh

if [[ "$WITH_BAZEL" == 1 ]]; then
  if command -v bazelisk >/dev/null 2>&1; then
    bazelisk test //...
  elif command -v bazel >/dev/null 2>&1; then
    bazel test //...
  else
    echo "error: --with-bazel but neither bazel nor bazelisk is on PATH" >&2
    echo "  macOS: brew install bazelisk   (then run: bazel test //...)" >&2
    exit 1
  fi
fi
