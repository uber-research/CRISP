"""Bazel entrypoint: run pytest against tests/ (mirrors local `python -m pytest`)."""

import os
import sys


def main() -> int:
    os.environ.setdefault("MPLBACKEND", "Agg")
    import pytest

    base = os.path.dirname(os.path.abspath(__file__))
    tests = os.path.join(base, "tests")
    if base not in sys.path:
        sys.path.insert(0, base)
    return pytest.main(
        [
            tests,
            "-q",
            "--import-mode=prepend",
        ]
    )


if __name__ == "__main__":
    raise SystemExit(main())
