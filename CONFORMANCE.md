# CRISP Conformance Contract

Deterministic outputs that alternative CRISP implementations must reproduce.
The Python implementation in this repo is the reference: its behavior, as
captured by these outputs, is the spec.

## Producing conformance outputs

```bash
crisp-trace --file trace.json -s SERVICE -a OPERATION --rootTrace --conformance
```

Writes two files next to the input trace:

| File | Contents | Comparison |
|---|---|---|
| `conformance.cct` | Folded-stack critical-path CCT, lines byte-sorted | byte-wise |
| `conformance.json` | Canonical JSON of the `AnalyzeResponse` built from the sorted CCT | structural (parse, then compare) |

`conformance.cct` is the primary artifact: all-integer and sorted, so two
conforming implementations must produce byte-identical files. Compare
`conformance.json` parsed, since JSON formatting conventions differ across
languages.

## CCT line format

```
[service1] operation1;[service2] operation2;...;[serviceN] operationN <excl> <<freq>>
```

- `<excl>`: exclusive critical-path time, integer microseconds.
- `<freq>`: integer frequency (number of traces containing this path).
- Literal `;` in names becomes `_` during profile-key construction; names are
  otherwise passed through verbatim.
- Lines sorted by code point (identical to UTF-8 byte order). Exactly one
  trailing newline; empty file if no lines.

## Determinism rules

Reference behaviors that affect output and are easy to get wrong in a port:

1. **Critical-path child ordering** (`computeCriticalPath`): children are
   stable-sorted by `endTime` ascending, then the list is reversed — on ties
   the later-inserted child comes first. A descending stable sort is not
   equivalent.
2. **Child insertion order** is significant (Python dict semantics). Use an
   order-preserving container; never iterate an unordered map for output.
3. **Integer arithmetic**: microsecond ints throughout. Averaging uses floor
   division (`//`), which differs from truncation on negatives.
4. **Timestamps** are microsecond epoch ints (~1.7e15); decode as 64-bit
   integers, never through a 64-bit float.
5. **Root selection**: the span with no in-trace `CHILD_OF` parent; ties broken
   by earliest `startTime`, then `spanID` (see `derive_root_span`).

Grow this list whenever differential testing finds a new
implementation-sensitive behavior.

## Goldens

`test_cases/golden/<fixture>/` holds conformance outputs for every fixture
(top-level `*.json` and `err_pattern*/*.json`; `/` becomes `_` in the name).

- Regenerate with `python scripts/generate_goldens.py`. It runs each fixture
  twice in fresh subprocesses and refuses to write unless both runs are
  byte-identical.
- `tests/test_conformance.py` compares every fixture against its golden
  byte-wise and keeps fixtures and goldens in sync.
- Intentional behavior changes land in the Python reference first and
  regenerate goldens in the same PR, with the output diff explained.
