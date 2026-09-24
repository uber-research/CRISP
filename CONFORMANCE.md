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

## Error breakdown

`--errorBreakdown {origins,propToRoot}` (light mode) also writes
`error-breakdown.json`: the call paths that end in an error, keyed by RPC
protocol and status code (`crisp/error_breakdown.py`,
`go/crisp/error_breakdown.go`). Compare byte-wise.

**Error detection.** A span errors if `parseForErrorReturn` flags it
(`returnError`, including error-propagation nodes) or its RPC status code
signals failure: HTTP codes `>= 400`, any other protocol's non-zero code.
This union applies to the breakdown only; critical-path outputs are
unchanged.

**Protocol and status code** come from the span's tags. Keys match exactly,
and a later status tag overrides an earlier one:

| Tag | Protocol | Code |
|---|---|---|
| `grpc.status`, `grpc.status_code`, `rpc.grpc.status_code` | `grpc` | tag value |
| `http.response.status_code`, `http.status_code` | `http` | tag value |
| `rpc.yarpc.status_code` | `yarpc_<rpc.transport>` if none of the above | tag value |
| `rpc.transport` (`http`, `grpc`, `tchannel`) | the transport, if nothing else set it | — |

A code is a non-empty string in `strconv.Atoi` syntax or an `int64`-typed
integer, within int64 range; anything else yields no code. A gRPC status
name such as `grpc.status = "UNAVAILABLE"` therefore sets only the protocol.

Deployment lists in `span_utils` (all empty by default) extend these rules:
`TCHANNEL_STATUS_TAGS` (tag keys carrying a `tchannel` code, like a row
above), `YARPC_STATUS_TAGS` (extra keys treated like
`rpc.yarpc.status_code`), `HTTP_COMPONENTS` (`component` values implying
HTTP, applied before `rpc.transport`), and `TCHANNEL_MARKER_TAGS` (tag keys
forcing TChannel).

Status codes count toward error detection on any span, but only RPC
(client/server) spans show protocol and code in their path element (see
Output).

**Tree and root.** The walk uses parent/child links as built from `CHILD_OF`
references (after proxy short-wiring and error propagation, before timeline
sanitization and op exclusion). `--errorBreakdownRoot`:

- `trace` (default): the first span in document order with no `CHILD_OF`
  reference that is not a proxy node or in `IGNORED_ROOT_OPS`. With none,
  spans whose parent is missing become children, in document order, of a
  virtual `[INCOMPLETE_TRACE]virtual_root` node. Other roots and orphans are
  ignored.
- `analysis`: the root chosen for `-s`/`-a` (`--rootTrace` semantics).

Only traces the light pipeline analyzes (usable analysis root, not a skipped
test trace) contribute, in both root modes.

**Walk.** From the root, each visited span extends the path. An erroring
span visits all its children; if none of them reports an error, the path is
recorded. A non-erroring span depends on the mode:

- `origins`: visits all children.
- `propToRoot`: an RPC (client/server) span stops the walk; any other span
  visits only children of its own service (the `computePropToRootGraph`
  walk). The virtual root always visits all children.

A span "reports an error" if it errors or a visited descendant does. Children
are visited in insertion order, and all of them are visited even after one
reports an error.

**Output.** Each path element is `[service]operation`, followed for RPC spans
by `:protocol` (if known) and `:code` (if present). Elements are joined with
`;` to form the key. Within a trace, a repeated key only increments its
count. Across traces, counts add up, and each node keeps up to
`--maxExemplars` distinct `{spanID, traceID}` exemplars in trace order; the
virtual root has none. `traces` counts contributing traces, and `paths` are
sorted by key. The JSON encoding is the same as `conformance.json`:

```json
{
  "mode": "origins",
  "paths": [
    {
      "count": 1,
      "key": "[gateway]/api:http:500;[users]GetUser:grpc:14",
      "nodes": [
        {"exemplars": [{"spanID": "…", "traceID": "…"}], "operation": "/api",
         "protocol": "http", "service": "gateway", "statusCode": 500},
        …
      ]
    }
  ],
  "root": "trace",
  "traces": 1
}
```

`protocol` and `statusCode` are `null` for non-RPC spans and when absent.

## Goldens

`test_cases/golden/<fixture>/` holds conformance outputs for every fixture
(top-level `*.json` and `err_pattern*/*.json`; `/` becomes `_` in the name).

- Each also holds `error-breakdown-{origins,propToRoot}.json` (`trace` root).
- `test_cases/error_breakdown/*.json` are error-breakdown fixtures; their
  goldens, `test_cases/error_breakdown/golden/<fixture>/<mode>-<root>.json`,
  cover all four mode/root combinations (with `--rootTrace` off).
- Regenerate with `python scripts/generate_goldens.py`. It runs each fixture
  twice per output in fresh subprocesses and refuses to write unless both
  runs are byte-identical.
- `tests/test_conformance.py` and `tests/test_error_breakdown.py` compare
  every fixture against its goldens byte-wise and keep fixtures and goldens
  in sync; the Go port's tests check the same goldens.
- Intentional behavior changes land in the Python reference first and
  regenerate goldens in the same PR, with the output diff explained.
