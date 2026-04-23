# crisp package layering

Modules in this package follow a strict top-down dependency order. Lower layers
must not import from higher layers. Each PR in the port maintains this invariant.

## Layers (low to high)

1. **`crisp.utils`** — pure stdlib helpers. No imports from other `crisp.*`.
2. **`crisp.shared`** — shared datatypes, constants, small pure functions.
   May import from `crisp.utils`.
3. **`crisp.metrics`** — aggregation and percentile helpers over shared types.
   May import from `crisp.shared` and `crisp.utils`.
4. **`crisp.output`** — formatters and CSV generators.
   May import from `crisp.shared` and `crisp.utils`.
5. **`crisp` core** — `graph`, `models`, `common`, `trace_merger`,
   `flamegraph`, `cct_utils`, `storage`, `tb_client`, `configuration`,
   `constants`, `exceptions`. May import from any of the layers above.
6. **Entry points** — `cli`, `pipeline`, `get_trace`, `process_trace`, `main`.
   May import from core and below.

## Rule of thumb

If you find yourself adding a `crisp.process_trace` import inside
`crisp/output/`, stop — the dependency is inverted. Move the shared piece
down to `crisp.shared` instead.

## Naming convention during the port

Most public functions in `crisp/` (e.g. `accumulateInDict`, `isProxyNode`,
`getCPSize`) are `camelCase`. This violates PEP 8 but is **deliberate**:
the port preserves the exact names used by the upstream internal codebase
so that every call site migrates with a single-line import change rather
than a rename sweep. Renaming to `snake_case` is a cross-cutting concern
that deserves its own dedicated PR once the port has stabilized — please
don't do it as a drive-by in an unrelated change.

## Legacy terminology preserved verbatim

Some attribute names and user-facing strings still reference the internal
framework they came from (most notably `Metrics.isCtfTest` and the display
string "CTF test traces"). These are preserved verbatim for the same
reason the `camelCase` names are: renaming them cascades through every
caller in every unported PR. A generic rename (e.g. `isCtfTest` →
`isTestTrace`) will happen in the same post-stabilization sweep that
handles `camelCase` → `snake_case`. The underlying classification logic
was already generalized in the `crisp.utils.span_utils` port (see the
configurable `TEST_TRACE_OP_PREFIXES` / `TEST_TRACE_SERVICES` lists), so
no behavior is Uber-specific even though the attribute name still is.

## Layer-crossing test dependencies — handling rule

Many tests in the internal codebase were written before this layering
existed, so they freely import types from what we now call deeper layers.
When porting, apply this decision in order:

1. **Audit the test's actually-used types.** If a test imports a heavy
   class but only reads a handful of attributes, the class may be a thin
   data container that does not genuinely need the deep dependencies its
   module imports at the top. If it can be relocated to a shallower
   layer *verbatim* (body unchanged, file home changed), **promote it**
   during the port — see `crisp.shared.models` for examples of types
   lifted from internal `critical_path/models.py`.
2. **If the type genuinely needs a deeper dependency** (e.g. it imports
   `common`, does I/O, or pulls in graph machinery), **defer the test**
   to the PR that completes its deepest dependency. Note the deferral in
   the source PR's commit message and, if the deferral spans multiple
   PRs, add an entry to the "Deferred tests" list below.
3. **Never** modify test bodies to work around the problem — no
   lightweight stand-ins, no mocks substituted for real types, no
   subset-test-selection. Either (1) or (2). Modifying test bodies
   silently weakens verification and breaks the verbatim-port rule.

### Deferred tests

| Test file (internal) | Deferred until PR | Reason                |
| -------------------- | ----------------- | --------------------- |
