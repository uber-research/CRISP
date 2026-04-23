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
