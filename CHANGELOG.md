# Changelog

## 0.2.2 — 2026-10-14

- Stop SSE parsing on cancellation, retaining the last delivered cursor when
  disposal interrupts a network chunk or a pending read completes after abort.
- Encode SSE resume IDs as UTF-8 header bytes, including Unicode IDs, and decode
  caller-provided resume headers without changing their cursor on reconnect.
- Preserve observer instances and their method receivers while keeping each
  subscription independently owned, including repeated acquisitions and replay.
- Preserve HTTP status/headers and content-type errors when response body
  cancellation fails, so authentication and rate-limit retry policies still work.
- Add regressions and real HTTP integration tests for these cases; retain 100%
  per-file statement, branch, function and line coverage.

## 0.2.1 — 2026-10-02

- Infer payload unions when selecting between Connect methods with different
  input/output schemas. Invalid input still fails against the selected method.
- Export `StreamValue<typeof source>` for deriving pool and application payload
  types from an adapter without repeating the message shape.
- Add installation/ownership, React rendering and troubleshooting guides. Check
  their complete TypeScript/TSX examples against public declarations in CI.
- Add compile-time regressions for mixed protocols, mixed Connect methods,
  generic wrappers, callback inference and incompatible pool/source pairs.

No runtime lifecycle or retry-policy changes. Existing imports and subscriptions
remain supported.

## 0.2.0 — 2026-10-02

- Add `adapter({ type, ...options })` for SSE, WebSocket, iterable and custom
  sources. The `/connect` entrypoint provides the same factory with Connect
  support, keeping SDK peers optional for base consumers.
- Add injectable SSE `fetch` and WebSocket `createSocket`, structured
  `SseHttpError` metadata and pool-level `onRetry` observation.
- Prevent repeated notifications during callback-driven resubscription and avoid
  opening already cancelled iterable/Connect streams.
- Check an isolated npm consumer without optional SDKs, plus authentication,
  recovery, cancellation and cleanup scenarios.

The named factories introduced in 0.1 remain supported.

## 0.1.0 — 2026-10-02

Initial release: protocol-neutral shared stream pool, named protocol adapters,
React subscriptions, lifecycle/retry policies, coverage gates and benchmarks.
