# watchpool

Share one upstream stream per key across subscribers. Keep the latest value,
reconnect after transient failures, and release resources when the last subscriber
leaves. Protocol-neutral TypeScript core, Connect RPC, SSE, WebSocket,
and React entrypoints.

```sh
pnpm add @heojeongbo/watchpool
```

Core, SSE, WebSocket and iterable adapters need no third-party runtime packages.
React, Connect and Protobuf are optional peers: install only the ones you use.
For Connect, install `@connectrpc/connect` and `@bufbuild/protobuf`, then import
`adapter` from `@heojeongbo/watchpool/connect`. That entrypoint also supports the
base protocols with the same API. ESM only; modern browsers or Node.js 22+.

`adapter({ type, ...options })` selects the protocol and infers its message type.
Use `type: "custom"` with an `implementation: StreamAdapter<T>` to inject another
protocol without changing the pool. The named factories from 0.1 remain available
for compatibility. Protocol selection happens once at creation, not per message.

## SSE

```ts
import { adapter, createWatchPool } from "@heojeongbo/watchpool";
import { type ServerEvent } from "@heojeongbo/watchpool/sse";

const pool = createWatchPool<ServerEvent>();
const source = adapter({ type: "sse", url: "https://example.com/status" });
const unsubscribe = pool.subscribe("status", source, {
  onMessage: (event) => console.log(event.data),
});

// Another subscriber to this key joins the same HTTP request.
const leave = pool.subscribe("status", source, { onMessage: console.log });
unsubscribe(); // The other subscriber keeps the request alive.
leave();       // Abort after the default 3-second linger window.
await pool.dispose(); // Immediately abort all keys and await their cleanup.
```

SSE uses `fetch`, supports custom `headers` and `credentials`, UTF-8 chunking,
CR/LF/CRLF framing, multiline data, named events and `Last-Event-ID` on reconnect.
It retains at most one partial event, bounded by `maxEventChars` (default 1 Mi UTF-16
characters). Incomplete events at EOF are discarded. Reconnect timing belongs to
the pool; SSE `retry:` fields are deliberately ignored. HTTP 204 and non-2xx
responses reject; configure `retry` if your endpoint treats them as terminal.
Use one SSE source instance per independent subscription identity: its resume ID
belongs to that logical stream. URL factories may refresh signed URLs per attempt.

### HTTP errors, authentication and retry visibility

SSE accepts `fetch: authenticatedFetch` for an application transport wrapper.
`SseHttpError` exposes `status` and a copy of response `headers`; response bodies
are cancelled before the error is delivered. Use structured metadata in policies:

```ts
import { createWatchPool, exponentialRetry } from "@heojeongbo/watchpool";
import { SseHttpError, type ServerEvent } from "@heojeongbo/watchpool/sse";

const pool = createWatchPool<ServerEvent>({
  retry(context) {
    const error = context.error;
    if (error instanceof SseHttpError) {
      if (error.status === 401 || error.status === 403) return false;
      const seconds = Number(error.headers.get("retry-after"));
      if (error.status === 429 && seconds > 0 && Number.isFinite(seconds)) {
        return Math.min(seconds * 1000, 30_000);
      }
    }
    return exponentialRetry(context);
  },
  onRetry: ({ key, attempt, delayMs, error }) =>
    console.log("retry", { key, attempt, delayMs, error }),
});
```

This example handles numeric Retry-After values; applications needing HTTP-date
values should parse those explicitly. `onRetry` reports the zero-based attempt,
EOF/failure, elapsed time and selected delay before scheduling. Callback failures
are isolated; calling `dispose()` there prevents the retry. Observer `onError`
remains reserved for terminal failures.

## WebSocket

```ts
import { adapter, createWatchPool } from "@heojeongbo/watchpool";

type Position = { x: number; y: number };
const pool = createWatchPool<Position>();
const source = adapter({
  type: "websocket",
  url: "wss://example.com/positions",
  decode: (data): Position => JSON.parse(String(data)),
});
const leave = pool.subscribe("positions", source, { onMessage: console.log });
```

The decoder receives the native message data (`string`, `Blob` or `ArrayBuffer`).
Validate untrusted payloads in the decoder. Decode failures, socket errors and
abnormal closes reject the source; code 1000 is clean EOF. This is a receive-only
adapter; sending application commands remains the application's responsibility.
Optional `protocols` are passed to the native WebSocket constructor.
Inject `createSocket(url, protocols)` to provide a compatible application socket
without modifying globals; return a fresh socket on every call.

## Connect RPC

```ts
import { createWatchPool } from "@heojeongbo/watchpool";
import { adapter, connectKey, connectRetry } from "@heojeongbo/watchpool/connect";

// `transport`, `watchMethod` and generated input types come from your application.
const source = adapter({
  type: "connect", transport, method: watchMethod, input: { id: "robot-1" },
});
const pool = createWatchPool({ retry: connectRetry });
const leave = pool.subscribe(
  connectKey(watchMethod, { id: "robot-1" }),
  source,
  { onMessage: console.log },
);
```

`connectRetry` treats InvalidArgument, NotFound, PermissionDenied,
Unauthenticated and Unimplemented as terminal. Other failures and EOF reconnect.
A pool is scoped to one transport and authentication identity: create separate
pools for distinct tenants, users or transports, and dispose on logout. A matching
string key alone is not an authorization boundary. Connect keys use protobuf JSON;
applications with unordered map inputs should normalize map insertion order.

## React

```tsx
import { adapter, createWatchPool } from "@heojeongbo/watchpool";
import { useWatch } from "@heojeongbo/watchpool/react";
import { type ServerEvent } from "@heojeongbo/watchpool/sse";

// Own this pool at the transport/session scope, outside component renders.
const pool = createWatchPool<ServerEvent>();
const source = adapter({ type: "sse", url: "/events" });

function Status() {
  const state = useWatch(pool, "status", source, {
    onMessage: (event) => console.log(event.data),
    staleAfterMs: 5_000,
  });
  return <span>{state.stale ? "Stale" : state.status}</span>;
}
```

`useWatch` uses `useSyncExternalStore`; state identity only changes when status,
first-value availability or staleness changes. Frames arrive via `onMessage`, not
via a React render on every frame. Use your own state/reducer when values must
render. Callbacks can change identity without resubscribing. Keys must include
all source-affecting input. `{ enabled: false }` does not subscribe. SSR returns
a stable idle snapshot and never opens a source. Never share server-side pools
across authenticated requests.

## Another protocol

Adapt an abortable async iterable:

```ts
import { adapter } from "@heojeongbo/watchpool";
const source = adapter({ type: "iterable", open: (signal) => client.watch({ signal }) });
```

Or implement `StreamAdapter<T>` directly:

```ts
import type { StreamAdapter } from "@heojeongbo/watchpool";

const source: StreamAdapter<number> = {
  async run(signal, sink) {
    const subscription = await client.subscribe({ signal });
    try {
      sink.opened();
      for await (const value of subscription) sink.emit(value);
    } finally {
      await subscription.close();
    }
  },
};
```

See [the BroadcastChannel adapter example](examples/broadcast-channel.ts): it
implements a fourth transport outside `src/`, using only the public adapter
contract. Its tests use a real channel and require no protocol registry or core
changes.

An adapter run resolves on EOF and rejects on failure. It **must respond to abort,
release all owned resources, stop emitting, and settle**. The pool awaits its
settlement before reopening the same key. A source that ignores abort can stall
`dispose()`; the library cannot forcibly release someone else's resource. Keep
source cleanup independent of `pool.dispose()` to avoid cyclic waiting.

## Lifecycle and policy

- The first subscriber chooses the source and staleness window for a key's
  lifetime. Every acquisition owns a separate reference, even with an identical
  observer object. Unsubscribe is idempotent.
- Late subscribers receive the retained latest value, including `undefined`.
  They do not receive `onOpen`; no new connection opened for them. This is
  snapshot replay, not lossless event history. Log/event consumers need their
  own accumulator or resumable protocol.
- The default retry is 2s, 4s, 8s, 16s, then 30s. Clean EOF or a connection that
  lasted at least 30s resets the sequence. It has no jitter. Supply `retry` to
  select terminal errors, finite-stream behavior, jitter or different timing.
- `retry({ error, ended, attempt, elapsedMs })` returns a delay from 0 to 2,147,483,647 milliseconds or
  `false`. Invalid values or thrown policy errors terminate the entry.
  `{ retry: () => false }` makes finite streams end once.
- Status is `connecting`, `receiving`, `retrying`, `error` or `ended`; disabled
  React subscriptions are `idle`. An open connection is not evidence of fresh
  data. `hasValue` distinguishes no data from a real `undefined` value.
- `staleAfterMs` is opt-in. It continues to age a retained value during outages.
  One 1Hz timer serves all tracked entries in a pool. Background browsers can
  throttle timers; inject `watchStaleness(tick)` for a worker-backed clock.
- Subscriber callback failures are isolated and counted; `onCallbackError`
  optionally reports them; reporter failures are isolated too. Callbacks are synchronous: slow
  work delays fanout and cannot be preempted. Offload heavy work to a worker.
- `getLatest(key)` reads the retained value without allocating a snapshot.
  `getSnapshot(key)` returns stable status; unknown keys return `connecting`.
  `stats()` exposes keys, subscribers, opens, frames, deliveries, retries and
  callback failures. Delivery counts exclude initial catch-up replay.
- `dispose()` is terminal and idempotent. It clears timers, aborts upstreams,
  removes observers and awaits cleanup. No new subscriptions are accepted.

Memory is O(keys + subscribers + latest payloads). There is no unbounded delivery
queue. Objects are shared by reference: treat received payloads as immutable.

## Development

```sh
pnpm install --frozen-lockfile
pnpm check
pnpm pack
```

`pnpm check` runs Biome, FSD boundary checks, TypeScript, Vitest coverage, the ESM
build and performance checks. CI repeats it on Node 22 and 24. Coverage thresholds
are 100% **per source file** for statements, branches, functions and lines; all
`src/**/*.ts` and `examples/**/*.ts` files are included, including newly added files. Tests and tooling
are outside the runtime coverage denominator. Unit tests cover lifecycle and
adapter failures; loopback integration tests exercise real Connect RPC, SSE and WebSocket
connections. HTML and JSON coverage reports are written to `coverage/`.

The layout follows the applicable parts of Feature-Sliced Design:

```text
src/
  shared/lib/watchpool/   # framework/protocol-neutral lifecycle and contracts
  shared/api/            # connect, sse, websocket, iterable adapters
  features/watch-stream/ # React subscription feature
  index.ts               # package entrypoints compose public barrels
```

Dependencies flow from features to shared. There are no artificial pages,
widgets or app layers in this library. Cross-layer imports use public barrels;
`scripts/check-boundaries.mjs` enforces direction and slice boundaries.
The design uses an explicit pool factory for ownership, source adapters for
protocol boundaries, a retry strategy for policy, and observers for delivery.
There is no global singleton, service locator, inheritance hierarchy or plugin
registry. Connect delegates iterable consumption to the shared iterable adapter;
new protocols implement the same small `StreamAdapter<T>` contract. SSE itself
is split into HTTP/body ownership, a line reader, field parsing and event
assembly; the parser facade only wires these responsibilities together.

Tooling conventions were informed by
[ui-app-template](https://github.com/heojeongbo/ui-app-template); stream ownership
and benchmark scenarios were informed by
[streamflight](https://github.com/heojeongbo/streamflight). Watchpool is an
independent TypeScript implementation, not a Go wrapper.

## Performance

```sh
pnpm build
pnpm perf        # write perf-results.json
pnpm perf:check  # also enforce runtime size budget and structural invariants
```

Measures fanout, latest-value reads and subscription churn with 1, 10, 100 and
1,000 subscribers, plus SSE framing at 64-byte and 4 KiB payload sizes. Each scenario warms up 1,000 operations and records nine
10,000-operation samples. Reports median nanoseconds, sample p95 and operations
per second alongside Node/OS/CPU metadata. The p95 describes sample averages, not
individual-operation tail latency. These are in-memory costs, not network
throughput. There is no cross-language performance claim against streamflight.

CI checks one upstream per key, no per-frame status notification churn, no leaked
subscriptions after churn/disposal, and a 20 KiB gzip budget for all runtime JS.
Timing is reported as an artifact rather than gated on noisy shared runners.
Compare timings on the same machine and Node version. `stats()` is diagnostic
and allocates; the benchmark keeps it outside the hot loop.

## Release

`npm publish --access public` runs `prepublishOnly` and the full check. Only
`dist/`, runtime `src/`, README, LICENSE, LICENSES.md and package metadata enter the tarball. Keep the release
commit and version aligned. npm authentication/2FA is owned by the maintainer;
never commit credentials.

MIT © heojeongbo

### User scenario regression tests

`tests/user-scenarios.test.ts` covers screen resubscription from state/open/error
callbacks, removal during notification, runtime protocol selection, HTTP 401
termination, HTTP 429 recovery, retry-observer failures and logout during retry.
Additional suites cover injected socket cleanup, Connect cancellation, React
StrictMode, real loopback connections and custom BroadcastChannel transport.
`pnpm package:check` installs the built tarball in an isolated temporary project
and checks strict TypeScript declarations and SSE execution without optional SDKs.
It runs in `pnpm check` and CI.
`tests/adapter-types.ts` checks invalid options at compile time, including React's
internally owned notification callback. Coverage thresholds are a guard against
unexecuted code, not a claim that every possible user scenario has been tested.
