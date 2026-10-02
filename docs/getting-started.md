# From installation to a shared stream

Watchpool fits applications where several consumers need the same live snapshot:
status cards, telemetry panels, presence indicators or a current position. It
retains one latest value per key. A durable event log, a command channel and an
application state store remain application responsibilities.

## Choose the entrypoint

| Use case | Factory import | Additional packages |
| --- | --- | --- |
| SSE, WebSocket, async iterable, custom transport | `@heojeongbo/watchpool` | None |
| Connect RPC, optionally mixed with the base protocols | `@heojeongbo/watchpool/connect` | `@connectrpc/connect`, `@bufbuild/protobuf` |
| React subscription hook | `@heojeongbo/watchpool/react` | `react` |

```sh
pnpm add @heojeongbo/watchpool
# Only when using Connect:
pnpm add @connectrpc/connect @bufbuild/protobuf
# Only when using React:
pnpm add react
```

The package is ESM. The supported runtime is a modern browser or Node.js 22+.
Importing `/connect` opts into its SDK; the base factory's public types and
runtime do not require it. Use `adapter({ type: ... })` in either entrypoint.
Creating an adapter or pool does not open a network connection.

## Connect two consumers

This browser example keeps one SSE request while either consumer is mounted.
Call the returned methods from the corresponding view/session cleanup handlers.
The server must send `Content-Type: text/event-stream` and SSE-framed events.

```ts
import { adapter, createWatchPool } from "@heojeongbo/watchpool";
import type { ServerEvent } from "@heojeongbo/watchpool/sse";

export function mountStatusViews(
  url: string,
  updateHeader: (text: string) => void,
  updatePanel: (text: string) => void,
) {
  const pool = createWatchPool<ServerEvent>({
    onCallbackError: (error) => console.error("Status callback failed", error),
  });
  const source = adapter({ type: "sse", url });
  const key = JSON.stringify(["sse", "status", url]);
  const leaveHeader = pool.subscribe(key, source, {
    onMessage: (event) => updateHeader(event.data),
  });
  const leavePanel = pool.subscribe(key, source, {
    onMessage: (event) => updatePanel(event.data),
  });
  return {
    leaveHeader,
    leavePanel,
    stats: () => pool.stats(),
    closeSession: () => pool.dispose(),
  };
}
```

The first subscription schedules the connection; the second shares it. Removing
one subscriber keeps it open. Removing both starts the default 3-second linger
window; remounting within it reuses the stream. `closeSession()` immediately
aborts all entries and resolves after their cleanup. A disposed pool cannot be
reused. Create a fresh one for the next session.

Late subscribers synchronously receive the latest retained value when available.
Do not make that callback depend on the unsubscribe function being assigned yet.
Treat received objects as immutable: other subscribers see the same object.

## Decide pool ownership and keys together

Create the pool at the authenticated session or transport scope, and pass it to
consumers. Do not construct a new pool inside every render or subscription. Do
not share a server-side module singleton between requests/users. Stop rendering
old consumers and `await pool.dispose()` when the owning session ends.

A key describes the stream's identity, including protocol, resource, filters and
any other input that changes its meaning. For example,
`JSON.stringify(["sse", "robot-status", robotId, region])` avoids delimiter
ambiguity. Keep credentials out of keys and logs. Distinct authenticated
identities need distinct pools even when their string keys match.

For a key that already exists, the **first subscriber's source and staleness
window win**. Passing a different URL, decoder, source object or `staleAfterMs`
does not reconfigure that entry. Use a different identity key for a different
stream. A new object with the same options does not require a new key. For an
explicit restart of a terminal stream, use an application-managed generation in
the key; unsubscribe the previous generation to avoid accumulating entries.

SSE adapters retain their resume cursor across attempts. Use one instance per
logical stream; do not share it between independent pools or keys. Each custom
adapter must support fresh runs after cleanup when retries are enabled.

## Keep message types through the pool

`adapter()` infers messages from the decoder, iterable or Connect method. A pool
is created before it sees a source, so it cannot infer its type from a later
`subscribe()` call. Use `createWatchPool<StreamValue<typeof source>>()` to derive the payload from
the adapter without duplicating its message shape. Import `StreamValue` from the
base entrypoint. An explicit `createWatchPool<MyMessage>()` also works; omitting
the type produces `WatchPool<unknown>`.

```ts
import { adapter, createWatchPool } from "@heojeongbo/watchpool";

export function createPositionWatch(url: string) {
  type Position = { x: number; y: number };
  const source = adapter({
    type: "websocket",
    url,
    decode(data): Position {
      if (typeof data !== "string") throw new TypeError("Expected text frame");
      const value: unknown = JSON.parse(data);
      if (
        typeof value !== "object" || value === null ||
        !("x" in value) || typeof value.x !== "number" ||
        !("y" in value) || typeof value.y !== "number"
      ) throw new TypeError("Expected a position");
      return { x: value.x, y: value.y };
    },
  });
  return { pool: createWatchPool<Position>(), source };
}
```

A type assertion after `JSON.parse()` does not validate the server's payload.
The decoder above rejects malformed frames. Decide whether decode failures
should terminate or retry using the pool's `retry` policy.

When extracting options into a variable, preserve the discriminator with
`const options = { type: "sse", url: "/events" } satisfies SseAdapterOptions`.
Import that type from the base entrypoint. A plain mutable object's `type` can
widen to `string`, which cannot select a protocol.

## Set policies before shipping

| Application decision | Configuration |
| --- | --- |
| Reconnect a long-lived watch | Default retry, or protocol-specific `connectRetry` |
| Consume a finite stream exactly once | `retry: () => false` |
| Stop on invalid credentials | Inspect `SseHttpError.status` or use `connectRetry` |
| Observe recoverable failures | Pool `onRetry` |
| Show final failure | Subscriber `onError` and snapshot `status` |
| Detect stale data after the first frame | Subscriber `staleAfterMs` |
| Diagnose callback exceptions | Pool `onCallbackError` |
| Replace fetch/socket implementations | SSE `fetch`, WebSocket `createSocket` |

The default policy also reconnects on clean EOF and has no jitter. A connected
stream that has never produced data stays `connecting`; `staleAfterMs` is not a
first-frame timeout. Put any required connection/first-frame deadline in your
transport or application policy.

See the [protocol examples and retry recipe](../README.md),
[React guide](react.md) and [troubleshooting guide](troubleshooting.md).
