# Troubleshooting and operational behavior

Start with the key, pool identity and `pool.stats()`. Stats are cumulative for
that pool and allocate a snapshot, so inspect them for diagnostics rather than
on every frame. `opens` counts attempts, not successful handshakes; `messages`
counts upstream frames, and `deliveries` counts live fanout, excluding late-join
replay. `callbackErrors` counts failures in callbacks, not network errors.

| Symptom | Check | Action |
| --- | --- | --- |
| Two connections for the same resource | Are consumers using the same pool and exactly equal key? | Share the session pool and key construction. Different pools never deduplicate. |
| URL/filter changes but old data continues | Does the key still describe the previous input? | Change the identity key. New source objects alone do not replace active entries. |
| Stream repeatedly reconnects after finishing | Does the producer end successfully? | The default retries EOF. Set `retry: () => false` for a finite stream. |
| No terminal error callback during an outage | Is retry still enabled for this failure? | Observe `onRetry`; `onError` is for terminal failures only. |
| Socket opened but status remains `connecting` | Has a frame arrived? | `onOpen` proves the transport opened, not that fresh application data exists. |
| No stale warning before the first value | Is `hasValue` false? | Staleness ages retained values. Implement an initial deadline separately if required. |
| React value seems frozen | Are values only read with `getLatest()`? | Set state from `onMessage`; the hook snapshot does not include the payload. |
| Old value visible after a key switch/pause | Is the application retaining its own state? | Reset/key the view state, or intentionally show it as stale. See the React guide. |
| Changing `staleAfterMs` has no effect | Does the key's entry already exist? | Its first subscriber chose that window. Use one shared configuration per logical stream. |
| Terminal watch does not restart on remount | Did the remount occur during linger or while another subscriber remains? | Existing terminal entries are reused. Use a new generation key for an explicit restart. |
| TypeScript callback value is `unknown` | Was the pool created without a message type? | Use `createWatchPool<Message>()`; source inference cannot flow backward into an existing pool. |
| `type: "connect"` is rejected | Which factory was imported? | Import `adapter` from `/connect` and install its optional SDK peers. |
| A moved options object no longer type-checks | Did `type` widen to `string`? | Use `satisfies SseAdapterOptions`, or pass an object literal directly. |
| `dispose()` never settles | Does each source stop and settle on abort? | Fix custom transport cleanup. A WebSocket waits for its close event; the pool cannot force another resource to finish. |

## Authentication refresh and identity changes

For SSE, inject a fetch wrapper that reads the current token on each request.
The following wrapper attaches it without replacing the adapter's headers or
abort signal. The supplied callback belongs to this session only.

```ts
import { adapter } from "@heojeongbo/watchpool";

export function authenticatedEvents(url: string, getAccessToken: () => string) {
  const authenticatedFetch: typeof fetch = (input, init) => {
    const headers = new Headers(init?.headers);
    headers.set("Authorization", `Bearer ${getAccessToken()}`);
    return fetch(input, { ...init, headers });
  };
  return adapter({ type: "sse", url, fetch: authenticatedFetch });
}
```

Refreshing a token does not modify headers on an already open HTTP request. It
applies to the next attempt. To force a new authenticated request, restart the
logical stream deliberately. For a different user/tenant, unmount old consumers,
await disposal of the old pool and create a new session pool. Do not switch an
old pool to another user's token while it still owns retained values or retries.

`SseHttpError` carries `status` and response `headers`. Use them to stop on 401/403
or implement server-directed retry. The response body is not retained. The
[README retry recipe](../README.md#http-errors-authentication-and-retry-visibility)
handles numeric Retry-After; HTTP-date values need their own parser. Browser
CORS rules still apply to authorization headers and exposed response headers.

## Retry and callback failures are separate

```ts
import { createWatchPool } from "@heojeongbo/watchpool";
import type { ServerEvent } from "@heojeongbo/watchpool/sse";

export function createObservablePool() {
  return createWatchPool<ServerEvent>({
    onRetry: ({ key, attempt, delayMs, ended, error }) => {
      console.info("Stream will retry", { key, attempt, delayMs, ended, error });
    },
    onCallbackError: (error) => console.error("Observer failed", error),
  });
}
```

Use non-sensitive keys and your application's error redaction when logging.
`onRetry` receives a zero-based attempt before its timer is scheduled. It may be
called for clean EOF with `ended: true` and no error. A callback may dispose the
pool and prevent that retry; a thrown callback error is isolated and reported.

`onError` runs after a source failure becomes terminal. Throwing from `onMessage`
does not fail the upstream or invoke the stream retry policy. This keeps one UI
consumer from interrupting the others. Late subscribers read the terminal status
but do not receive a replay of the original error object.

## Guarantees and boundaries

- Equal keys within a pool share one upstream. A replacement for a closing key
  waits for its prior adapter cleanup to settle.
- Only the latest value is retained. If you need every event, use durable IDs,
  acknowledgements/history or an application accumulator with its own limits.
- Delivery is synchronous and ordered by the source's calls to `emit`. There is
  no asynchronous queue or backpressure scheduler inside the pool.
- An adapter must stop emitting on abort and release owned resources before its
  `run()` promise settles. Pending open operations also need to respect abort.
- Native WebSocket support is receive-only. Use your transport directly for
  commands, or inject a custom implementation with explicit ownership.
- SSE is decoded as `ServerEvent` with string `data`. Parse/validate application
  payloads explicitly. EOF discards an incomplete event; `retry:` fields do not
  override the pool policy.
- Existing tests and 100% coverage verify specified scenarios, not arbitrary
  adapters, browser networks, server buffering or every possible interleaving.

When reporting an issue, include the package/runtime versions, factory import,
redacted key construction, lifecycle/cleanup sequence and a minimal reproduction.
State whether it occurs with a built-in transport or an injected implementation.
