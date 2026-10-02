# Render live values in React

`useWatch()` returns connection/liveness state: `status`, `hasValue` and `stale`.
It does not return the payload or rerender a component for every frame. Use
`onMessage` with React state for values that should render. Use a ref or external
store for high-frequency data whose every frame should not cause a React render.

## Share the pool, own the view state

The application's session owner creates `createWatchPool<ServerEvent>()` once,
passes it to the views and disposes it when that session ends. These components
only acquire and release their own subscriptions.

```tsx
import { useMemo, useState } from "react";
import { adapter, type WatchPool } from "@heojeongbo/watchpool";
import { useWatch } from "@heojeongbo/watchpool/react";
import type { ServerEvent } from "@heojeongbo/watchpool/sse";

interface Props {
  pool: WatchPool<ServerEvent>;
  sessionId: string;
  robotId: string;
  enabled?: boolean;
}

export function RobotStatus({ enabled = true, ...props }: Props) {
  if (!enabled) return <p>Live updates paused</p>;
  return <StatusView key={JSON.stringify([props.sessionId, props.robotId])} {...props} />;
}

function StatusView({ pool, robotId }: Props) {
  const source = useMemo(() => adapter({
    type: "sse",
    url: `/robots/${encodeURIComponent(robotId)}/events`,
  }), [robotId]);
  const streamKey = JSON.stringify(["sse", "robot-status", robotId]);
  const [text, setText] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const state = useWatch(pool, streamKey, source, {
    onMessage: (event) => setText(event.data),
    onError: () => setError("Live updates stopped"),
    staleAfterMs: 5_000,
  });

  return <section>
    <p role="status">{state.stale ? "Last update is stale" : state.status}</p>
    <p>{text ?? "Waiting for the first update"}</p>
    {state.status === "error" && <p role="alert">{error ?? "Live updates stopped"}</p>}
  </section>;
}
```

Changing `robotId` remounts `StatusView`, clearing its old text immediately. The
new key subscribes to the new stream. If another component already watches it,
its retained value is replayed. Otherwise the view displays its loading state.
`sessionId` is an application session generation, not a token; changing it also
resets view state. Change the pool when the authenticated identity changes.

Hiding the example unmounts the child and clears displayed data. With the hook's
`enabled: false` option instead, status becomes `idle` but application-owned
`useState` values are not cleared automatically. Choose that approach when
retaining the last display while paused is intentional.

Unsubscribing does not necessarily stop the upstream immediately: other consumers
may still use it, and the linger window applies. Dispose the pool only from its
owner, never from an individual status card.

## Rendering and callback behavior

- Recreating callbacks does not reopen an equal key. Memoizing callbacks is not
  required for connection stability.
- Memoizing the source avoids repeated adapter construction. The key still
  determines sharing; changing only the source does not replace an existing
  connection or its retry source.
- `hasValue` changes only when the first value arrives; `status` and `stale`
  change independently. Reading `getLatest()` during render does not subscribe
  React to every value change.
- `onOpen` is for a new upstream attempt, not for each subscriber mounting.
  A late subscriber can receive a value without receiving `onOpen`.
- A terminal error callback is not replayed to a late subscriber. Read
  `state.status === "error"` for a reliable fallback error UI, as above.
- Observer callbacks are synchronous. Expensive parsing and rendering affect
  throughput. Validate once in a decoder where possible and aggregate updates
  before setting UI state when appropriate.

## StrictMode, SSR and cleanup

Keep the pool outside the component effects that only own a subscription.
StrictMode may run setup/cleanup again. `useWatch()` unsubscribes idempotently;
the default linger window allows a brief remount to reuse an existing stream.
A disposed pool is terminal, so an effect that disposes and then reuses the same
pool is invalid.

The hook's server snapshot is `idle` and opens no connection. Use a per-request
server scope if a pool is supplied while rendering on the server, and a separate
browser session scope after hydration. Do not let a server module singleton
retain one user's values for another request. This guide does not prescribe a
framework-specific provider or routing integration.

For React-only projects, install React; Connect SDKs are not required. For
connection policy and key selection, see [getting started](getting-started.md).
