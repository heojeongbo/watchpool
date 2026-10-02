import type { DescMethodServerStreaming } from "@bufbuild/protobuf";
import type {
	Int32ValueSchema,
	StringValueSchema,
} from "@bufbuild/protobuf/wkt";
import type { Transport } from "@connectrpc/connect";
import { adapter } from "../src/connect.js";

// Compiled by type:check; invalid configurations must remain compile errors.
export function checkInvalidOptions(
	transport: Transport,
	method: DescMethodServerStreaming<
		typeof StringValueSchema,
		typeof Int32ValueSchema
	>,
) {
	// @ts-expect-error SSE requires a URL.
	adapter({ type: "sse" });
	// @ts-expect-error WebSocket requires a decoder.
	adapter({ type: "websocket", url: "ws://localhost" });
	// @ts-expect-error Generated protobuf input rejects a numeric string field.
	adapter({ type: "connect", transport, method, input: { value: 42 } });
	// @ts-expect-error A custom protocol requires its implementation.
	adapter({ type: "custom" });
}

// Options owned by useSyncExternalStore must not leak into the hook's public API.
export const invalidHookOptions: import("../src/react.js").UseWatchOptions<number> =
	{
		// @ts-expect-error notify is managed by React, not a user callback.
		notify() {},
	};
