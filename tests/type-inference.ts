import type {
	DescMethodServerStreaming,
	MessageShape,
} from "@bufbuild/protobuf";
import type {
	Int32ValueSchema,
	StringValueSchema,
} from "@bufbuild/protobuf/wkt";
import type { Transport } from "@connectrpc/connect";
import { expectTypeOf } from "vitest";
import { adapter, type ConnectAdapterOptions } from "../src/connect.js";
import {
	adapter as baseAdapter,
	createWatchPool,
	type SseAdapterOptions,
	type StreamAdapter,
	type StreamValue,
} from "../src/index.js";
import { useWatch } from "../src/react.js";
import type { ServerEvent } from "../src/sse.js";

export function useInferenceFixture(
	transport: Transport,
	method: DescMethodServerStreaming<
		typeof StringValueSchema,
		typeof Int32ValueSchema
	>,
	options:
		| ConnectAdapterOptions<typeof StringValueSchema, typeof Int32ValueSchema>
		| SseAdapterOptions,
) {
	const mixed = adapter(options);
	expectTypeOf(mixed).toEqualTypeOf<
		StreamAdapter<MessageShape<typeof Int32ValueSchema> | ServerEvent>
	>();
	const numeric = baseAdapter({
		type: "websocket",
		url: "ws://local",
		decode: Number,
	});
	expectTypeOf<StreamValue<typeof numeric>>().toEqualTypeOf<number>();
	const pool = createWatchPool<StreamValue<typeof numeric>>();
	pool.subscribe("value", numeric, {
		onMessage(value) {
			expectTypeOf(value).toEqualTypeOf<number>();
		},
	});
	useWatch(pool, "value", numeric, {
		onMessage(value) {
			expectTypeOf(value).toEqualTypeOf<number>();
		},
	});
	const strings = baseAdapter({
		type: "iterable",
		open: async function* () {
			yield "text";
		},
	});
	// @ts-expect-error A numeric pool must reject string sources.
	pool.subscribe("wrong", strings);
	// @ts-expect-error A hook must reject a source incompatible with its pool.
	useWatch(pool, "wrong", strings);
	// @ts-expect-error Wrong observer payload must not widen the source or pool.
	pool.subscribe("wrong", numeric, { onMessage: (value: string) => value });
	// @ts-expect-error Wrong Connect input must not widen the method descriptor.
	adapter({ type: "connect", transport, method, input: { value: 123 } });
}

export function checkDifferentConnectMethods(
	options:
		| ConnectAdapterOptions<typeof StringValueSchema, typeof Int32ValueSchema>
		| ConnectAdapterOptions<typeof Int32ValueSchema, typeof StringValueSchema>,
) {
	expectTypeOf(adapter(options)).toEqualTypeOf<
		StreamAdapter<
			| MessageShape<typeof Int32ValueSchema>
			| MessageShape<typeof StringValueSchema>
		>
	>();
}

export function checkUnionPayload(
	source: StreamAdapter<number> | StreamAdapter<string>,
) {
	expectTypeOf<StreamValue<typeof source>>().toEqualTypeOf<number | string>();
}

export function genericConnect<
	I extends import("@bufbuild/protobuf").DescMessage,
	O extends import("@bufbuild/protobuf").DescMessage,
>(
	transport: Transport,
	method: DescMethodServerStreaming<I, O>,
	input: import("@bufbuild/protobuf").MessageInitShape<I>,
): StreamAdapter<MessageShape<O>> {
	return adapter({ type: "connect", transport, method, input });
}

export function checkContextualOptions() {
	const source = baseAdapter({
		type: "iterable",
		async *open(signal) {
			expectTypeOf(signal).toEqualTypeOf<AbortSignal>();
			yield { count: 1, name: "sensor" };
		},
	});
	expectTypeOf<StreamValue<typeof source>>().toEqualTypeOf<{
		count: number;
		name: string;
	}>();
	const socket = baseAdapter({
		type: "websocket",
		url: "ws://local",
		decode(data) {
			expectTypeOf(data).toEqualTypeOf<unknown>();
			return { value: Number(data) };
		},
	});
	expectTypeOf<StreamValue<typeof socket>>().toEqualTypeOf<{ value: number }>();
}
