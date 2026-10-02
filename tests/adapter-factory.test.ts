import {
	create,
	type DescMethodServerStreaming,
	type MessageShape,
} from "@bufbuild/protobuf";
import type {
	Int32ValueSchema,
	StringValueSchema,
} from "@bufbuild/protobuf/wkt";
import type { Transport } from "@connectrpc/connect";
import { expect, expectTypeOf, it, vi } from "vitest";
import { adapter } from "../src/connect.js";
import type { StreamAdapter } from "../src/index.js";
import type { ServerEvent } from "../src/sse.js";
import { method } from "./connect-fixture.js";

it("infers protocol payloads and accepts custom implementations unchanged", async () => {
	const source = adapter({
		type: "iterable",
		open: async function* () {
			yield 42;
		},
	});
	expectTypeOf(source).toEqualTypeOf<StreamAdapter<number>>();
	const custom = adapter({ type: "custom", implementation: source });
	expectTypeOf(custom).toEqualTypeOf<StreamAdapter<number>>();
	expect(custom).toBe(source);
	const sink = { opened: vi.fn(), emit: vi.fn() };
	await custom.run(new AbortController().signal, sink);
	expect(sink.emit).toHaveBeenCalledWith(42);
	expectTypeOf(adapter({ type: "sse", url: "/events" })).toEqualTypeOf<
		StreamAdapter<ServerEvent>
	>();
	expectTypeOf(
		adapter({ type: "websocket", url: "ws://localhost", decode: () => 1 }),
	).toEqualTypeOf<StreamAdapter<number>>();
	const typedMethod = method as DescMethodServerStreaming<
		typeof StringValueSchema,
		typeof Int32ValueSchema
	>;
	expectTypeOf(
		adapter({
			type: "connect",
			transport: {} as Transport,
			method: typedMethod,
			input: { value: "hello" },
		}),
	).toEqualTypeOf<StreamAdapter<MessageShape<typeof Int32ValueSchema>>>();
});

it("rejects unknown protocols passed from JavaScript", () => {
	// @ts-expect-error Unknown protocols must provide a custom implementation.
	expect(() => adapter({ type: "mqtt" })).toThrow("Unsupported adapter type");
});

it("snapshots Connect input, reuses the source, and does not open an already cancelled stream", async () => {
	const seen: unknown[] = [];
	const stream = vi.fn(
		async (
			_method,
			_signal,
			_timeout,
			_headers,
			inputs: AsyncIterable<unknown>,
		) => {
			for await (const value of inputs) seen.push(value);
			return {
				message: (async function* () {
					yield create(method.output, { value: 7 });
				})(),
			};
		},
	);
	const input = { id: "original" };
	const source = adapter({
		type: "connect",
		transport: { stream } as unknown as Transport,
		method,
		input,
	});
	input.id = "changed";
	const sink = { opened: vi.fn(), emit: vi.fn() };
	const before = new AbortController();
	before.abort();
	await source.run(before.signal, sink);
	expect(stream).not.toHaveBeenCalled();
	await source.run(new AbortController().signal, sink);
	await source.run(new AbortController().signal, sink);
	expect(seen).toHaveLength(2);
	expect(seen).toEqual([
		expect.objectContaining({ id: "original" }),
		expect.objectContaining({ id: "original" }),
	]);
	expect(sink.emit).toHaveBeenCalledTimes(2);
});
