import { create, type Message } from "@bufbuild/protobuf";
import { Code, ConnectError, type Transport } from "@connectrpc/connect";
import { describe, expect, it, vi } from "vitest";
import { connectAdapter, connectKey, connectRetry } from "../src/connect.js";
import { createWatchPool, iterableAdapter } from "../src/index.js";
import { method } from "./connect-fixture.js";
import { flush } from "./helpers.js";

function transportFor(
	messages: AsyncIterable<Message>,
	seen: unknown[] = [],
): Transport {
	return {
		stream: async (
			_method: unknown,
			_signal: unknown,
			_timeout: unknown,
			_headers: unknown,
			input: AsyncIterable<unknown>,
		) => {
			for await (const value of input) seen.push(value);
			return { message: messages };
		},
	} as unknown as Transport;
}

describe("Connect adapter", () => {
	it("serializes input once, exposes a key, and fans out a real protobuf-shaped response", async () => {
		const input = { id: "first" };
		const seen: unknown[] = [];
		const message = create(method.output, { value: 4 });
		async function* messages() {
			yield message;
		}
		const transport = transportFor(messages(), seen);
		const source = connectAdapter(transport, method, input);
		input.id = "changed";
		const sink = { opened: vi.fn(), emit: vi.fn() };
		await source.run(new AbortController().signal, sink);
		expect(seen[0]).toMatchObject({ id: "first" });
		expect(sink.opened).toHaveBeenCalledOnce();
		expect(sink.emit).toHaveBeenCalledWith(message);
		expect(connectKey(method, { id: "one" })).toBe(
			'example.Status/Watch:{"id":"one"}',
		);
	});
	it("does not deliver after abort before open or during iteration", async () => {
		const abort = new AbortController();
		const sink = { opened: vi.fn(), emit: vi.fn() };
		async function* messages() {
			abort.abort();
			yield create(method.output);
		}
		await connectAdapter(transportFor(messages()), method, {}).run(
			abort.signal,
			sink,
		);
		expect(sink.emit).not.toHaveBeenCalled();
		expect(sink.opened).toHaveBeenCalledOnce();
		await connectAdapter(transportFor(messages()), method, {}).run(
			abort.signal,
			sink,
		);
		expect(sink.opened).toHaveBeenCalledOnce();
	});
	it.each([
		Code.InvalidArgument,
		Code.NotFound,
		Code.PermissionDenied,
		Code.Unauthenticated,
		Code.Unimplemented,
	])("terminates permanent Connect error %s", (code) => {
		expect(
			connectRetry({
				attempt: 0,
				elapsedMs: 0,
				ended: false,
				error: new ConnectError("terminal", code),
			}),
		).toBe(false);
	});
	it("retries transient errors and EOF", () => {
		expect(
			connectRetry({
				attempt: 1,
				elapsedMs: 0,
				ended: false,
				error: new ConnectError("offline", Code.Unavailable),
			}),
		).toBe(4_000);
		expect(
			connectRetry({ attempt: 9, elapsedMs: 0, ended: true, error: undefined }),
		).toBe(2_000);
	});
});

describe("protocol-neutral iterable adapter", () => {
	it("supports synchronous iterable factories and closes generators on cancellation", async () => {
		const abort = new AbortController();
		let closed = false;
		const values: number[] = [];
		const source = iterableAdapter(async function* () {
			try {
				yield 1;
				yield 2;
			} finally {
				closed = true;
			}
		});
		await source.run(abort.signal, {
			opened() {},
			emit(value) {
				values.push(value);
				abort.abort();
			},
		});
		expect(values).toEqual([1]);
		expect(closed).toBe(true);
	});
	it("handles cancellation before opening and clean completion", async () => {
		const abort = new AbortController();
		abort.abort();
		const opened = vi.fn();
		const emit = vi.fn();
		const source = iterableAdapter(async function* () {
			yield 1;
		});
		await source.run(abort.signal, { opened, emit });
		expect(opened).not.toHaveBeenCalled();
		await source.run(new AbortController().signal, { opened, emit });
		expect(emit).toHaveBeenCalledWith(1);
	});
	it("shares a custom protocol and propagates a factory failure", async () => {
		const p = createWatchPool<number>({ retry: () => false });
		const onError = vi.fn();
		const source = iterableAdapter<number>(async () => {
			throw new Error("custom transport");
		});
		p.subscribe("sensor", source, { onError });
		p.subscribe("sensor", source);
		await flush();
		expect(p.stats().opens).toBe(1);
		expect(onError).toHaveBeenCalledWith(new Error("custom transport"));
		await p.dispose();
	});
});
