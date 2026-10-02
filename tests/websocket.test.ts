import { afterEach, describe, expect, it, vi } from "vitest";
import { webSocketAdapter } from "../src/websocket.js";

class FakeSocket extends EventTarget {
	static CLOSING = 2;
	static latest: FakeSocket;
	readyState = 0;
	close = vi.fn(() => {
		this.readyState = 2;
	});
	constructor(
		readonly url: string,
		readonly protocols?: string | string[],
	) {
		super();
		FakeSocket.latest = this;
	}
}
afterEach(() => vi.unstubAllGlobals());
function closeSocket(code = 1000): void {
	FakeSocket.latest.readyState = 3;
	const event = new Event("close");
	Object.assign(event, { code });
	FakeSocket.latest.dispatchEvent(event);
}
const sink = () => ({ opened: vi.fn(), emit: vi.fn() });

describe("WebSocket adapter", () => {
	it("opens, decodes messages and aborts without reconnecting itself", async () => {
		vi.stubGlobal("WebSocket", FakeSocket);
		const abort = new AbortController();
		const target = sink();
		const source = webSocketAdapter({
			url: () => "wss://example.test",
			protocols: "json",
			decode: (data) => JSON.parse(String(data)) as number,
		});
		const done = source.run(abort.signal, target);
		const socket = FakeSocket.latest;
		socket.dispatchEvent(new Event("open"));
		socket.dispatchEvent(new MessageEvent("message", { data: "42" }));
		expect(target.opened).toHaveBeenCalledOnce();
		expect(target.emit).toHaveBeenCalledWith(42);
		abort.abort();
		closeSocket();
		await done;
		expect(socket.close).toHaveBeenCalledOnce();
		socket.dispatchEvent(new MessageEvent("message", { data: "43" }));
		expect(target.emit).toHaveBeenCalledOnce();
		await source.run(abort.signal, target);
		expect(FakeSocket.latest).toBe(socket);
	});
	it("rejects decode failures, including throwing undefined", async () => {
		vi.stubGlobal("WebSocket", FakeSocket);
		const done = webSocketAdapter({
			url: "ws://test",
			decode() {
				throw undefined;
			},
		}).run(new AbortController().signal, sink());
		FakeSocket.latest.dispatchEvent(
			new MessageEvent("message", { data: "bad" }),
		);
		closeSocket();
		await expect(done).rejects.toBeUndefined();
	});
	it("reports socket errors", async () => {
		vi.stubGlobal("WebSocket", FakeSocket);
		const done = webSocketAdapter({ url: "ws://test", decode: String }).run(
			new AbortController().signal,
			sink(),
		);
		FakeSocket.latest.dispatchEvent(new Event("error"));
		closeSocket();
		await expect(done).rejects.toThrow("connection failed");
	});
	it.each([1000, 1006])(
		"distinguishes clean close %s from abnormal close",
		async (code) => {
			vi.stubGlobal("WebSocket", FakeSocket);
			const done = webSocketAdapter({ url: "ws://test", decode: String }).run(
				new AbortController().signal,
				sink(),
			);
			FakeSocket.latest.readyState = 3;
			const event = new Event("close");
			Object.assign(event, { code });
			FakeSocket.latest.dispatchEvent(event);
			if (code === 1000) await done;
			else await expect(done).rejects.toThrow("1006");
			expect(FakeSocket.latest.close).not.toHaveBeenCalled();
		},
	);
});

it("does not settle cancellation until the close handshake completes", async () => {
	vi.stubGlobal("WebSocket", FakeSocket);
	const abort = new AbortController();
	let finished = false;
	const done = webSocketAdapter({ url: "ws://test", decode: String })
		.run(abort.signal, sink())
		.then(() => {
			finished = true;
		});
	abort.abort();
	await Promise.resolve();
	expect(finished).toBe(false);
	closeSocket(1006);
	await done;
	expect(finished).toBe(true);
});
