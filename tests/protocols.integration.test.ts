import { once } from "node:events";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { describe, expect, it, vi } from "vitest";
import { WebSocketServer } from "ws";
import { adapter, createWatchPool } from "../src/index.js";
import { type ServerEvent, SseHttpError } from "../src/sse.js";

describe("real loopback protocols", () => {
	it("reconnects with the exact UTF-8 bytes of a Unicode SSE cursor", async () => {
		const id = "이벤트-café-☕";
		const headers: (string | undefined)[] = [];
		const server = createServer((request, response) => {
			headers.push(request.headers["last-event-id"] as string | undefined);
			response.writeHead(200, { "Content-Type": "text/event-stream" });
			response.end(
				headers.length === 1
					? `id: ${id}\ndata: first\n\n`
					: "data: second\n\n",
			);
		});
		server.listen(0, "127.0.0.1");
		await once(server, "listening");
		const source = adapter({
			type: "sse",
			url: `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
		});
		const emit = vi.fn();
		try {
			for (let attempt = 0; attempt < 2; attempt++)
				await source.run(new AbortController().signal, { opened() {}, emit });
			expect(headers).toEqual([
				undefined,
				Buffer.from(id, "utf8").toString("latin1"),
			]);
			expect(emit).toHaveBeenLastCalledWith({
				id,
				event: "message",
				data: "second",
			});
		} finally {
			server.closeAllConnections();
			await new Promise<void>((resolve) => server.close(() => resolve()));
		}
	});
	it("retains HTTP 401 metadata after a native fetch response body fails", async () => {
		const server = createServer((_request, response) => {
			response.writeHead(401, {
				"Content-Type": "text/plain",
				"Content-Length": "1000",
				"WWW-Authenticate": "Bearer",
			});
			response.write("unauthorized");
		});
		server.listen(0, "127.0.0.1");
		await once(server, "listening");
		const source = adapter({
			type: "sse",
			url: `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
			fetch: async (...args) => {
				const response = await fetch(...args);
				if (!response.body) throw new Error("Expected a streaming HTTP body");
				const reader = response.body.getReader();
				server.closeAllConnections();
				try {
					while (!(await reader.read()).done) {
						/* Drain until the truncated body rejects. */
					}
				} catch {
					// Return the original response with its body now errored and unlocked.
				} finally {
					reader.releaseLock();
				}
				return response;
			},
		});
		try {
			const done = source.run(new AbortController().signal, {
				opened() {},
				emit() {},
			});
			await expect(done).rejects.toBeInstanceOf(SseHttpError);
			await expect(done).rejects.toMatchObject({ status: 401 });
			await done.catch((error: SseHttpError) =>
				expect(error.headers.get("www-authenticate")).toBe("Bearer"),
			);
		} finally {
			server.closeAllConnections();
			await new Promise<void>((resolve) => server.close(() => resolve()));
		}
	});
	it("shares one HTTP SSE connection, delivers split events and closes the request", async () => {
		let requests = 0;
		let closed = 0;
		const server = createServer((_request, response) => {
			requests++;
			response.writeHead(200, { "Content-Type": "text/event-stream" });
			response.write("id: 1\ndata: hel");
			response.write("lo\n\n");
			response.on("close", () => {
				closed++;
			});
		});
		server.listen(0, "127.0.0.1");
		await once(server, "listening");
		const url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
		const pool = createWatchPool<ServerEvent>();
		const a = vi.fn();
		const b = vi.fn();
		const source = adapter({ type: "sse", url });
		try {
			pool.subscribe("status", source, { onMessage: a });
			pool.subscribe("status", source, { onMessage: b });
			await vi.waitFor(() =>
				expect(a).toHaveBeenCalledWith({
					data: "hello",
					id: "1",
					event: "message",
				}),
			);
			expect(b).toHaveBeenCalledOnce();
			expect(requests).toBe(1);
			await pool.dispose();
			await vi.waitFor(() => expect(closed).toBe(1));
		} finally {
			await pool.dispose();
			server.closeAllConnections();
			await new Promise<void>((resolve) => server.close(() => resolve()));
		}
	});
	it("shares one WebSocket and releases it on pool disposal", async () => {
		const server = new WebSocketServer({ host: "127.0.0.1", port: 0 });
		await once(server, "listening");
		let connections = 0;
		let closed = 0;
		server.on("connection", (socket) => {
			connections++;
			socket.send('{"value":42}');
			socket.on("close", () => {
				closed++;
			});
		});
		const pool = createWatchPool<{ value: number }>();
		const source = adapter({
			type: "websocket",
			url: `ws://127.0.0.1:${(server.address() as AddressInfo).port}`,
			decode: (data) => JSON.parse(String(data)) as { value: number },
		});
		const a = vi.fn();
		const b = vi.fn();
		try {
			pool.subscribe("status", source, { onMessage: a });
			pool.subscribe("status", source, { onMessage: b });
			await vi.waitFor(() => expect(a).toHaveBeenCalledWith({ value: 42 }));
			expect(b).toHaveBeenCalledOnce();
			expect(connections).toBe(1);
			await pool.dispose();
			await vi.waitFor(() => expect(closed).toBe(1));
		} finally {
			await pool.dispose();
			for (const socket of server.clients) socket.terminate();
			await new Promise<void>((resolve) => server.close(() => resolve()));
		}
	});
});

it("shares a real Connect RPC stream and retains it when one observer leaves", async () => {
	const { createConnectTransport } = await import("@connectrpc/connect-web");
	const {
		adapter: connectFactory,
		connectKey,
		connectRetry,
	} = await import("../src/connect.js");
	const { method } = await import("./connect-fixture.js");
	let requests = 0;
	let closed = 0;
	let send = (_value: number) => {};
	const server = createServer((request, response) => {
		requests++;
		request.resume();
		request.on("end", () => {
			response.writeHead(200, { "Content-Type": "application/connect+json" });
			send = (value) => {
				const payload = Buffer.from(JSON.stringify({ value }));
				const header = Buffer.alloc(5);
				header.writeUInt32BE(payload.length, 1);
				response.write(header);
				response.write(payload);
			};
			send(42);
		});
		response.on("close", () => {
			closed++;
		});
	});
	server.listen(0, "127.0.0.1");
	await once(server, "listening");
	const transport = createConnectTransport({
		baseUrl: `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
		useBinaryFormat: false,
	});
	const pool = createWatchPool({ retry: connectRetry });
	const source = connectFactory({
		type: "connect",
		transport,
		method,
		input: { id: "one" },
	});
	const key = connectKey(method, { id: "one" });
	const a = vi.fn();
	const b = vi.fn();
	try {
		const leave = pool.subscribe(key, source, { onMessage: a });
		pool.subscribe(key, source, { onMessage: b });
		await vi.waitFor(() =>
			expect(a).toHaveBeenCalledWith(expect.objectContaining({ value: 42 })),
		);
		expect(b).toHaveBeenCalledOnce();
		expect(requests).toBe(1);
		leave();
		send(43);
		await vi.waitFor(() => expect(b).toHaveBeenCalledTimes(2));
		expect(a).toHaveBeenCalledOnce();
		await pool.dispose();
		await vi.waitFor(() => expect(closed).toBe(1));
	} finally {
		await pool.dispose();
		server.closeAllConnections();
		await new Promise<void>((resolve) => server.close(() => resolve()));
	}
});
