import { once } from "node:events";
import { createServer } from "node:http";
import type { AddressInfo } from "node:net";
import { describe, expect, it, vi } from "vitest";
import { WebSocketServer } from "ws";
import { adapter, createWatchPool } from "../src/index.js";
import type { ServerEvent } from "../src/sse.js";

describe("real loopback protocols", () => {
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
	const { connectKey, connectRetry } = await import("../src/connect.js");
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
	const source = adapter({
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
