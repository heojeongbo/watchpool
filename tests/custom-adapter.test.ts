import { describe, expect, it, vi } from "vitest";
import { broadcastChannelAdapter } from "../examples/broadcast-channel.js";
import { createWatchPool, type StreamAdapter } from "../src/index.js";
import { flush } from "./helpers.js";

describe("an external adapter requires no changes to watchpool", () => {
	it("shares a real BroadcastChannel, then releases it", async () => {
		const name = `watchpool-${crypto.randomUUID()}`;
		const sender = new BroadcastChannel(name);
		const pool = createWatchPool<number>();
		const adapter: StreamAdapter<number> = broadcastChannelAdapter(
			name,
			Number,
		);
		const a = vi.fn();
		const b = vi.fn();
		const opened = vi.fn();
		try {
			pool.subscribe(name, adapter, { onMessage: a, onOpen: opened });
			pool.subscribe(name, adapter, { onMessage: b });
			await flush();
			sender.postMessage(42);
			await vi.waitFor(() => expect(a).toHaveBeenCalledWith(42));
			expect(b).toHaveBeenCalledOnce();
			expect(opened).toHaveBeenCalledOnce();
			await pool.dispose();
			sender.postMessage(43);
			await flush();
			expect(a).toHaveBeenCalledOnce();
		} finally {
			sender.close();
			await pool.dispose();
		}
	});
	it("reports decoder errors and handles cancellation before opening", async () => {
		const name = `watchpool-${crypto.randomUUID()}`;
		const sender = new BroadcastChannel(name);
		const pool = createWatchPool({ retry: () => false });
		const onError = vi.fn();
		const adapter = broadcastChannelAdapter(name, () => {
			throw new Error("invalid frame");
		});
		try {
			pool.subscribe(name, adapter, { onError });
			await flush();
			sender.postMessage(0);
			await vi.waitFor(() =>
				expect(onError).toHaveBeenCalledWith(new Error("invalid frame")),
			);
			const abort = new AbortController();
			abort.abort();
			const opened = vi.fn();
			await adapter.run(abort.signal, { opened, emit() {} });
			expect(opened).not.toHaveBeenCalled();
		} finally {
			sender.close();
			await pool.dispose();
		}
	});
});
