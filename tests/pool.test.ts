import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
	CONNECTING_STATE,
	createWatchPool,
	exponentialRetry,
	IDLE_STATE,
	type StreamAdapter,
	type WatchPool,
} from "../src/index.js";
import { controlled, flush } from "./helpers.js";

const pools: WatchPool<unknown>[] = [];
function pool(
	options: Parameters<typeof createWatchPool>[0] = {},
): WatchPool<unknown> {
	const value = createWatchPool(options);
	pools.push(value);
	return value;
}
beforeEach(() => {
	vi.useFakeTimers();
	vi.setSystemTime(0);
});
afterEach(async () => {
	await Promise.all(pools.splice(0).map((p) => p.dispose()));
	vi.useRealTimers();
});
describe("ownership and fanout", () => {
	it("shares by key, isolates pools, retains undefined and stable snapshots", async () => {
		const p = pool();
		const q = pool();
		const s = controlled();
		const one = vi.fn();
		const two = vi.fn();
		const notify = vi.fn();
		expect(p.getSnapshot("a")).toBe(CONNECTING_STATE);
		expect(p.getLatest("a")).toBeUndefined();
		expect(IDLE_STATE.status).toBe("idle");
		p.subscribe("a", s.source, { onMessage: one, notify });
		p.subscribe("a", s.source, { onMessage: two });
		p.subscribe("b", s.source);
		q.subscribe("a", s.source);
		await flush();
		expect(s.connections).toHaveLength(3);
		s.connections[0].sink.emit(undefined);
		const snapshot = p.getSnapshot("a");
		s.connections[0].sink.emit(2);
		expect(p.getSnapshot("a")).toBe(snapshot);
		expect(snapshot.hasValue).toBe(true);
		expect(notify).toHaveBeenCalledTimes(1);
		expect(one.mock.calls).toEqual([[undefined], [2]]);
		expect(two).toHaveBeenCalledTimes(2);
		const late = vi.fn();
		const open = vi.fn();
		p.subscribe("a", s.source, { onMessage: late, onOpen: open });
		expect(late).toHaveBeenCalledWith(2);
		expect(open).not.toHaveBeenCalled();
		expect(p.stats()).toMatchObject({
			keys: 2,
			subscribers: 4,
			messages: 2,
			opens: 2,
			deliveries: 4,
		});
		expect(p.getLatest("a")).toBe(2);
	});
	it("replays undefined as a real latest value", async () => {
		const p = pool();
		const s = controlled();
		p.subscribe("a", s.source);
		await flush();
		s.connections[0].sink.emit(undefined);
		const latest = vi.fn();
		p.subscribe("a", s.source, { onMessage: latest });
		expect(latest).toHaveBeenCalledWith(undefined);
	});
	it("counts identical observer acquisitions separately and closes after linger", async () => {
		const p = pool({ lingerMs: 10 });
		const s = controlled();
		const observer = { onMessage: vi.fn() };
		const a = p.subscribe("a", s.source, observer);
		const b = p.subscribe("a", s.source, observer);
		await flush();
		a();
		a();
		await vi.advanceTimersByTimeAsync(20);
		expect(s.connections[0].signal.aborted).toBe(false);
		b();
		await vi.advanceTimersByTimeAsync(9);
		const c = p.subscribe("a", s.source);
		await vi.advanceTimersByTimeAsync(20);
		expect(s.connections).toHaveLength(1);
		c();
		await vi.advanceTimersByTimeAsync(10);
		expect(s.connections[0].signal.aborted).toBe(true);
		expect(p.stats().keys).toBe(0);
		c();
	});
	it("does not open if disposed before the opening microtask", async () => {
		const p = pool();
		const s = controlled();
		const off = p.subscribe("a", s.source);
		await p.dispose();
		off();
		expect(s.connections).toHaveLength(0);
		expect(() => p.subscribe("a", s.source)).toThrow("disposed");
		await p.dispose();
	});
	it("waits for old cleanup before reopening the same key", async () => {
		const p = pool({ lingerMs: 0 });
		let finish = () => {};
		const source = vi.fn<StreamAdapter<unknown>["run"]>(
			() =>
				new Promise<void>((resolve) => {
					finish = resolve;
				}),
		);
		const off = p.subscribe("a", { run: source });
		await flush();
		off();
		await vi.advanceTimersByTimeAsync(0);
		const next = controlled();
		p.subscribe("a", next.source);
		await flush();
		expect(next.connections).toHaveLength(0);
		finish();
		await flush();
		expect(next.connections).toHaveLength(1);
	});
	it("handles closing a queued reopen while an earlier connection drains", async () => {
		const p = pool({ lingerMs: 0 });
		let finish = () => {};
		const off = p.subscribe("a", {
			run: () =>
				new Promise<void>((resolve) => {
					finish = resolve;
				}),
		});
		await flush();
		off();
		await vi.advanceTimersByTimeAsync(0);
		const next = controlled();
		const offNext = p.subscribe("a", next.source);
		await flush();
		offNext();
		await vi.advanceTimersByTimeAsync(0);
		finish();
		await flush();
		expect(next.connections).toHaveLength(0);
	});
	it("ignores late adapter callbacks after disposal", async () => {
		const p = pool();
		const s = controlled();
		const value = vi.fn();
		const opened = vi.fn();
		p.subscribe("a", s.source, { onMessage: value, onOpen: opened });
		await flush();
		await p.dispose();
		s.connections[0].sink.emit(1);
		s.connections[0].sink.opened();
		expect(value).not.toHaveBeenCalled();
		expect(opened).toHaveBeenCalledTimes(1);
	});
	it("isolates callback errors and supports removing another observer during delivery", async () => {
		const report = vi.fn();
		const p = pool({ onCallbackError: report, lingerMs: 0 });
		const s = controlled();
		const bad = () => {
			throw new Error("consumer");
		};
		p.subscribe("a", s.source, { onOpen: bad, notify: bad, onMessage: bad });
		let remove = () => {};
		p.subscribe("a", s.source, { onMessage: () => remove() });
		const later = vi.fn();
		remove = p.subscribe("a", s.source, { onMessage: later });
		await flush();
		s.connections[0].sink.emit(1);
		expect(later).not.toHaveBeenCalled();
		expect(report).toHaveBeenCalledTimes(3);
		expect(p.stats().callbackErrors).toBe(3);
		const quiet = pool();
		const other = controlled();
		quiet.subscribe("a", other.source, { onMessage: bad });
		await flush();
		other.connections[0].sink.emit(1);
	});
});
describe("retry and staleness", () => {
	it("retries transient failures with capped exponential backoff and resets on stable life or EOF", async () => {
		const p = pool();
		const s = controlled();
		const opened = vi.fn();
		p.subscribe("a", s.source, { onOpen: opened });
		await flush();
		s.connections[0].fail("offline");
		await flush();
		expect(p.getSnapshot("a").status).toBe("retrying");
		await vi.advanceTimersByTimeAsync(2000);
		s.connections[1].fail("offline");
		await flush();
		await vi.advanceTimersByTimeAsync(3999);
		expect(s.connections).toHaveLength(2);
		await vi.advanceTimersByTimeAsync(1);
		expect(s.connections).toHaveLength(3);
		await vi.advanceTimersByTimeAsync(30000);
		s.connections[2].fail("offline");
		await flush();
		await vi.advanceTimersByTimeAsync(2000);
		expect(s.connections).toHaveLength(4);
		s.connections[3].end();
		await flush();
		await vi.advanceTimersByTimeAsync(2000);
		expect(opened).toHaveBeenCalledTimes(5);
		expect(
			exponentialRetry({ attempt: 99, elapsedMs: 0, ended: false, error: 0 }),
		).toBe(30000);
		expect(
			exponentialRetry({
				attempt: 9,
				elapsedMs: 30000,
				ended: false,
				error: 0,
			}),
		).toBe(2000);
	});
	it("stops on terminal failure or a finite source, without retrying", async () => {
		const p = pool({ retry: () => false });
		const s = controlled();
		const err = vi.fn();
		p.subscribe("a", s.source, { onError: err });
		p.subscribe("a", s.source);
		p.subscribe("b", s.source);
		await flush();
		s.connections[0].fail("denied");
		s.connections[1].end();
		await flush();
		expect(err).toHaveBeenCalledWith("denied");
		expect(p.getSnapshot("a").status).toBe("error");
		expect(p.getSnapshot("b").status).toBe("ended");
	});
	it("cancels retry timers when the last subscriber leaves", async () => {
		const p = pool({ lingerMs: 0 });
		const s = controlled();
		const off = p.subscribe("a", s.source);
		await flush();
		s.connections[0].fail("offline");
		await flush();
		off();
		await vi.advanceTimersByTimeAsync(60000);
		expect(s.connections).toHaveLength(1);
	});
	it("allows a state observer to dispose during a retry transition", async () => {
		const p = pool();
		const s = controlled();
		p.subscribe("a", s.source, {
			notify: () => {
				void p.dispose();
			},
		});
		await flush();
		s.connections[0].fail("offline");
		await flush();
		await vi.advanceTimersByTimeAsync(60000);
		expect(s.connections).toHaveLength(1);
	});
	it("uses one clock and stays stale across failures; epoch-zero timestamps are valid", async () => {
		let tick = () => {};
		const stop = vi.fn();
		const watch = vi.fn((cb: () => void) => {
			tick = cb;
			return stop;
		});
		const p = pool({ watchStaleness: watch, lingerMs: 0 });
		const s = controlled();
		const a = p.subscribe("a", s.source, {}, { staleAfterMs: 1000 });
		const b = p.subscribe("b", s.source, {}, { staleAfterMs: 3000 });
		await flush();
		tick();
		expect(p.getSnapshot("a").stale).toBe(false);
		expect(watch).toHaveBeenCalledTimes(1);
		s.connections[0].sink.emit(1);
		s.connections[0].fail("offline");
		await flush();
		vi.setSystemTime(1001);
		tick();
		expect(p.getSnapshot("a").stale).toBe(true);
		a();
		await vi.advanceTimersByTimeAsync(0);
		expect(stop).not.toHaveBeenCalled();
		b();
		await vi.advanceTimersByTimeAsync(0);
		expect(stop).toHaveBeenCalledTimes(1);
	});
	it("provides a default clock, refreshes on frames and keeps silence unclassified", async () => {
		const p = pool();
		const s = controlled();
		p.subscribe("a", s.source, {}, { staleAfterMs: 500 });
		p.subscribe("b", s.source);
		await flush();
		s.connections[0].sink.emit(1);
		await vi.advanceTimersByTimeAsync(1000);
		expect(p.getSnapshot("a").stale).toBe(true);
		s.connections[0].sink.emit(2);
		expect(p.getSnapshot("a").stale).toBe(false);
		expect(p.getSnapshot("b").stale).toBe(false);
	});
	it("rejects invalid duration configuration", () => {
		expect(() => createWatchPool({ lingerMs: -1 })).toThrow(RangeError);
		expect(() =>
			pool().subscribe(
				"a",
				controlled().source,
				{},
				{ staleAfterMs: Number.NaN },
			),
		).toThrow(RangeError);
	});
});
it("turns an invalid or throwing retry policy into a terminal error", async () => {
	for (const retry of [
		() => -1,
		() => {
			throw new Error("policy");
		},
	]) {
		const p = pool({ retry });
		const s = controlled();
		const onError = vi.fn();
		p.subscribe("a", s.source, { onError });
		await flush();
		s.connections[0].end();
		await flush();
		expect(p.getSnapshot("a").status).toBe("error");
		expect(onError).toHaveBeenCalledOnce();
	}
});
it("delivers only once to a subscriber joining from a state notification", async () => {
	const p = pool();
	const s = controlled();
	const joined = vi.fn();
	p.subscribe("a", s.source, {
		notify: () => {
			p.subscribe("a", s.source, { onMessage: joined });
		},
	});
	await flush();
	s.connections[0].sink.emit(1);
	expect(joined).toHaveBeenCalledExactlyOnceWith(1);
});
it("isolates a throwing error reporter and rolls back failed clock setup", async () => {
	const p = pool({
		onCallbackError() {
			throw new Error("reporter");
		},
	});
	const s = controlled();
	p.subscribe("a", s.source, {
		onMessage() {
			throw new Error("observer");
		},
	});
	await flush();
	s.connections[0].sink.emit(1);
	expect(p.stats().callbackErrors).toBe(2);
	const q = pool({
		watchStaleness() {
			throw new Error("clock");
		},
	});
	expect(() => q.subscribe("a", s.source, {}, { staleAfterMs: 1 })).toThrow(
		"clock",
	);
	expect(q.stats().keys).toBe(0);
	expect(() => createWatchPool({ lingerMs: 2 ** 31 })).toThrow(RangeError);
});
it("waits for cleanup when a retry's onOpen synchronously disposes the pool", async () => {
	const p = pool({ retry: () => 1 });
	let opened = 0;
	let finish = () => {};
	let disposal: Promise<void> | undefined;
	const source: StreamAdapter<unknown> = {
		run: async (_signal, sink) => {
			opened++;
			if (opened === 1) return;
			const cleanup = new Promise<void>((resolve) => {
				finish = resolve;
			});
			sink.opened();
			await cleanup;
		},
	};
	p.subscribe("a", source, {
		onOpen() {
			disposal = p.dispose();
		},
	});
	await flush();
	await vi.advanceTimersByTimeAsync(1);
	let disposed = false;
	void disposal?.then(() => {
		disposed = true;
	});
	await flush();
	expect(disposed).toBe(false);
	finish();
	await disposal;
	expect(disposed).toBe(true);
});
it("cleans every timer and subscription after multi-key churn", async () => {
	const p = pool({ lingerMs: 1 });
	const s = controlled();
	for (let i = 0; i < 100; i++) {
		p.subscribe(String(i), s.source, {}, { staleAfterMs: 100 })();
	}
	await flush();
	await vi.advanceTimersByTimeAsync(1);
	expect(p.stats()).toMatchObject({ keys: 0, subscribers: 0 });
	expect(vi.getTimerCount()).toBe(0);
	expect(s.connections.every((connection) => connection.signal.aborted)).toBe(
		true,
	);
});
it("finishes cleanup even when a custom clock's stop callback throws", async () => {
	const errors = vi.fn();
	const p = pool({
		watchStaleness: () => () => {
			throw new Error("clock stop");
		},
		onCallbackError: errors,
	});
	const s = controlled();
	p.subscribe("a", s.source, {}, { staleAfterMs: 10 });
	await flush();
	await p.dispose();
	expect(errors).toHaveBeenCalledWith(new Error("clock stop"));
	expect(s.connections[0].signal.aborted).toBe(true);
});
