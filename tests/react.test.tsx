// @vitest-environment happy-dom
import { act, cleanup, renderHook } from "@testing-library/react";
import { StrictMode } from "react";
import { renderToString } from "react-dom/server";
import { afterEach, describe, expect, it, vi } from "vitest";
import { createWatchPool } from "../src/index.js";
import { useWatch } from "../src/react.js";
import { controlled, flush } from "./helpers.js";

afterEach(cleanup);

describe("React binding", () => {
	it("shares across StrictMode remounts and updates callbacks without reopening", async () => {
		const pool = createWatchPool<number>();
		const s = controlled<number>();
		const first = vi.fn();
		const second = vi.fn();
		const opened = vi.fn();
		const view = renderHook(
			({ callback }) =>
				useWatch(pool, "a", s.source, { onMessage: callback, onOpen: opened }),
			{ initialProps: { callback: first }, wrapper: StrictMode },
		);
		await act(flush);
		expect(s.connections).toHaveLength(1);
		expect(opened).toHaveBeenCalledOnce();
		act(() => s.connections[0].sink.emit(1));
		expect(first).toHaveBeenCalledWith(1);
		expect(view.result.current.hasValue).toBe(true);
		view.rerender({ callback: second });
		act(() => s.connections[0].sink.emit(2));
		expect(second).toHaveBeenCalledWith(2);
		expect(s.connections).toHaveLength(1);
		view.unmount();
		await pool.dispose();
	});
	it("supports missing callbacks, disabled mode and changing keys", async () => {
		const pool = createWatchPool<number>();
		const s = controlled<number>();
		const view = renderHook(
			({ enabled, key }) => useWatch(pool, key, s.source, { enabled }),
			{ initialProps: { enabled: false, key: "a" } },
		);
		expect(view.result.current.status).toBe("idle");
		expect(s.connections).toHaveLength(0);
		view.rerender({ enabled: true, key: "a" });
		await act(flush);
		act(() => s.connections[0].sink.emit(1));
		view.rerender({ enabled: true, key: "b" });
		await act(flush);
		expect(s.connections).toHaveLength(2);
		view.rerender({ enabled: false, key: "b" });
		expect(view.result.current.status).toBe("idle");
		view.unmount();
		await pool.dispose();
	});
	it("delivers terminal errors and defaults options", async () => {
		const pool = createWatchPool<number>({ retry: () => false });
		const s = controlled<number>();
		const onError = vi.fn();
		const one = renderHook(() => useWatch(pool, "a", s.source, { onError }));
		const two = renderHook(() => useWatch(pool, "a", s.source));
		await act(flush);
		await act(async () => {
			s.connections[0].fail("terminal");
			await flush();
		});
		expect(onError).toHaveBeenCalledWith("terminal");
		expect(one.result.current.status).toBe("error");
		one.unmount();
		two.unmount();
		await pool.dispose();
	});
	it("renders a stable idle server snapshot without opening a connection", async () => {
		const pool = createWatchPool<number>();
		const s = controlled<number>();
		function Server() {
			return <span>{useWatch(pool, "a", s.source).status}</span>;
		}
		expect(renderToString(<Server />)).toContain("idle");
		expect(s.connections).toHaveLength(0);
		await pool.dispose();
	});
});

it("retains the original source while an old key retries during its linger window", async () => {
	vi.useFakeTimers();
	const pool = createWatchPool<number>({ retry: () => 1 });
	const a = controlled<number>();
	const b = controlled<number>();
	const view = renderHook(({ key, source }) => useWatch(pool, key, source), {
		initialProps: { key: "a", source: a.source },
	});
	try {
		await act(flush);
		view.rerender({ key: "b", source: b.source });
		await act(flush);
		await act(async () => {
			a.connections[0].fail("offline");
			await flush();
			await vi.advanceTimersByTimeAsync(1);
		});
		expect(a.connections).toHaveLength(2);
		expect(b.connections).toHaveLength(1);
	} finally {
		view.unmount();
		await pool.dispose();
		vi.useRealTimers();
	}
});
