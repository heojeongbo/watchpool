import { afterEach, expect, expectTypeOf, it, vi } from "vitest";
import { adapter as connectFactory } from "../src/connect.js";
import {
	adapter,
	createWatchPool,
	type IterableAdapterOptions,
	type Observer,
	type SseAdapterOptions,
	type StreamAdapter,
} from "../src/index.js";
import { type ServerEvent, SseHttpError } from "../src/sse.js";
import { controlled, flush } from "./helpers.js";

afterEach(() => vi.useRealTimers());

it.each(["notify", "onOpen", "onError"] as const)(
	"a screen may resubscribe inside %s without repeated delivery",
	async (callback) => {
		const source = controlled<number>();
		const pool = createWatchPool<number>({ retry: () => false });
		let calls = 0;
		let leave: () => void;
		const observer: Observer<number> = {
			[callback]: () => {
				calls++;
				// Bounded so a regression fails instead of hanging the test worker.
				if (calls < 5) {
					leave();
					leave = pool.subscribe("sensor", source.source, observer);
				}
			},
		};
		leave = pool.subscribe("sensor", source.source, observer);
		try {
			await flush();
			if (callback === "notify") source.connections[0].sink.emit(1);
			if (callback === "onError") {
				source.connections[0].fail(new Error("offline"));
				await flush();
			}
			expect(calls).toBe(1);
			expect(pool.stats().subscribers).toBe(1);
		} finally {
			await pool.dispose();
		}
	},
);

it("a screen removed by another observer receives no pending state notification", async () => {
	const source = controlled<number>();
	const pool = createWatchPool<number>();
	const removed = vi.fn();
	let remove: () => void;
	pool.subscribe("sensor", source.source, { notify: () => remove() });
	remove = pool.subscribe("sensor", source.source, { notify: removed });
	try {
		await flush();
		source.connections[0].sink.emit(1);
		expect(removed).not.toHaveBeenCalled();
	} finally {
		await pool.dispose();
	}
});

it("a protocol selected at runtime retains only its possible message types", async () => {
	const iterable: IterableAdapterOptions<number> = {
		type: "iterable",
		open: async function* () {
			yield 42;
		},
	};
	const sse: SseAdapterOptions = {
		type: "sse",
		url: "/events",
		fetch: async () =>
			new Response("data: ready\n\n", {
				headers: { "content-type": "text/event-stream" },
			}),
	};
	function select(options: SseAdapterOptions | IterableAdapterOptions<number>) {
		const source = adapter(options);
		expectTypeOf(source).toEqualTypeOf<StreamAdapter<ServerEvent | number>>();
		expectTypeOf(connectFactory(options)).toEqualTypeOf<
			StreamAdapter<ServerEvent | number>
		>();
		return source;
	}
	const emit = vi.fn();
	for (const options of [iterable, sse])
		await select(options).run(new AbortController().signal, {
			opened() {},
			emit,
		});
	expect(emit.mock.calls.map(([value]) => value)).toEqual([
		42,
		{ data: "ready", id: "", event: "message" },
	]);
});

it("an injected auth transport exposes a 401 without retrying or leaking its response body", async () => {
	const cancelled = vi.fn();
	const fetcher = vi.fn<typeof fetch>(async (_url, init) => {
		expect(new Headers(init?.headers).get("authorization")).toBe(
			"Bearer token",
		);
		return new Response(new ReadableStream({ cancel: cancelled }), {
			status: 401,
			headers: { "www-authenticate": "Bearer" },
		});
	});
	const retry = vi.fn(({ error }) =>
		error instanceof SseHttpError && error.status === 401
			? (false as const)
			: 10,
	);
	const onError = vi.fn();
	const pool = createWatchPool<ServerEvent>({ retry });
	pool.subscribe(
		"private",
		adapter({
			type: "sse",
			url: "/private",
			headers: { authorization: "Bearer token" },
			fetch: fetcher,
		}),
		{ onError },
	);
	try {
		await vi.waitFor(() => expect(onError).toHaveBeenCalledOnce());
		const error = onError.mock.calls[0][0];
		expect(error).toBeInstanceOf(SseHttpError);
		expect(error.status).toBe(401);
		expect(error.headers.get("www-authenticate")).toBe("Bearer");
		expect(cancelled).toHaveBeenCalledOnce();
		expect(fetcher).toHaveBeenCalledOnce();
		expect(pool.getSnapshot("private").status).toBe("error");
	} finally {
		await pool.dispose();
	}
});

it("a rate-limited dashboard observes Retry-After and then receives recovery data", async () => {
	vi.useFakeTimers();
	const fetcher = vi
		.fn<typeof fetch>()
		.mockResolvedValueOnce(
			new Response("slow down", {
				status: 429,
				headers: { "retry-after": "2" },
			}),
		)
		.mockResolvedValueOnce(
			new Response("data: recovered\n\n", {
				headers: { "content-type": "text/event-stream" },
			}),
		);
	const onRetry = vi.fn();
	const onError = vi.fn();
	const onMessage = vi.fn();
	const pool = createWatchPool<ServerEvent>({
		retry: ({ error }) =>
			error instanceof SseHttpError
				? Number(error.headers.get("retry-after")) * 1000
				: false,
		onRetry,
	});
	pool.subscribe(
		"dashboard",
		adapter({ type: "sse", url: "/events", fetch: fetcher }),
		{ onError, onMessage },
	);
	try {
		await vi.advanceTimersByTimeAsync(0);
		expect(onRetry).toHaveBeenCalledWith(
			expect.objectContaining({
				key: "dashboard",
				attempt: 0,
				delayMs: 2000,
				ended: false,
				error: expect.any(SseHttpError),
			}),
		);
		expect(onError).not.toHaveBeenCalled();
		await vi.advanceTimersByTimeAsync(1999);
		expect(fetcher).toHaveBeenCalledTimes(1);
		await vi.advanceTimersByTimeAsync(1);
		expect(fetcher).toHaveBeenCalledTimes(2);
		expect(onMessage).toHaveBeenCalledWith({
			data: "recovered",
			id: "",
			event: "message",
		});
		expect(pool.getSnapshot("dashboard").status).toBe("ended");
	} finally {
		await pool.dispose();
	}
});

it("a failing retry observer is isolated and logout from a retry observer stops reopening", async () => {
	vi.useFakeTimers();
	const source = controlled<number>();
	const reporter = vi.fn();
	const pool = createWatchPool<number>({
		retry: () => 1,
		onCallbackError: reporter,
		onRetry: () => {
			throw new Error("analytics unavailable");
		},
	});
	pool.subscribe("sensor", source.source);
	await flush();
	source.connections[0].end();
	await flush();
	expect(reporter).toHaveBeenCalledWith(new Error("analytics unavailable"));
	await vi.advanceTimersByTimeAsync(1);
	expect(source.connections).toHaveLength(2);
	await pool.dispose();
	const other = controlled<number>();
	let disposed: Promise<void> | undefined;
	const logoutPool = createWatchPool<number>({
		retry: () => 1,
		onRetry: () => {
			disposed = logoutPool.dispose();
		},
	});
	logoutPool.subscribe("sensor", other.source);
	await flush();
	other.connections[0].end();
	await flush();
	await disposed;
	await vi.advanceTimersByTimeAsync(10);
	expect(other.connections).toHaveLength(1);
	expect(logoutPool.stats().keys).toBe(0);
});

it.each([true, false])(
	"cancellation during async open closes an acquired iterator (return method: %s)",
	async (hasReturn) => {
		const abort = new AbortController();
		const close = vi.fn(async () => ({
			done: true as const,
			value: undefined,
		}));
		const next = vi.fn(async () => ({ done: false as const, value: 1 }));
		const source = adapter({
			type: "iterable",
			open: async () => {
				abort.abort();
				return {
					[Symbol.asyncIterator]: () => ({
						next,
						...(hasReturn ? { return: close } : {}),
					}),
				};
			},
		});
		const opened = vi.fn();
		await source.run(abort.signal, { opened, emit: vi.fn() });
		expect(opened).not.toHaveBeenCalled();
		expect(next).not.toHaveBeenCalled();
		expect(close).toHaveBeenCalledTimes(hasReturn ? 1 : 0);
	},
);
