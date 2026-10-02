import type {
	Observer,
	PoolOptions,
	PoolStats,
	RetryContext,
	StreamAdapter,
	WatchOptions,
	WatchPool,
	WatchState,
} from "./types.js";

export const IDLE_STATE: WatchState = Object.freeze({
	status: "idle",
	hasValue: false,
	stale: false,
});
export const CONNECTING_STATE: WatchState = Object.freeze({
	status: "connecting",
	hasValue: false,
	stale: false,
});

/** Reset only after a stable connection or clean EOF; never reset on each frame. */
export function exponentialRetry({
	attempt,
	elapsedMs,
	ended,
}: RetryContext): number {
	return Math.min(
		2_000 * 2 ** (ended || elapsedMs >= 30_000 ? 0 : attempt),
		30_000,
	);
}

type Entry<T> = {
	state: WatchState;
	latest: T | undefined;
	lastAt: number;
	staleAfterMs: number | undefined;
	observers: Set<Observer<T>>;
	abort: AbortController;
	closeTimer: ReturnType<typeof setTimeout> | undefined;
	retryTimer: ReturnType<typeof setTimeout> | undefined;
	task: Promise<void>;
	stopped: boolean;
	attempt: number;
};

function duration(value: number): number {
	if (!Number.isFinite(value) || value < 0 || value > 2_147_483_647)
		throw new RangeError(
			"Duration must be between 0 and 2147483647 milliseconds",
		);
	return value;
}

/** One pool is one security/transport scope. Equal keys must describe equal streams. */
export function createWatchPool<T>(options: PoolOptions = {}): WatchPool<T> {
	const lingerMs = duration(options.lingerMs ?? 3_000);
	const retry = options.retry ?? exponentialRetry;
	const entries = new Map<string, Entry<T>>();
	const draining = new Map<string, Promise<void>>();
	const tracked = new Set<Entry<T>>();
	const counters = {
		opens: 0,
		messages: 0,
		deliveries: 0,
		retries: 0,
		callbackErrors: 0,
	};
	let stopClock: (() => void) | undefined;
	let disposed = false;

	function call(fn: (() => void) | undefined): void {
		try {
			fn?.();
		} catch (error) {
			counters.callbackErrors++;
			try {
				options.onCallbackError?.(error);
			} catch {
				counters.callbackErrors++;
			}
		}
	}

	function dispatch(
		entry: Entry<T>,
		deliver: (observer: Observer<T>) => void,
	): void {
		for (const observer of [...entry.observers]) {
			if (entry.observers.has(observer)) call(() => deliver(observer));
		}
	}

	function state(entry: Entry<T>, patch: Partial<WatchState>): void {
		const next = { ...entry.state, ...patch };
		if (
			next.status === entry.state.status &&
			next.hasValue === entry.state.hasValue &&
			next.stale === entry.state.stale
		)
			return;
		entry.state = Object.freeze(next);
		dispatch(entry, (observer) => observer.notify?.());
	}

	function tick(): void {
		for (const entry of tracked) {
			if (entry.state.hasValue)
				state(entry, {
					stale: Date.now() - entry.lastAt > (entry.staleAfterMs as number),
				});
		}
	}

	function track(entry: Entry<T>): void {
		if (entry.staleAfterMs === undefined) return;
		tracked.add(entry);
		if (stopClock) return;
		if (options.watchStaleness) stopClock = options.watchStaleness(tick);
		else {
			const timer = setInterval(tick, 1_000);
			stopClock = () => clearInterval(timer);
		}
	}

	function close(key: string, entry: Entry<T>): void {
		entry.stopped = true;
		clearTimeout(entry.closeTimer);
		clearTimeout(entry.retryTimer);
		entries.delete(key);
		tracked.delete(entry);
		if (tracked.size === 0) {
			call(stopClock);
			stopClock = undefined;
		}
		entry.observers.clear();
		entry.latest = undefined;
		draining.set(key, entry.task);
		entry.abort.abort();
		void entry.task.then(() => {
			if (draining.get(key) === entry.task) draining.delete(key);
		});
	}

	async function run(
		key: string,
		entry: Entry<T>,
		source: StreamAdapter<T>,
	): Promise<void> {
		if (entry.stopped) return;
		entry.abort = new AbortController();
		const startedAt = Date.now();
		let ended = true;
		let error: unknown;
		try {
			counters.opens++;
			await source.run(entry.abort.signal, {
				opened: () => {
					if (entry.stopped) return;
					dispatch(entry, (observer) => observer.onOpen?.());
				},
				emit: (value) => {
					if (entry.stopped) return;
					counters.messages++;
					entry.latest = value;
					entry.lastAt = Date.now();
					const recipients = [...entry.observers];
					state(entry, { status: "receiving", hasValue: true, stale: false });
					// Snapshot before notifying: callback-driven joins receive only the replay.
					for (const observer of recipients) {
						if (!entry.observers.has(observer)) continue;
						counters.deliveries++;
						call(() => observer.onMessage?.(value));
					}
				},
			});
		} catch (cause) {
			ended = false;
			error = cause;
		}
		if (entry.stopped) return;
		const elapsedMs = Date.now() - startedAt;
		if (ended || elapsedMs >= 30_000) entry.attempt = 0;
		let delay: number | false;
		try {
			delay = retry({ attempt: entry.attempt, elapsedMs, ended, error });
			if (delay !== false) duration(delay);
		} catch (cause) {
			// Invalid policy code is terminal, never an unhandled background rejection.
			ended = false;
			error = cause;
			delay = false;
		}
		if (delay === false) {
			state(entry, { status: ended ? "ended" : "error" });
			if (!ended) dispatch(entry, (observer) => observer.onError?.(error));
			return;
		}
		const delayMs = delay;
		call(() =>
			options.onRetry?.({
				key,
				attempt: entry.attempt,
				elapsedMs,
				ended,
				error,
				delayMs,
			}),
		);
		if (entry.stopped) return;
		entry.attempt++;
		counters.retries++;
		state(entry, { status: "retrying" });
		// A notification may dispose the pool synchronously.
		if (entry.stopped) return;
		entry.retryTimer = setTimeout(() => {
			entry.task = Promise.resolve().then(() => run(key, entry, source));
		}, delay);
	}

	return {
		subscribe(
			key: string,
			source: StreamAdapter<T>,
			observer: Observer<T> = {},
			watch: WatchOptions = {},
		): () => void {
			if (disposed) throw new Error("Watchpool is disposed");
			if (watch.staleAfterMs !== undefined) duration(watch.staleAfterMs);
			let entry = entries.get(key);
			const fresh = !entry;
			if (!entry) {
				entry = {
					state: CONNECTING_STATE,
					latest: undefined,
					lastAt: 0,
					staleAfterMs: watch.staleAfterMs,
					observers: new Set(),
					abort: new AbortController(),
					closeTimer: undefined,
					retryTimer: undefined,
					task: Promise.resolve(),
					stopped: false,
					attempt: 0,
				};
				try {
					track(entry);
				} catch (error) {
					tracked.delete(entry);
					throw error;
				}
				entries.set(key, entry);
			}
			clearTimeout(entry.closeTimer);
			// Every acquisition owns a distinct subscription, even for the same observer object.
			const subscription = { ...observer };
			entry.observers.add(subscription);
			const target = entry;
			if (fresh) {
				// Defer opening so dispose/unsubscribe can act before source callbacks run.
				entry.task = Promise.resolve(draining.get(key)).then(() =>
					run(key, target, source),
				);
			} else if (entry.state.hasValue)
				call(() => subscription.onMessage?.(target.latest as T));
			let active = true;
			return () => {
				if (!active || target.stopped) return;
				active = false;
				target.observers.delete(subscription);
				if (target.observers.size !== 0) return;
				target.closeTimer = setTimeout(() => close(key, target), lingerMs);
			};
		},
		getSnapshot: (key: string): WatchState =>
			entries.get(key)?.state ?? CONNECTING_STATE,
		getLatest: (key: string): T | undefined => entries.get(key)?.latest,
		stats: (): PoolStats => ({
			...counters,
			keys: entries.size,
			subscribers: [...entries.values()].reduce(
				(n, entry) => n + entry.observers.size,
				0,
			),
		}),
		async dispose(): Promise<void> {
			disposed = true;
			for (const [key, entry] of entries) close(key, entry);
			await Promise.all(draining.values());
		},
	};
}
