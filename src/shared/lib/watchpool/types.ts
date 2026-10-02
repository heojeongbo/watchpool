export type WatchStatus =
	| "idle"
	| "connecting"
	| "receiving"
	| "retrying"
	| "error"
	| "ended";
export interface WatchState {
	readonly status: WatchStatus;
	readonly hasValue: boolean;
	readonly stale: boolean;
}
export interface Observer<T> {
	onMessage?: (value: T) => void;
	onOpen?: () => void;
	onError?: (error: unknown) => void;
	notify?: () => void;
}
export interface Sink<T> {
	opened(): void;
	emit(value: T): void;
}
/** Resolve on clean EOF, reject on failure. Abort must release resources and settle. */
export interface StreamAdapter<T> {
	/** Own one connection until it ends; abort must release it before this promise settles. */
	run(signal: AbortSignal, sink: Sink<T>): Promise<void>;
}
export interface RetryContext {
	readonly attempt: number;
	readonly elapsedMs: number;
	readonly ended: boolean;
	readonly error: unknown;
}
export type RetryPolicy = (context: RetryContext) => number | false;
export interface PoolOptions {
	lingerMs?: number;
	retry?: RetryPolicy;
	/** Called for an isolated callback failure. Reporter failures are isolated too. */
	onCallbackError?: (error: unknown) => void;
	/** Optional worker-backed clock. Return a function that stops ticking. */
	watchStaleness?: (tick: () => void) => () => void;
}
export interface WatchOptions {
	/** First subscriber sets this for the key's lifetime. Silence is not stale by default. */
	staleAfterMs?: number;
}
export interface PoolStats {
	keys: number;
	subscribers: number;
	opens: number;
	messages: number;
	deliveries: number;
	retries: number;
	callbackErrors: number;
}
export interface WatchPool<T> {
	subscribe(
		key: string,
		source: StreamAdapter<T>,
		observer?: Observer<T>,
		options?: WatchOptions,
	): () => void;
	getSnapshot(key: string): WatchState;
	getLatest(key: string): T | undefined;
	stats(): PoolStats;
	/** Stop all entries, reject new subscriptions, await upstream cleanup. Idempotent. */
	dispose(): Promise<void>;
}
