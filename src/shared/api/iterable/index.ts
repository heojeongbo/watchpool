import type { StreamAdapter } from "../../lib/watchpool/index.js";

/** Any abortable AsyncIterable becomes a protocol adapter without changing the pool. */
export function iterableAdapter<T>(
	open: (signal: AbortSignal) => AsyncIterable<T> | Promise<AsyncIterable<T>>,
): StreamAdapter<T> {
	return {
		async run(signal, sink): Promise<void> {
			if (signal.aborted) return;
			const iterable = await open(signal);
			if (signal.aborted) {
				await iterable[Symbol.asyncIterator]().return?.();
				return;
			}
			sink.opened();
			for await (const value of iterable) {
				if (signal.aborted) return;
				sink.emit(value);
			}
		},
	};
}
