import type { Sink, StreamAdapter } from "../src/index.js";
export function controlled<T>() {
	const connections: {
		signal: AbortSignal;
		sink: Sink<T>;
		end: () => void;
		fail: (error: unknown) => void;
	}[] = [];
	const source: StreamAdapter<T> = {
		run: (signal, sink) =>
			new Promise<void>((resolve, reject) => {
				connections.push({ signal, sink, end: resolve, fail: reject });
				signal.addEventListener("abort", () => resolve(), { once: true });
				sink.opened();
			}),
	};
	return { source, connections };
}
export async function flush(): Promise<void> {
	for (let i = 0; i < 12; i++) await Promise.resolve();
}
