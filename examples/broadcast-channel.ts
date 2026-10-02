import type { StreamAdapter } from "../src/index.js";

/** A fourth transport implemented outside the library: no pool changes or registration. */
export function broadcastChannelAdapter<T>(
	name: string,
	decode: (data: unknown) => T,
): StreamAdapter<T> {
	return {
		run(signal, sink): Promise<void> {
			if (signal.aborted) return Promise.resolve();
			return new Promise<void>((resolve, reject) => {
				const channel = new BroadcastChannel(name);
				function close(): void {
					channel.close();
					signal.removeEventListener("abort", abort);
				}
				function abort(): void {
					close();
					resolve();
				}
				channel.onmessage = (event: MessageEvent<unknown>) => {
					try {
						sink.emit(decode(event.data));
					} catch (error) {
						close();
						reject(error);
					}
				};
				signal.addEventListener("abort", abort, { once: true });
				sink.opened();
			});
		},
	};
}
