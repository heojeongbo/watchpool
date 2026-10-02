import type { StreamAdapter } from "../../lib/watchpool/index.js";

export interface WebSocketOptions<T> {
	url: string | (() => string);
	protocols?: string | string[];
	/** Decode text, Blob or ArrayBuffer according to the application's wire format. */
	decode(data: unknown): T;
}

/** One socket per execution. Settle after close so a replacement never overlaps cleanup. */
export function webSocketAdapter<T>(
	options: WebSocketOptions<T>,
): StreamAdapter<T> {
	return {
		run: (signal, sink) => {
			if (signal.aborted) return Promise.resolve();
			return new Promise<void>((resolve, reject) => {
				const socket = new WebSocket(
					typeof options.url === "function" ? options.url() : options.url,
					options.protocols,
				);
				let failure: { error: unknown } | undefined;
				function stop(error?: { error: unknown }): void {
					failure = error;
					socket.removeEventListener("open", opened);
					socket.removeEventListener("message", message);
					socket.removeEventListener("error", failedSocket);
					signal.removeEventListener("abort", aborted);
					if (socket.readyState < WebSocket.CLOSING) socket.close();
					// Keep the close listener: close() starts a handshake, it does not finish it.
				}
				function opened(): void {
					sink.opened();
				}
				function message(event: MessageEvent<unknown>): void {
					try {
						sink.emit(options.decode(event.data));
					} catch (error) {
						stop({ error });
					}
				}
				function failedSocket(): void {
					stop({ error: new Error("WebSocket connection failed") });
				}
				function closed(event: CloseEvent): void {
					const terminal = failure;
					stop();
					socket.removeEventListener("close", closed);
					if (terminal) reject(terminal.error);
					else if (!signal.aborted && event.code !== 1000)
						reject(new Error(`WebSocket closed (${event.code})`));
					else resolve();
				}
				function aborted(): void {
					stop();
				}
				socket.addEventListener("open", opened);
				socket.addEventListener("message", message);
				socket.addEventListener("error", failedSocket);
				socket.addEventListener("close", closed);
				signal.addEventListener("abort", aborted, { once: true });
			});
		},
	};
}
