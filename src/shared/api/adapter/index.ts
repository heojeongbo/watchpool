import type { StreamAdapter } from "../../lib/watchpool/index.js";
import { iterableAdapter } from "../iterable/index.js";
import { type ServerEvent, type SseOptions, sseAdapter } from "../sse/index.js";
import { type WebSocketOptions, webSocketAdapter } from "../websocket/index.js";

export type SseAdapterOptions = SseOptions & { type: "sse" };
export type WebSocketAdapterOptions<T> = WebSocketOptions<T> & {
	type: "websocket";
};
export interface IterableAdapterOptions<T> {
	type: "iterable";
	open(signal: AbortSignal): AsyncIterable<T> | Promise<AsyncIterable<T>>;
}
export interface CustomAdapterOptions<T> {
	type: "custom";
	implementation: StreamAdapter<T>;
}
export type AdapterOptions<T = unknown> =
	| SseAdapterOptions
	| WebSocketAdapterOptions<T>
	| IterableAdapterOptions<T>
	| CustomAdapterOptions<T>;
/** Distributes over option unions, retaining only the selected protocols' payloads. */
export type AdapterValue<O> = O extends SseAdapterOptions
	? ServerEvent
	: O extends WebSocketAdapterOptions<infer T>
		? T
		: O extends IterableAdapterOptions<infer T>
			? T
			: O extends CustomAdapterOptions<infer T>
				? T
				: never;

export function adapter<O extends AdapterOptions>(
	options: O,
): StreamAdapter<AdapterValue<O>>;
/** Protocol selection runs once; the pool depends only on StreamAdapter. */
export function adapter(options: AdapterOptions): StreamAdapter<unknown> {
	switch (options.type) {
		case "sse":
			return sseAdapter(options);
		case "websocket":
			return webSocketAdapter(options);
		case "iterable":
			return iterableAdapter(options.open);
		case "custom":
			return options.implementation;
		default:
			throw new TypeError("Unsupported adapter type");
	}
}
