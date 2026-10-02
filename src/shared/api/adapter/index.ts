import type {
	DescMessage,
	DescMethodServerStreaming,
	MessageInitShape,
	MessageShape,
} from "@bufbuild/protobuf";
import type { Transport } from "@connectrpc/connect";
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
export interface ConnectAdapterOptions<
	I extends DescMessage,
	O extends DescMessage,
> {
	type: "connect";
	transport: Transport;
	method: DescMethodServerStreaming<I, O>;
	input: MessageInitShape<I>;
}
export type AdapterOptions<
	T,
	I extends DescMessage = DescMessage,
	O extends DescMessage = DescMessage,
> =
	| SseAdapterOptions
	| WebSocketAdapterOptions<T>
	| IterableAdapterOptions<T>
	| CustomAdapterOptions<T>
	| ConnectAdapterOptions<I, O>;

export function adapter(options: SseAdapterOptions): StreamAdapter<ServerEvent>;
export function adapter<T>(
	options: WebSocketAdapterOptions<T>,
): StreamAdapter<T>;
export function adapter<T>(
	options: IterableAdapterOptions<T>,
): StreamAdapter<T>;
export function adapter<T>(options: CustomAdapterOptions<T>): StreamAdapter<T>;
export function adapter<I extends DescMessage, O extends DescMessage>(
	options: ConnectAdapterOptions<I, O>,
): StreamAdapter<MessageShape<O>>;
export function adapter<
	T,
	I extends DescMessage = DescMessage,
	O extends DescMessage = DescMessage,
>(
	options: AdapterOptions<T, I, O>,
): StreamAdapter<T | ServerEvent | MessageShape<O>>;

/** Select a typed protocol adapter. Lifecycle and pooling remain protocol-neutral. */
export function adapter<T, I extends DescMessage, O extends DescMessage>(
	options: AdapterOptions<T, I, O>,
): StreamAdapter<T | ServerEvent | MessageShape<O>> {
	switch (options.type) {
		case "sse":
			return sseAdapter(options);
		case "websocket":
			return webSocketAdapter(options);
		case "iterable":
			return iterableAdapter(options.open);
		case "custom":
			return options.implementation;
		case "connect":
			return lazyConnect(options);
		default:
			throw new TypeError("Unsupported adapter type");
	}
}

/** The Connect SDK is loaded only when a Connect stream actually starts. */
function lazyConnect<I extends DescMessage, O extends DescMessage>(
	options: ConnectAdapterOptions<I, O>,
): StreamAdapter<MessageShape<O>> {
	const { transport, method } = options;
	const input = structuredClone(options.input);
	let source: StreamAdapter<MessageShape<O>> | undefined;
	return {
		async run(signal, sink): Promise<void> {
			if (signal.aborted) return;
			const { connectAdapter } = await import("../connect/index.js");
			if (signal.aborted) return;
			source ??= connectAdapter(transport, method, input);
			await source.run(signal, sink);
		},
	};
}
