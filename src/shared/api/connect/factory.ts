import type {
	DescMessage,
	DescMethodServerStreaming,
	MessageInitShape,
	MessageShape,
} from "@bufbuild/protobuf";
import type { Transport } from "@connectrpc/connect";
import type { StreamAdapter } from "../../lib/watchpool/index.js";
import {
	type AdapterOptions,
	type AdapterValue,
	adapter as baseAdapter,
} from "../adapter/index.js";
import { connectAdapter } from "./index.js";

export interface ConnectAdapterOptions<
	I extends DescMessage,
	O extends DescMessage,
> {
	type: "connect";
	transport: Transport;
	method: DescMethodServerStreaming<I, O>;
	input: MessageInitShape<I>;
}
export function adapter<I extends DescMessage, O extends DescMessage>(
	options: ConnectAdapterOptions<I, O>,
): StreamAdapter<MessageShape<O>>;
export function adapter<A extends AdapterOptions>(
	options: A,
): StreamAdapter<AdapterValue<A>>;
export function adapter<
	I extends DescMessage,
	O extends DescMessage,
	A extends AdapterOptions,
>(
	options: ConnectAdapterOptions<I, O> | A,
): StreamAdapter<MessageShape<O> | AdapterValue<A>>;
/** Connect opt-in entrypoint: retains the same typed factory API without core SDK dependencies. */
export function adapter(
	options: ConnectAdapterOptions<DescMessage, DescMessage> | AdapterOptions,
): StreamAdapter<unknown> {
	if (options.type === "connect")
		return connectAdapter(options.transport, options.method, options.input);
	return baseAdapter(options);
}
