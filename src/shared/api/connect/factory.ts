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
type FactoryOptions =
	| ConnectAdapterOptions<DescMessage, DescMessage>
	| AdapterOptions;
type CheckedOptions<A> = A extends {
	type: "connect";
	method: DescMethodServerStreaming<infer I, infer O>;
}
	? ConnectAdapterOptions<I, O>
	: A;
export type ConnectAdapterValue<A> = A extends {
	type: "connect";
	method: DescMethodServerStreaming<DescMessage, infer O>;
}
	? MessageShape<O>
	: AdapterValue<A>;

/** Infer each selected method independently; input validation cannot widen its schema. */
export function adapter<A extends FactoryOptions>(
	options: A & NoInfer<CheckedOptions<A>>,
): StreamAdapter<ConnectAdapterValue<A>>;
/** Connect opt-in entrypoint: retains the same typed factory API without core SDK dependencies. */
export function adapter(
	options: ConnectAdapterOptions<DescMessage, DescMessage> | AdapterOptions,
): StreamAdapter<unknown> {
	if (options.type === "connect")
		return connectAdapter(options.transport, options.method, options.input);
	return baseAdapter(options);
}
