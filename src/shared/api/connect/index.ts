import {
	create,
	type DescMessage,
	type DescMethodServerStreaming,
	fromJsonString,
	type MessageInitShape,
	type MessageShape,
	toJsonString,
} from "@bufbuild/protobuf";
import { Code, ConnectError, type Transport } from "@connectrpc/connect";
import {
	exponentialRetry,
	type RetryContext,
	type StreamAdapter,
} from "../../lib/watchpool/index.js";
import { iterableAdapter } from "../iterable/index.js";

const permanent = new Set([
	Code.InvalidArgument,
	Code.NotFound,
	Code.PermissionDenied,
	Code.Unauthenticated,
	Code.Unimplemented,
]);
export function connectRetry(context: RetryContext): number | false {
	if (!context.ended && permanent.has(ConnectError.from(context.error).code))
		return false;
	return exponentialRetry(context);
}

export function connectKey<I extends DescMessage, O extends DescMessage>(
	method: DescMethodServerStreaming<I, O>,
	input: MessageInitShape<I>,
): string {
	return `${method.parent.typeName}/${method.name}:${toJsonString(method.input, create(method.input, input))}`;
}

/** Scope the owning pool per transport/authentication identity. Input is captured by value. */
export function connectAdapter<I extends DescMessage, O extends DescMessage>(
	transport: Transport,
	method: DescMethodServerStreaming<I, O>,
	input: MessageInitShape<I>,
): StreamAdapter<MessageShape<O>> {
	const json = toJsonString(method.input, create(method.input, input));
	return iterableAdapter(async (signal) => {
		const response = await transport.stream(
			method,
			signal,
			undefined,
			undefined,
			once(fromJsonString(method.input, json)),
		);
		return response.message;
	});
}

async function* once<T>(value: T): AsyncIterable<T> {
	yield value;
}
