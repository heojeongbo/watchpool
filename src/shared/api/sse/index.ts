import type { StreamAdapter } from "../../lib/watchpool/index.js";
import { createEventParser, type ServerEvent } from "./parser.js";
import { openEventBody, readEventBody } from "./transport.js";
import type { SseOptions } from "./types.js";

export type { ServerEvent } from "./parser.js";
export type { SseOptions } from "./types.js";

/** One logical SSE stream, including its resume cursor across connection attempts. */
export function sseAdapter(options: SseOptions): StreamAdapter<ServerEvent> {
	const maxEventChars = options.maxEventChars ?? 1_048_576;
	if (!Number.isSafeInteger(maxEventChars) || maxEventChars <= 0) {
		throw new RangeError("maxEventChars must be a positive integer");
	}
	let lastId = new Headers(options.headers).get("Last-Event-ID") ?? "";
	return {
		async run(signal, sink): Promise<void> {
			const body = await openEventBody(options, lastId, signal);
			const parser = createEventParser({
				maxEventChars,
				initialId: lastId,
				rememberId: (id) => {
					lastId = id;
				},
				emit: (event) => {
					if (!signal.aborted) sink.emit(event);
				},
			});
			await readEventBody(body, {
				signal,
				opened: () => sink.opened(),
				write: (chunk) => parser.write(chunk),
			});
		},
	};
}
