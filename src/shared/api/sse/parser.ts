import { EventBuilder, type EventBuilderOptions } from "./event-builder.js";
import { LineReader } from "./line-reader.js";

export type { ServerEvent } from "./event-builder.js";

/** Wire framing and event assembly together; unfinished events are discarded at EOF. */
export function createEventParser(options: EventBuilderOptions): LineReader {
	const events = new EventBuilder(options);
	return new LineReader({
		onLine: (line) => events.acceptLine(line),
		checkLength: (length) => events.checkPendingLine(length),
	});
}
