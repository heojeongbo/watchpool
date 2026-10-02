import { parseEventField } from "./field.js";

export interface ServerEvent {
	readonly data: string;
	readonly event: string;
	readonly id: string;
}
export interface EventBuilderOptions {
	emit(event: ServerEvent): void;
	rememberId(id: string): void;
	maxEventChars: number;
	initialId: string;
}

/** Assemble one event. The resume ID survives event boundaries; data and event name do not. */
export class EventBuilder {
	private readonly options: EventBuilderOptions;
	private data: string[] = [];
	private size = 0;
	private eventName = "message";
	private lastId: string;

	constructor(options: EventBuilderOptions) {
		this.options = options;
		this.lastId = options.initialId;
	}

	checkPendingLine(length: number): void {
		if (this.size + length > this.options.maxEventChars) {
			throw new RangeError("SSE event exceeds maxEventChars");
		}
	}

	acceptLine(line: string): void {
		if (line === "") {
			this.dispatch();
			return;
		}
		const field = parseEventField(line);
		if (!field) return;
		this.checkPendingLine(line.length);
		this.size += line.length;
		switch (field.name) {
			case "data":
				this.data.push(field.value);
				break;
			case "event":
				this.eventName = field.value || "message";
				break;
			case "id":
				if (!field.value.includes("\0")) this.lastId = field.value;
				break;
		}
	}

	private dispatch(): void {
		this.options.rememberId(this.lastId);
		if (this.data.length > 0) {
			this.options.emit({
				data: this.data.join("\n"),
				event: this.eventName,
				id: this.lastId,
			});
		}
		this.data = [];
		this.size = 0;
		this.eventName = "message";
	}
}
