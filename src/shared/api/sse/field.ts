export interface EventField {
	name: string;
	value: string;
}

/** Comments are ignored. Only one optional space after the colon is removed. */
export function parseEventField(line: string): EventField | undefined {
	if (line.startsWith(":")) return undefined;
	const colon = line.indexOf(":");
	if (colon === -1) return { name: line, value: "" };
	const rawValue = line.slice(colon + 1);
	return {
		name: line.slice(0, colon),
		value: rawValue.startsWith(" ") ? rawValue.slice(1) : rawValue,
	};
}
