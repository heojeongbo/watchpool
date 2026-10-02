interface LineReaderOptions {
	onLine(line: string): void;
	checkLength(length: number): void;
}

/** Split CR, LF and CRLF without rescanning or joining unfinished lines on each chunk. */
export class LineReader {
	private fragments: string[] = [];
	private length = 0;
	private skipLeadingLF = false;
	private readonly options: LineReaderOptions;

	constructor(options: LineReaderOptions) {
		this.options = options;
	}

	write(chunk: string): void {
		if (chunk.length === 0) return;
		let start = this.skipLeadingLF && chunk[0] === "\n" ? 1 : 0;
		this.skipLeadingLF = false;
		const delimiter = /[\r\n]/g;
		delimiter.lastIndex = start;
		let match = delimiter.exec(chunk);
		while (match) {
			this.append(chunk.slice(start, match.index));
			this.finishLine();
			if (chunk[match.index] === "\r") {
				if (chunk[delimiter.lastIndex] === "\n") delimiter.lastIndex++;
				else this.skipLeadingLF = delimiter.lastIndex === chunk.length;
			}
			start = delimiter.lastIndex;
			match = delimiter.exec(chunk);
		}
		this.append(chunk.slice(start));
	}

	private append(fragment: string): void {
		if (fragment.length === 0) return;
		this.options.checkLength(this.length + fragment.length);
		this.fragments.push(fragment);
		this.length += fragment.length;
	}

	private finishLine(): void {
		const line = this.fragments.join("");
		this.fragments = [];
		this.length = 0;
		this.options.onLine(line);
	}
}
