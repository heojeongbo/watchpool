import type { SseOptions } from "./types.js";

/** Validate the HTTP response before handing ownership of its body to the reader. */
export async function openEventBody(
	options: SseOptions,
	lastId: string,
	signal: AbortSignal,
): Promise<ReadableStream<Uint8Array>> {
	const headers = new Headers(options.headers);
	headers.set("Accept", "text/event-stream");
	headers.delete("Last-Event-ID");
	if (lastId) headers.set("Last-Event-ID", lastId);
	const url = typeof options.url === "function" ? options.url() : options.url;
	const response = await fetch(url, {
		headers,
		credentials: options.credentials,
		signal,
	});
	if (!response.ok) {
		await response.body?.cancel();
		throw new Error(`SSE HTTP ${response.status}`);
	}
	if (!response.body) throw new Error("SSE response has no body");
	const contentType = response.headers
		.get("content-type")
		?.split(";")[0]
		.trim()
		.toLowerCase();
	if (contentType !== "text/event-stream") {
		await response.body.cancel();
		throw new Error("Expected text/event-stream");
	}
	return response.body;
}

interface BodyReaderOptions {
	signal: AbortSignal;
	opened(): void;
	write(chunk: string): void;
}

/** Own the reader, incremental UTF-8 decoder and cleanup for one response body. */
export async function readEventBody(
	body: ReadableStream<Uint8Array>,
	options: BodyReaderOptions,
): Promise<void> {
	const reader = body.getReader();
	const decoder = new TextDecoder();
	try {
		if (options.signal.aborted) return;
		options.opened();
		while (!options.signal.aborted) {
			const { value, done } = await reader.read();
			if (done) {
				options.write(decoder.decode());
				return;
			}
			options.write(decoder.decode(value, { stream: true }));
		}
	} finally {
		try {
			await reader.cancel();
		} finally {
			reader.releaseLock();
		}
	}
}
