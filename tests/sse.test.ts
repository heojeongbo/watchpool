import { afterEach, describe, expect, it, vi } from "vitest";
import {
	createEventParser,
	type ServerEvent,
} from "../src/shared/api/sse/parser.js";
import { sseAdapter } from "../src/sse.js";

afterEach(() => vi.unstubAllGlobals());
function response(
	chunks: string[],
	headers: HeadersInit = { "content-type": "text/event-stream; charset=utf-8" },
) {
	return new Response(
		new ReadableStream({
			start(controller) {
				for (const chunk of chunks)
					controller.enqueue(new TextEncoder().encode(chunk));
				controller.close();
			},
		}),
		{ headers },
	);
}
const sink = () => ({ opened: vi.fn(), emit: vi.fn() });
describe("SSE framing", () => {
	it("handles split CRLF, comments, multiline data, names, ids and empty fields", () => {
		const events: ServerEvent[] = [];
		const ids: string[] = [];
		const parse = createEventParser({
			emit: (event) => events.push(event),
			rememberId: (id) => ids.push(id),
			maxEventChars: 1024,
			initialId: "old",
		});
		parse.write(": comment\r");
		parse.write("\ndata: first\r\ndata:second\nevent: update\nid: 7\n\n");
		parse.write(
			"id: invalid\0id\nevent:\ndata\nunknown: ignored\nretry: 500\n\n",
		);
		parse.write("id\r\r");
		expect(events).toEqual([
			{ data: "first\nsecond", event: "update", id: "7" },
			{ data: "", event: "message", id: "7" },
		]);
		expect(ids).toEqual(["7", "7", ""]);
	});
	it("limits partial lines and accumulated event fields", () => {
		const parse = createEventParser({
			emit: () => {},
			rememberId: () => {},
			maxEventChars: 10,
			initialId: "",
		});
		expect(() => parse.write("data: very long\n")).toThrow(RangeError);
		expect(() =>
			createEventParser({
				emit: () => {},
				rememberId: () => {},
				maxEventChars: 2,
				initialId: "",
			}).write("abc"),
		).toThrow(RangeError);
	});
});
describe("fetch SSE adapter", () => {
	it("dispatches frames and resumes with Last-Event-ID on a fresh connection", async () => {
		const fetcher = vi
			.fn()
			.mockResolvedValueOnce(
				response(["id: 9\ndata: hello\n\n", "data: incomplete"]),
			)
			.mockResolvedValueOnce(response(["data: again\r\r"]));
		vi.stubGlobal("fetch", fetcher);
		const source = sseAdapter({
			url: () => "https://example.test/events",
			headers: { Authorization: "test" },
		});
		const target = sink();
		await source.run(new AbortController().signal, target);
		await source.run(new AbortController().signal, target);
		expect(target.emit.mock.calls.map(([event]) => event)).toEqual([
			{ data: "hello", id: "9", event: "message" },
			{ data: "again", id: "9", event: "message" },
		]);
		const headers = fetcher.mock.calls[1][1].headers as Headers;
		expect(headers.get("Last-Event-ID")).toBe("9");
		expect(headers.get("Accept")).toBe("text/event-stream");
	});
	it("decodes UTF-8 codepoints split across network chunks", async () => {
		const bytes = new TextEncoder().encode("data: 한글\n\n");
		vi.stubGlobal(
			"fetch",
			vi.fn().mockResolvedValue(
				new Response(
					new ReadableStream({
						start(controller) {
							for (const byte of bytes)
								controller.enqueue(new Uint8Array([byte]));
							controller.close();
						},
					}),
					{ headers: { "content-type": "text/event-stream" } },
				),
			),
		);
		const target = sink();
		await sseAdapter({ url: "/events" }).run(
			new AbortController().signal,
			target,
		);
		expect(target.emit).toHaveBeenCalledWith({
			data: "한글",
			event: "message",
			id: "",
		});
	});
	it("rejects bad status, missing body, missing/wrong content type and invalid size", async () => {
		expect(() => sseAdapter({ url: "/", maxEventChars: 0 })).toThrow(
			RangeError,
		);
		for (const res of [
			new Response(null, { status: 403 }),
			new Response("denied", { status: 403 }),
			new Response(null),
			response([], {}),
			response([], { "content-type": "application/json" }),
		]) {
			vi.stubGlobal("fetch", vi.fn().mockResolvedValue(res));
			await expect(
				sseAdapter({ url: "/" }).run(new AbortController().signal, sink()),
			).rejects.toThrow();
		}
	});
	it("cancels on abort before open or while emitting a chunk", async () => {
		const abort = new AbortController();
		abort.abort();
		vi.stubGlobal(
			"fetch",
			vi.fn().mockResolvedValue(response(["data: late\n\n"])),
		);
		const target = sink();
		await sseAdapter({ url: "/" }).run(abort.signal, target);
		expect(target.opened).not.toHaveBeenCalled();
		const active = new AbortController();
		vi.stubGlobal(
			"fetch",
			vi.fn().mockResolvedValue(response(["data: one\n\ndata: two\n\n"])),
		);
		target.emit.mockImplementation(() => active.abort());
		await sseAdapter({ url: "/" }).run(active.signal, target);
		expect(target.emit).toHaveBeenCalledTimes(1);
		const opening = new AbortController();
		target.opened.mockImplementation(() => opening.abort());
		vi.stubGlobal("fetch", vi.fn().mockResolvedValue(response([])));
		await sseAdapter({ url: "/" }).run(opening.signal, target);
	});
});
it("clears a caller-provided Last-Event-ID when the server resets the cursor", async () => {
	const fetcher = vi
		.fn()
		.mockResolvedValueOnce(response(["id:\ndata: reset\n\n"]))
		.mockResolvedValueOnce(response([]));
	vi.stubGlobal("fetch", fetcher);
	const source = sseAdapter({
		url: "/",
		headers: { "Last-Event-ID": "initial" },
	});
	await source.run(new AbortController().signal, sink());
	await source.run(new AbortController().signal, sink());
	expect(fetcher.mock.calls[0][1].headers.get("Last-Event-ID")).toBe("initial");
	expect(fetcher.mock.calls[1][1].headers.has("Last-Event-ID")).toBe(false);
});
it("parses the same events at every possible network chunk boundary", () => {
	const wire =
		"id: 1\r\ndata: first\r\ndata: second\r\n\r\nevent: changed\ndata: third\n\n";
	for (let split = 0; split <= wire.length; split++) {
		const events: ServerEvent[] = [];
		const parse = createEventParser({
			emit: (event) => events.push(event),
			rememberId: () => {},
			maxEventChars: 1024,
			initialId: "",
		});
		parse.write(wire.slice(0, split));
		parse.write(wire.slice(split));
		expect(events).toEqual([
			{ data: "first\nsecond", event: "message", id: "1" },
			{ data: "third", event: "changed", id: "1" },
		]);
	}
});
