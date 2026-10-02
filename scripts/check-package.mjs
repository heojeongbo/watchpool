import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { existsSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

// Exercise a consumer outside this repository, so devDependencies cannot mask
// accidental SDK imports in the base entrypoint or its declaration graph.
const directory = mkdtempSync(join(tmpdir(), "watchpool-consumer-"));
try {
	const packed = JSON.parse(
		execFileSync("npm", ["pack", "--json", "--pack-destination", directory], {
			encoding: "utf8",
		}),
	);
	writeFileSync(
		join(directory, "package.json"),
		JSON.stringify({ private: true, type: "module" }),
	);
	execFileSync(
		"npm",
		[
			"install",
			"--ignore-scripts",
			"--no-audit",
			"--no-fund",
			join(directory, packed[0].filename),
		],
		{ cwd: directory, stdio: "pipe" },
	);
	for (const name of ["@connectrpc/connect", "@bufbuild/protobuf", "react"]) {
		assert.equal(
			existsSync(join(directory, "node_modules", name)),
			false,
			`${name} must remain optional`,
		);
	}
	writeFileSync(
		join(directory, "consumer.ts"),
		`
import { adapter, createWatchPool, type StreamAdapter } from "@heojeongbo/watchpool";
import { SseHttpError, type ServerEvent } from "@heojeongbo/watchpool/sse";
const stream: StreamAdapter<ServerEvent> = adapter({ type: "sse", url: "/events", fetch: async () => new Response("data: ready\\n\\n", { headers: { "content-type": "text/event-stream" } }) });
let received = "";
await stream.run(new AbortController().signal, { opened() {}, emit(event) { received = event.data; } });
if (received !== "ready") throw new Error("SSE consumer did not receive data");
const error = new SseHttpError(401, {});
if (error.status !== 401) throw new Error("HTTP metadata unavailable");
await createWatchPool<ServerEvent>().dispose();
`,
	);
	execFileSync(
		process.execPath,
		[
			resolve("node_modules/typescript/bin/tsc"),
			"--strict",
			"--skipLibCheck",
			"false",
			"--module",
			"NodeNext",
			"--target",
			"ES2022",
			"consumer.ts",
		],
		{ cwd: directory, stdio: "inherit" },
	);
	execFileSync(process.execPath, ["consumer.js"], {
		cwd: directory,
		stdio: "inherit",
	});
	console.log(
		"Packed consumer passed: strict declarations and SSE runtime without Connect, Protobuf or React.",
	);
} finally {
	rmSync(directory, { recursive: true, force: true });
}
