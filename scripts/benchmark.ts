import assert from "node:assert/strict";
import { readdirSync, readFileSync, writeFileSync } from "node:fs";
import { cpus, platform, release } from "node:os";
import { performance } from "node:perf_hooks";
import { gzipSync } from "node:zlib";
import {
	createWatchPool,
	type Sink,
	type StreamAdapter,
} from "../src/index.js";
import { createEventParser } from "../src/shared/api/sse/parser.js";

const results: {
	scenario: string;
	subscribers: number;
	medianNs: number;
	p95Ns: number;
	operationsPerSecond: number;
}[] = [];
function measure(
	scenario: string,
	subscribers: number,
	action: () => void,
): void {
	for (let i = 0; i < 1000; i++) action();
	const samples: number[] = [];
	for (let sample = 0; sample < 9; sample++) {
		const start = performance.now();
		for (let i = 0; i < 10000; i++) action();
		samples.push(((performance.now() - start) * 1e6) / 10000);
	}
	samples.sort((a, b) => a - b);
	const medianNs = samples[4];
	results.push({
		scenario,
		subscribers,
		medianNs,
		p95Ns: samples[8],
		operationsPerSecond: 1e9 / medianNs,
	});
}
for (const subscribers of [1, 10, 100, 1000]) {
	const pool = createWatchPool<number>();
	let sink: Sink<number>;
	let delivered = 0;
	let notifies = 0;
	const source: StreamAdapter<number> = {
		run: (signal, target) =>
			new Promise<void>((resolve) => {
				sink = target;
				target.opened();
				signal.addEventListener("abort", () => resolve(), { once: true });
			}),
	};
	for (let i = 0; i < subscribers; i++)
		pool.subscribe("shared", source, {
			onMessage: () => {
				delivered++;
			},
			notify: () => {
				notifies++;
			},
		});
	await Promise.resolve();
	await Promise.resolve();
	measure("fanout", subscribers, () => sink.emit(42));
	assert.equal(pool.stats().opens, 1, "fanout must share exactly one upstream");
	assert.equal(delivered, pool.stats().messages * subscribers);
	assert.equal(
		notifies,
		subscribers,
		"steady frames must not churn state notifications",
	);
	measure("latest", subscribers, () => {
		assert.equal(pool.getLatest("shared"), 42);
	});
	measure("join-leave", subscribers, () => {
		pool.subscribe("shared", source)();
	});
	assert.equal(
		pool.stats().subscribers,
		subscribers,
		"churn leaked subscribers",
	);
	await pool.dispose();
	assert.equal(pool.stats().keys, 0);
}
for (const payloadSize of [64, 4096]) {
	let delivered = 0;
	const parse = createEventParser({
		emit: () => {
			delivered++;
		},
		rememberId: () => {},
		maxEventChars: 8192,
		initialId: "",
	});
	const event = `data: ${"x".repeat(payloadSize)}\n\n`;
	measure(`sse-frame-${payloadSize}B`, 1, () => parse.write(event));
	assert.equal(delivered, 91000);
}
const js = readdirSync("dist", { recursive: true, encoding: "utf8" }).filter(
	(path) => path.endsWith(".js"),
);
const gzipBytes = gzipSync(
	Buffer.concat(js.map((path) => readFileSync(`dist/${path}`))),
).byteLength;
if (process.argv.includes("--check"))
	assert.ok(gzipBytes <= 20480, `runtime gzip budget exceeded: ${gzipBytes}`);
const report = {
	environment: {
		node: process.version,
		platform: platform(),
		release: release(),
		cpu: cpus()[0].model,
	},
	methodology:
		"1000 warmup operations; 9 samples of 10000 operations; p95 is the sample p95, not per-operation tail latency",
	gzipBytes,
	results,
};
writeFileSync("perf-results.json", `${JSON.stringify(report, null, 2)}\n`);
console.table(
	results.map((row) => ({
		...row,
		medianNs: Math.round(row.medianNs),
		p95Ns: Math.round(row.p95Ns),
		operationsPerSecond: Math.round(row.operationsPerSecond),
	})),
);
console.log(
	`Runtime gzip: ${gzipBytes} bytes. Sharing, notification and cleanup invariants passed.`,
);
