import { readdirSync, readFileSync } from "node:fs";
import { resolve } from "node:path";
import ts from "typescript";

// Keep the complete, copyable guide examples checked against public declarations.
// README excerpts intentionally depend on application-generated schemas.
const virtual = new Map();
for (const name of readdirSync("docs").filter((name) => name.endsWith(".md"))) {
	const content = readFileSync(resolve("docs", name), "utf8");
	let index = 0;
	for (const match of content.matchAll(/```(ts|tsx)\n([\s\S]*?)```/g)) {
		const file = resolve("docs", `.example-${name}-${++index}.${match[1]}`);
		virtual.set(file, match[2]);
	}
}
const config = {
	strict: true,
	noEmit: true,
	skipLibCheck: false,
	target: ts.ScriptTarget.ES2022,
	module: ts.ModuleKind.NodeNext,
	moduleResolution: ts.ModuleResolutionKind.NodeNext,
	jsx: ts.JsxEmit.ReactJSX,
	moduleDetection: ts.ModuleDetectionKind.Force,
};
const host = ts.createCompilerHost(config);
const read = host.readFile.bind(host);
const exists = host.fileExists.bind(host);
host.readFile = (file) => virtual.get(file) ?? read(file);
host.fileExists = (file) => virtual.has(file) || exists(file);
const program = ts.createProgram([...virtual.keys()], config, host);
const diagnostics = ts.getPreEmitDiagnostics(program);
if (diagnostics.length) {
	console.error(
		ts.formatDiagnosticsWithColorAndContext(diagnostics, {
			getCanonicalFileName: (file) => file,
			getCurrentDirectory: () => process.cwd(),
			getNewLine: () => "\n",
		}),
	);
	process.exitCode = 1;
} else
	console.log(
		`Documentation examples passed (${virtual.size} standalone TypeScript/TSX blocks).`,
	);
