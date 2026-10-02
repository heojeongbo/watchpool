import { readdirSync, readFileSync } from "node:fs";
import { dirname, relative, resolve } from "node:path";
import ts from "typescript";

const root = resolve("src");
const files = readdirSync(root, { recursive: true }).filter((file) =>
	file.endsWith(".ts"),
);
const rank = {
	shared: 0,
	entities: 1,
	features: 2,
	widgets: 3,
	pages: 4,
	app: 5,
};
for (const file of files) {
	const from = file.split("/");
	const ast = ts.createSourceFile(
		file,
		readFileSync(resolve(root, file), "utf8"),
		ts.ScriptTarget.Latest,
	);
	for (const node of ast.statements) {
		if (
			(!ts.isImportDeclaration(node) && !ts.isExportDeclaration(node)) ||
			!node.moduleSpecifier ||
			!ts.isStringLiteral(node.moduleSpecifier)
		)
			continue;
		const specifier = node.moduleSpecifier.text;
		if (!specifier.startsWith(".")) continue;
		const target = relative(
			root,
			resolve(dirname(resolve(root, file)), specifier),
		);
		const to = target.split("/");
		if (target.startsWith("..")) throw new Error(`${file}: import escapes src`);
		if (from.length === 1) continue; // Package entrypoints compose public APIs.
		if (!(to[0] in rank) || rank[to[0]] > rank[from[0]])
			throw new Error(`${file}: upward dependency ${target}`);
		if (from[0] === to[0] && from[0] !== "shared" && from[1] !== to[1])
			throw new Error(`${file}: cross-slice dependency ${target}`);
		if (from[0] !== to[0] && !target.endsWith("/index.js"))
			throw new Error(`${file}: bypassed public API ${target}`);
	}
}
console.log(`FSD boundaries passed (${files.length} source files).`);
