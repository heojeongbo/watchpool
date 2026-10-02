# License and distribution

Watchpool is distributed under the MIT license in [LICENSE](LICENSE), with
copyright attributed to heojeongbo. Package metadata declares `MIT`.

The npm tarball contains watchpool's compiled modules, runtime TypeScript source,
README, LICENSE, this inventory and package metadata. It does not bundle vendor
implementations, development tools, tests or benchmark output. Optional peers are
installed separately by consumers and retain their own licenses:

| Optional peer | License declared by the verified package |
| --- | --- |
| React 19.0.0 | MIT |
| @connectrpc/connect 2.1.0 | Apache-2.0 |
| @bufbuild/protobuf 2.10.0 | Apache-2.0 AND BSD-3-Clause |

Core, SSE and WebSocket entrypoints have no third-party runtime dependencies.
Their platform APIs (`fetch`, `WebSocket`, `TextDecoder`, timers) come from the
browser or Node.js. Connect and React imports are isolated to their entrypoints.

Development dependencies have MIT, Apache-2.0, BSD-3-Clause, ISC, dual MIT/Apache,
or MPL-2.0 declarations. The MPL-2.0 packages are Lightning CSS development-tool
components; they are not part of the npm tarball. This inventory describes the
release inputs, not a relicensing of those packages.

Recheck when changing dependencies or distribution files:

```sh
pnpm licenses list --json
pnpm audit
npm pack --dry-run --json
```

Preserve the copyright and permission notice when redistributing watchpool, as
required by [the MIT license](https://opensource.org/license/mit).
