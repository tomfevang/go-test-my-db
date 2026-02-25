# Changelog

## [1.0.1](https://github.com/tomfevang/go-test-my-db/compare/v1.0.0...v1.0.1) (2026-02-25)


### Bug Fixes

* run goreleaser inline in release-please workflow ([89ba0fd](https://github.com/tomfevang/go-test-my-db/commit/89ba0fd009200320295c7f65185241aa2d7d60e4))


### Code Refactoring

* rename project from go-seed-my-db to go-test-my-db ([741ed6d](https://github.com/tomfevang/go-test-my-db/commit/741ed6d27a72a480b92823c07ef4bd7cbf48f155))

## 1.0.0 (2026-02-25)


### Features

* add `examples` command to extract bundled example files ([c132779](https://github.com/tomfevang/go-seed-my-db/commit/c132779490fa00bd142e35be570d4a568a6f321d))
* add dedicated comparison config file format ([9281c83](https://github.com/tomfevang/go-seed-my-db/commit/9281c83e08bb6ecf2fc73fec7f07dcc3b5ddc8ef))
* add dry-run mode, init command, and TTY progress bars ([c77ae32](https://github.com/tomfevang/go-seed-my-db/commit/c77ae32b1ad339afc1fb174ec3c9aaf6ae38824e))
* add FK correlation, SampleRow queries, ephemeral containers, and MCP test tool ([4d8ce45](https://github.com/tomfevang/go-seed-my-db/commit/4d8ce456fac9547b2e0fac588fe427e71c1a7daf))
* add MCP server mode for Claude Code integration ([8629a32](https://github.com/tomfevang/go-seed-my-db/commit/8629a32ee61c1446015e705cd84f2654a19ed336))
* defer secondary index building during seeding ([a2e8c88](https://github.com/tomfevang/go-seed-my-db/commit/a2e8c88653d16d644615115b2823941bd98a820b))
* memory-bounded FK cache via reservoir sampling and eviction ([76cc582](https://github.com/tomfevang/go-seed-my-db/commit/76cc5823cae206ad1b07b1740ec699e2d24aa0c1))


### Bug Fixes

* add missing --load-data and --fk-sample-size flags to test, preview, and compare ([4c02f10](https://github.com/tomfevang/go-seed-my-db/commit/4c02f1073db07d94837ae6028135c15d4522fdae))
* align test report table and reduce example row counts ([eb06893](https://github.com/tomfevang/go-seed-my-db/commit/eb0689389c89788583772d998b3962e7052d42be))
* **benchmarks:** update comparison queries to match 2000-company dataset ([2404f97](https://github.com/tomfevang/go-seed-my-db/commit/2404f97de753b6d55fc2d3eae8caabcd92cf918b))
* check os.WriteFile errors in compare tests ([d3dafa7](https://github.com/tomfevang/go-seed-my-db/commit/d3dafa7edf7f2a7492d4b0b5a51e6571a171f725))
* **ci:** add version field to golangci-lint config for v2 ([a6a1d32](https://github.com/tomfevang/go-seed-my-db/commit/a6a1d3246cded829f4e4ce7ccd851708362ad130))
* **ci:** nest settings under linters in golangci-lint v2 config ([cb66f81](https://github.com/tomfevang/go-seed-my-db/commit/cb66f814ad553a8c1ec95d3d3c4212123a3040d7))
* **ci:** resolve golangci-lint v2 errors ([ebef09e](https://github.com/tomfevang/go-seed-my-db/commit/ebef09ed4e4885bc95e59672901b3f6e11f76da8))
* **ci:** upgrade golangci-lint action for Go 1.25 compatibility ([8b92ad4](https://github.com/tomfevang/go-seed-my-db/commit/8b92ad44a6053e666b38532490e6f565575478a3))
* **ci:** use golangci-lint-action v9 with pinned v2.6 ([8b14019](https://github.com/tomfevang/go-seed-my-db/commit/8b14019f32e62a2cc8bfe02e5d5c2105c9879201))
* **ci:** use v2 config key 'settings' instead of 'linters-settings' ([8ab9fe6](https://github.com/tomfevang/go-seed-my-db/commit/8ab9fe6731820d9786c70952ccd61d78fb322d49))
* prevent MCP tools from returning empty structuredContent ([0c17748](https://github.com/tomfevang/go-seed-my-db/commit/0c1774883ad91466c98fba7dd0bc4c7ed1714703))
* remove unused computeRowCounts duplicate in mcptools ([ec4cf9f](https://github.com/tomfevang/go-seed-my-db/commit/ec4cf9f224ed4013a157cc66055ec21a683b7e5d))
* use fmt.Fprintf instead of WriteString(Sprintf(...)) ([3b17f64](https://github.com/tomfevang/go-seed-my-db/commit/3b17f64153450715471a471f883a1dec032b5033))


### Documentation

* rename benchmarks/ to examples/ and polish for release ([2c0a891](https://github.com/tomfevang/go-seed-my-db/commit/2c0a891401e2b6fa70da1de7baab4503038446e8))
* rewrite README to reflect current feature set ([e18b971](https://github.com/tomfevang/go-seed-my-db/commit/e18b9713eebaecde1e70c1e1835ba56c0527e103))


### Code Refactoring

* **benchmarks:** add companyId FK and composite indexes to benchmark schemas ([5c628ab](https://github.com/tomfevang/go-seed-my-db/commit/5c628ab7b1da049a3c65b43be89f4fd7dc16ee97))


### Tests

* add unit tests for pure, DB-free functions ([8c8f63a](https://github.com/tomfevang/go-seed-my-db/commit/8c8f63ab046451fa11ab6482533baecc635fe33f))
