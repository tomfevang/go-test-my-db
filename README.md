# go-test-my-db

A MySQL testing toolkit that introspects your schema, generates millions of rows of realistic fake data, benchmarks queries, and compares schema designs side by side.

## Features

- **Schema introspection** — discovers tables, columns, types, foreign keys, unique indexes, and enums
- **FK-aware seeding** — topological sort resolves dependency order; auto-includes parent tables
- **Concurrent inserts** — configurable worker pool with batched INSERTs or `LOAD DATA LOCAL INFILE`
- **Template-based generation** — customize data per column using [gofakeit v7](https://github.com/brianvoe/gofakeit) templates
- **Smart heuristics** — auto-detects column intent from names (email, phone, address, price, etc.)
- **Value distributions** — Zipf, normal, weighted, or uniform distributions for any column
- **Correlated columns** — generate coherent data across column groups (address, person, lat/long)
- **Unique constraints** — enforces single-column and composite unique indexes during generation
- **Logical foreign keys** — define FK relationships in config without real database constraints
- **Test mode** — create tables from DDL, seed, benchmark queries, and drop tables in one command
- **Compare mode** — run the test pipeline across multiple schema configs and compare results side by side
- **AI analysis** — pipe benchmark results to Claude for automated performance insights
- **Dry-run mode** — preview seeding plans (tables, row counts, per-column strategies) without writing data
- **Init command** — generate a starter config from your live schema with detected heuristics
- **Preview command** — inspect generated sample rows before committing to a full seed
- **TTY progress bars** — inline progress indicators in interactive terminals

## Installation

### Go install

```bash
go install github.com/tomfevang/go-test-my-db@latest
```

### Binary download

Download pre-built binaries from the [Releases](https://github.com/tomfevang/go-test-my-db/releases) page.

## Quick start

Generate a config from your live schema, then seed it:

```bash
# Generate a starter config with detected heuristics
go-test-my-db init --dsn "user:pass@tcp(localhost:3306)/mydb"

# Review and customize go-test-my-db.yaml, then seed
go-test-my-db --dsn "user:pass@tcp(localhost:3306)/mydb" --rows 100000
```

## Commands

### `go-test-my-db` (seed)

Seed an existing database with fake data:

```bash
go-test-my-db \
  --dsn "user:pass@tcp(localhost:3306)/mydb" \
  --rows 100000 \
  --workers 8 \
  --batch-size 5000
```

Seed specific tables:

```bash
go-test-my-db \
  --dsn "user:pass@tcp(localhost:3306)/mydb" \
  --table orders \
  --table order_items \
  --rows 50000
```

Preview what would be seeded without writing data:

```bash
go-test-my-db --dsn "..." --rows 100000 --dry-run
```

| Flag | Default | Description |
|---|---|---|
| `--dsn` | *(required)* | MySQL DSN, e.g. `user:pass@tcp(host:3306)/db` |
| `--table` | all tables | Table(s) to seed (repeatable) |
| `--rows` | 1000 | Rows per root table |
| `--batch-size` | 1000 | Rows per INSERT statement |
| `--workers` | 4 | Concurrent insert workers |
| `--clear` | false | Truncate tables before seeding |
| `--config` | auto-detect | Path to config YAML |
| `--load-data` | false | Use `LOAD DATA LOCAL INFILE` for faster bulk loading |
| `--defer-indexes` | false | Drop secondary indexes before seeding and rebuild after |
| `--dry-run` | false | Print seeding plan without inserting |
| `--min-children` | 10 | Min child rows per parent row |
| `--max-children` | 100 | Max child rows per parent row |
| `--max-rows` | 10,000,000 | Absolute row cap per table |
| `--fk-sample-size` | 500,000 | Max FK parent values cached per column (0 = unlimited) |

The DSN can also be set via the `SEED_DSN` environment variable or the `options.dsn` config field. Priority: CLI flag > env var > config > default.

### `go-test-my-db init`

Generate a starter config file from your live schema:

```bash
go-test-my-db init --dsn "user:pass@tcp(localhost:3306)/mydb"
```

The generated `go-test-my-db.yaml` includes detected heuristics, FK references, and enum values as comments so you can quickly customize it.

| Flag | Default | Description |
|---|---|---|
| `--dsn` | *(required)* | MySQL DSN |
| `--output` | `go-test-my-db.yaml` | Output file path |
| `--force` | false | Overwrite if file exists |

### `go-test-my-db test`

Create tables from a DDL file, seed them, benchmark queries, then drop everything:

```bash
go-test-my-db test \
  --dsn "user:pass@tcp(localhost:3306)/mydb" \
  --schema schema.sql \
  --config config.yaml \
  --rows 100000
```

Results include avg, min, max, and p95 latency per query. Add `--ai` to pipe results to Claude for analysis.

| Flag | Default | Description |
|---|---|---|
| `--dsn` | *(required)* | MySQL DSN |
| `--schema` | *(required)* | Path to SQL DDL file |
| `--config` | auto-detect | Config YAML path |
| `--rows` | 1000 | Rows per root table |
| `--batch-size` | 1000 | Rows per INSERT |
| `--workers` | 4 | Insert workers |
| `--table` | all schema tables | Tables to seed (repeatable) |
| `--ai` | false | Pipe results to Claude for AI analysis |
| `--load-data` | false | Use LOAD DATA mode |
| `--defer-indexes` | false | Drop secondary indexes before seeding and rebuild after |
| `--fk-sample-size` | 500,000 | Max FK parent values cached per column (0 = unlimited) |
| `--min-children` | 10 | Min children per parent |
| `--max-children` | 100 | Max children per parent |
| `--max-rows` | 10,000,000 | Row cap |

### `go-test-my-db compare`

Run the test pipeline across multiple schema configurations and display a side-by-side comparison:

```bash
# Using a comparison config file
go-test-my-db compare --dsn "..." comparison.yaml

# Or pass multiple config files directly
go-test-my-db compare --dsn "..." config-star.yaml config-flat.yaml
```

A comparison config file lets you define per-schema query variants in one place:

```yaml
configs:
  - label: star
    file: config-star.yaml
  - label: flat
    file: config-flat.yaml

tests:
  - name: "Filter by status"
    repeat: 100
    queries:
      star: "SELECT * FROM dim_status ds JOIN fact t ON t.status_id = ds.id WHERE ds.name = 'active'"
      flat: "SELECT * FROM events WHERE status = 'active'"
```

| Flag | Default | Description |
|---|---|---|
| `--dsn` | *(required)* | MySQL DSN (shared across configs) |
| `--rows` | 0 | Override rows for all configs |
| `--batch-size` | 0 | Override batch size for all configs |
| `--workers` | 0 | Override worker count for all configs |
| `--ai` | false | Pipe comparison report to Claude |
| `--load-data` | false | Use LOAD DATA mode (overrides all configs) |
| `--defer-indexes` | false | Drop secondary indexes before seeding and rebuild after |
| `--fk-sample-size` | 0 | Override max FK parent values cached per column |
| `--min-children` | 0 | Override min children per parent |
| `--max-children` | 0 | Override max children per parent |
| `--max-rows` | 0 | Override max rows per table |

### `go-test-my-db preview`

Preview generated sample rows without a full seed:

```bash
# From a DDL file (creates temp tables, seeds, shows samples, drops)
go-test-my-db preview --dsn "..." --schema schema.sql --config config.yaml

# From an existing database (in-memory generation, no writes)
go-test-my-db preview --dsn "..."
```

| Flag | Default | Description |
|---|---|---|
| `--dsn` | *(required)* | MySQL DSN |
| `--schema` | | Path to SQL DDL file (creates temporary tables) |
| `--config` | auto-detect | Config YAML path |
| `--table` | all tables | Table(s) to preview (repeatable) |
| `--sample-rows` | 5 | Number of sample rows to display per table |
| `--rows` | 1000 | Base row count for root tables |
| `--batch-size` | 1000 | Rows per INSERT |
| `--workers` | 4 | Insert workers |
| `--load-data` | false | Use LOAD DATA mode |
| `--defer-indexes` | false | Drop secondary indexes before seeding and rebuild after |
| `--fk-sample-size` | 500,000 | Max FK parent values cached per column (0 = unlimited) |
| `--min-children` | 10 | Min children per parent |
| `--max-children` | 100 | Max children per parent |
| `--max-rows` | 10,000,000 | Row cap |

### `go-test-my-db examples`

Extract the bundled example schemas and configs to the current directory:

```bash
go-test-my-db examples
```

This writes an `examples/` directory containing two schema designs (generated-columns and star-schema) with matching seed configs and a comparison config. See [`examples/README.md`](examples/README.md) for details.

| Flag | Default | Description |
|---|---|---|
| `--force` | false | Overwrite existing files |

## MCP server

go-test-my-db includes a built-in [Model Context Protocol](https://modelcontextprotocol.io/) server, letting AI tools like Claude Code interact with your database directly — introspect schemas, generate configs, seed data, and benchmark queries through natural conversation.

### Setup

Add to your Claude Code project settings (`.claude/settings.json`):

```json
{
  "mcpServers": {
    "seed-my-db": {
      "command": "go-test-my-db",
      "args": ["mcp"],
      "env": { "SEED_DSN": "user:pass@tcp(localhost:3306)/mydb" }
    }
  }
}
```

If `SEED_DSN` is not set and Docker or Podman is available, the `test` and `compare` tools automatically start an ephemeral MySQL container — no configuration needed.

### Available tools

| Tool | Description |
|---|---|
| `list_tables` | List all tables and their FK relationships |
| `describe_table` | Inspect column types, indexes, and constraints for a table |
| `preview_data` | Dry-run: see sample rows the seeder would generate (no writes) |
| `generate_config` | Scaffold a `go-test-my-db.yaml` from the live schema |
| `seed_database` | Insert fake data into the database |
| `test` | Benchmark query performance for a single schema config |
| `compare` | Benchmark and compare query performance across multiple schemas side by side |

### Typical workflow

```
list_tables → describe_table → preview_data → generate_config → seed_database → test / compare
```

Start with `list_tables` to orient yourself, then drill into individual tables. Use `preview_data` to validate generation strategies before committing to a full seed. Use `test` when benchmarking one schema, and `compare` when evaluating alternatives (e.g., different index strategies).

### Skills

The MCP server exposes skill resources that guide you through complex workflows:

- **benchmark-migration** — Parse a Java migration file, convert to DDL, generate a seed config, and benchmark query performance. Invoke with `/benchmark-migration` in Claude Code.

## Config file

Place a `go-test-my-db.yaml` in your working directory or pass `--config`. Use `go-test-my-db init` to generate one from your schema.

```yaml
options:
  dsn: "user:pass@tcp(localhost:3306)/mydb"
  schema: "schema.sql"
  seed_tables: [users, orders]
  rows: 100000
  batch_size: 5000
  workers: 8
  load_data: false
  defer_indexes: false
  fk_sample_size: 500000
  max_rows: 10000000
  children_per_parent:
    min: 10
    max: 100

tables:
  users:
    rows: 5000  # per-table override
    columns:
      email: "{{Email}}"
      name: "{{Name}}"
      status: '{{RandomString (SliceString "active" "inactive" "suspended")}}'
    distributions:
      status:
        type: weighted
        weights:
          active: 0.8
          inactive: 0.15
          suspended: 0.05
    correlations:
      - columns: [city, state, zip]
        source: address
      - columns: [first_name, last_name, email]
        source: person

  orders:
    references:
      user_id: users.id  # logical FK (no real constraint needed)
    columns:
      total: "{{Price 10 500}}"

tests:
  - name: "Filter by status"
    query: "SELECT * FROM users WHERE status = 'active'"
    repeat: 50
```

Templates use [gofakeit v7](https://github.com/brianvoe/gofakeit) functions. Query templates are re-evaluated on each repeat for randomized parameters.

### Value distributions

Control how values are distributed across rows:

| Type | Description |
|---|---|
| `uniform` | Equal probability (default) |
| `zipf` | Power-law / long-tail distribution |
| `normal` | Gaussian with configurable mean/stddev |
| `weighted` | Explicit per-value weights |

### Correlated column groups

Generate coherent data across multiple columns:

| Source | Description |
|---|---|
| `address` | City, state, zip from the same address |
| `person` | First name, last name, email from the same person |
| `latlong` | Latitude and longitude from the same location |
| `template` | Custom templates where columns can reference each other |

## Examples

Extract the bundled examples, then try them out:

```bash
go-test-my-db examples
```

This gives you two complete schema designs with configs and benchmark queries:

- **Generated columns** — JSON fields with `STORED GENERATED` columns and indexes
- **Star schema** — dimension tables with integer FK lookups

Run one individually with `test`:

```bash
go-test-my-db test \
  --dsn "user:pass@tcp(localhost:3306)/mydb" \
  --schema examples/schema.sql \
  --config examples/config-generated.yaml
```

Or compare both side by side:

```bash
go-test-my-db compare --dsn "..." examples/comparison.yaml
```

See [`examples/README.md`](examples/README.md) for details.

## Contributing

This project uses [conventional commits](https://www.conventionalcommits.org/). Commit messages are enforced in CI via commitlint.

```
feat: add new feature
fix: resolve bug
perf: improve performance
docs: update documentation
refactor: restructure code
test: add tests
chore: maintenance tasks
```

## License

MIT
