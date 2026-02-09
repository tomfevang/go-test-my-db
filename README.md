# go-seed-my-db

A CLI tool that introspects your MySQL schema and generates millions of rows of realistic fake data. Perfect for performance testing with believable datasets.

## Features

- **Schema introspection** — automatically discovers tables, columns, types, and foreign keys
- **FK-aware seeding** — resolves dependency order and auto-includes parent tables
- **Concurrent inserts** — configurable worker count for fast bulk loading
- **Template-based generation** — customize data per column using [gofakeit](https://github.com/brianvoe/gofakeit) templates
- **Test mode** — create tables from DDL, seed, benchmark queries, drop tables in one command

## Installation

### Go install

```bash
go install github.com/tomfevang/go-seed-my-db@latest
```

### Binary download

Download pre-built binaries from the [Releases](https://github.com/tomfevang/go-seed-my-db/releases) page.

## Usage

### Seed an existing database

```bash
go-seed-my-db \
  --dsn "user:pass@tcp(localhost:3306)/mydb" \
  --rows 100000 \
  --workers 8 \
  --batch-size 5000
```

Seed specific tables:

```bash
go-seed-my-db \
  --dsn "user:pass@tcp(localhost:3306)/mydb" \
  --table orders \
  --table order_items \
  --rows 50000
```

### Test mode

Create tables from a DDL file, seed, run benchmark queries, then clean up:

```bash
go-seed-my-db test \
  --dsn "user:pass@tcp(localhost:3306)/mydb" \
  --schema schema.sql \
  --config config.yaml \
  --table fact_table \
  --rows 100000
```

### Flags

| Flag | Default | Description |
|---|---|---|
| `--dsn` | *(required)* | MySQL DSN, e.g. `user:pass@tcp(host:3306)/db` |
| `--table` | all tables | Table(s) to seed (repeatable) |
| `--rows` | 1000 | Rows to generate per table |
| `--batch-size` | 1000 | Rows per INSERT statement |
| `--workers` | 4 | Concurrent insert workers |
| `--clear` | false | Truncate tables before seeding |
| `--config` | auto-detect | Path to config YAML |

## Config file

Place a `go-seed-my-db.yaml` in your working directory or pass `--config`. The config file lets you customize data generation per column and define test queries.

```yaml
tables:
  users:
    columns:
      email: "{{Email}}"
      name: "{{Name}}"
      status: '{{RandomString (SliceString "active" "inactive" "suspended")}}'

tests:
  - name: "Filter by status"
    query: "SELECT * FROM users WHERE status = 'active'"
    repeat: 10
```

Templates use [gofakeit v7](https://github.com/brianvoe/gofakeit) functions.

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
