---
name: benchmark-migration
description: >
  Benchmark a database migration. Parses a Java migration (DatabaseUtil.createTable DSL),
  converts it to DDL, generates a go-seed-my-db config, seeds realistic data, and runs
  query benchmarks. Use when the user wants to test index performance on a new or modified table.
---

# Benchmark a Database Migration

You are helping the user benchmark a database schema defined in a Java migration file
that uses the `DatabaseUtil.createTable` / `Table.withName` DSL from the tripletex-migrations
project.

## Step 0 — Check prerequisites

The benchmark tools need a MySQL database. There are two ways to provide one:

**Option A — Ephemeral (recommended, zero config)**: If Docker or Podman is available and no `SEED_DSN`
is configured, the MCP tools automatically start a temporary MySQL container,
run the benchmark, and tear it down. No configuration needed.

**Option B — Existing database**: If the user already has a MySQL instance, set `SEED_DSN`
in the MCP server environment:
```json
"env": { "SEED_DSN": "user:pass@tcp(localhost:3306)/dbname" }
```
Look for credentials in the project's `CLAUDE.md`, `.env`, or config files.

You can verify connectivity by calling `list_tables` — if it fails and Docker/Podman is available,
the tools will fall back to ephemeral mode automatically.

## Step 1 — Read the migration

The user will provide either:
- A file path to a Java migration class (look in the tripletex-migrations project)
- Pasted Java code containing `DatabaseUtil.createTable(...)`

Parse the `Table.withName(...)` builder chain and extract:
- Table name, columns (name, type, nullable, defaults, comments)
- Indexes (name, columns, primary/unique)
- Foreign key constraints (column, referenced table, referenced column)

## Step 2 — Generate DDL

Convert the Java DSL to a MySQL `CREATE TABLE` statement.

Type mapping from the Java DSL:
| Java DSL          | MySQL DDL              |
|-------------------|------------------------|
| `DataType.BIGINT` | `BIGINT`               |
| `DataType.INT`    | `INT`                  |
| `DataType.VARCHAR(n)` | `VARCHAR(n)`       |
| `DataType.TEXT`   | `TEXT`                 |
| `DataType.DATETIME` | `DATETIME`           |
| `DataType.DATE`   | `DATE`                 |
| `DataType.DECIMAL(p,s)` | `DECIMAL(p,s)`  |
| `DataType.BOOLEAN`| `TINYINT(1)`           |
| `DataType.BLOB`   | `BLOB`                 |

Modifiers: `.unsigned()` → `UNSIGNED`, `.notNull()` → `NOT NULL`, `.defaultValue("x")` → `DEFAULT x`,
`.comment("x")` → `COMMENT 'x'`.

For auto-increment: if the PK column is `BIGINT` or `INT` and has no other generation strategy,
make it `AUTO_INCREMENT`.

**Important**: Always use `DATETIME` instead of `TIMESTAMP` in the DDL for benchmarking.
MySQL `TIMESTAMP` columns apply timezone conversion, which causes errors when randomly
generated times hit DST gaps (e.g., 2:00-3:00 AM during spring-forward). `DATETIME`
stores values as-is and avoids this issue entirely.

### Foreign keys → plain indexes

**Do NOT generate `FOREIGN KEY` constraints in the benchmark DDL.** Instead, add a plain
`INDEX` on every FK column. The query optimizer uses the index, not the constraint, so
benchmark results are identical. Omitting FK constraints avoids issues with `defer_indexes`
(MySQL refuses to drop an index that's the sole backing index for a FK) and speeds up seeding.

The config's `references:` section still ensures the seeder picks realistic values from
parent tables — no constraint needed.

Example — for a column `companyId` that references `company.id`:
```sql
-- In the CREATE TABLE:
  companyId INT UNSIGNED NOT NULL,
  INDEX idx_companyId (companyId),
  -- NOT: FOREIGN KEY (companyId) REFERENCES company(id)
```

### Parent stub tables

You still need parent tables in the DDL so the seeder can pick valid FK values from them.
Keep these minimal:

**Common base tables**:
```sql
CREATE TABLE company (
  id INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(100) NOT NULL
);
```

Other common stubs: `employee(id, companyId → index only)`, `customer(id, companyId → index only)`.

**Domain-specific parents** — if the FK points to a table from a *related* migration the user
can provide, ask for it. Otherwise create a sensible stub with just the PK and a name/label column.

## Step 3 — Ask about queries

Ask the user:

> What queries will your application run against this table? Think about:
> - **Filtering**: Which WHERE clauses? (e.g. `companyId = ? AND status = ?`)
> - **Sorting**: Any ORDER BY patterns?
> - **Joins**: Do you join to other tables? Which columns?
> - **Aggregations**: Any GROUP BY / COUNT / SUM queries?
>
> Even rough descriptions help — I'll turn them into benchmark queries.

## Step 4 — Generate the config

Create a `go-seed-my-db.yaml` config file with:

```yaml
options:
  schema: "<path-to-generated-ddl.sql>"
  defer_indexes: true

tables:
  <table_name>:
    rows: <pick sensible count: 100k-1M for the main table, fewer for parents>
    columns:
      # Use gofakeit template functions matched to column semantics.
      # Templates use Go text/template syntax with gofakeit Faker methods.
      #
      # Common patterns:
      # - Dates (date-only): '{{printf "%04d-%02d-%02d" (Number 2020 2025) (Number 1 12) (Number 1 28)}}'
      # - Datetimes:         '{{printf "%04d-%02d-%02d %02d:%02d:%02d" (Number 2020 2025) (Number 1 12) (Number 1 28) (Number 0 23) (Number 0 59) (Number 0 59)}}'
      # - Enum-like ints:    '{{Number 0 5}}'
      # - Status strings:    '{{RandomString (SliceString "ACTIVE" "CLOSED")}}'
      # - JSON:              use |- block scalar with embedded templates
      # - Money:             '{{printf "%.2f" (Float64Range 0.01 99999.99)}}'
      # - Hex hashes:        '{{Regex "[a-f0-9]{64}"}}'
      # - UUIDs:             '{{UUID}}'
      # - Short strings:     '{{LetterN 20}}'
      #
      # Available functions: all gofakeit.Faker methods (Name, Email, Company, Number,
      # RandomString, Regex, Numerify, UUID, DateRange, Price, Sentence, Paragraph,
      # LetterN, HackerPhrase, IPv4Address, Bool, Float64Range, etc.)
      # Plus helpers: SliceString, SliceInt, SliceAny, ToUpper, ToLower, IntRange, printf.
      # Plus SampleRow (see below).
      #
      # NOT available: SHA256, MD5, Generate — these are standalone gofakeit functions,
      # not methods on the Faker struct. Use Regex for hex strings instead.
    references:
      <fk_column>: <ParentTable>.<referenced_column>

tests:
  - name: "<descriptive name matching the user's query pattern>"
    query: |-
      <SQL query with {{Number}}/{{RandomString}} for parameterized values>
    repeat: 100
```

### SampleRow — correlated query parameters

**IMPORTANT**: When a test query filters on two or more FK columns from the same table
(e.g. `WHERE companyId = ? AND voucherId = ?`), do **NOT** use independent `{{Number}}`
calls — the random values will almost never form a valid combination and the query will
return 0 rows, producing meaningless benchmarks.

Instead, use `SampleRow` to pick a real row from the seeded data:

```yaml
tests:
  - name: "filter by company + voucher"
    query: |-
      {{- with SampleRow "MyTable" "companyId" "voucherId" -}}
      SELECT * FROM MyTable
      WHERE companyId = {{.companyId}} AND voucherId = {{.voucherId}}
      {{- end}}
    repeat: 100
```

`SampleRow "table" "col1" "col2" ...` fetches real rows from the database (cached, no
impact on timing) and returns a map. Use `{{with}}` to set the context, then access
columns as `{{.colName}}`. The values are guaranteed to be a valid combination that
exists in the seeded data.

**When to use SampleRow vs Number/RandomString:**
- Single FK column → `{{Number 1 500}}` is fine (always matches some rows)
- Multiple FK columns on the same table → **always use SampleRow**
- Non-FK columns (status, severity, date) → `{{RandomString ...}}` / `{{Number ...}}` is fine,
  these have few distinct values so random picks work well

Write the DDL `.sql` file and the config `.yaml` file to a temp directory or the project's
working directory.

## Step 5 — Run the benchmark

Use the `seed-my-db` MCP **`test`** tool to run the benchmark for a single schema:

```
MCP tool: test(config_path: "<config.yaml>")
```

Or via CLI: `go-seed-my-db test --config <config.yaml> --ephemeral`

## Step 6 — Analyze and suggest improvements

After the benchmark completes, analyze the results:

1. **Identify slow queries** — any query with high p95/p99 or that shows "filesort" / "full scan" behavior
2. **Check index coverage** — for each query's WHERE/ORDER BY/JOIN columns, verify a composite
   index exists with columns in the right order (equality columns first, then range, then sort)
3. **Suggest improvements**:
   - Missing composite indexes
   - Column reordering in existing indexes (selectivity matters)
   - Covering indexes (adding columns to avoid table lookups)
   - Redundant indexes that are prefixes of other indexes
4. **Offer to compare** — if improvements are identified, create an alternative DDL with
   the suggested indexes and benchmark both side-by-side using `compare` (see below)

Present suggestions as concrete `ADD INDEX` statements the user can add to their migration.

## Step 7 (optional) — Compare alternatives

If you suggested index improvements in step 6 and the user wants to verify them, set up a
side-by-side comparison:

1. Create a second DDL file with the suggested indexes (e.g., `schema-optimized.sql`)
2. Create a second seed config pointing to the new DDL (e.g., `config-optimized.yaml`)
3. Create a comparison YAML:

```yaml
configs:
  - label: baseline
    file: config-baseline.yaml
  - label: optimized
    file: config-optimized.yaml

tests:
  - name: "<query description>"
    repeat: 100
    queries:
      baseline: |-
        <SQL query for baseline schema>
      optimized: |-
        <SQL query for optimized schema (may differ if table/column names changed)>
```

4. Run via MCP: `compare(config_path: "comparison.yaml")`
   Or via CLI: `go-seed-my-db compare comparison.yaml --ephemeral`
