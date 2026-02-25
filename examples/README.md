# Examples

Two example schemas for benchmarking different MySQL design patterns.

## Schemas

| Schema | Config | Description |
|--------|--------|-------------|
| `schema.sql` | `config-generated.yaml` | Generated columns — JSON fields with `STORED GENERATED` columns and indexes |
| `schema-star.sql` | `config-star.yaml` | Star schema — dimension tables (`DimSeverity`, `DimStatus`, etc.) with integer FK lookups |

Both schemas model the same data (companies, account checks, anomalies) so query performance is directly comparable.

## Running individually

Use `test` to create tables, seed, benchmark, and tear down in one shot:

```bash
go-test-my-db test \
  --dsn "user:pass@tcp(localhost:3306)/mydb" \
  --schema examples/schema.sql \
  --config examples/config-generated.yaml
```

## Comparing side by side

Use `compare` with the comparison config to run both schemas and see results side by side:

```bash
go-test-my-db compare \
  --dsn "user:pass@tcp(localhost:3306)/mydb" \
  examples/comparison.yaml
```

Add `--ai` to pipe the comparison report to Claude for analysis.

## File inventory

| File | Purpose |
|------|---------|
| `schema.sql` | DDL for the generated-columns schema |
| `schema-star.sql` | DDL for the star schema (includes dimension table INSERTs) |
| `config-generated.yaml` | Seed config + test queries for generated-columns |
| `config-star.yaml` | Seed config + test queries for star schema |
| `comparison.yaml` | Side-by-side comparison config with per-schema query variants |
