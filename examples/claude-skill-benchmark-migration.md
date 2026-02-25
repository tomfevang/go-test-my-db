# Benchmark Migration — Claude Code Skill

A Claude Code skill for benchmarking database migrations against realistic data.
Parses Java migration files (DatabaseUtil.createTable DSL), generates DDL and a
go-seed-my-db config, seeds data, runs query benchmarks, and suggests index improvements.

## Setup

### 1. Install go-seed-my-db

```bash
go install github.com/tomfevang/go-seed-my-db@latest
```

### 2. Configure the MCP server

Add to your project's `.claude/settings.local.json` (or global `~/.claude/settings.json`):

```json
{
  "mcpServers": {
    "seed-my-db": {
      "command": "go-seed-my-db",
      "args": ["mcp"],
      "env": { "SEED_DSN": "user:pass@tcp(localhost:3306)/mydb" }
    }
  }
}
```

### 3. Install the skill

Copy the `benchmark-migration/` directory into your project:

```bash
mkdir -p .claude/skills
cp -r <go-seed-my-db>/examples/benchmark-migration .claude/skills/
```

Or add it to your personal skills (available across all projects):

```bash
cp -r <go-seed-my-db>/examples/benchmark-migration ~/.claude/skills/
```

### 4. Use it

In Claude Code, invoke with:

```
/benchmark-migration path/to/V2025_11_17__create_my_table.java
```

Or just paste a migration and say "benchmark this migration".

## What it does

1. **Parses** the Java `DatabaseUtil.createTable` DSL
2. **Generates** MySQL DDL with FK parent stubs (company, employee, etc.)
3. **Asks** what queries you plan to run against the table
4. **Creates** a go-seed-my-db config with realistic data generators and benchmark queries
5. **Runs** the benchmark (seeds data, executes queries, measures performance)
6. **Suggests** index improvements based on query performance
7. **Offers** to re-run with alternative indexes for side-by-side comparison
