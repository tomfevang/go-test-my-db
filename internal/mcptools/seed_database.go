package mcptools

import (
	"context"
	"strconv"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type seedDatabaseArgs struct {
	Tables       []string `json:"tables,omitempty" jsonschema:"Tables to seed. If omitted, seeds all tables in FK-safe order."`
	Rows         int      `json:"rows,omitempty" jsonschema:"Base row count per root table (default 1000)."`
	BatchSize    int      `json:"batch_size,omitempty" jsonschema:"Rows per INSERT statement (default 1000)."`
	Workers      int      `json:"workers,omitempty" jsonschema:"Concurrent insert workers (default 4)."`
	Clear        bool     `json:"clear,omitempty" jsonschema:"Truncate target tables before seeding."`
	MinChildren  int      `json:"min_children,omitempty" jsonschema:"Min children per parent row (default 10)."`
	MaxChildren  int      `json:"max_children,omitempty" jsonschema:"Max children per parent row (default 100)."`
	MaxRows      int      `json:"max_rows,omitempty" jsonschema:"Maximum rows per table safeguard (default 10000000)."`
	DeferIndexes bool     `json:"defer_indexes,omitempty" jsonschema:"Drop secondary indexes before seeding and rebuild after (faster for large tables)."`
	ConfigPath   string   `json:"config_path,omitempty" jsonschema:"Path to a go-seed-my-db.yaml config file for custom column generators and references."`
}

func registerSeedDatabase(s *mcp.Server) {
	mcp.AddTool(s, &mcp.Tool{
		Name:        "seed_database",
		Description: "Insert realistic fake data into MySQL tables. Automatically introspects the schema, resolves FK dependencies, and seeds tables in topological order. Call with no arguments to seed all tables with 1000 rows each using auto-detected column heuristics.",
	}, handleSeedDatabase)
}

func handleSeedDatabase(ctx context.Context, _ *mcp.CallToolRequest, args seedDatabaseArgs) (*mcp.CallToolResult, any, error) {
	dsn := resolveDSN()
	if dsn == "" {
		return errResult("SEED_DSN environment variable is not set"), nil, nil
	}

	// Build CLI args.
	cliArgs := []string{"--dsn", dsn}

	for _, t := range args.Tables {
		cliArgs = append(cliArgs, "--table", t)
	}
	if args.Rows > 0 {
		cliArgs = append(cliArgs, "--rows", strconv.Itoa(args.Rows))
	}
	if args.BatchSize > 0 {
		cliArgs = append(cliArgs, "--batch-size", strconv.Itoa(args.BatchSize))
	}
	if args.Workers > 0 {
		cliArgs = append(cliArgs, "--workers", strconv.Itoa(args.Workers))
	}
	if args.Clear {
		cliArgs = append(cliArgs, "--clear")
	}
	if args.MinChildren > 0 {
		cliArgs = append(cliArgs, "--min-children", strconv.Itoa(args.MinChildren))
	}
	if args.MaxChildren > 0 {
		cliArgs = append(cliArgs, "--max-children", strconv.Itoa(args.MaxChildren))
	}
	if args.MaxRows > 0 {
		cliArgs = append(cliArgs, "--max-rows", strconv.Itoa(args.MaxRows))
	}
	if args.DeferIndexes {
		cliArgs = append(cliArgs, "--defer-indexes")
	}
	if args.ConfigPath != "" {
		cliArgs = append(cliArgs, "--config", args.ConfigPath)
	}

	output, err := runSelf(ctx, cliArgs...)
	if err != nil {
		return errResult("seeding failed: " + err.Error()), nil, nil
	}

	return textResult(output), nil, nil
}
