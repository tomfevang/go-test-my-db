package mcptools

import (
	"context"
	"strconv"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type testArgs struct {
	ConfigPath string `json:"config_path" jsonschema:"Path to a go-seed-my-db.yaml config file. Must have options.schema pointing to a DDL file and a tests section with benchmark queries."`
	Rows       int    `json:"rows,omitempty" jsonschema:"Override rows per table (0 = use config value or default 1000)."`
	BatchSize  int    `json:"batch_size,omitempty" jsonschema:"Rows per INSERT statement (0 = use config value or default 1000)."`
	Workers    int    `json:"workers,omitempty" jsonschema:"Concurrent insert workers (0 = use config value or default 4)."`
}

func registerTest(s *mcp.Server) {
	mcp.AddTool(s, &mcp.Tool{
		Name: "test",
		Description: `Benchmark query performance for a single schema configuration.

Creates tables from DDL, seeds with fake data, runs test queries, drops the tables, and returns timing results. Use this when benchmarking one schema. Use the compare tool instead when comparing two or more alternative schemas side-by-side.`,
	}, handleTest)
}

func handleTest(ctx context.Context, _ *mcp.CallToolRequest, args testArgs) (*mcp.CallToolResult, any, error) {
	if args.ConfigPath == "" {
		return errResult("config_path is required: provide a seed config YAML file"), nil, nil
	}

	cliArgs := []string{"test", "--config", args.ConfigPath}

	dsn := resolveDSN()
	if dsn != "" {
		cliArgs = append(cliArgs, "--dsn", dsn)
	} else {
		cliArgs = append(cliArgs, "--ephemeral")
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

	output, err := runSelf(ctx, cliArgs...)
	if err != nil {
		return errResult("test failed: " + err.Error()), nil, nil
	}

	return textResult(output), nil, nil
}
