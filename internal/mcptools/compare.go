package mcptools

import (
	"context"
	"strconv"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type compareArgs struct {
	ConfigPath string `json:"config_path" jsonschema:"Path to a comparison YAML file. The file must have a 'configs' key listing seed config files (each with label and file) and a 'tests' key with per-config query variants (each test has a name, repeat count, and a queries map keyed by config label)."`
	Rows       int    `json:"rows,omitempty" jsonschema:"Override rows per table for all configs (0 = use each config's value)."`
	BatchSize  int    `json:"batch_size,omitempty" jsonschema:"Override batch size for all configs (0 = use each config's value)."`
	Workers    int    `json:"workers,omitempty" jsonschema:"Override worker count for all configs (0 = use each config's value)."`
}

func registerCompare(s *mcp.Server) {
	mcp.AddTool(s, &mcp.Tool{
		Name: "compare",
		Description: `Compare query performance across multiple schema configurations side-by-side.

Takes a single comparison YAML file that references 2+ seed configs and defines per-config query variants. For each config: creates tables from DDL, seeds with fake data, runs test queries, drops the tables, then returns a side-by-side timing comparison.

For benchmarking a single schema without comparison, use the test tool instead.`,
	}, handleCompare)
}

func handleCompare(ctx context.Context, _ *mcp.CallToolRequest, args compareArgs) (*mcp.CallToolResult, any, error) {
	if args.ConfigPath == "" {
		return errResult("config_path is required: provide a comparison YAML file"), nil, nil
	}

	cliArgs := []string{"compare", args.ConfigPath}

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
		return errResult("compare failed: " + err.Error()), nil, nil
	}

	return textResult(output), nil, nil
}
