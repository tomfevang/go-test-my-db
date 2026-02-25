package mcptools

import (
	"context"
	"strconv"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type compareArgs struct {
	ConfigPaths []string `json:"config_paths" jsonschema:"Config file paths: either a single comparison YAML or 2+ seed config files."`
	Rows        int      `json:"rows,omitempty" jsonschema:"Override rows per table for all configs (0 = use each config's value)."`
	BatchSize   int      `json:"batch_size,omitempty" jsonschema:"Override batch size for all configs (0 = use each config's value)."`
	Workers     int      `json:"workers,omitempty" jsonschema:"Override worker count for all configs (0 = use each config's value)."`
}

func registerCompare(s *mcp.Server) {
	mcp.AddTool(s, &mcp.Tool{
		Name:        "compare",
		Description: "Benchmark query performance across multiple schema configurations. For each config: creates tables from DDL, seeds with fake data, runs test queries, drops the tables, then returns a side-by-side timing comparison. Requires config file paths — either a single comparison YAML or 2+ seed config files.",
	}, handleCompare)
}

func handleCompare(ctx context.Context, _ *mcp.CallToolRequest, args compareArgs) (*mcp.CallToolResult, any, error) {
	if len(args.ConfigPaths) == 0 {
		return errResult("config_paths is required: provide a comparison config or 2+ seed config files"), nil, nil
	}

	cliArgs := []string{"compare"}
	cliArgs = append(cliArgs, args.ConfigPaths...)

	dsn := resolveDSN()
	if dsn != "" {
		cliArgs = append(cliArgs, "--dsn", dsn)
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
