package mcptools

import (
	"context"
	"strconv"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

type previewDataArgs struct {
	Tables     []string `json:"tables,omitempty" jsonschema:"Tables to preview. If omitted, previews all tables."`
	SampleRows int      `json:"sample_rows,omitempty" jsonschema:"Number of sample rows per table (default 5, max 20)."`
	ConfigPath string   `json:"config_path,omitempty" jsonschema:"Path to a go-seed-my-db.yaml config file."`
}

func registerPreviewData(s *mcp.Server) {
	mcp.AddTool(s, &mcp.Tool{
		Name: "preview_data",
		Description: `Generate sample rows of fake data for the given tables without modifying the database.
Shows what the seeder would generate, including column heuristics and FK relationships.

When the config file contains options.schema pointing to a DDL file, preview creates
temporary tables from that DDL, seeds them, queries sample data, then drops the tables.
Without a schema file, preview generates sample rows in-memory using the live database schema.`,
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, handlePreviewData)
}

func handlePreviewData(ctx context.Context, _ *mcp.CallToolRequest, args previewDataArgs) (*mcp.CallToolResult, any, error) {
	cliArgs := []string{"preview"}

	dsn := resolveDSN()
	if dsn != "" {
		cliArgs = append(cliArgs, "--dsn", dsn)
	}
	if args.ConfigPath != "" {
		cliArgs = append(cliArgs, "--config", args.ConfigPath)
	}
	for _, t := range args.Tables {
		cliArgs = append(cliArgs, "--table", t)
	}
	if args.SampleRows > 0 {
		cliArgs = append(cliArgs, "--sample-rows", strconv.Itoa(args.SampleRows))
	}

	output, err := runSelf(ctx, cliArgs...)
	if err != nil {
		return errResult("preview failed: " + err.Error()), nil, nil
	}

	return textResult(output), nil, nil
}
