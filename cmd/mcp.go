package cmd

import (
	"context"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/spf13/cobra"

	"github.com/tomfevang/go-seed-my-db/internal/mcptools"
	"github.com/tomfevang/go-seed-my-db/internal/version"
)

var mcpCmd = &cobra.Command{
	Use:   "mcp",
	Short: "Start an MCP stdio server for use with Claude Code and other AI tools",
	Long: `The mcp subcommand starts a Model Context Protocol server that communicates
over stdin/stdout using JSON-RPC. This allows AI tools like Claude Code to
introspect schemas, preview data, seed databases, and compare performance.

Configure in .claude/settings.json:

  "mcpServers": {
    "seed-my-db": {
      "command": "go-seed-my-db",
      "args": ["mcp"],
      "env": { "SEED_DSN": "user:pass@tcp(localhost:3306)/mydb" }
    }
  }`,
	RunE: runMCP,
}

func init() {
	rootCmd.AddCommand(mcpCmd)
}

const mcpInstructions = `go-seed-my-db seeds MySQL databases with realistic fake data for performance testing.

## Connection

The MySQL DSN is pre-configured via the SEED_DSN environment variable. You do NOT need to ask the user for connection details — just call the tools directly.

## Workflow

1. **list_tables** → see what tables exist and their FK relationships
2. **describe_table** → inspect column types, indexes, and constraints for a specific table
3. **preview_data** → dry-run: see sample rows the seeder would generate (no writes)
4. **generate_config** → scaffold a go-seed-my-db.yaml config from the live schema
5. **seed_database** → insert fake data into the database
6. **compare** → benchmark query performance across different schema configs

Start with list_tables to orient yourself, then use the other tools as needed. Most tools work without any arguments.`

func runMCP(_ *cobra.Command, _ []string) error {
	server := mcp.NewServer(
		&mcp.Implementation{
			Name:    "go-seed-my-db",
			Version: version.Version(),
		},
		&mcp.ServerOptions{
			Instructions: mcpInstructions,
		},
	)

	mcptools.RegisterAll(server)

	if err := server.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		return fmt.Errorf("MCP server error: %w", err)
	}
	return nil
}
