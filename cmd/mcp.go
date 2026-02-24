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

func runMCP(_ *cobra.Command, _ []string) error {
	server := mcp.NewServer(
		&mcp.Implementation{
			Name:    "go-seed-my-db",
			Version: version.Version(),
		},
		nil,
	)

	mcptools.RegisterAll(server)

	if err := server.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		return fmt.Errorf("MCP server error: %w", err)
	}
	return nil
}
