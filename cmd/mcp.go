package cmd

import (
	"context"
	"embed"
	"fmt"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/spf13/cobra"

	"github.com/tomfevang/go-test-my-db/internal/mcptools"
	"github.com/tomfevang/go-test-my-db/internal/version"
)

// SkillsFS holds the embedded skills directory, set by main before Execute().
var SkillsFS embed.FS

var mcpCmd = &cobra.Command{
	Use:   "mcp",
	Short: "Start an MCP stdio server for use with Claude Code and other AI tools",
	Long: `The mcp subcommand starts a Model Context Protocol server that communicates
over stdin/stdout using JSON-RPC. This allows AI tools like Claude Code to
introspect schemas, preview data, seed databases, and compare performance.

Configure in .claude/settings.json:

  "mcpServers": {
    "test-my-db": {
      "command": "go-test-my-db",
      "args": ["mcp"],
      "env": { "SEED_DSN": "user:pass@tcp(localhost:3306)/mydb" }
    }
  }`,
	RunE: runMCP,
}

func init() {
	rootCmd.AddCommand(mcpCmd)
}

const mcpInstructions = `go-test-my-db seeds MySQL databases with realistic fake data for performance testing.

## Connection

The MySQL DSN can be pre-configured via the SEED_DSN environment variable. If SEED_DSN is not set and Docker or Podman is available, the tools automatically start an ephemeral MySQL container — no configuration needed.

## Workflow

1. **list_tables** → see what tables exist and their FK relationships
2. **describe_table** → inspect column types, indexes, and constraints for a specific table
3. **preview_data** → dry-run: see sample rows the seeder would generate (no writes)
4. **generate_config** → scaffold a go-test-my-db.yaml config from the live schema
5. **seed_database** → insert fake data into the database
6. **test** → benchmark query performance for a single schema config
7. **compare** → benchmark and compare query performance across multiple schema configs side-by-side

Use **test** when benchmarking one schema. Use **compare** when comparing alternative schemas (e.g., different index strategies). The compare tool requires a comparison YAML that references 2+ seed configs with per-config query variants.

Start with list_tables to orient yourself, then use the other tools as needed. Most tools work without any arguments.

## Skills (MCP Resources)

This server provides skill resources that guide you through complex workflows:

- **benchmark-migration** (skill://benchmark-migration) — Read this resource when the user wants to benchmark a database migration. It provides a step-by-step workflow for parsing migration files, generating DDL, seeding data, and analyzing query performance.

To use a skill, read the resource and follow its instructions.`

func runMCP(_ *cobra.Command, _ []string) error {
	server := mcp.NewServer(
		&mcp.Implementation{
			Name:    "go-test-my-db",
			Version: version.Version(),
		},
		&mcp.ServerOptions{
			Instructions: mcpInstructions,
		},
	)

	mcptools.RegisterAll(server, SkillsFS)

	if err := server.Run(context.Background(), &mcp.StdioTransport{}); err != nil {
		return fmt.Errorf("MCP server error: %w", err)
	}
	return nil
}
