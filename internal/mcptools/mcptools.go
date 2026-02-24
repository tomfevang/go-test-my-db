package mcptools

import "github.com/modelcontextprotocol/go-sdk/mcp"

// RegisterAll registers all go-seed-my-db tools on the given MCP server.
func RegisterAll(s *mcp.Server) {
	registerListTables(s)
	registerDescribeTable(s)
	registerPreviewData(s)
	registerGenerateConfig(s)
	registerSeedDatabase(s)
	registerCompare(s)
}
