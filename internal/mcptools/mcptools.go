package mcptools

import (
	"io/fs"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// RegisterAll registers all go-seed-my-db tools and resources on the given MCP server.
// The embeddedFS provides access to embedded skill files served as MCP resources.
func RegisterAll(s *mcp.Server, embeddedFS fs.ReadFileFS) {
	registerListTables(s)
	registerDescribeTable(s)
	registerPreviewData(s)
	registerGenerateConfig(s)
	registerSeedDatabase(s)
	registerTest(s)
	registerCompare(s)

	registerResources(s, embeddedFS)
}
