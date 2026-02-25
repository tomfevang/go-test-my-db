package mcptools

import (
	"context"
	"fmt"
	"io/fs"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// registerResources registers embedded skill files as MCP resources so that
// AI clients automatically discover them when the server is connected.
func registerResources(s *mcp.Server, embeddedFS fs.ReadFileFS) {
	registerSkillResource(s, embeddedFS,
		"benchmark-migration",
		"Benchmark a database migration",
		"Step-by-step skill for parsing Java migrations (DatabaseUtil.createTable DSL), "+
			"generating DDL, seeding realistic data, running query benchmarks, and suggesting "+
			"index improvements. Read this resource when the user wants to benchmark a migration.",
		"examples/benchmark-migration/SKILL.md",
	)
}

func registerSkillResource(
	s *mcp.Server,
	embeddedFS fs.ReadFileFS,
	name, title, description, path string,
) {
	uri := "skill://" + name

	s.AddResource(&mcp.Resource{
		Name:        name,
		URI:         uri,
		Title:       title,
		Description: description,
		MIMEType:    "text/markdown",
	}, func(_ context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
		data, err := embeddedFS.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("reading embedded skill %s: %w", name, err)
		}
		return &mcp.ReadResourceResult{
			Contents: []*mcp.ResourceContents{
				{
					URI:      req.Params.URI,
					MIMEType: "text/markdown",
					Text:     string(data),
				},
			},
		}, nil
	})
}
