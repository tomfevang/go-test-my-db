package mcptools

import (
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/tomfevang/go-seed-my-db/internal/config"
	"github.com/tomfevang/go-seed-my-db/internal/depgraph"
)

// resolveDSN returns the DSN from the SEED_DSN environment variable.
func resolveDSN() string {
	return os.Getenv("SEED_DSN")
}

// extractSchema extracts the database name from a MySQL DSN.
func extractSchema(dsn string) string {
	idx := strings.LastIndex(dsn, "/")
	if idx == -1 || idx == len(dsn)-1 {
		return ""
	}
	schema := dsn[idx+1:]
	if qIdx := strings.Index(schema, "?"); qIdx != -1 {
		schema = schema[:qIdx]
	}
	return schema
}

// textResult builds a CallToolResult with a single TextContent.
func textResult(text string) *mcp.CallToolResult {
	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: text},
		},
	}
}

// errResult builds a CallToolResult that reports an error.
func errResult(msg string) *mcp.CallToolResult {
	return &mcp.CallToolResult{
		Content: []mcp.Content{
			&mcp.TextContent{Text: msg},
		},
		IsError: true,
	}
}

// computeRowCounts determines how many rows to generate for each table.
// Root tables get the base row count. Child tables get parent_rows * random_multiplier.
func computeRowCounts(
	order []string,
	relations *depgraph.TableRelations,
	cfg *config.Config,
	baseRows, minC, maxC, maxRowsCap int,
) map[string]int {
	rowCounts := make(map[string]int, len(order))
	for _, tableName := range order {
		if tc, ok := cfg.Tables[tableName]; ok && tc.Rows > 0 {
			rowCounts[tableName] = tc.Rows
			continue
		}
		parents := relations.Parents[tableName]
		if len(parents) == 0 {
			rowCounts[tableName] = baseRows
			continue
		}
		maxParentRows := 0
		for _, parent := range parents {
			if pr, ok := rowCounts[parent]; ok && pr > maxParentRows {
				maxParentRows = pr
			}
		}
		multiplier := minC
		if maxC > minC {
			multiplier = minC + rand.IntN(maxC-minC+1)
		}
		computed := maxParentRows * multiplier
		if computed > maxRowsCap {
			computed = maxRowsCap
		}
		rowCounts[tableName] = computed
	}
	return rowCounts
}

// runSelf executes the go-seed-my-db binary (itself) with the given arguments
// and returns its stdout output.
func runSelf(ctx context.Context, args ...string) (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("finding executable: %w", err)
	}
	cmd := exec.CommandContext(ctx, exe, args...)
	cmd.Env = os.Environ()
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("%w\n%s", err, stderr.String())
	}
	return stdout.String(), nil
}
