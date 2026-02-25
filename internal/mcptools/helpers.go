package mcptools

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
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

// runSelf executes the go-test-my-db binary (itself) with the given arguments
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
