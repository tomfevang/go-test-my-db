package mcptools

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/tomfevang/go-test-my-db/internal/introspect"
)

type listTablesArgs struct{}

func registerListTables(s *mcp.Server) {
	mcp.AddTool(s, &mcp.Tool{
		Name:        "list_tables",
		Description: "List all tables in the connected MySQL database with their foreign key relationships. Takes no arguments — the connection is configured via the SEED_DSN environment variable.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, handleListTables)
}

func handleListTables(_ context.Context, _ *mcp.CallToolRequest, args listTablesArgs) (*mcp.CallToolResult, any, error) {
	dsn := resolveDSN()
	if dsn == "" {
		return errResult("SEED_DSN environment variable is not set"), nil, nil
	}

	schema := extractSchema(dsn)
	if schema == "" {
		return errResult("could not extract database name from DSN"), nil, nil
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return errResult(fmt.Sprintf("connecting to MySQL: %v", err)), nil, nil
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return errResult(fmt.Sprintf("pinging MySQL: %v", err)), nil, nil
	}

	tableNames, err := introspect.ListTables(db, schema)
	if err != nil {
		return errResult(fmt.Sprintf("listing tables: %v", err)), nil, nil
	}

	if len(tableNames) == 0 {
		return textResult(fmt.Sprintf("No tables found in schema %s", schema)), nil, nil
	}

	// Introspect each table to show FK relationships.
	var sb strings.Builder
	fmt.Fprintf(&sb, "Schema: %s\nTables (%d):\n", schema, len(tableNames))
	for _, name := range tableNames {
		t, err := introspect.IntrospectTable(db, schema, name)
		if err != nil {
			fmt.Fprintf(&sb, "  - %s (introspection error: %v)\n", name, err)
			continue
		}
		var fks []string
		for _, col := range t.Columns {
			if col.FK != nil {
				fks = append(fks, fmt.Sprintf("%s -> %s.%s", col.Name, col.FK.ReferencedTable, col.FK.ReferencedColumn))
			}
		}
		if len(fks) > 0 {
			fmt.Fprintf(&sb, "  - %s [FK: %s]\n", name, strings.Join(fks, ", "))
		} else {
			fmt.Fprintf(&sb, "  - %s\n", name)
		}
	}

	return textResult(sb.String()), nil, nil
}
