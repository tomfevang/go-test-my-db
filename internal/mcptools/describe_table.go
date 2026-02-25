package mcptools

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/tomfevang/go-seed-my-db/internal/introspect"
)

type describeTableArgs struct {
	Table string `json:"table" jsonschema:"Name of the table to describe."`
}

func registerDescribeTable(s *mcp.Server) {
	mcp.AddTool(s, &mcp.Tool{
		Name:        "describe_table",
		Description: "Show detailed column metadata for a single table: data types, nullability, primary keys, auto-increment, foreign keys, generated columns, and unique indexes.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, handleDescribeTable)
}

func handleDescribeTable(_ context.Context, _ *mcp.CallToolRequest, args describeTableArgs) (*mcp.CallToolResult, any, error) {
	dsn := resolveDSN()
	if dsn == "" {
		return errResult("SEED_DSN environment variable is not set"), nil, nil
	}
	if args.Table == "" {
		return errResult("table name is required"), nil, nil
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

	table, err := introspect.IntrospectTable(db, schema, args.Table)
	if err != nil {
		return errResult(fmt.Sprintf("introspecting table: %v", err)), nil, nil
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "Table: %s.%s\n\n", schema, table.Name)
	fmt.Fprintf(&sb, "Columns (%d):\n", len(table.Columns))

	for _, col := range table.Columns {
		var flags []string
		if col.IsPrimaryKey {
			flags = append(flags, "PK")
		}
		if col.IsAutoInc {
			flags = append(flags, "AUTO_INC")
		}
		if col.IsUnique {
			flags = append(flags, "UNIQUE")
		}
		if col.IsNullable {
			flags = append(flags, "NULLABLE")
		}
		if col.IsGenerated {
			flags = append(flags, "GENERATED")
		}
		if col.FK != nil {
			flags = append(flags, fmt.Sprintf("FK->%s.%s", col.FK.ReferencedTable, col.FK.ReferencedColumn))
		}

		flagStr := ""
		if len(flags) > 0 {
			flagStr = " [" + strings.Join(flags, ", ") + "]"
		}

		typeStr := col.ColumnType
		if len(col.EnumValues) > 0 {
			vals := col.EnumValues
			if len(vals) > 8 {
				vals = append(vals[:8], "...")
			}
			typeStr = fmt.Sprintf("enum(%s)", strings.Join(vals, ", "))
		}

		fmt.Fprintf(&sb, "  %-30s %-20s%s\n", col.Name, typeStr, flagStr)
	}

	if len(table.UniqueIndexes) > 0 {
		fmt.Fprintf(&sb, "\nUnique Indexes:\n")
		for _, idx := range table.UniqueIndexes {
			fmt.Fprintf(&sb, "  %s: (%s)\n", idx.Name, strings.Join(idx.Columns, ", "))
		}
	}

	return textResult(sb.String()), nil, nil
}
