package mcptools

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	"github.com/tomfevang/go-seed-my-db/internal/config"
	"github.com/tomfevang/go-seed-my-db/internal/depgraph"
	"github.com/tomfevang/go-seed-my-db/internal/generator"
	"github.com/tomfevang/go-seed-my-db/internal/introspect"
)

type previewDataArgs struct {
	DSN        string   `json:"dsn,omitempty" jsonschema:"MySQL DSN. Falls back to SEED_DSN env var if omitted."`
	Tables     []string `json:"tables,omitempty" jsonschema:"Tables to preview. If omitted previews all tables."`
	SampleRows int      `json:"sample_rows,omitempty" jsonschema:"Number of sample rows per table (default 5, max 20)."`
	ConfigPath string   `json:"config_path,omitempty" jsonschema:"Path to a go-seed-my-db.yaml config file."`
}

func registerPreviewData(s *mcp.Server) {
	mcp.AddTool(s, &mcp.Tool{
		Name:        "preview_data",
		Description: "Generate sample rows of fake data for the given tables without modifying the database. Shows what the seeder would generate, including column heuristics and FK relationships.",
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: true},
	}, handlePreviewData)
}

func handlePreviewData(_ context.Context, _ *mcp.CallToolRequest, args previewDataArgs) (*mcp.CallToolResult, struct{}, error) {
	dsn := resolveDSN(args.DSN)
	if dsn == "" {
		return errResult("DSN is required"), struct{}{}, nil
	}

	schema := extractSchema(dsn)
	if schema == "" {
		return errResult("could not extract database name from DSN"), struct{}{}, nil
	}

	sampleRows := args.SampleRows
	if sampleRows <= 0 {
		sampleRows = 5
	}
	if sampleRows > 20 {
		sampleRows = 20
	}

	cfg, err := config.LoadOrDefault(args.ConfigPath)
	if err != nil {
		return errResult(fmt.Sprintf("loading config: %v", err)), struct{}{}, nil
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return errResult(fmt.Sprintf("connecting: %v", err)), struct{}{}, nil
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return errResult(fmt.Sprintf("pinging: %v", err)), struct{}{}, nil
	}

	// Introspect all tables.
	allTableNames, err := introspect.ListTables(db, schema)
	if err != nil {
		return errResult(fmt.Sprintf("listing tables: %v", err)), struct{}{}, nil
	}
	allTables := make(map[string]*introspect.Table, len(allTableNames))
	for _, name := range allTableNames {
		t, err := introspect.IntrospectTable(db, schema, name)
		if err != nil {
			return errResult(fmt.Sprintf("introspecting %s: %v", name, err)), struct{}{}, nil
		}
		allTables[name] = t
	}

	if refs := cfg.GetReferences(); refs != nil {
		introspect.ApplyReferences(allTables, refs)
	}

	// Determine which tables to preview.
	tableNames := args.Tables
	if len(tableNames) == 0 {
		tableNames = allTableNames
	}

	for _, name := range tableNames {
		if _, ok := allTables[name]; !ok {
			return errResult(fmt.Sprintf("table %q not found in schema %s", name, schema)), struct{}{}, nil
		}
	}

	requestedTables := make(map[string]*introspect.Table, len(tableNames))
	for _, name := range tableNames {
		requestedTables[name] = allTables[name]
	}

	order, _, relations, err := depgraph.Resolve(requestedTables, allTables)
	if err != nil {
		return errResult(fmt.Sprintf("resolving dependencies: %v", err)), struct{}{}, nil
	}

	rowCounts := computeRowCounts(order, relations, cfg, 1000, 10, 100, 10_000_000)

	var sb strings.Builder
	fkCache := make(map[string][]any)

	for _, tableName := range order {
		table := requestedTables[tableName]
		rc := rowCounts[tableName]
		parents := relations.Parents[tableName]

		label := "root"
		if len(parents) > 0 {
			label = "child of " + strings.Join(parents, ", ")
		}
		fmt.Fprintf(&sb, "=== %s (%s, %d rows planned) ===\n\n", tableName, label, rc)

		// Show generation strategy per column.
		fmt.Fprintf(&sb, "Columns:\n")
		for _, col := range table.Columns {
			strategy := generator.DescribeGenerator(col, tableName, cfg)
			fmt.Fprintf(&sb, "  %-25s %-15s %s\n", col.Name, col.DataType, strategy)
		}

		// Build FK values from cache.
		tableFKValues := make(map[string][]any)
		for _, col := range table.Columns {
			if col.FK == nil {
				continue
			}
			key := col.FK.ReferencedTable + "." + col.FK.ReferencedColumn
			if vals, ok := fkCache[key]; ok {
				tableFKValues[col.Name] = vals
			}
		}

		pkStartValues := make(map[string]int64)
		for _, col := range table.Columns {
			if col.IsPrimaryKey && !col.IsAutoInc && col.IsIntegerType() {
				pkStartValues[col.Name] = 1
			}
		}

		gen := generator.NewRowGenerator(table, tableFKValues, cfg, pkStartValues, nil, nil)
		genCols := gen.Columns()

		if len(genCols) == 0 {
			fmt.Fprintf(&sb, "\n(no columns to generate)\n\n")
			continue
		}

		// Generate sample rows.
		fmt.Fprintf(&sb, "\nSample rows:\n")
		fmt.Fprintf(&sb, "  %s\n", strings.Join(genCols, " | "))
		fmt.Fprintf(&sb, "  %s\n", strings.Repeat("-", len(strings.Join(genCols, " | "))))

		rows := make([][]any, sampleRows)
		for i := range rows {
			rows[i] = gen.GenerateRow()
		}

		for _, row := range rows {
			vals := make([]string, len(row))
			for i, v := range row {
				vals[i] = formatValue(v)
			}
			fmt.Fprintf(&sb, "  %s\n", strings.Join(vals, " | "))
		}
		sb.WriteString("\n")

		// Cache PK values for downstream FK columns.
		for _, col := range table.Columns {
			if !col.IsPrimaryKey {
				continue
			}
			key := tableName + "." + col.Name
			if col.IsAutoInc {
				vals := make([]any, sampleRows)
				for i := range vals {
					vals[i] = int64(i + 1)
				}
				fkCache[key] = vals
			} else {
				for gi, gc := range genCols {
					if gc == col.Name {
						vals := make([]any, len(rows))
						for i := range rows {
							vals[i] = rows[i][gi]
						}
						fkCache[key] = vals
						break
					}
				}
			}
		}
	}

	return textResult(sb.String()), struct{}{}, nil
}

func formatValue(v any) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case []byte:
		s := string(val)
		if len(s) > 40 {
			return s[:37] + "..."
		}
		return s
	default:
		s := fmt.Sprintf("%v", v)
		if len(s) > 40 {
			return s[:37] + "..."
		}
		return s
	}
}
