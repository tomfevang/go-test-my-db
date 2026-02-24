package cmd

import (
	"database/sql"
	"fmt"
	"os"
	"strings"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/tomfevang/go-seed-my-db/internal/config"
	"github.com/tomfevang/go-seed-my-db/internal/depgraph"
	"github.com/tomfevang/go-seed-my-db/internal/generator"
	"github.com/tomfevang/go-seed-my-db/internal/introspect"
	"github.com/tomfevang/go-seed-my-db/internal/seeder"
)

var (
	previewDSN         string
	previewSchemaFile  string
	previewConfigPath  string
	previewTables      []string
	previewSampleRows  int
	previewRows        int
	previewBatchSize   int
	previewWorkers     int
	previewMinChildren int
	previewMaxChildren int
	previewMaxRows     int
	previewLoadData     bool
	previewDeferIndexes bool
	previewFKSampleSize int
)

var previewCmd = &cobra.Command{
	Use:   "preview",
	Short: "Preview generated data without affecting existing tables",
	Long: `The preview subcommand shows sample rows for each table to verify data
quality, column heuristics, and relationship structure before running a full seed.

When --schema is provided (or options.schema in the config), preview creates
temporary tables from the DDL file, seeds them, queries sample data, then drops
the tables — just like the test subcommand but for inspecting data instead of
running benchmarks.

Without --schema, preview generates sample rows in-memory using the existing
database schema for introspection.

Child tables are displayed grouped by their parent foreign key so you can
verify that relationships make sense.`,
	RunE: runPreview,
}

func init() {
	previewCmd.Flags().StringVar(&previewDSN, "dsn", "", "MySQL DSN (required), e.g. user:pass@tcp(localhost:3306)/mydb")
	previewCmd.Flags().StringVar(&previewSchemaFile, "schema", "", "Path to SQL DDL file (creates temporary tables)")
	previewCmd.Flags().StringVar(&previewConfigPath, "config", "", "Path to config YAML file (default: auto-detect go-seed-my-db.yaml)")
	previewCmd.Flags().StringSliceVar(&previewTables, "table", nil, "Table(s) to preview (repeatable). If omitted, previews all tables")
	previewCmd.Flags().IntVar(&previewSampleRows, "sample-rows", 5, "Number of sample rows/groups to display per table")
	previewCmd.Flags().IntVar(&previewRows, "rows", 1000, "Base row count for root tables")
	previewCmd.Flags().IntVar(&previewBatchSize, "batch-size", 1000, "Rows per INSERT statement")
	previewCmd.Flags().IntVar(&previewWorkers, "workers", 4, "Concurrent insert workers")
	previewCmd.Flags().IntVar(&previewMinChildren, "min-children", 10, "Min children per parent row for child tables")
	previewCmd.Flags().IntVar(&previewMaxChildren, "max-children", 100, "Max children per parent row for child tables")
	previewCmd.Flags().IntVar(&previewMaxRows, "max-rows", 10_000_000, "Maximum rows per table")
	previewCmd.Flags().BoolVar(&previewLoadData, "load-data", false, "Use LOAD DATA LOCAL INFILE for faster bulk loading (requires server local_infile=ON)")
	previewCmd.Flags().BoolVar(&previewDeferIndexes, "defer-indexes", false, "Drop secondary indexes before seeding and rebuild after (faster for large tables)")
	previewCmd.Flags().IntVar(&previewFKSampleSize, "fk-sample-size", 500_000, "Max FK parent values to cache per column (0 = unlimited)")

	rootCmd.AddCommand(previewCmd)
}

func runPreview(cmd *cobra.Command, args []string) error {
	// Load config.
	cfg, err := config.LoadOrDefault(previewConfigPath)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	// Resolve parameters.
	previewDSN = resolveString(cmd, "dsn", previewDSN, "SEED_DSN", cfg.Options.DSN, "")
	previewSchemaFile = resolveString(cmd, "schema", previewSchemaFile, "", cfg.Options.Schema, "")
	previewRows = resolveInt(cmd, "rows", previewRows, cfg.Options.Rows, 1000)
	previewBatchSize = resolveInt(cmd, "batch-size", previewBatchSize, cfg.Options.BatchSize, 1000)
	previewWorkers = resolveInt(cmd, "workers", previewWorkers, cfg.Options.Workers, 4)
	previewMinChildren = resolveInt(cmd, "min-children", previewMinChildren, cfg.Options.ChildrenPerParent.Min, 10)
	previewMaxChildren = resolveInt(cmd, "max-children", previewMaxChildren, cfg.Options.ChildrenPerParent.Max, 100)
	previewMaxRows = resolveInt(cmd, "max-rows", previewMaxRows, cfg.Options.MaxRows, 10_000_000)
	if !cmd.Flags().Changed("load-data") && cfg.Options.LoadData {
		previewLoadData = true
	}
	if !cmd.Flags().Changed("defer-indexes") && cfg.Options.DeferIndexes {
		previewDeferIndexes = true
	}
	previewFKSampleSize = resolveInt(cmd, "fk-sample-size", previewFKSampleSize, cfg.Options.FKSampleSize, 500_000)

	if previewDSN == "" {
		return fmt.Errorf("DSN is required — set via --dsn flag, SEED_DSN env var, or options.dsn in config file")
	}

	schema := extractSchema(previewDSN)
	if schema == "" {
		return fmt.Errorf("could not extract database name from DSN — ensure it ends with /dbname")
	}

	if previewLoadData {
		previewDSN = ensureAllowAllFiles(previewDSN)
	}

	// Connect to MySQL.
	db, err := sql.Open("mysql", previewDSN)
	if err != nil {
		return fmt.Errorf("connecting to MySQL: %w", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(previewWorkers + 2)
	if err := db.Ping(); err != nil {
		return fmt.Errorf("pinging MySQL: %w", err)
	}
	fmt.Printf("Connected to %s\n", schema)

	if previewSchemaFile != "" {
		return runPreviewWithSchema(db, schema, cfg)
	}
	return runPreviewInMemory(db, schema, cfg)
}

// runPreviewWithSchema creates temporary tables from a DDL file, seeds them,
// queries sample data, and drops everything — like the test pipeline but for
// inspecting generated data.
func runPreviewWithSchema(db *sql.DB, schema string, cfg *config.Config) error {
	// Parse DDL file.
	statements, tableNames, err := parseDDLFile(previewSchemaFile)
	if err != nil {
		return fmt.Errorf("parsing schema file: %w", err)
	}
	if len(tableNames) == 0 {
		return fmt.Errorf("no CREATE TABLE statements found in %s", previewSchemaFile)
	}

	// Create tables.
	if err := createTables(db, tableNames, statements); err != nil {
		return fmt.Errorf("creating tables: %w", err)
	}
	fmt.Printf("Created %d tables from %s\n", len(tableNames), previewSchemaFile)

	// Guarantee cleanup.
	defer func() {
		dropTables(db, tableNames)
		fmt.Println("Cleaned up: dropped preview tables")
	}()

	// Introspect.
	allTables := make(map[string]*introspect.Table, len(tableNames))
	for _, name := range tableNames {
		t, err := introspect.IntrospectTable(db, schema, name)
		if err != nil {
			return fmt.Errorf("introspecting %s: %w", name, err)
		}
		allTables[name] = t
	}

	// Apply config-declared references.
	if refs := cfg.GetReferences(); refs != nil {
		introspect.ApplyReferences(allTables, refs)
	}

	// Determine which tables to seed.
	seedTableNames := tableNames
	if len(previewTables) > 0 {
		for _, n := range previewTables {
			if _, ok := allTables[n]; !ok {
				return fmt.Errorf("table %q not found in schema file", n)
			}
		}
		seedTableNames = previewTables
	} else if len(cfg.Options.SeedTables) > 0 {
		seedTableNames = cfg.Options.SeedTables
	}

	// Resolve FK ordering.
	requestedTables := make(map[string]*introspect.Table, len(seedTableNames))
	for _, name := range seedTableNames {
		requestedTables[name] = allTables[name]
	}

	order, autoIncluded, relations, err := depgraph.Resolve(requestedTables, allTables)
	if err != nil {
		return err
	}
	if len(autoIncluded) > 0 {
		fmt.Printf("Auto-included parent tables: %s\n", strings.Join(autoIncluded, ", "))
	}

	orderedTables := make([]*introspect.Table, len(order))
	for i, name := range order {
		orderedTables[i] = requestedTables[name]
	}

	// Compute row counts.
	rowCounts := computeRowCounts(order, relations, cfg, previewRows, previewMinChildren, previewMaxChildren, previewMaxRows)

	// Seed tables.
	fmt.Printf("Seeding %d tables...\n", len(orderedTables))
	if err := seeder.SeedAll(seeder.Config{
		DB:           db,
		Schema:       schema,
		Tables:       orderedTables,
		RowsPerTable: rowCounts,
		BatchSize:    previewBatchSize,
		Workers:      previewWorkers,
		Clear:        false,
		LoadData:     previewLoadData,
		DeferIndexes: previewDeferIndexes,
		GenConfig:    cfg,
		FKSampleSize: previewFKSampleSize,
	}); err != nil {
		return fmt.Errorf("seeding tables: %w", err)
	}

	// Query and display sample data from each table.
	fmt.Println()
	for _, tableName := range order {
		table := requestedTables[tableName]
		rc := rowCounts[tableName]
		parents := relations.Parents[tableName]

		label := "root"
		if len(parents) > 0 {
			label = "child of " + strings.Join(parents, ", ")
		}
		fmt.Printf("=== %s (%s, %d rows) ===\n\n", tableName, label, rc)

		// Find the first FK column pointing to a parent in the seed set.
		var groupByFK *introspect.Column
		if len(parents) > 0 {
			for i, col := range table.Columns {
				if col.FK == nil {
					continue
				}
				for _, p := range parents {
					if col.FK.ReferencedTable == p {
						groupByFK = &table.Columns[i]
						break
					}
				}
				if groupByFK != nil {
					break
				}
			}
		}

		if groupByFK != nil {
			if err := printChildTableGrouped(db, tableName, groupByFK, previewSampleRows); err != nil {
				fmt.Fprintf(os.Stderr, "  error querying %s: %v\n\n", tableName, err)
			}
		} else {
			if err := printTableSample(db, tableName, previewSampleRows); err != nil {
				fmt.Fprintf(os.Stderr, "  error querying %s: %v\n\n", tableName, err)
			}
		}
	}

	return nil
}

// printTableSample queries and prints N sample rows from a table (flat view).
func printTableSample(db *sql.DB, tableName string, limit int) error {
	rows, err := db.Query(fmt.Sprintf("SELECT * FROM `%s` LIMIT %d", tableName, limit))
	if err != nil {
		return err
	}
	defer rows.Close()

	printResultSet(rows, "  ")
	fmt.Println()
	return nil
}

// printChildTableGrouped displays child rows grouped by parent FK values.
func printChildTableGrouped(db *sql.DB, tableName string, fkCol *introspect.Column, limit int) error {
	// Get sample parent IDs.
	parentIDs, err := fetchSamplePKs(db, fkCol.FK.ReferencedTable, fkCol.FK.ReferencedColumn, limit)
	if err != nil {
		return err
	}

	for i, pid := range parentIDs {
		if i > 0 {
			fmt.Println()
		}
		fmt.Printf("  %s = %s:\n", fkCol.Name, formatPreviewValue(pid))

		rows, err := db.Query(
			fmt.Sprintf("SELECT * FROM `%s` WHERE `%s` = ? LIMIT %d", tableName, fkCol.Name, limit),
			pid,
		)
		if err != nil {
			fmt.Fprintf(os.Stderr, "    error: %v\n", err)
			continue
		}
		printResultSet(rows, "    ")
		rows.Close()
	}
	fmt.Println()
	return nil
}

// fetchSamplePKs returns the first N PK values from a table.
func fetchSamplePKs(db *sql.DB, table, column string, limit int) ([]any, error) {
	rows, err := db.Query(fmt.Sprintf("SELECT `%s` FROM `%s` ORDER BY `%s` LIMIT %d", column, table, column, limit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var vals []any
	for rows.Next() {
		var v any
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		vals = append(vals, v)
	}
	return vals, rows.Err()
}

// printResultSet writes a tabwriter table from a *sql.Rows. Does not close rows.
func printResultSet(rows *sql.Rows, indent string) {
	colNames, err := rows.Columns()
	if err != nil {
		fmt.Fprintf(os.Stderr, "%serror reading columns: %v\n", indent, err)
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)

	fmt.Fprintf(w, "%s%s\n", indent, strings.Join(colNames, "\t"))
	seps := make([]string, len(colNames))
	for i, h := range colNames {
		seps[i] = strings.Repeat("-", len(h))
	}
	fmt.Fprintf(w, "%s%s\n", indent, strings.Join(seps, "\t"))

	scanDest := make([]any, len(colNames))
	scanPtrs := make([]any, len(colNames))
	for i := range scanDest {
		scanPtrs[i] = &scanDest[i]
	}

	for rows.Next() {
		if err := rows.Scan(scanPtrs...); err != nil {
			fmt.Fprintf(os.Stderr, "%serror scanning: %v\n", indent, err)
			return
		}
		vals := make([]string, len(colNames))
		for i, v := range scanDest {
			vals[i] = formatPreviewValue(v)
		}
		fmt.Fprintf(w, "%s%s\n", indent, strings.Join(vals, "\t"))
	}

	w.Flush()
}

// runPreviewInMemory generates sample rows in-memory using the existing
// database schema for introspection (no tables are created or modified).
func runPreviewInMemory(db *sql.DB, schema string, cfg *config.Config) error {
	// Determine which tables to preview.
	var tableNames []string
	var err error
	if len(previewTables) > 0 {
		tableNames = previewTables
	} else if len(cfg.Options.SeedTables) > 0 {
		tableNames = cfg.Options.SeedTables
	} else {
		tableNames, err = introspect.ListTables(db, schema)
		if err != nil {
			return err
		}
		if len(tableNames) == 0 {
			return fmt.Errorf("no tables found in schema %s", schema)
		}
	}

	// Introspect all tables.
	allTableNames, err := introspect.ListTables(db, schema)
	if err != nil {
		return err
	}
	allTables := make(map[string]*introspect.Table, len(allTableNames))
	for _, name := range allTableNames {
		t, err := introspect.IntrospectTable(db, schema, name)
		if err != nil {
			return err
		}
		allTables[name] = t
	}

	// Apply config-declared references.
	if refs := cfg.GetReferences(); refs != nil {
		introspect.ApplyReferences(allTables, refs)
	}

	// Build requested table set.
	requestedTables := make(map[string]*introspect.Table, len(tableNames))
	for _, name := range tableNames {
		t, ok := allTables[name]
		if !ok {
			return fmt.Errorf("table %q not found in schema %s", name, schema)
		}
		requestedTables[name] = t
	}

	// Resolve FK dependencies.
	order, autoIncluded, relations, err := depgraph.Resolve(requestedTables, allTables)
	if err != nil {
		return err
	}
	if len(autoIncluded) > 0 {
		fmt.Printf("Auto-included parent tables: %s\n", strings.Join(autoIncluded, ", "))
	}

	// Compute row counts.
	rowCounts := computeRowCounts(order, relations, cfg, previewRows, previewMinChildren, previewMaxChildren, previewMaxRows)

	// Generate and display sample rows for each table.
	fkCache := make(map[string][]any)

	n := previewSampleRows

	for _, tableName := range order {
		table := requestedTables[tableName]
		rc := rowCounts[tableName]
		parents := relations.Parents[tableName]

		// Build FK values from previously generated parents.
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

		// Starting IDs for non-auto-inc integer PKs.
		pkStartValues := make(map[string]int64)
		for _, col := range table.Columns {
			if col.IsPrimaryKey && !col.IsAutoInc && col.IsIntegerType() {
				pkStartValues[col.Name] = 1
			}
		}

		// Find the FK column to group by (first FK to a parent in the seed set).
		var groupByFK *introspect.Column
		if len(parents) > 0 {
			for i, col := range table.Columns {
				if col.FK == nil {
					continue
				}
				for _, p := range parents {
					if col.FK.ReferencedTable == p {
						groupByFK = &table.Columns[i]
						break
					}
				}
				if groupByFK != nil {
					break
				}
			}
		}

		// Generate rows — more for child tables so we get meaningful groups.
		genCount := n
		if groupByFK != nil {
			genCount = n * n * 4 // e.g., 100 rows to group across 5 parents
		}

		gen := generator.NewRowGenerator(table, tableFKValues, cfg, pkStartValues, nil, nil)
		genCols := gen.Columns()
		sampleRows := make([][]any, genCount)
		for i := range sampleRows {
			sampleRows[i] = gen.GenerateRow()
		}

		// Build column names (all except generated).
		var colNames []string
		for _, col := range table.Columns {
			if col.IsGenerated {
				continue
			}
			colNames = append(colNames, col.Name)
		}

		// Convert generated rows to display rows (with auto-inc values merged in).
		toDisplayRow := func(row []any, rowIdx int) []string {
			display := make([]string, len(colNames))
			colPos := 0
			genIdx := 0
			for _, col := range table.Columns {
				if col.IsGenerated {
					continue
				}
				if col.IsAutoInc {
					display[colPos] = fmt.Sprintf("%d", rowIdx+1)
				} else if genIdx < len(row) {
					display[colPos] = formatPreviewValue(row[genIdx])
					genIdx++
				}
				colPos++
			}
			return display
		}

		// Cache PK values for downstream FK references.
		for _, col := range table.Columns {
			if !col.IsPrimaryKey {
				continue
			}
			key := tableName + "." + col.Name
			if col.IsAutoInc {
				vals := make([]any, genCount)
				for i := range vals {
					vals[i] = int64(i + 1)
				}
				fkCache[key] = vals
			} else {
				for gi, gc := range genCols {
					if gc == col.Name {
						vals := make([]any, genCount)
						for i := range sampleRows {
							vals[i] = sampleRows[i][gi]
						}
						fkCache[key] = vals
						break
					}
				}
			}
		}

		// Print header.
		label := "root"
		if len(parents) > 0 {
			label = "child of " + strings.Join(parents, ", ")
		}
		fmt.Printf("\n=== %s (%s, %d rows) ===\n\n", tableName, label, rc)

		if groupByFK != nil {
			// Find the FK column index within genCols.
			fkGenIdx := -1
			for gi, gc := range genCols {
				if gc == groupByFK.Name {
					fkGenIdx = gi
					break
				}
			}

			if fkGenIdx < 0 {
				// FK column not in generated columns; fall back to flat display.
				printInMemoryFlat(colNames, sampleRows[:n], toDisplayRow)
			} else {
				// Group rows by FK value.
				type group struct {
					key  any
					rows []int // indices into sampleRows
				}
				var groups []group
				seen := make(map[any]int) // FK value -> index in groups
				for i, row := range sampleRows {
					fkVal := row[fkGenIdx]
					keyStr := fmt.Sprintf("%v", fkVal)
					if idx, ok := seen[keyStr]; ok {
						groups[idx].rows = append(groups[idx].rows, i)
					} else {
						seen[keyStr] = len(groups)
						groups = append(groups, group{key: fkVal, rows: []int{i}})
					}
				}

				// Show up to N groups, each with up to N rows.
				shown := 0
				for _, g := range groups {
					if shown >= n {
						break
					}
					if shown > 0 {
						fmt.Println()
					}
					fmt.Printf("  %s = %s:\n", groupByFK.Name, formatPreviewValue(g.key))

					w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
					fmt.Fprintf(w, "    %s\n", strings.Join(colNames, "\t"))
					seps := make([]string, len(colNames))
					for si, h := range colNames {
						seps[si] = strings.Repeat("-", len(h))
					}
					fmt.Fprintf(w, "    %s\n", strings.Join(seps, "\t"))

					limit := n
					if len(g.rows) < limit {
						limit = len(g.rows)
					}
					for _, ri := range g.rows[:limit] {
						display := toDisplayRow(sampleRows[ri], ri)
						fmt.Fprintf(w, "    %s\n", strings.Join(display, "\t"))
					}
					w.Flush()
					shown++
				}
			}
		} else {
			printInMemoryFlat(colNames, sampleRows[:n], toDisplayRow)
		}
	}

	fmt.Println()
	return nil
}

// printInMemoryFlat prints a flat table from in-memory generated rows.
func printInMemoryFlat(colNames []string, rows [][]any, toDisplay func([]any, int) []string) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "  %s\n", strings.Join(colNames, "\t"))
	seps := make([]string, len(colNames))
	for i, h := range colNames {
		seps[i] = strings.Repeat("-", len(h))
	}
	fmt.Fprintf(w, "  %s\n", strings.Join(seps, "\t"))
	for i, row := range rows {
		display := toDisplay(row, i)
		fmt.Fprintf(w, "  %s\n", strings.Join(display, "\t"))
	}
	w.Flush()
}

func formatPreviewValue(v any) string {
	if v == nil {
		return "NULL"
	}
	switch val := v.(type) {
	case []byte:
		s := string(val)
		for _, r := range s {
			if r < 32 && r != '\n' && r != '\r' && r != '\t' {
				return fmt.Sprintf("[%d bytes]", len(val))
			}
		}
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
