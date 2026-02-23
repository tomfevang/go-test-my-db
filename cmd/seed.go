package cmd

import (
	"database/sql"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/spf13/cobra"

	"github.com/tomfevang/go-seed-my-db/internal/config"
	"github.com/tomfevang/go-seed-my-db/internal/depgraph"
	"github.com/tomfevang/go-seed-my-db/internal/introspect"
	"github.com/tomfevang/go-seed-my-db/internal/seeder"
)

var (
	dsn         string
	tables      []string
	rows        int
	batchSize   int
	workers     int
	clear       bool
	loadData    bool
	configPath  string
	minChildren int
	maxChildren int
	maxRows     int
)

var rootCmd = &cobra.Command{
	Use:   "go-seed-my-db",
	Short: "Seed MySQL databases with realistic fake data",
	Long: `go-seed-my-db introspects your MySQL schema and generates millions of rows
of realistic fake data based on column names and types. Perfect for
performance testing with believable datasets.`,
	RunE: runSeed,
}

func init() {
	rootCmd.Flags().StringVar(&dsn, "dsn", "", "MySQL DSN (required), e.g. user:pass@tcp(localhost:3306)/mydb")
	rootCmd.Flags().StringSliceVar(&tables, "table", nil, "Table(s) to seed (repeatable). If omitted, seeds all tables")
	rootCmd.Flags().IntVar(&rows, "rows", 1000, "Number of rows per table")
	rootCmd.Flags().IntVar(&batchSize, "batch-size", 1000, "Rows per INSERT statement")
	rootCmd.Flags().IntVar(&workers, "workers", 4, "Concurrent insert workers")
	rootCmd.Flags().BoolVar(&clear, "clear", false, "Truncate target tables before seeding")
	rootCmd.Flags().StringVar(&configPath, "config", "", "Path to config YAML file (default: auto-detect go-seed-my-db.yaml)")
	rootCmd.Flags().BoolVar(&loadData, "load-data", false, "Use LOAD DATA LOCAL INFILE for faster bulk loading (requires server local_infile=ON)")
	rootCmd.Flags().IntVar(&minChildren, "min-children", 10, "Min children per parent row for child tables")
	rootCmd.Flags().IntVar(&maxChildren, "max-children", 100, "Max children per parent row for child tables")
	rootCmd.Flags().IntVar(&maxRows, "max-rows", 10_000_000, "Maximum rows per table (safeguard for deep hierarchies)")
}

func Execute() error {
	return rootCmd.Execute()
}

func runSeed(cmd *cobra.Command, args []string) error {
	start := time.Now()

	// Load config file.
	cfg, err := config.LoadOrDefault(configPath)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	// Resolve operational parameters: CLI flag > env var > config > default.
	dsn = resolveString(cmd, "dsn", dsn, "SEED_DSN", cfg.Options.DSN, "")
	rows = resolveInt(cmd, "rows", rows, cfg.Options.Rows, 1000)
	batchSize = resolveInt(cmd, "batch-size", batchSize, cfg.Options.BatchSize, 1000)
	workers = resolveInt(cmd, "workers", workers, cfg.Options.Workers, 4)
	minChildren = resolveInt(cmd, "min-children", minChildren, cfg.Options.ChildrenPerParent.Min, 10)
	maxChildren = resolveInt(cmd, "max-children", maxChildren, cfg.Options.ChildrenPerParent.Max, 100)
	maxRows = resolveInt(cmd, "max-rows", maxRows, cfg.Options.MaxRows, 10_000_000)
	if !cmd.Flags().Changed("load-data") && cfg.Options.LoadData {
		loadData = true
	}

	if dsn == "" {
		return fmt.Errorf("DSN is required — set via --dsn flag, SEED_DSN env var, or options.dsn in config file")
	}

	// Extract schema name from DSN. The DSN format is user:pass@tcp(host:port)/dbname
	schema := extractSchema(dsn)
	if schema == "" {
		return fmt.Errorf("could not extract database name from DSN — ensure it ends with /dbname")
	}

	// Enable allowAllFiles in the DSN when using LOAD DATA mode.
	if loadData {
		dsn = ensureAllowAllFiles(dsn)
	}

	// Connect to MySQL.
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("connecting to MySQL: %w", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(workers + 2) // workers + main thread + FK queries
	if err := db.Ping(); err != nil {
		return fmt.Errorf("pinging MySQL: %w", err)
	}
	fmt.Printf("Connected to %s\n", schema)

	// Determine which tables to seed: CLI flag > config > all tables.
	var tableNames []string
	if len(tables) > 0 {
		tableNames = tables
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
		fmt.Printf("Found %d tables\n", len(tableNames))
	}

	// Introspect all tables in the database (needed for FK resolution).
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

	// Apply config-declared references (logical FKs without actual constraints).
	if refs := cfg.GetReferences(); refs != nil {
		introspect.ApplyReferences(allTables, refs)
	}

	// Build the requested table set.
	requestedTables := make(map[string]*introspect.Table, len(tableNames))
	for _, name := range tableNames {
		t, ok := allTables[name]
		if !ok {
			return fmt.Errorf("table %q not found in schema %s", name, schema)
		}
		requestedTables[name] = t
	}

	// Resolve FK dependencies (topological order, auto-include parents).
	order, autoIncluded, relations, err := depgraph.Resolve(requestedTables, allTables)
	if err != nil {
		return err
	}

	if len(autoIncluded) > 0 {
		fmt.Printf("Auto-included parent tables: %s\n", strings.Join(autoIncluded, ", "))
	}

	// Build ordered table slice.
	orderedTables := make([]*introspect.Table, len(order))
	for i, name := range order {
		orderedTables[i] = requestedTables[name]
	}

	// Compute per-table row counts.
	rowCounts := computeRowCounts(order, relations, cfg, rows, minChildren, maxChildren, maxRows)

	totalRowCount := 0
	fmt.Printf("Seeding %d tables (%d workers, batch size %d):\n", len(orderedTables), workers, batchSize)
	for _, t := range orderedTables {
		rc := rowCounts[t.Name]
		totalRowCount += rc
		if parents := relations.Parents[t.Name]; len(parents) > 0 {
			fmt.Printf("  %-30s %10d rows (child of %s)\n", t.Name, rc, strings.Join(parents, ", "))
		} else {
			fmt.Printf("  %-30s %10d rows (root)\n", t.Name, rc)
		}
	}
	fmt.Println()

	// Seed!
	if err := seeder.SeedAll(seeder.Config{
		DB:           db,
		Schema:       schema,
		Tables:       orderedTables,
		RowsPerTable: rowCounts,
		BatchSize:    batchSize,
		Workers:      workers,
		Clear:        clear,
		LoadData:     loadData,
		GenConfig:    cfg,
	}); err != nil {
		return err
	}

	elapsed := time.Since(start)
	fmt.Printf("\nDone! Inserted %d total rows across %d tables in %s\n",
		totalRowCount, len(orderedTables), elapsed.Round(time.Millisecond))

	return nil
}

// computeRowCounts determines how many rows to generate for each table.
// Root tables (no FK parents in the seed set) get the base row count.
// Child tables get parent_rows * random_multiplier from [minC, maxC].
// Per-table config overrides take highest priority.
func computeRowCounts(
	order []string,
	relations *depgraph.TableRelations,
	cfg *config.Config,
	baseRows, minC, maxC, maxRowsCap int,
) map[string]int {
	rowCounts := make(map[string]int, len(order))

	for _, tableName := range order {
		// Priority 1: Per-table config override.
		if tc, ok := cfg.Tables[tableName]; ok && tc.Rows > 0 {
			rowCounts[tableName] = tc.Rows
			continue
		}

		// Priority 2: Compute based on parentage.
		parents := relations.Parents[tableName]
		if len(parents) == 0 {
			rowCounts[tableName] = baseRows
			continue
		}

		// Child table: find the parent with the most rows.
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

// ensureAllowAllFiles appends allowAllFiles=true to the DSN if not already present.
// This is required by the go-sql-driver/mysql driver for LOAD DATA LOCAL INFILE.
func ensureAllowAllFiles(dsn string) string {
	if strings.Contains(dsn, "allowAllFiles") {
		return dsn
	}
	if strings.Contains(dsn, "?") {
		return dsn + "&allowAllFiles=true"
	}
	return dsn + "?allowAllFiles=true"
}

func extractSchema(dsn string) string {
	// DSN format: user:pass@tcp(host:port)/dbname?params
	idx := strings.LastIndex(dsn, "/")
	if idx == -1 || idx == len(dsn)-1 {
		return ""
	}
	schema := dsn[idx+1:]
	// Strip query params.
	if qIdx := strings.Index(schema, "?"); qIdx != -1 {
		schema = schema[:qIdx]
	}
	return schema
}
