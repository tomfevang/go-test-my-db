package cmd

import (
	"database/sql"
	"fmt"
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
	dsn        string
	tables     []string
	rows       int
	batchSize  int
	workers    int
	clear      bool
	configPath string
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

	rootCmd.MarkFlagRequired("dsn")
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

	// Extract schema name from DSN. The DSN format is user:pass@tcp(host:port)/dbname
	schema := extractSchema(dsn)
	if schema == "" {
		return fmt.Errorf("could not extract database name from DSN — ensure it ends with /dbname")
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

	// Determine which tables to seed.
	var tableNames []string
	if len(tables) > 0 {
		tableNames = tables
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
	order, autoIncluded, err := depgraph.Resolve(requestedTables, allTables)
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

	fmt.Printf("Seeding %d tables with %d rows each (%d workers, batch size %d)\n\n",
		len(orderedTables), rows, workers, batchSize)

	// Seed!
	if err := seeder.SeedAll(seeder.Config{
		DB:        db,
		Schema:    schema,
		Tables:    orderedTables,
		Rows:      rows,
		BatchSize: batchSize,
		Workers:   workers,
		Clear:     clear,
		GenConfig: cfg,
	}); err != nil {
		return err
	}

	elapsed := time.Since(start)
	totalRows := len(orderedTables) * rows
	fmt.Printf("\nDone! Inserted %d total rows across %d tables in %s\n",
		totalRows, len(orderedTables), elapsed.Round(time.Millisecond))

	return nil
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
