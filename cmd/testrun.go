package cmd

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/tomfevang/go-test-my-db/internal/config"
	"github.com/tomfevang/go-test-my-db/internal/depgraph"
	"github.com/tomfevang/go-test-my-db/internal/introspect"
	"github.com/tomfevang/go-test-my-db/internal/seeder"
)

// ConfigResult holds the full test output for a single config file.
type ConfigResult struct {
	ConfigPath string
	Label      string
	SchemaFile string
	Rows       int
	TableCount int
	Results    []TestResult
	Error      error
	Duration   time.Duration
}

// runTestPipeline runs the full create→seed→test→drop pipeline for a single
// config against the given database connection. Tables are always dropped,
// even on error.
func runTestPipeline(db *sql.DB, schema string, cfg *config.Config, schemaFile string, rows, batchSize, workers, minChildren, maxChildren, maxRowsCap int, loadData, deferIndexes bool, fkSampleSize int, seedTables []string) ([]TestResult, int, error) {
	// Parse DDL file.
	statements, tableNames, err := parseDDLFile(schemaFile)
	if err != nil {
		return nil, 0, fmt.Errorf("parsing schema file: %w", err)
	}
	if len(tableNames) == 0 {
		return nil, 0, fmt.Errorf("no CREATE TABLE statements found in %s", schemaFile)
	}

	// Create tables.
	if err := createTables(db, tableNames, statements); err != nil {
		return nil, 0, fmt.Errorf("creating tables: %w", err)
	}
	fmt.Printf("Created %d tables\n", len(tableNames))

	// Defer cleanup — guarantees table drop even on failure.
	defer func() {
		dropTables(db, tableNames)
		fmt.Println("Cleaned up: dropped test tables")
	}()

	// Introspect newly created tables.
	allTables := make(map[string]*introspect.Table, len(tableNames))
	for _, name := range tableNames {
		t, err := introspect.IntrospectTable(db, schema, name)
		if err != nil {
			return nil, len(tableNames), fmt.Errorf("introspecting %s: %w", name, err)
		}
		allTables[name] = t
	}

	// Apply config-declared references (logical FKs without actual constraints).
	if refs := cfg.GetReferences(); refs != nil {
		introspect.ApplyReferences(allTables, refs)
	}

	// Determine which tables to seed.
	seedTableNames := tableNames
	if len(seedTables) > 0 {
		for _, n := range seedTables {
			if _, ok := allTables[n]; !ok {
				return nil, len(tableNames), fmt.Errorf("table %q not found in schema file", n)
			}
		}
		seedTableNames = seedTables
	}

	// Resolve FK ordering.
	requestedTables := make(map[string]*introspect.Table, len(seedTableNames))
	for _, name := range seedTableNames {
		requestedTables[name] = allTables[name]
	}

	order, autoIncluded, relations, err := depgraph.Resolve(requestedTables, allTables)
	if err != nil {
		return nil, len(tableNames), err
	}
	if len(autoIncluded) > 0 {
		fmt.Printf("Auto-included parent tables: %s\n", strings.Join(autoIncluded, ", "))
	}

	orderedTables := make([]*introspect.Table, len(order))
	for i, name := range order {
		orderedTables[i] = requestedTables[name]
	}

	// Compute per-table row counts.
	rowCounts := computeRowCounts(order, relations, cfg, rows, minChildren, maxChildren, maxRowsCap)

	// Seed tables.
	fmt.Printf("Seeding %d tables...\n", len(orderedTables))
	if err := seeder.SeedAll(seeder.Config{
		DB:           db,
		Schema:       schema,
		Tables:       orderedTables,
		RowsPerTable: rowCounts,
		BatchSize:    batchSize,
		Workers:      workers,
		Clear:        false,
		LoadData:     loadData,
		DeferIndexes: deferIndexes,
		GenConfig:    cfg,
		FKSampleSize: fkSampleSize,
	}); err != nil {
		return nil, len(tableNames), fmt.Errorf("seeding tables: %w", err)
	}

	// Run test queries.
	if len(cfg.Tests) == 0 {
		fmt.Println("\nNo test queries configured.")
		return nil, len(tableNames), nil
	}

	fmt.Printf("\nRunning %d test queries...\n", len(cfg.Tests))
	results := runTests(db, cfg.Tests)

	return results, len(tableNames), nil
}

