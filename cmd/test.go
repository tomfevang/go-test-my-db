package cmd

import (
	"database/sql"
	"fmt"
	"math"
	"os"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/tomfevang/go-seed-my-db/internal/config"
	"github.com/tomfevang/go-seed-my-db/internal/depgraph"
	"github.com/tomfevang/go-seed-my-db/internal/introspect"
	"github.com/tomfevang/go-seed-my-db/internal/seeder"
)

var (
	testDSN        string
	testSchemaFile string
	testConfigPath string
	testRows       int
	testBatchSize  int
	testWorkers    int
	testTables     []string
)

var testCmd = &cobra.Command{
	Use:   "test",
	Short: "Create tables from DDL, seed, run performance test queries, then drop",
	Long: `The test subcommand enables schema experimentation workflows:
create tables from a DDL file, seed them with fake data, run performance
test queries, drop the tables, and report results.`,
	RunE: runTest,
}

func init() {
	testCmd.Flags().StringVar(&testDSN, "dsn", "", "MySQL DSN (required), e.g. user:pass@tcp(localhost:3306)/mydb")
	testCmd.Flags().StringVar(&testSchemaFile, "schema", "", "Path to SQL DDL file (required)")
	testCmd.Flags().StringVar(&testConfigPath, "config", "", "Path to config YAML file (default: auto-detect go-seed-my-db.yaml)")
	testCmd.Flags().IntVar(&testRows, "rows", 1000, "Number of rows per table")
	testCmd.Flags().IntVar(&testBatchSize, "batch-size", 1000, "Rows per INSERT statement")
	testCmd.Flags().IntVar(&testWorkers, "workers", 4, "Concurrent insert workers")
	testCmd.Flags().StringSliceVar(&testTables, "table", nil, "Table(s) to seed (repeatable). Seeds all tables from schema if omitted")

	testCmd.MarkFlagRequired("dsn")
	testCmd.MarkFlagRequired("schema")

	rootCmd.AddCommand(testCmd)
}

type TestResult struct {
	Name     string
	Query    string
	Repeat   int
	Timings  []time.Duration
	RowCount int
	Error    error
}

func runTest(cmd *cobra.Command, args []string) error {
	// 1. Load config.
	cfg, err := config.LoadOrDefault(testConfigPath)
	if err != nil {
		return fmt.Errorf("loading config: %w", err)
	}

	// 2. Parse DDL file.
	statements, tableNames, err := parseDDLFile(testSchemaFile)
	if err != nil {
		return fmt.Errorf("parsing schema file: %w", err)
	}
	if len(tableNames) == 0 {
		return fmt.Errorf("no CREATE TABLE statements found in %s", testSchemaFile)
	}

	// Extract schema name from DSN.
	schema := extractSchema(testDSN)
	if schema == "" {
		return fmt.Errorf("could not extract database name from DSN — ensure it ends with /dbname")
	}

	// 3. Connect to MySQL.
	db, err := sql.Open("mysql", testDSN)
	if err != nil {
		return fmt.Errorf("connecting to MySQL: %w", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(testWorkers + 2)
	if err := db.Ping(); err != nil {
		return fmt.Errorf("pinging MySQL: %w", err)
	}
	fmt.Printf("Connected to %s\n", schema)

	// 4. Defer cleanup — guarantees table drop even on failure.
	defer func() {
		dropTables(db, tableNames)
		fmt.Println("\nCleaned up: dropped test tables")
	}()

	// 5. Create tables.
	if err := createTables(db, tableNames, statements); err != nil {
		return fmt.Errorf("creating tables: %w", err)
	}
	fmt.Printf("Created %d tables\n", len(tableNames))

	// 6. Introspect newly created tables.
	allTables := make(map[string]*introspect.Table, len(tableNames))
	for _, name := range tableNames {
		t, err := introspect.IntrospectTable(db, schema, name)
		if err != nil {
			return fmt.Errorf("introspecting %s: %w", name, err)
		}
		allTables[name] = t
	}

	// Determine which tables to seed.
	seedTableNames := tableNames
	if len(testTables) > 0 {
		for _, n := range testTables {
			if _, ok := allTables[n]; !ok {
				return fmt.Errorf("table %q not found in schema file", n)
			}
		}
		seedTableNames = testTables
	}

	// 7. Resolve FK ordering.
	requestedTables := make(map[string]*introspect.Table, len(seedTableNames))
	for _, name := range seedTableNames {
		requestedTables[name] = allTables[name]
	}

	order, autoIncluded, err := depgraph.Resolve(requestedTables, allTables)
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

	// 8. Seed tables.
	fmt.Printf("Seeding %d tables with %d rows each...\n", len(orderedTables), testRows)
	if err := seeder.SeedAll(seeder.Config{
		DB:        db,
		Schema:    schema,
		Tables:    orderedTables,
		Rows:      testRows,
		BatchSize: testBatchSize,
		Workers:   testWorkers,
		Clear:     false,
		GenConfig: cfg,
	}); err != nil {
		return fmt.Errorf("seeding tables: %w", err)
	}

	// 9. Run test queries.
	if len(cfg.Tests) == 0 {
		fmt.Println("\nNo test queries configured.")
		return nil
	}

	fmt.Printf("\nRunning %d test queries...\n", len(cfg.Tests))
	results := runTests(db, cfg.Tests)

	// 10. Print report.
	printReport(results)

	// 11. Cleanup happens via defer.
	return nil
}

// parseDDLFile reads a SQL file, splits on semicolons, and extracts table names
// from CREATE TABLE statements.
func parseDDLFile(path string) (statements []string, tableNames []string, err error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, err
	}

	tableNameRe := regexp.MustCompile(`(?i)CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?` + "`?" + `(\w+)` + "`?")

	raw := strings.Split(string(data), ";")
	for _, s := range raw {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		statements = append(statements, s)

		if m := tableNameRe.FindStringSubmatch(s); m != nil {
			tableNames = append(tableNames, m[1])
		}
	}
	return statements, tableNames, nil
}

// createTables drops any existing tables with the given names, then executes
// each DDL statement.
func createTables(db *sql.DB, tableNames []string, statements []string) error {
	// Drop in reverse order to avoid FK conflicts.
	if _, err := db.Exec("SET FOREIGN_KEY_CHECKS=0"); err != nil {
		return err
	}
	for i := len(tableNames) - 1; i >= 0; i-- {
		db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tableNames[i]))
	}
	if _, err := db.Exec("SET FOREIGN_KEY_CHECKS=1"); err != nil {
		return err
	}

	for _, stmt := range statements {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("executing DDL: %w\nStatement: %s", err, stmt)
		}
	}
	return nil
}

// dropTables is best-effort cleanup: disables FK checks, drops all tables,
// re-enables FK checks. Warnings go to stderr, never returns an error.
func dropTables(db *sql.DB, tableNames []string) {
	if _, err := db.Exec("SET FOREIGN_KEY_CHECKS=0"); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not disable FK checks: %v\n", err)
	}
	for i := len(tableNames) - 1; i >= 0; i-- {
		if _, err := db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tableNames[i])); err != nil {
			fmt.Fprintf(os.Stderr, "warning: could not drop table %s: %v\n", tableNames[i], err)
		}
	}
	if _, err := db.Exec("SET FOREIGN_KEY_CHECKS=1"); err != nil {
		fmt.Fprintf(os.Stderr, "warning: could not re-enable FK checks: %v\n", err)
	}
}

// runTests executes each test query N times, collecting timings.
func runTests(db *sql.DB, tests []config.TestCase) []TestResult {
	results := make([]TestResult, 0, len(tests))
	for _, tc := range tests {
		repeat := tc.Repeat
		if repeat <= 0 {
			repeat = 1
		}

		result := TestResult{
			Name:    tc.Name,
			Query:   tc.Query,
			Repeat:  repeat,
			Timings: make([]time.Duration, 0, repeat),
		}

		for i := 0; i < repeat; i++ {
			start := time.Now()
			rows, err := db.Query(tc.Query)
			if err != nil {
				result.Error = err
				break
			}
			// Drain all rows to measure full execution.
			n := 0
			for rows.Next() {
				n++
			}
			rows.Close()
			result.Timings = append(result.Timings, time.Since(start))
			if i == 0 {
				result.RowCount = n
			}
		}

		results = append(results, result)
	}
	return results
}

// printReport outputs a formatted performance summary for each test.
func printReport(results []TestResult) {
	fmt.Println("\n=== Performance Test Results ===")

	for _, r := range results {
		fmt.Printf("\n--- %s ---\n", r.Name)
		fmt.Printf("  Query:    %s\n", r.Query)

		if r.Error != nil {
			fmt.Printf("  Error:    %v\n", r.Error)
			continue
		}

		fmt.Printf("  Rows:     %d\n", r.RowCount)
		fmt.Printf("  Runs:     %d\n", len(r.Timings))
		fmt.Printf("  Avg:      %s\n", formatDuration(avg(r.Timings)))
		fmt.Printf("  Min:      %s\n", formatDuration(min(r.Timings)))
		fmt.Printf("  Max:      %s\n", formatDuration(max(r.Timings)))
		fmt.Printf("  p95:      %s\n", formatDuration(percentile(r.Timings, 95)))
		fmt.Printf("  Total:    %s\n", total(r.Timings))
	}
}

func avg(timings []time.Duration) time.Duration {
	if len(timings) == 0 {
		return 0
	}
	var sum time.Duration
	for _, t := range timings {
		sum += t
	}
	return sum / time.Duration(len(timings))
}

func total(timings []time.Duration) time.Duration {
	var sum time.Duration
	for _, t := range timings {
		sum += t
	}
	return sum
}

func min(timings []time.Duration) time.Duration {
	if len(timings) == 0 {
		return 0
	}
	m := timings[0]
	for _, t := range timings[1:] {
		if t < m {
			m = t
		}
	}
	return m
}

func max(timings []time.Duration) time.Duration {
	if len(timings) == 0 {
		return 0
	}
	m := timings[0]
	for _, t := range timings[1:] {
		if t > m {
			m = t
		}
	}
	return m
}

func percentile(timings []time.Duration, pct float64) time.Duration {
	if len(timings) == 0 {
		return 0
	}
	sorted := make([]time.Duration, len(timings))
	copy(sorted, timings)
	slices.Sort(sorted)

	idx := int(math.Ceil(pct/100*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// formatDuration prints a duration in a human-friendly way.
func formatDuration(d time.Duration) string {
	switch {
	case d < time.Millisecond:
		return fmt.Sprintf("%.2fµs", float64(d.Nanoseconds())/1000)
	case d < time.Second:
		return fmt.Sprintf("%.2fms", float64(d.Nanoseconds())/1e6)
	default:
		return d.Round(time.Millisecond).String()
	}
}
