package cmd

import (
	"bytes"
	"database/sql"
	"fmt"
	"math"
	"os"
	"regexp"
	"slices"
	"strings"
	"text/template"
	"text/tabwriter"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/tomfevang/go-seed-my-db/internal/config"
	"github.com/tomfevang/go-seed-my-db/internal/generator"
)

const (
	colorReset  = "\033[0m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorRed    = "\033[31m"
)

var (
	testDSN         string
	testSchemaFile  string
	testConfigPath  string
	testRows        int
	testBatchSize   int
	testWorkers     int
	testTables      []string
	testAI          bool
	testMinChildren int
	testMaxChildren int
	testMaxRows     int
	testLoadData    bool
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
	testCmd.Flags().BoolVar(&testAI, "ai", false, "Pipe results to Claude for AI-powered analysis")
	testCmd.Flags().IntVar(&testMinChildren, "min-children", 10, "Min children per parent row for child tables")
	testCmd.Flags().IntVar(&testMaxChildren, "max-children", 100, "Max children per parent row for child tables")
	testCmd.Flags().IntVar(&testMaxRows, "max-rows", 10_000_000, "Maximum rows per table (safeguard for deep hierarchies)")
	testCmd.Flags().BoolVar(&testLoadData, "load-data", false, "Use LOAD DATA LOCAL INFILE for faster bulk loading (requires server local_infile=ON)")

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

	// Resolve operational parameters: CLI flag > env var > config > default.
	testDSN = resolveString(cmd, "dsn", testDSN, "SEED_DSN", cfg.Options.DSN, "")
	testSchemaFile = resolveString(cmd, "schema", testSchemaFile, "", cfg.Options.Schema, "")
	testRows = resolveInt(cmd, "rows", testRows, cfg.Options.Rows, 1000)
	testBatchSize = resolveInt(cmd, "batch-size", testBatchSize, cfg.Options.BatchSize, 1000)
	testWorkers = resolveInt(cmd, "workers", testWorkers, cfg.Options.Workers, 4)
	testMinChildren = resolveInt(cmd, "min-children", testMinChildren, cfg.Options.ChildrenPerParent.Min, 10)
	testMaxChildren = resolveInt(cmd, "max-children", testMaxChildren, cfg.Options.ChildrenPerParent.Max, 100)
	testMaxRows = resolveInt(cmd, "max-rows", testMaxRows, cfg.Options.MaxRows, 10_000_000)
	if !cmd.Flags().Changed("load-data") && cfg.Options.LoadData {
		testLoadData = true
	}

	if testDSN == "" {
		return fmt.Errorf("DSN is required — set via --dsn flag, SEED_DSN env var, or options.dsn in config file")
	}
	if testSchemaFile == "" {
		return fmt.Errorf("schema file is required — set via --schema flag or options.schema in config file")
	}

	// Extract schema name from DSN.
	schema := extractSchema(testDSN)
	if schema == "" {
		return fmt.Errorf("could not extract database name from DSN — ensure it ends with /dbname")
	}

	if testLoadData {
		testDSN = ensureAllowAllFiles(testDSN)
	}

	// Connect to MySQL.
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

	// Resolve seed tables: CLI flag > config > all tables.
	seedTables := testTables
	if len(seedTables) == 0 && len(cfg.Options.SeedTables) > 0 {
		seedTables = cfg.Options.SeedTables
	}

	// Run the pipeline: create → seed → test → drop.
	results, _, err := runTestPipeline(db, schema, cfg, testSchemaFile, testRows, testBatchSize, testWorkers, testMinChildren, testMaxChildren, testMaxRows, testLoadData, seedTables)
	if err != nil {
		return err
	}

	if len(results) > 0 {
		printReport(results)
	}

	if testAI && len(results) > 0 {
		fmt.Println()
		report := buildTestReport(results)
		if err := analyzeTestWithAI(report, testSchemaFile, testRows); err != nil {
			fmt.Fprintf(os.Stderr, "AI analysis failed: %v\n", err)
		}
	}

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
// Queries containing {{...}} are treated as Go templates and rendered
// before each execution, giving each run fresh random parameter values.
func runTests(db *sql.DB, tests []config.TestCase) []TestResult {
	// Set up template rendering once for all tests.
	fm := generator.FuncMap(gofakeit.New(0))
	var buf bytes.Buffer

	results := make([]TestResult, 0, len(tests))
	for ti, tc := range tests {
		repeat := tc.Repeat
		if repeat <= 0 {
			repeat = 1
		}

		// Pre-parse template if the query contains template syntax.
		var tmpl *template.Template
		if strings.Contains(tc.Query, "{{") {
			var err error
			tmpl, err = template.New(tc.Name).Funcs(fm).Parse(tc.Query)
			if err != nil {
				results = append(results, TestResult{
					Name:  tc.Name,
					Query: tc.Query,
					Error: fmt.Errorf("invalid query template: %w", err),
				})
				fmt.Printf("[%d/%d] %s ... ERROR: invalid template\n", ti+1, len(tests), tc.Name)
				continue
			}
		}

		result := TestResult{
			Name:    tc.Name,
			Query:   tc.Query,
			Repeat:  repeat,
			Timings: make([]time.Duration, 0, repeat),
		}

		fmt.Printf("\r[%d/%d] %s ... 0/%d runs", ti+1, len(tests), tc.Name, repeat)

		for i := 0; i < repeat; i++ {
			// Render query template if present, otherwise use static query.
			query := tc.Query
			if tmpl != nil {
				buf.Reset()
				if err := tmpl.Execute(&buf, nil); err != nil {
					result.Error = fmt.Errorf("template exec failed on run %d: %w", i+1, err)
					break
				}
				query = buf.String()
			}

			start := time.Now()
			rows, err := db.Query(query)
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

			// Update progress every 10 runs or on the last run.
			if (i+1)%10 == 0 || i+1 == repeat {
				fmt.Printf("\r[%d/%d] %s ... %d/%d runs", ti+1, len(tests), tc.Name, i+1, repeat)
			}
		}

		if result.Error != nil {
			fmt.Printf("\r[%d/%d] %s ... ERROR: %v\n", ti+1, len(tests), tc.Name, result.Error)
		} else {
			fmt.Printf("\r[%d/%d] %s ... done (avg %s)\n", ti+1, len(tests), tc.Name, formatDuration(avg(result.Timings)))
		}

		results = append(results, result)
	}
	return results
}

// printReport outputs a formatted performance summary table with color coding.
func printReport(results []TestResult) {
	fmt.Println("\n=== Performance Test Results ===")

	useColor := term.IsTerminal(int(os.Stdout.Fd()))

	// Compute tercile thresholds from avg times.
	var thresholds [2]time.Duration
	if useColor {
		thresholds = avgTerciles(results)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "  Test\tAvg\tMin\tMax\tp95\tRows\tRuns\n")
	fmt.Fprintf(w, "  ----\t---\t---\t---\t---\t----\t----\n")

	for _, r := range results {
		if r.Error != nil {
			line := fmt.Sprintf("  %s\tERROR: %v\t\t\t\t\t", r.Name, r.Error)
			if useColor {
				line = colorRed + line + colorReset
			}
			fmt.Fprintln(w, line)
			continue
		}

		line := fmt.Sprintf("  %s\t%s\t%s\t%s\t%s\t%d\t%d",
			r.Name,
			formatDuration(avg(r.Timings)),
			formatDuration(min(r.Timings)),
			formatDuration(max(r.Timings)),
			formatDuration(percentile(r.Timings, 95)),
			r.RowCount,
			len(r.Timings),
		)

		if useColor {
			a := avg(r.Timings)
			switch {
			case a <= thresholds[0]:
				line = colorGreen + line + colorReset
			case a <= thresholds[1]:
				line = colorYellow + line + colorReset
			default:
				line = colorRed + line + colorReset
			}
		}

		fmt.Fprintln(w, line)
	}
	w.Flush()
}

// avgTerciles computes the 33rd and 67th percentile of avg times
// to split results into three tiers: fast (green), moderate (yellow), slow (red).
func avgTerciles(results []TestResult) [2]time.Duration {
	var avgs []time.Duration
	for _, r := range results {
		if r.Error == nil && len(r.Timings) > 0 {
			avgs = append(avgs, avg(r.Timings))
		}
	}
	if len(avgs) < 2 {
		return [2]time.Duration{time.Hour, time.Hour}
	}
	slices.Sort(avgs)
	return [2]time.Duration{
		avgs[len(avgs)/3],
		avgs[len(avgs)*2/3],
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

// buildTestReport returns the test results as a string for AI analysis.
func buildTestReport(results []TestResult) string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "Test\tAvg\tMin\tMax\tp95\tRows\tRuns\n")
	for _, r := range results {
		if r.Error != nil {
			fmt.Fprintf(w, "%s\tERROR: %v\t\t\t\t\t\n", r.Name, r.Error)
			continue
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%d\t%d\n",
			r.Name,
			formatDuration(avg(r.Timings)),
			formatDuration(min(r.Timings)),
			formatDuration(max(r.Timings)),
			formatDuration(percentile(r.Timings, 95)),
			r.RowCount,
			len(r.Timings),
		)
	}
	w.Flush()
	return sb.String()
}

// analyzeTestWithAI pipes test results to Claude for AI analysis.
func analyzeTestWithAI(report, schemaFile string, rows int) error {
	var prompt strings.Builder
	prompt.WriteString("You are analyzing MySQL query performance test results.\n\n")
	fmt.Fprintf(&prompt, "Schema: %s, %d rows per table\n\n", schemaFile, rows)
	prompt.WriteString("Results:\n\n")
	prompt.WriteString(report)
	prompt.WriteString("\n\nAnalyze:\n")
	prompt.WriteString("1. Which queries perform well and which are slow, and why\n")
	prompt.WriteString("2. The impact of index usage vs full table scans\n")
	prompt.WriteString("3. Any notable patterns in the timing data (e.g. p95 outliers)\n")
	prompt.WriteString("4. Suggestions for improving slow queries\n")

	return runClaude(prompt.String())
}
