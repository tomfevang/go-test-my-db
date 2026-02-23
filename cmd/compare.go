package cmd

import (
	"bytes"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/charmbracelet/glamour"
	"github.com/spf13/cobra"
	"golang.org/x/term"

	"github.com/tomfevang/go-seed-my-db/internal/config"
)

var (
	compareDSN         string
	compareRows        int
	compareBatchSize   int
	compareWorkers     int
	compareAI          bool
	compareMinChildren int
	compareMaxChildren int
	compareMaxRows     int
)

var compareCmd = &cobra.Command{
	Use:   "compare config1.yaml config2.yaml [config3.yaml ...]",
	Short: "Compare schema performance across multiple configurations",
	Long: `The compare subcommand runs the test workflow for each config file
and presents a side-by-side comparison of query performance.

Each config provides its own schema, table definitions, seed data generators,
and test queries. Tests are matched by name across configs where possible.

Use --ai to get an AI-powered analysis of the results via Claude.`,
	Args: cobra.MinimumNArgs(2),
	RunE: runCompare,
}

func init() {
	compareCmd.Flags().StringVar(&compareDSN, "dsn", "", "MySQL DSN (shared across all configs)")
	compareCmd.Flags().IntVar(&compareRows, "rows", 0, "Override rows per table for all configs (0 = use each config's value)")
	compareCmd.Flags().IntVar(&compareBatchSize, "batch-size", 0, "Override batch size for all configs (0 = use each config's value)")
	compareCmd.Flags().IntVar(&compareWorkers, "workers", 0, "Override worker count for all configs (0 = use each config's value)")
	compareCmd.Flags().BoolVar(&compareAI, "ai", false, "Pipe results to Claude for AI-powered analysis")
	compareCmd.Flags().IntVar(&compareMinChildren, "min-children", 0, "Override min children per parent row (0 = use each config's value)")
	compareCmd.Flags().IntVar(&compareMaxChildren, "max-children", 0, "Override max children per parent row (0 = use each config's value)")
	compareCmd.Flags().IntVar(&compareMaxRows, "max-rows", 0, "Override max rows per table (0 = use each config's value)")

	rootCmd.AddCommand(compareCmd)
}

func runCompare(cmd *cobra.Command, args []string) error {
	// 1. Load all configs — fail fast if any can't be loaded.
	configs := make([]*config.Config, len(args))
	for i, path := range args {
		cfg, err := config.Load(path)
		if err != nil {
			return fmt.Errorf("loading config %s: %w", path, err)
		}
		configs[i] = cfg
	}

	// 2. Resolve DSN: CLI flag → SEED_DSN env → first config with a DSN.
	dsnVal := compareDSN
	if !cmd.Flags().Changed("dsn") {
		if v := os.Getenv("SEED_DSN"); v != "" {
			dsnVal = v
		} else {
			for _, cfg := range configs {
				if cfg.Options.DSN != "" {
					dsnVal = cfg.Options.DSN
					break
				}
			}
		}
	}
	if dsnVal == "" {
		return fmt.Errorf("DSN is required — set via --dsn flag, SEED_DSN env var, or options.dsn in a config file")
	}

	schema := extractSchema(dsnVal)
	if schema == "" {
		return fmt.Errorf("could not extract database name from DSN — ensure it ends with /dbname")
	}

	// 3. Open a single DB connection.
	db, err := sql.Open("mysql", dsnVal)
	if err != nil {
		return fmt.Errorf("connecting to MySQL: %w", err)
	}
	defer db.Close()

	maxWorkers := 4
	for _, cfg := range configs {
		if cfg.Options.Workers > maxWorkers {
			maxWorkers = cfg.Options.Workers
		}
	}
	if compareWorkers > maxWorkers {
		maxWorkers = compareWorkers
	}
	db.SetMaxOpenConns(maxWorkers + 2)

	if err := db.Ping(); err != nil {
		return fmt.Errorf("pinging MySQL: %w", err)
	}
	fmt.Printf("Connected to %s\n\n", schema)

	// 4. Run each config sequentially.
	results := make([]ConfigResult, len(args))
	for i, path := range args {
		cfg := configs[i]
		label := deriveLabel(path)

		schemaFile := cfg.Options.Schema
		if schemaFile == "" {
			return fmt.Errorf("config %s does not specify a schema file (options.schema)", path)
		}

		rows := resolveOverride(compareRows, cfg.Options.Rows, 1000)
		batchSize := resolveOverride(compareBatchSize, cfg.Options.BatchSize, 1000)
		workers := resolveOverride(compareWorkers, cfg.Options.Workers, 4)
		minC := resolveOverride(compareMinChildren, cfg.Options.ChildrenPerParent.Min, 10)
		maxC := resolveOverride(compareMaxChildren, cfg.Options.ChildrenPerParent.Max, 100)
		maxR := resolveOverride(compareMaxRows, cfg.Options.MaxRows, 10_000_000)

		fmt.Printf("[%d/%d] Running: %s (%s, %d base rows)...\n", i+1, len(args), label, schemaFile, rows)
		start := time.Now()

		testResults, tableCount, err := runTestPipeline(db, schema, cfg, schemaFile, rows, batchSize, workers, minC, maxC, maxR, cfg.Options.LoadData, cfg.Options.SeedTables)
		duration := time.Since(start)

		results[i] = ConfigResult{
			ConfigPath: path,
			Label:      label,
			SchemaFile: schemaFile,
			Rows:       rows,
			TableCount: tableCount,
			Results:    testResults,
			Error:      err,
			Duration:   duration,
		}

		if err != nil {
			fmt.Printf("[%d/%d] Error: %s — %v\n\n", i+1, len(args), label, err)
		} else {
			fmt.Printf("[%d/%d] Complete: %s (%d tables, %d tests, %s)\n\n",
				i+1, len(args), label, tableCount, len(testResults), duration.Round(time.Millisecond))
		}
	}

	// 5. Print comparison report.
	report := buildComparisonReport(results)
	fmt.Print(report)

	// 6. If --ai flag set, pipe to Claude.
	if compareAI {
		fmt.Println()
		if err := analyzeWithAI(report, results); err != nil {
			fmt.Fprintf(os.Stderr, "AI analysis failed: %v\n", err)
		}
	}

	return nil
}

// resolveOverride picks: CLI override (if >0) → config value (if >0) → default.
func resolveOverride(override, cfgVal, defaultVal int) int {
	if override > 0 {
		return override
	}
	if cfgVal > 0 {
		return cfgVal
	}
	return defaultVal
}

// buildComparisonReport formats a side-by-side performance comparison.
func buildComparisonReport(configs []ConfigResult) string {
	var sb strings.Builder

	sb.WriteString("=== Schema Comparison Report ===\n\n")

	// Header: config metadata.
	sb.WriteString("Configs:\n")
	for _, c := range configs {
		if c.Error != nil {
			fmt.Fprintf(&sb, "  %-15s %s — ERROR: %v\n", c.Label, c.SchemaFile, c.Error)
		} else {
			fmt.Fprintf(&sb, "  %-15s %s (%d rows, %d tables, %s)\n",
				c.Label, c.SchemaFile, c.Rows, c.TableCount, c.Duration.Round(time.Millisecond))
		}
	}

	// Collect all unique test names, preserving order.
	testNames := collectTestNames(configs)
	if len(testNames) == 0 {
		sb.WriteString("\nNo test results to compare.\n")
		return sb.String()
	}

	// Build a lookup: config label → test name → TestResult.
	type resultKey struct {
		config int
		test   string
	}
	lookup := make(map[resultKey]*TestResult)
	for ci, c := range configs {
		for ri := range c.Results {
			lookup[resultKey{ci, c.Results[ri].Name}] = &c.Results[ri]
		}
	}

	// Print each test.
	for _, testName := range testNames {
		fmt.Fprintf(&sb, "\n--- %s ---\n", testName)

		w := tabwriter.NewWriter(&sb, 0, 0, 2, ' ', 0)
		fmt.Fprintf(w, "  Config\tAvg\tMin\tMax\tp95\tRows\n")

		for ci, c := range configs {
			r, ok := lookup[resultKey{ci, testName}]
			if !ok || c.Error != nil {
				fmt.Fprintf(w, "  %s\t-\t-\t-\t-\t-\n", c.Label)
				continue
			}
			if r.Error != nil {
				fmt.Fprintf(w, "  %s\tERROR: %v\t\t\t\t\n", c.Label, r.Error)
				continue
			}
			fmt.Fprintf(w, "  %s\t%s\t%s\t%s\t%s\t%d\n",
				c.Label,
				formatDuration(avg(r.Timings)),
				formatDuration(min(r.Timings)),
				formatDuration(max(r.Timings)),
				formatDuration(percentile(r.Timings, 95)),
				r.RowCount,
			)
		}
		w.Flush()
	}

	return sb.String()
}

// collectTestNames gathers all unique test names across configs, preserving
// the order from the first config each name appears in.
func collectTestNames(configs []ConfigResult) []string {
	seen := make(map[string]bool)
	var names []string
	for _, c := range configs {
		for _, r := range c.Results {
			if !seen[r.Name] {
				seen[r.Name] = true
				names = append(names, r.Name)
			}
		}
	}
	return names
}

// terminalWidth returns the current terminal width, falling back to 120.
func terminalWidth() int {
	w, _, err := term.GetSize(int(os.Stdout.Fd()))
	if err != nil || w <= 0 {
		return 120
	}
	return w
}

// runClaude sends a prompt to the claude CLI and renders the markdown response.
func runClaude(prompt string) error {
	if _, err := exec.LookPath("claude"); err != nil {
		return fmt.Errorf("claude CLI not found in PATH — install Claude Code from https://docs.anthropic.com/en/docs/claude-code")
	}

	fmt.Println("Sending results to Claude for analysis...")

	cmd := exec.Command("claude", "-p", prompt)
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return err
	}

	width := terminalWidth()
	renderer, err := glamour.NewTermRenderer(
		glamour.WithAutoStyle(),
		glamour.WithWordWrap(width),
	)
	if err != nil {
		fmt.Print(out.String())
		return nil
	}
	rendered, err := renderer.Render(out.String())
	if err != nil {
		fmt.Print(out.String())
		return nil
	}
	fmt.Print(rendered)
	return nil
}

// analyzeWithAI pipes the comparison report to Claude for AI analysis.
func analyzeWithAI(report string, configs []ConfigResult) error {
	var prompt strings.Builder
	prompt.WriteString("You are analyzing MySQL schema performance test results comparing different schema designs.\n\n")
	prompt.WriteString("Configurations tested:\n")
	for _, c := range configs {
		if c.Error != nil {
			fmt.Fprintf(&prompt, "- %s: schema=%s — FAILED: %v\n", c.Label, c.SchemaFile, c.Error)
		} else {
			fmt.Fprintf(&prompt, "- %s: schema=%s, %d rows, %d tables\n", c.Label, c.SchemaFile, c.Rows, c.TableCount)
		}
	}
	prompt.WriteString("\nResults:\n\n")
	prompt.WriteString(report)
	prompt.WriteString("\n\nAnalyze:\n")
	prompt.WriteString("1. Which schema design performs best for each query pattern and why\n")
	prompt.WriteString("2. The tradeoffs between each schema approach\n")
	prompt.WriteString("3. How each schema handles off-index queries\n")
	prompt.WriteString("4. Specific recommendations based on the timing data\n")

	return runClaude(prompt.String())
}
