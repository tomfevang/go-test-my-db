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
	compareDeferIndexes bool
)

var compareCmd = &cobra.Command{
	Use:   "compare [comparison.yaml | config1.yaml config2.yaml ...]",
	Short: "Compare schema performance across multiple configurations",
	Long: `The compare subcommand runs the test workflow for multiple configs
and presents a side-by-side comparison of query performance.

Two modes are supported:

  1. Comparison config (single arg): A YAML file with a 'configs' key that
     references seed config files and defines per-config query variants
     side by side in one place.

  2. Multi-config (2+ args): Multiple seed config files passed as positional
     args. Tests are matched by name across configs.

Use --ai to get an AI-powered analysis of the results via Claude.`,
	Args: cobra.MinimumNArgs(1),
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
	compareCmd.Flags().BoolVar(&compareDeferIndexes, "defer-indexes", false, "Drop secondary indexes before seeding and rebuild after (overrides all configs)")

	rootCmd.AddCommand(compareCmd)
}

// compareEntry groups a loaded seed config with its label and source path.
type compareEntry struct {
	cfg   *config.Config
	label string
	path  string
}

func runCompare(cmd *cobra.Command, args []string) error {
	// Single arg that looks like a comparison config → comparison mode.
	if len(args) == 1 && config.IsCompareConfig(args[0]) {
		return runCompareFromComparisonConfig(cmd, args[0])
	}

	// Legacy mode requires 2+ seed config files.
	if len(args) < 2 {
		return fmt.Errorf("expected a comparison config file or at least 2 seed config files")
	}

	entries := make([]compareEntry, len(args))
	for i, path := range args {
		cfg, err := config.Load(path)
		if err != nil {
			return fmt.Errorf("loading config %s: %w", path, err)
		}
		entries[i] = compareEntry{cfg: cfg, label: deriveLabel(path), path: path}
	}

	return executeComparison(cmd, entries)
}

func runCompareFromComparisonConfig(cmd *cobra.Command, path string) error {
	cc, err := config.LoadCompare(path)
	if err != nil {
		return fmt.Errorf("loading comparison config %s: %w", path, err)
	}

	entries := make([]compareEntry, len(cc.Configs))
	for i, entry := range cc.Configs {
		cfg, err := config.Load(entry.File)
		if err != nil {
			return fmt.Errorf("loading seed config %s (label %q): %w", entry.File, entry.Label, err)
		}
		// Replace the seed config's tests with the comparison-config tests for this label.
		cfg.Tests = cc.TestCasesForLabel(entry.Label)
		entries[i] = compareEntry{cfg: cfg, label: entry.Label, path: entry.File}
	}

	return executeComparison(cmd, entries)
}

// executeComparison runs the shared comparison pipeline: connect, seed, test, report.
func executeComparison(cmd *cobra.Command, entries []compareEntry) error {
	// Resolve DSN: CLI flag → SEED_DSN env → first config with a DSN.
	dsnVal := compareDSN
	if !cmd.Flags().Changed("dsn") {
		if v := os.Getenv("SEED_DSN"); v != "" {
			dsnVal = v
		} else {
			for _, e := range entries {
				if e.cfg.Options.DSN != "" {
					dsnVal = e.cfg.Options.DSN
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

	// Open a single DB connection.
	db, err := sql.Open("mysql", dsnVal)
	if err != nil {
		return fmt.Errorf("connecting to MySQL: %w", err)
	}
	defer db.Close()

	maxWorkers := 4
	for _, e := range entries {
		if e.cfg.Options.Workers > maxWorkers {
			maxWorkers = e.cfg.Options.Workers
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

	// Run each config sequentially.
	total := len(entries)
	results := make([]ConfigResult, total)
	for i, e := range entries {
		schemaFile := e.cfg.Options.Schema
		if schemaFile == "" {
			return fmt.Errorf("config %s (label %q) does not specify a schema file (options.schema)", e.path, e.label)
		}

		rows := resolveOverride(compareRows, e.cfg.Options.Rows, 1000)
		batchSize := resolveOverride(compareBatchSize, e.cfg.Options.BatchSize, 1000)
		workers := resolveOverride(compareWorkers, e.cfg.Options.Workers, 4)
		minC := resolveOverride(compareMinChildren, e.cfg.Options.ChildrenPerParent.Min, 10)
		maxC := resolveOverride(compareMaxChildren, e.cfg.Options.ChildrenPerParent.Max, 100)
		maxR := resolveOverride(compareMaxRows, e.cfg.Options.MaxRows, 10_000_000)

		fmt.Printf("[%d/%d] Running: %s (%s, %d base rows)...\n", i+1, total, e.label, schemaFile, rows)
		start := time.Now()

		deferIdx := e.cfg.Options.DeferIndexes || compareDeferIndexes
		testResults, tableCount, err := runTestPipeline(db, schema, e.cfg, schemaFile, rows, batchSize, workers, minC, maxC, maxR, e.cfg.Options.LoadData, deferIdx, e.cfg.Options.SeedTables)
		duration := time.Since(start)

		results[i] = ConfigResult{
			ConfigPath: e.path,
			Label:      e.label,
			SchemaFile: schemaFile,
			Rows:       rows,
			TableCount: tableCount,
			Results:    testResults,
			Error:      err,
			Duration:   duration,
		}

		if err != nil {
			fmt.Printf("[%d/%d] Error: %s — %v\n\n", i+1, total, e.label, err)
		} else {
			fmt.Printf("[%d/%d] Complete: %s (%d tables, %d tests, %s)\n\n",
				i+1, total, e.label, tableCount, len(testResults), duration.Round(time.Millisecond))
		}
	}

	// Print comparison report.
	report := buildComparisonReport(results)
	fmt.Print(report)

	// If --ai flag set, pipe to Claude.
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
