package cmd

import (
	"embed"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

// ExamplesFS holds the embedded examples directory, set by main before Execute().
var ExamplesFS embed.FS

var examplesForce bool

var examplesCmd = &cobra.Command{
	Use:   "examples",
	Short: "Extract bundled example schemas and configs to ./examples/",
	Long: `The examples command writes the bundled example files to an examples/
directory in the current working directory. These include two schema
designs (generated-columns and star-schema) with matching configs
and a comparison config for side-by-side benchmarking.`,
	RunE: runExamples,
}

func init() {
	examplesCmd.Flags().BoolVar(&examplesForce, "force", false, "Overwrite existing files")

	rootCmd.AddCommand(examplesCmd)
}

func runExamples(cmd *cobra.Command, args []string) error {
	if !examplesForce {
		if _, err := os.Stat("examples"); err == nil {
			return fmt.Errorf("examples/ already exists — use --force to overwrite")
		}
	}

	if err := os.MkdirAll("examples", 0755); err != nil {
		return fmt.Errorf("creating examples directory: %w", err)
	}

	entries, err := fs.ReadDir(ExamplesFS, "examples")
	if err != nil {
		return fmt.Errorf("reading embedded examples: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		data, err := ExamplesFS.ReadFile(filepath.Join("examples", entry.Name()))
		if err != nil {
			return fmt.Errorf("reading embedded file %s: %w", entry.Name(), err)
		}
		outPath := filepath.Join("examples", entry.Name())
		if err := os.WriteFile(outPath, data, 0644); err != nil {
			return fmt.Errorf("writing %s: %w", outPath, err)
		}
		fmt.Printf("  %s\n", outPath)
	}

	fmt.Println("Done. See examples/README.md for usage instructions.")
	return nil
}
