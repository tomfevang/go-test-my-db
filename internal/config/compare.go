package config

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// CompareConfig defines a comparison benchmark with multiple seed configs
// and shared test definitions with per-config query variants.
type CompareConfig struct {
	Configs []CompareConfigEntry `yaml:"configs"`
	Tests   []CompareTest        `yaml:"tests"`
}

// CompareConfigEntry references a seed config file with a display label.
type CompareConfigEntry struct {
	Label string `yaml:"label"`
	File  string `yaml:"file"`
}

// CompareTest defines a named test with per-config query variants.
type CompareTest struct {
	Name    string            `yaml:"name"`
	Repeat  int               `yaml:"repeat"`
	Queries map[string]string `yaml:"queries"` // label -> query
}

// LoadCompare reads and parses a comparison config YAML file.
// File paths in the config are resolved relative to the config file's directory.
func LoadCompare(path string) (*CompareConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cc CompareConfig
	if err := yaml.Unmarshal(data, &cc); err != nil {
		return nil, err
	}

	if len(cc.Configs) == 0 {
		return nil, fmt.Errorf("comparison config must define at least one entry in 'configs'")
	}

	// Validate labels: non-empty and unique.
	seen := make(map[string]bool, len(cc.Configs))
	for i, entry := range cc.Configs {
		if entry.Label == "" {
			return nil, fmt.Errorf("comparison config entry at index %d has empty label", i)
		}
		if seen[entry.Label] {
			return nil, fmt.Errorf("duplicate label %q in comparison config", entry.Label)
		}
		seen[entry.Label] = true
	}

	// Validate that query labels reference defined configs.
	for _, test := range cc.Tests {
		for label := range test.Queries {
			if !seen[label] {
				return nil, fmt.Errorf("test %q references undefined label %q", test.Name, label)
			}
		}
	}

	// Resolve file paths relative to the comparison config file's directory.
	dir := filepath.Dir(path)
	for i := range cc.Configs {
		if !filepath.IsAbs(cc.Configs[i].File) {
			cc.Configs[i].File = filepath.Join(dir, cc.Configs[i].File)
		}
	}

	return &cc, nil
}

// IsCompareConfig reports whether the YAML file at path looks like a comparison
// config (has a non-empty top-level "configs" key).
func IsCompareConfig(path string) bool {
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}

	var probe struct {
		Configs []any `yaml:"configs"`
	}
	if err := yaml.Unmarshal(data, &probe); err != nil {
		return false
	}
	return len(probe.Configs) > 0
}

// TestCasesForLabel converts comparison tests into a []TestCase for the given
// config label. Tests without a query for the label are omitted.
func (cc *CompareConfig) TestCasesForLabel(label string) []TestCase {
	var cases []TestCase
	for _, ct := range cc.Tests {
		query, ok := ct.Queries[label]
		if !ok || query == "" {
			continue
		}
		repeat := ct.Repeat
		if repeat <= 0 {
			repeat = 1
		}
		cases = append(cases, TestCase{
			Name:   ct.Name,
			Query:  query,
			Repeat: repeat,
		})
	}
	return cases
}
