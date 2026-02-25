package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type DistributionConfig struct {
	Type    string             `yaml:"type"`    // zipf | normal | weighted (default: uniform)
	S       float64            `yaml:"s"`       // zipf exponent
	Mean    float64            `yaml:"mean"`    // normal mean (0-1, default 0.5)
	StdDev  float64            `yaml:"stddev"`  // normal stddev (default 0.15)
	Weights map[string]float64 `yaml:"weights"` // weighted: value -> weight
}

type CorrelationGroup struct {
	Columns  []string          `yaml:"columns"`
	Source   string            `yaml:"source"`   // built-in preset name or "template"
	Template map[string]string `yaml:"template"` // column -> template string
}

type TableConfig struct {
	Rows           int                            `yaml:"rows"`
	References     map[string]string              `yaml:"references"` // column -> "RefTable.RefColumn"
	Columns        map[string]string              `yaml:"columns"`
	Distributions  map[string]DistributionConfig  `yaml:"distributions"`
	Correlations   []CorrelationGroup             `yaml:"correlations"`
}

type TestCase struct {
	Name   string `yaml:"name"`
	Query  string `yaml:"query"`
	Repeat int    `yaml:"repeat"` // <= 0 treated as 1
}

type ChildrenPerParent struct {
	Min int `yaml:"min"`
	Max int `yaml:"max"`
}

type Options struct {
	DSN               string           `yaml:"dsn"`
	Schema            string           `yaml:"schema"`
	SeedTables        []string         `yaml:"seed_tables"`
	Rows              int              `yaml:"rows"`
	BatchSize         int              `yaml:"batch_size"`
	Workers           int              `yaml:"workers"`
	ChildrenPerParent ChildrenPerParent `yaml:"children_per_parent"`
	MaxRows           int              `yaml:"max_rows"`
	LoadData          bool             `yaml:"load_data"`
	DeferIndexes      bool             `yaml:"defer_indexes"`
	FKSampleSize      int              `yaml:"fk_sample_size"`
}

type Config struct {
	Options Options                `yaml:"options"`
	Tables  map[string]TableConfig `yaml:"tables"`
	Tests   []TestCase             `yaml:"tests"`
}

// Load reads and parses a YAML config file.
// If path is empty, it returns an empty Config.
// If the file does not exist and path was auto-detected, the caller should
// handle the error; use LoadOrDefault for auto-detection.
func Load(path string) (*Config, error) {
	if path == "" {
		return &Config{}, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	if cfg.Tables == nil {
		cfg.Tables = make(map[string]TableConfig)
	}

	return &cfg, nil
}

// LoadOrDefault tries to load from the given path. If path is empty, it
// attempts to auto-detect "go-test-my-db.yaml" in the current directory.
// Returns an empty Config if no file is found at the auto-detect path.
func LoadOrDefault(path string) (*Config, error) {
	if path != "" {
		return Load(path)
	}

	const defaultFile = "go-test-my-db.yaml"
	if _, err := os.Stat(defaultFile); err != nil {
		// File doesn't exist — that's fine, return empty config.
		return &Config{}, nil
	}

	return Load(defaultFile)
}

// GetReferences returns all configured references as
// tableName -> columnName -> "RefTable.RefColumn".
func (c *Config) GetReferences() map[string]map[string]string {
	if c == nil {
		return nil
	}
	refs := make(map[string]map[string]string)
	for tableName, tc := range c.Tables {
		if len(tc.References) > 0 {
			refs[tableName] = tc.References
		}
	}
	if len(refs) == 0 {
		return nil
	}
	return refs
}

// GetDistribution returns the distribution config for a given table and column,
// or nil if none is configured.
func (c *Config) GetDistribution(table, column string) *DistributionConfig {
	if c == nil {
		return nil
	}
	tc, ok := c.Tables[table]
	if !ok {
		return nil
	}
	if d, ok := tc.Distributions[column]; ok {
		return &d
	}
	return nil
}

// GetCorrelations returns the correlation groups for a given table,
// or nil if none are configured.
func (c *Config) GetCorrelations(table string) []CorrelationGroup {
	if c == nil {
		return nil
	}
	tc, ok := c.Tables[table]
	if !ok {
		return nil
	}
	return tc.Correlations
}

// GetTemplate returns the template string for a given table and column,
// or empty string if none is configured.
func (c *Config) GetTemplate(table, column string) string {
	if c == nil {
		return ""
	}
	tc, ok := c.Tables[table]
	if !ok {
		return ""
	}
	return tc.Columns[column]
}
