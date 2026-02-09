package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

type TableConfig struct {
	Columns map[string]string `yaml:"columns"`
}

type TestCase struct {
	Name   string `yaml:"name"`
	Query  string `yaml:"query"`
	Repeat int    `yaml:"repeat"` // <= 0 treated as 1
}

type Config struct {
	Tables map[string]TableConfig `yaml:"tables"`
	Tests  []TestCase             `yaml:"tests"`
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
// attempts to auto-detect "go-seed-my-db.yaml" in the current directory.
// Returns an empty Config if no file is found at the auto-detect path.
func LoadOrDefault(path string) (*Config, error) {
	if path != "" {
		return Load(path)
	}

	const defaultFile = "go-seed-my-db.yaml"
	if _, err := os.Stat(defaultFile); err != nil {
		// File doesn't exist — that's fine, return empty config.
		return &Config{}, nil
	}

	return Load(defaultFile)
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
