package cmd

import (
	"testing"

	"github.com/tomfevang/go-test-my-db/internal/config"
	"github.com/tomfevang/go-test-my-db/internal/depgraph"
)

func TestComputeRowCounts(t *testing.T) {
	tests := []struct {
		name      string
		order     []string
		relations *depgraph.TableRelations
		cfg       *config.Config
		baseRows  int
		minC      int
		maxC      int
		maxCap    int
		expected  map[string]int
	}{
		{
			name:      "root_only",
			order:     []string{"users"},
			relations: &depgraph.TableRelations{Parents: map[string][]string{}},
			cfg:       &config.Config{},
			baseRows:  100,
			minC:      10,
			maxC:      10,
			maxCap:    10_000_000,
			expected:  map[string]int{"users": 100},
		},
		{
			name:  "parent_and_child",
			order: []string{"users", "orders"},
			relations: &depgraph.TableRelations{
				Parents: map[string][]string{"orders": {"users"}},
			},
			cfg:      &config.Config{},
			baseRows: 100,
			minC:     10,
			maxC:     10,
			maxCap:   10_000_000,
			expected: map[string]int{"users": 100, "orders": 1000},
		},
		{
			name:  "three_levels",
			order: []string{"countries", "cities", "addresses"},
			relations: &depgraph.TableRelations{
				Parents: map[string][]string{
					"cities":    {"countries"},
					"addresses": {"cities"},
				},
			},
			cfg:      &config.Config{},
			baseRows: 10,
			minC:     5,
			maxC:     5,
			maxCap:   10_000_000,
			expected: map[string]int{"countries": 10, "cities": 50, "addresses": 250},
		},
		{
			name:  "config_override",
			order: []string{"users", "orders"},
			relations: &depgraph.TableRelations{
				Parents: map[string][]string{"orders": {"users"}},
			},
			cfg: &config.Config{
				Tables: map[string]config.TableConfig{
					"orders": {Rows: 500},
				},
			},
			baseRows: 100,
			minC:     10,
			maxC:     10,
			maxCap:   10_000_000,
			expected: map[string]int{"users": 100, "orders": 500},
		},
		{
			name:  "max_rows_cap",
			order: []string{"users", "orders"},
			relations: &depgraph.TableRelations{
				Parents: map[string][]string{"orders": {"users"}},
			},
			cfg:      &config.Config{},
			baseRows: 10000,
			minC:     50,
			maxC:     50,
			maxCap:   100000,
			expected: map[string]int{"users": 10000, "orders": 100000},
		},
		{
			name:  "multiple_parents_picks_max",
			order: []string{"users", "products", "reviews"},
			relations: &depgraph.TableRelations{
				Parents: map[string][]string{"reviews": {"users", "products"}},
			},
			cfg:      &config.Config{},
			baseRows: 100,
			minC:     5,
			maxC:     5,
			maxCap:   10_000_000,
			// Both parents have 100 rows, so max is 100 * 5 = 500.
			expected: map[string]int{"users": 100, "products": 100, "reviews": 500},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := computeRowCounts(tt.order, tt.relations, tt.cfg, tt.baseRows, tt.minC, tt.maxC, tt.maxCap)
			for table, want := range tt.expected {
				if got[table] != want {
					t.Errorf("table %q: got %d rows, want %d", table, got[table], want)
				}
			}
		})
	}
}

func TestExtractSchema(t *testing.T) {
	tests := []struct {
		name     string
		dsn      string
		expected string
	}{
		{"standard", "user:pass@tcp(localhost:3306)/mydb", "mydb"},
		{"with_params", "user:pass@tcp(localhost:3306)/mydb?charset=utf8", "mydb"},
		{"no_slash", "user:pass@tcp(localhost:3306)", ""},
		{"trailing_slash", "user:pass@tcp(localhost:3306)/", ""},
		{"empty", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractSchema(tt.dsn)
			if got != tt.expected {
				t.Errorf("extractSchema(%q) = %q, want %q", tt.dsn, got, tt.expected)
			}
		})
	}
}

func TestEnsureAllowAllFiles(t *testing.T) {
	tests := []struct {
		name     string
		dsn      string
		expected string
	}{
		{
			"no_params",
			"user:pass@tcp(localhost:3306)/mydb",
			"user:pass@tcp(localhost:3306)/mydb?allowAllFiles=true",
		},
		{
			"existing_params",
			"user:pass@tcp(localhost:3306)/mydb?charset=utf8",
			"user:pass@tcp(localhost:3306)/mydb?charset=utf8&allowAllFiles=true",
		},
		{
			"already_present",
			"user:pass@tcp(localhost:3306)/mydb?allowAllFiles=true",
			"user:pass@tcp(localhost:3306)/mydb?allowAllFiles=true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ensureAllowAllFiles(tt.dsn)
			if got != tt.expected {
				t.Errorf("ensureAllowAllFiles(%q) = %q, want %q", tt.dsn, got, tt.expected)
			}
		})
	}
}
