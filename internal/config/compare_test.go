package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadCompare(t *testing.T) {
	dir := t.TempDir()

	content := `configs:
  - label: alpha
    file: seed.yaml
  - label: beta
    file: seed.yaml

tests:
  - name: test1
    repeat: 50
    queries:
      alpha: "SELECT 1"
      beta: "SELECT 2"
  - name: test2
    repeat: 10
    queries:
      alpha: "SELECT 3"
`
	configPath := filepath.Join(dir, "compare.yaml")
	os.WriteFile(configPath, []byte(content), 0644)

	cc, err := LoadCompare(configPath)
	if err != nil {
		t.Fatalf("LoadCompare() error: %v", err)
	}

	if len(cc.Configs) != 2 {
		t.Fatalf("expected 2 configs, got %d", len(cc.Configs))
	}
	if cc.Configs[0].Label != "alpha" {
		t.Errorf("expected label 'alpha', got %q", cc.Configs[0].Label)
	}
	if !filepath.IsAbs(cc.Configs[0].File) {
		t.Errorf("expected absolute path, got %q", cc.Configs[0].File)
	}
	// Resolved path should point into the temp dir.
	if filepath.Dir(cc.Configs[0].File) != dir {
		t.Errorf("expected file in %s, got %q", dir, cc.Configs[0].File)
	}

	if len(cc.Tests) != 2 {
		t.Fatalf("expected 2 tests, got %d", len(cc.Tests))
	}
	if cc.Tests[0].Name != "test1" {
		t.Errorf("expected name 'test1', got %q", cc.Tests[0].Name)
	}
	if cc.Tests[0].Repeat != 50 {
		t.Errorf("expected repeat 50, got %d", cc.Tests[0].Repeat)
	}
	if cc.Tests[0].Queries["alpha"] != "SELECT 1" {
		t.Errorf("unexpected query for alpha: %q", cc.Tests[0].Queries["alpha"])
	}
}

func TestLoadCompare_AbsolutePaths(t *testing.T) {
	dir := t.TempDir()

	content := `configs:
  - label: a
    file: /absolute/path/seed.yaml
`
	configPath := filepath.Join(dir, "compare.yaml")
	os.WriteFile(configPath, []byte(content), 0644)

	cc, err := LoadCompare(configPath)
	if err != nil {
		t.Fatalf("LoadCompare() error: %v", err)
	}

	// Absolute path should be preserved, not joined with dir.
	if cc.Configs[0].File != "/absolute/path/seed.yaml" {
		t.Errorf("expected absolute path preserved, got %q", cc.Configs[0].File)
	}
}

func TestLoadCompare_DuplicateLabel(t *testing.T) {
	dir := t.TempDir()

	content := `configs:
  - label: same
    file: a.yaml
  - label: same
    file: b.yaml
`
	configPath := filepath.Join(dir, "compare.yaml")
	os.WriteFile(configPath, []byte(content), 0644)

	_, err := LoadCompare(configPath)
	if err == nil {
		t.Fatal("expected error for duplicate label")
	}
}

func TestLoadCompare_EmptyLabel(t *testing.T) {
	dir := t.TempDir()

	content := `configs:
  - label: ""
    file: a.yaml
`
	configPath := filepath.Join(dir, "compare.yaml")
	os.WriteFile(configPath, []byte(content), 0644)

	_, err := LoadCompare(configPath)
	if err == nil {
		t.Fatal("expected error for empty label")
	}
}

func TestLoadCompare_UndefinedQueryLabel(t *testing.T) {
	dir := t.TempDir()

	content := `configs:
  - label: alpha
    file: a.yaml

tests:
  - name: test1
    queries:
      alpha: "SELECT 1"
      typo: "SELECT 2"
`
	configPath := filepath.Join(dir, "compare.yaml")
	os.WriteFile(configPath, []byte(content), 0644)

	_, err := LoadCompare(configPath)
	if err == nil {
		t.Fatal("expected error for undefined query label 'typo'")
	}
}

func TestLoadCompare_NoConfigs(t *testing.T) {
	dir := t.TempDir()

	content := `configs: []
tests: []
`
	configPath := filepath.Join(dir, "compare.yaml")
	os.WriteFile(configPath, []byte(content), 0644)

	_, err := LoadCompare(configPath)
	if err == nil {
		t.Fatal("expected error for empty configs")
	}
}

func TestIsCompareConfig(t *testing.T) {
	dir := t.TempDir()

	comparePath := filepath.Join(dir, "compare.yaml")
	os.WriteFile(comparePath, []byte("configs:\n  - label: x\n    file: y.yaml\n"), 0644)

	seedPath := filepath.Join(dir, "seed.yaml")
	os.WriteFile(seedPath, []byte("options:\n  schema: s.sql\ntables: {}\n"), 0644)

	if !IsCompareConfig(comparePath) {
		t.Error("expected true for comparison config")
	}
	if IsCompareConfig(seedPath) {
		t.Error("expected false for seed config")
	}
	if IsCompareConfig(filepath.Join(dir, "nonexistent.yaml")) {
		t.Error("expected false for nonexistent file")
	}
}

func TestTestCasesForLabel(t *testing.T) {
	cc := &CompareConfig{
		Tests: []CompareTest{
			{Name: "t1", Repeat: 50, Queries: map[string]string{"a": "SELECT 1", "b": "SELECT 2"}},
			{Name: "t2", Repeat: 10, Queries: map[string]string{"a": "SELECT 3"}},
			{Name: "t3", Repeat: 0, Queries: map[string]string{"b": "SELECT 4"}},
			{Name: "t4", Repeat: 5, Queries: map[string]string{"a": ""}}, // empty query = skipped
		},
	}

	casesA := cc.TestCasesForLabel("a")
	if len(casesA) != 2 {
		t.Fatalf("expected 2 cases for 'a', got %d", len(casesA))
	}
	if casesA[0].Name != "t1" || casesA[0].Query != "SELECT 1" || casesA[0].Repeat != 50 {
		t.Errorf("unexpected case[0]: %+v", casesA[0])
	}
	if casesA[1].Name != "t2" || casesA[1].Repeat != 10 {
		t.Errorf("unexpected case[1]: %+v", casesA[1])
	}

	casesB := cc.TestCasesForLabel("b")
	if len(casesB) != 2 {
		t.Fatalf("expected 2 cases for 'b', got %d", len(casesB))
	}
	if casesB[1].Repeat != 1 {
		t.Errorf("expected repeat 1 (normalized from 0), got %d", casesB[1].Repeat)
	}

	casesNone := cc.TestCasesForLabel("nonexistent")
	if len(casesNone) != 0 {
		t.Errorf("expected 0 cases for nonexistent label, got %d", len(casesNone))
	}
}
