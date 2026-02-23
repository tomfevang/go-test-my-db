package cmd

import (
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/tomfevang/go-seed-my-db/internal/introspect"
)

var update = flag.Bool("update", false, "update golden files")

func TestRedactDSNPassword(t *testing.T) {
	tests := []struct {
		name     string
		dsn      string
		expected string
	}{
		{
			"standard",
			"user:secret@tcp(localhost:3306)/mydb",
			"user:***@tcp(localhost:3306)/mydb",
		},
		{
			"empty_password",
			"user:@tcp(localhost:3306)/mydb",
			"user:***@tcp(localhost:3306)/mydb",
		},
		{
			"no_at_sign",
			"localhost:3306/mydb",
			"localhost:3306/mydb",
		},
		{
			"no_colon_before_at",
			"user@tcp(localhost:3306)/mydb",
			"user@tcp(localhost:3306)/mydb",
		},
		{
			"empty_string",
			"",
			"",
		},
		{
			"with_query_params",
			"user:secret@tcp(localhost:3306)/mydb?charset=utf8",
			"user:***@tcp(localhost:3306)/mydb?charset=utf8",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := redactDSNPassword(tt.dsn)
			if got != tt.expected {
				t.Errorf("redactDSNPassword(%q) = %q, want %q", tt.dsn, got, tt.expected)
			}
		})
	}
}

func TestBuildInitYAML(t *testing.T) {
	tables := []*introspect.Table{
		{
			Name: "users",
			Columns: []introspect.Column{
				{Name: "id", DataType: "int", IsAutoInc: true, IsPrimaryKey: true},
				{Name: "email", DataType: "varchar"},
				{Name: "first_name", DataType: "varchar"},
				{Name: "last_name", DataType: "varchar"},
				{Name: "created_at", DataType: "datetime"},
			},
		},
		{
			Name: "orders",
			Columns: []introspect.Column{
				{Name: "id", DataType: "int", IsAutoInc: true, IsPrimaryKey: true},
				{Name: "user_id", DataType: "int", FK: &introspect.ForeignKey{
					ReferencedTable: "users", ReferencedColumn: "id",
				}},
				{Name: "status", DataType: "enum", EnumValues: []string{"pending", "shipped", "delivered"}},
				{Name: "total", DataType: "decimal"},
			},
		},
		{
			Name: "products",
			Columns: []introspect.Column{
				{Name: "id", DataType: "int", IsAutoInc: true, IsPrimaryKey: true},
				{Name: "name", DataType: "varchar"},
				{Name: "slug", DataType: "varchar", IsGenerated: true},
				{Name: "price", DataType: "decimal"},
			},
		},
	}

	got := buildInitYAML("testuser:secret@tcp(localhost:3306)/testdb", tables)

	golden := filepath.Join("testdata", "init_golden.yaml")
	if *update {
		if err := os.WriteFile(golden, []byte(got), 0644); err != nil {
			t.Fatalf("failed to update golden file: %v", err)
		}
	}

	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("failed to read golden file (run with -update to create): %v", err)
	}

	if got != string(want) {
		t.Errorf("buildInitYAML() output does not match golden file.\nGot:\n%s\nWant:\n%s", got, string(want))
	}
}
