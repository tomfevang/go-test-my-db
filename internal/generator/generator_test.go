package generator

import (
	"testing"

	"github.com/tomfevang/go-test-my-db/internal/config"
	"github.com/tomfevang/go-test-my-db/internal/introspect"
)

func TestNameBasedLabel(t *testing.T) {
	tests := []struct {
		name     string
		col      introspect.Column
		expected string
	}{
		// String heuristics (varchar).
		{"uuid", introspect.Column{Name: "uuid", DataType: "varchar"}, "UUID()"},
		{"email", introspect.Column{Name: "email", DataType: "varchar"}, "Email()"},
		{"user_email", introspect.Column{Name: "user_email", DataType: "varchar"}, "Email()"},
		{"first_name", introspect.Column{Name: "first_name", DataType: "varchar"}, "FirstName()"},
		{"last_name", introspect.Column{Name: "last_name", DataType: "varchar"}, "LastName()"},
		{"name", introspect.Column{Name: "name", DataType: "varchar"}, "Name()"},
		{"display_name", introspect.Column{Name: "display_name", DataType: "varchar"}, "Name()"},
		{"phone", introspect.Column{Name: "phone", DataType: "varchar"}, "Phone()"},
		{"username", introspect.Column{Name: "username", DataType: "varchar"}, "Username()"},
		{"login", introspect.Column{Name: "login", DataType: "varchar"}, "Username()"},
		{"password", introspect.Column{Name: "password", DataType: "varchar"}, "Password()"},
		{"address", introspect.Column{Name: "address", DataType: "varchar"}, "Street()"},
		{"city", introspect.Column{Name: "city", DataType: "varchar"}, "City()"},
		{"state", introspect.Column{Name: "state", DataType: "varchar"}, "State()"},
		{"zip_code", introspect.Column{Name: "zip_code", DataType: "varchar"}, "Zip()"},
		{"country", introspect.Column{Name: "country", DataType: "varchar"}, "Country()"},
		{"url", introspect.Column{Name: "url", DataType: "varchar"}, "URL()"},
		{"ip", introspect.Column{Name: "ip", DataType: "varchar"}, "IPv4Address()"},
		{"company", introspect.Column{Name: "company", DataType: "varchar"}, "Company()"},
		{"title", introspect.Column{Name: "title", DataType: "varchar"}, "JobTitle()"},
		{"description", introspect.Column{Name: "description", DataType: "varchar"}, "Sentence(10)"},
		{"currency", introspect.Column{Name: "currency", DataType: "varchar"}, "CurrencyShort()"},
		{"color", introspect.Column{Name: "color", DataType: "varchar"}, "Color()"},
		{"avatar", introspect.Column{Name: "avatar", DataType: "varchar"}, "URL()"},

		// Date heuristics (datetime).
		{"created_at", introspect.Column{Name: "created_at", DataType: "datetime"}, "DateRange(2020-2025)"},
		{"date_of_birth", introspect.Column{Name: "date_of_birth", DataType: "datetime"}, "DateRange(1950-2005)"},

		// Numeric heuristics (int).
		{"price", introspect.Column{Name: "price", DataType: "int"}, "Price(1, 1000)"},
		{"latitude", introspect.Column{Name: "latitude", DataType: "int"}, "Latitude()"},
		{"longitude", introspect.Column{Name: "longitude", DataType: "int"}, "Longitude()"},

		// No-match cases.
		{"id_int", introspect.Column{Name: "id", DataType: "int"}, ""},
		{"email_wrong_type", introspect.Column{Name: "email", DataType: "int"}, ""},
		{"created_at_wrong_type", introspect.Column{Name: "created_at", DataType: "varchar"}, ""},
		{"random_column", introspect.Column{Name: "some_random_column", DataType: "varchar"}, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NameBasedLabel(tt.col)
			if got != tt.expected {
				t.Errorf("NameBasedLabel(%q, %q) = %q, want %q",
					tt.col.Name, tt.col.DataType, got, tt.expected)
			}
		})
	}
}

func TestDescribeGenerator(t *testing.T) {
	tests := []struct {
		name      string
		col       introspect.Column
		tableName string
		cfg       *config.Config
		expected  string
	}{
		{
			name:      "auto_increment",
			col:       introspect.Column{Name: "id", DataType: "int", IsAutoInc: true},
			tableName: "users",
			cfg:       &config.Config{},
			expected:  "auto_increment",
		},
		{
			name:      "generated",
			col:       introspect.Column{Name: "full_name", DataType: "varchar", IsGenerated: true},
			tableName: "users",
			cfg:       &config.Config{},
			expected:  "generated (skipped)",
		},
		{
			name: "foreign_key",
			col: introspect.Column{
				Name: "user_id", DataType: "int",
				FK: &introspect.ForeignKey{ReferencedTable: "users", ReferencedColumn: "id"},
			},
			tableName: "orders",
			cfg:       &config.Config{},
			expected:  "fk -> users.id",
		},
		{
			name:      "sequential_pk",
			col:       introspect.Column{Name: "id", DataType: "int", IsPrimaryKey: true},
			tableName: "users",
			cfg:       &config.Config{},
			expected:  "sequential",
		},
		{
			name:      "enum",
			col:       introspect.Column{Name: "status", DataType: "enum", EnumValues: []string{"a", "b", "c"}},
			tableName: "orders",
			cfg:       &config.Config{},
			expected:  "enum: [a, b, c]",
		},
		{
			name: "enum_truncated",
			col: introspect.Column{
				Name: "status", DataType: "enum",
				EnumValues: []string{"a", "b", "c", "d", "e", "f", "g"},
			},
			tableName: "orders",
			cfg:       &config.Config{},
			expected:  "enum: [a, b, c, d, e]",
		},
		{
			name:      "template",
			col:       introspect.Column{Name: "status", DataType: "varchar"},
			tableName: "orders",
			cfg: &config.Config{
				Tables: map[string]config.TableConfig{
					"orders": {Columns: map[string]string{"status": "{{ SliceAny \"active\" \"inactive\" | Randomize }}"}},
				},
			},
			expected: "template: {{ SliceAny \"active\" \"inactive\" | Randomize }}",
		},
		{
			name:      "heuristic",
			col:       introspect.Column{Name: "email", DataType: "varchar"},
			tableName: "users",
			cfg:       &config.Config{},
			expected:  "heuristic: Email()",
		},
		{
			name:      "type_based",
			col:       introspect.Column{Name: "foo", DataType: "varchar"},
			tableName: "users",
			cfg:       &config.Config{},
			expected:  "type-based: varchar",
		},
		{
			name: "auto_inc_beats_fk",
			col: introspect.Column{
				Name: "id", DataType: "int", IsAutoInc: true,
				FK: &introspect.ForeignKey{ReferencedTable: "other", ReferencedColumn: "id"},
			},
			tableName: "users",
			cfg:       &config.Config{},
			expected:  "auto_increment",
		},
		{
			name: "fk_beats_heuristic",
			col: introspect.Column{
				Name: "email", DataType: "varchar",
				FK: &introspect.ForeignKey{ReferencedTable: "emails", ReferencedColumn: "addr"},
			},
			tableName: "users",
			cfg:       &config.Config{},
			expected:  "fk -> emails.addr",
		},
		{
			name:      "template_beats_heuristic",
			col:       introspect.Column{Name: "email", DataType: "varchar"},
			tableName: "users",
			cfg: &config.Config{
				Tables: map[string]config.TableConfig{
					"users": {Columns: map[string]string{"email": "{{ Email }}@example.com"}},
				},
			},
			expected: "template: {{ Email }}@example.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DescribeGenerator(tt.col, tt.tableName, tt.cfg)
			if got != tt.expected {
				t.Errorf("DescribeGenerator() = %q, want %q", got, tt.expected)
			}
		})
	}
}
