package seeder

import (
	"database/sql"
	"testing"
)

func TestBuildDropStatement(t *testing.T) {
	indexes := []SecondaryIndex{
		{Name: "idx_email", Unique: false, Columns: []IndexColumn{{Name: "email"}}},
		{Name: "idx_name", Unique: false, Columns: []IndexColumn{{Name: "name"}}},
	}

	got := buildDropStatement("users", indexes)
	want := "ALTER TABLE `users` DROP INDEX `idx_email`, DROP INDEX `idx_name`"
	if got != want {
		t.Errorf("got:\n  %s\nwant:\n  %s", got, want)
	}
}

func TestBuildDropStatement_Single(t *testing.T) {
	indexes := []SecondaryIndex{
		{Name: "idx_status", Unique: false, Columns: []IndexColumn{{Name: "status"}}},
	}

	got := buildDropStatement("orders", indexes)
	want := "ALTER TABLE `orders` DROP INDEX `idx_status`"
	if got != want {
		t.Errorf("got:\n  %s\nwant:\n  %s", got, want)
	}
}

func TestBuildRestoreStatement_Simple(t *testing.T) {
	indexes := []SecondaryIndex{
		{Name: "idx_email", Unique: false, Columns: []IndexColumn{{Name: "email"}}},
	}

	got := buildRestoreStatement("users", indexes)
	want := "ALTER TABLE `users` ADD INDEX `idx_email` (`email`)"
	if got != want {
		t.Errorf("got:\n  %s\nwant:\n  %s", got, want)
	}
}

func TestBuildRestoreStatement_UniqueIndex(t *testing.T) {
	indexes := []SecondaryIndex{
		{Name: "uniq_email", Unique: true, Columns: []IndexColumn{{Name: "email"}}},
	}

	got := buildRestoreStatement("users", indexes)
	want := "ALTER TABLE `users` ADD UNIQUE INDEX `uniq_email` (`email`)"
	if got != want {
		t.Errorf("got:\n  %s\nwant:\n  %s", got, want)
	}
}

func TestBuildRestoreStatement_CompositeWithPrefix(t *testing.T) {
	indexes := []SecondaryIndex{
		{
			Name:   "idx_name_bio",
			Unique: false,
			Columns: []IndexColumn{
				{Name: "name"},
				{Name: "bio", SubPart: sql.NullInt64{Int64: 100, Valid: true}},
			},
		},
	}

	got := buildRestoreStatement("users", indexes)
	want := "ALTER TABLE `users` ADD INDEX `idx_name_bio` (`name`, `bio`(100))"
	if got != want {
		t.Errorf("got:\n  %s\nwant:\n  %s", got, want)
	}
}

func TestBuildRestoreStatement_Mixed(t *testing.T) {
	indexes := []SecondaryIndex{
		{Name: "idx_status", Unique: false, Columns: []IndexColumn{{Name: "status"}}},
		{
			Name:   "uniq_email_tenant",
			Unique: true,
			Columns: []IndexColumn{
				{Name: "email"},
				{Name: "tenant_id"},
			},
		},
	}

	got := buildRestoreStatement("users", indexes)
	want := "ALTER TABLE `users` ADD INDEX `idx_status` (`status`), ADD UNIQUE INDEX `uniq_email_tenant` (`email`, `tenant_id`)"
	if got != want {
		t.Errorf("got:\n  %s\nwant:\n  %s", got, want)
	}
}

func TestFilterFKBackingIndexes_NoFKs(t *testing.T) {
	indexes := []SecondaryIndex{
		{Name: "idx_email", Columns: []IndexColumn{{Name: "email"}}},
		{Name: "idx_status", Columns: []IndexColumn{{Name: "status"}}},
	}

	droppable, kept := filterFKBackingIndexes(indexes, nil)
	if len(droppable) != 2 {
		t.Errorf("expected 2 droppable, got %d", len(droppable))
	}
	if len(kept) != 0 {
		t.Errorf("expected 0 kept, got %d", len(kept))
	}
}

func TestFilterFKBackingIndexes_SoleBacker(t *testing.T) {
	// FK on companyId has only one backing index → must keep it.
	indexes := []SecondaryIndex{
		{Name: "idx_companyId", Columns: []IndexColumn{{Name: "companyId"}}},
		{Name: "idx_status", Columns: []IndexColumn{{Name: "status"}}},
	}
	fkColSets := [][]string{{"companyId"}}

	droppable, kept := filterFKBackingIndexes(indexes, fkColSets)
	if len(droppable) != 1 || droppable[0].Name != "idx_status" {
		t.Errorf("expected idx_status droppable, got %v", droppable)
	}
	if len(kept) != 1 || kept[0].Name != "idx_companyId" {
		t.Errorf("expected idx_companyId kept, got %v", kept)
	}
}

func TestFilterFKBackingIndexes_MultipleBacker(t *testing.T) {
	// FK on companyId has two backing indexes (single + composite) → both droppable.
	indexes := []SecondaryIndex{
		{Name: "idx_companyId", Columns: []IndexColumn{{Name: "companyId"}}},
		{Name: "idx_companyId_status", Columns: []IndexColumn{{Name: "companyId"}, {Name: "status"}}},
		{Name: "idx_name", Columns: []IndexColumn{{Name: "name"}}},
	}
	fkColSets := [][]string{{"companyId"}}

	droppable, kept := filterFKBackingIndexes(indexes, fkColSets)
	if len(droppable) != 3 {
		t.Errorf("expected 3 droppable, got %d", len(droppable))
	}
	if len(kept) != 0 {
		t.Errorf("expected 0 kept, got %d", len(kept))
	}
}

func TestFilterFKBackingIndexes_CompositeFK(t *testing.T) {
	// Composite FK (companyId, employeeId) — only the composite index backs it.
	indexes := []SecondaryIndex{
		{Name: "idx_companyId", Columns: []IndexColumn{{Name: "companyId"}}},
		{Name: "idx_compound", Columns: []IndexColumn{{Name: "companyId"}, {Name: "employeeId"}}},
	}
	fkColSets := [][]string{{"companyId", "employeeId"}}

	droppable, kept := filterFKBackingIndexes(indexes, fkColSets)
	if len(droppable) != 1 || droppable[0].Name != "idx_companyId" {
		t.Errorf("expected idx_companyId droppable, got %v", droppable)
	}
	if len(kept) != 1 || kept[0].Name != "idx_compound" {
		t.Errorf("expected idx_compound kept, got %v", kept)
	}
}

func TestFilterFKBackingIndexes_CaseInsensitive(t *testing.T) {
	// FK column name differs in case from index column name.
	indexes := []SecondaryIndex{
		{Name: "idx_companyid", Columns: []IndexColumn{{Name: "companyId"}}},
	}
	fkColSets := [][]string{{"companyid"}}

	droppable, kept := filterFKBackingIndexes(indexes, fkColSets)
	if len(droppable) != 0 {
		t.Errorf("expected 0 droppable, got %d", len(droppable))
	}
	if len(kept) != 1 {
		t.Errorf("expected 1 kept, got %d", len(kept))
	}
}

func TestIndexBacksFK(t *testing.T) {
	tests := []struct {
		name   string
		idx    SecondaryIndex
		fkCols []string
		want   bool
	}{
		{
			name:   "exact match",
			idx:    SecondaryIndex{Columns: []IndexColumn{{Name: "companyId"}}},
			fkCols: []string{"companyId"},
			want:   true,
		},
		{
			name:   "prefix match",
			idx:    SecondaryIndex{Columns: []IndexColumn{{Name: "companyId"}, {Name: "status"}}},
			fkCols: []string{"companyId"},
			want:   true,
		},
		{
			name:   "no match different column",
			idx:    SecondaryIndex{Columns: []IndexColumn{{Name: "status"}}},
			fkCols: []string{"companyId"},
			want:   false,
		},
		{
			name:   "index too short for composite FK",
			idx:    SecondaryIndex{Columns: []IndexColumn{{Name: "companyId"}}},
			fkCols: []string{"companyId", "employeeId"},
			want:   false,
		},
		{
			name:   "case insensitive match",
			idx:    SecondaryIndex{Columns: []IndexColumn{{Name: "CompanyId"}}},
			fkCols: []string{"companyid"},
			want:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := indexBacksFK(tt.idx, tt.fkCols)
			if got != tt.want {
				t.Errorf("indexBacksFK() = %v, want %v", got, tt.want)
			}
		})
	}
}
