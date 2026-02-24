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
