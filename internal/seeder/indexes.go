package seeder

import (
	"database/sql"
	"fmt"
	"strings"
)

// SecondaryIndex holds the metadata for a single secondary (non-PRIMARY) index.
type SecondaryIndex struct {
	Name     string
	Unique   bool
	Columns  []IndexColumn
}

// IndexColumn represents one column in an index, with optional prefix length.
type IndexColumn struct {
	Name    string
	SubPart sql.NullInt64 // prefix length, NULL when the full column is indexed
}

// fetchSecondaryIndexes retrieves all non-PRIMARY indexes for a table from
// information_schema.STATISTICS, preserving multi-column order via SEQ_IN_INDEX.
func fetchSecondaryIndexes(db *sql.DB, schema, table string) ([]SecondaryIndex, error) {
	rows, err := db.Query(`
		SELECT INDEX_NAME, NON_UNIQUE, COLUMN_NAME, SUB_PART
		FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND INDEX_NAME != 'PRIMARY'
		ORDER BY INDEX_NAME, SEQ_IN_INDEX`,
		schema, table,
	)
	if err != nil {
		return nil, fmt.Errorf("querying indexes for %s: %w", table, err)
	}
	defer rows.Close()

	// Collect columns grouped by index name, preserving order.
	type entry struct {
		unique  bool
		columns []IndexColumn
	}
	var indexOrder []string
	indexMap := make(map[string]*entry)

	for rows.Next() {
		var name, colName string
		var nonUnique int
		var subPart sql.NullInt64
		if err := rows.Scan(&name, &nonUnique, &colName, &subPart); err != nil {
			return nil, err
		}
		e, ok := indexMap[name]
		if !ok {
			e = &entry{unique: nonUnique == 0}
			indexMap[name] = e
			indexOrder = append(indexOrder, name)
		}
		e.columns = append(e.columns, IndexColumn{Name: colName, SubPart: subPart})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	indexes := make([]SecondaryIndex, 0, len(indexOrder))
	for _, name := range indexOrder {
		e := indexMap[name]
		indexes = append(indexes, SecondaryIndex{
			Name:    name,
			Unique:  e.unique,
			Columns: e.columns,
		})
	}
	return indexes, nil
}

// buildDropStatement builds a single ALTER TABLE that drops all given indexes.
// Example: ALTER TABLE `t` DROP INDEX `idx1`, DROP INDEX `idx2`
func buildDropStatement(table string, indexes []SecondaryIndex) string {
	drops := make([]string, len(indexes))
	for i, idx := range indexes {
		drops[i] = fmt.Sprintf("DROP INDEX `%s`", idx.Name)
	}
	return fmt.Sprintf("ALTER TABLE `%s` %s", table, strings.Join(drops, ", "))
}

// buildRestoreStatement builds a single ALTER TABLE that recreates all given indexes.
// Example: ALTER TABLE `t` ADD INDEX `idx1` (`col1`), ADD UNIQUE INDEX `idx2` (`col2`(10))
func buildRestoreStatement(table string, indexes []SecondaryIndex) string {
	adds := make([]string, len(indexes))
	for i, idx := range indexes {
		cols := make([]string, len(idx.Columns))
		for j, c := range idx.Columns {
			if c.SubPart.Valid {
				cols[j] = fmt.Sprintf("`%s`(%d)", c.Name, c.SubPart.Int64)
			} else {
				cols[j] = fmt.Sprintf("`%s`", c.Name)
			}
		}
		kind := "INDEX"
		if idx.Unique {
			kind = "UNIQUE INDEX"
		}
		adds[i] = fmt.Sprintf("ADD %s `%s` (%s)", kind, idx.Name, strings.Join(cols, ", "))
	}
	return fmt.Sprintf("ALTER TABLE `%s` %s", table, strings.Join(adds, ", "))
}

// dropSecondaryIndexes drops all given secondary indexes in a single ALTER TABLE.
func dropSecondaryIndexes(db *sql.DB, table string, indexes []SecondaryIndex) error {
	stmt := buildDropStatement(table, indexes)
	if _, err := db.Exec(stmt); err != nil {
		return fmt.Errorf("dropping indexes on %s: %w", table, err)
	}
	return nil
}

// restoreSecondaryIndexes recreates all given secondary indexes in a single ALTER TABLE.
func restoreSecondaryIndexes(db *sql.DB, table string, indexes []SecondaryIndex) error {
	stmt := buildRestoreStatement(table, indexes)
	if _, err := db.Exec(stmt); err != nil {
		return fmt.Errorf("restoring indexes on %s: %w", table, err)
	}
	return nil
}
