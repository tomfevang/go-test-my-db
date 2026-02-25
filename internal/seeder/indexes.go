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

// fetchFKColumnSets returns the ordered column lists for each FK constraint on
// the table. Each entry maps a constraint name to its columns in ordinal order.
func fetchFKColumnSets(db *sql.DB, schema, table string) ([][]string, error) {
	rows, err := db.Query(`
		SELECT CONSTRAINT_NAME, COLUMN_NAME
		FROM information_schema.KEY_COLUMN_USAGE
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		  AND REFERENCED_TABLE_NAME IS NOT NULL
		ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION`,
		schema, table,
	)
	if err != nil {
		return nil, fmt.Errorf("querying FK constraints for %s: %w", table, err)
	}
	defer rows.Close()

	var order []string
	groups := make(map[string][]string)
	for rows.Next() {
		var cname, col string
		if err := rows.Scan(&cname, &col); err != nil {
			return nil, err
		}
		if _, ok := groups[cname]; !ok {
			order = append(order, cname)
		}
		groups[cname] = append(groups[cname], col)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	result := make([][]string, 0, len(order))
	for _, name := range order {
		result = append(result, groups[name])
	}
	return result, nil
}

// filterFKBackingIndexes returns indexes that are safe to drop, excluding any
// index that is the sole backing index for a FK constraint. MySQL requires at
// least one index whose leading columns match each FK's column set.
func filterFKBackingIndexes(indexes []SecondaryIndex, fkColSets [][]string) (droppable, kept []SecondaryIndex) {
	if len(fkColSets) == 0 {
		return indexes, nil
	}

	// For each FK, count how many indexes can back it and record which ones.
	type fkInfo struct {
		cols     []string
		backers  []int // index positions in the indexes slice
	}
	fks := make([]fkInfo, len(fkColSets))
	for i, cols := range fkColSets {
		fks[i] = fkInfo{cols: cols}
		for j, idx := range indexes {
			if indexBacksFK(idx, cols) {
				fks[i].backers = append(fks[i].backers, j)
			}
		}
	}

	// Mark indexes that must be kept: if a FK has exactly one backing index,
	// that index cannot be dropped.
	mustKeep := make(map[int]bool)
	for _, fk := range fks {
		if len(fk.backers) == 1 {
			mustKeep[fk.backers[0]] = true
		}
	}

	for i, idx := range indexes {
		if mustKeep[i] {
			kept = append(kept, idx)
		} else {
			droppable = append(droppable, idx)
		}
	}
	return droppable, kept
}

// indexBacksFK reports whether an index can serve as the backing index for a FK
// constraint. MySQL requires the FK columns to be a leftmost prefix of the
// index columns.
func indexBacksFK(idx SecondaryIndex, fkCols []string) bool {
	if len(idx.Columns) < len(fkCols) {
		return false
	}
	for i, fkCol := range fkCols {
		if !strings.EqualFold(idx.Columns[i].Name, fkCol) {
			return false
		}
	}
	return true
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
