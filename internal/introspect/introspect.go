package introspect

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"
)

type ForeignKey struct {
	ReferencedTable  string
	ReferencedColumn string
}

type Column struct {
	Name         string
	DataType     string   // e.g. "varchar", "int", "enum"
	ColumnType   string   // e.g. "enum('a','b')", "int unsigned"
	IsNullable   bool
	IsAutoInc    bool
	IsGenerated  bool
	IsPrimaryKey bool
	IsUnique     bool
	MaxLength    *int64
	Precision    *int64
	Scale        *int64
	EnumValues   []string // parsed from ColumnType for enums
	Default      *string
	FK           *ForeignKey
}

// IsIntegerType returns true if the column's data type is an integer type.
func (c Column) IsIntegerType() bool {
	switch strings.ToLower(c.DataType) {
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint":
		return true
	default:
		return false
	}
}

// UniqueIndex represents a unique index (excluding PRIMARY KEY).
type UniqueIndex struct {
	Name    string
	Columns []string
}

type Table struct {
	Name          string
	Columns       []Column
	UniqueIndexes []UniqueIndex
}

var enumRegex = regexp.MustCompile(`'([^']*)'`)

func parseEnumValues(columnType string) []string {
	matches := enumRegex.FindAllStringSubmatch(columnType, -1)
	values := make([]string, 0, len(matches))
	for _, m := range matches {
		values = append(values, m[1])
	}
	return values
}

// ListTables returns all base table names in the given database schema.
func ListTables(db *sql.DB, schema string) ([]string, error) {
	rows, err := db.Query(
		`SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
		 WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'
		 ORDER BY TABLE_NAME`, schema)
	if err != nil {
		return nil, fmt.Errorf("listing tables: %w", err)
	}
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("scanning table name: %w", err)
		}
		tables = append(tables, name)
	}
	return tables, rows.Err()
}

// IntrospectTable returns the full column and FK metadata for a single table.
func IntrospectTable(db *sql.DB, schema, tableName string) (*Table, error) {
	columns, err := introspectColumns(db, schema, tableName)
	if err != nil {
		return nil, err
	}

	fks, err := introspectFKs(db, schema, tableName)
	if err != nil {
		return nil, err
	}

	// Attach FKs to their columns.
	for i := range columns {
		if fk, ok := fks[columns[i].Name]; ok {
			columns[i].FK = fk
		}
	}

	uniqueIdxs, err := introspectUniqueIndexes(db, schema, tableName)
	if err != nil {
		return nil, err
	}

	return &Table{Name: tableName, Columns: columns, UniqueIndexes: uniqueIdxs}, nil
}

func introspectColumns(db *sql.DB, schema, tableName string) ([]Column, error) {
	rows, err := db.Query(`
		SELECT COLUMN_NAME, DATA_TYPE, COLUMN_TYPE, IS_NULLABLE,
		       COLUMN_KEY, EXTRA, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION,
		       NUMERIC_SCALE, COLUMN_DEFAULT
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION`, schema, tableName)
	if err != nil {
		return nil, fmt.Errorf("introspecting columns for %s: %w", tableName, err)
	}
	defer rows.Close()

	var columns []Column
	for rows.Next() {
		var (
			col        Column
			isNullable string
			colKey     string
			extra      string
			maxLen     sql.NullInt64
			precision  sql.NullInt64
			scale      sql.NullInt64
			def        sql.NullString
		)
		if err := rows.Scan(
			&col.Name, &col.DataType, &col.ColumnType,
			&isNullable, &colKey, &extra,
			&maxLen, &precision, &scale, &def,
		); err != nil {
			return nil, fmt.Errorf("scanning column for %s: %w", tableName, err)
		}

		col.IsNullable = isNullable == "YES"
		col.IsPrimaryKey = colKey == "PRI"
		col.IsUnique = colKey == "UNI"
		col.IsAutoInc = strings.Contains(extra, "auto_increment")
		col.IsGenerated = strings.Contains(extra, "GENERATED")

		if maxLen.Valid {
			col.MaxLength = &maxLen.Int64
		}
		if precision.Valid {
			col.Precision = &precision.Int64
		}
		if scale.Valid {
			col.Scale = &scale.Int64
		}
		if def.Valid {
			col.Default = &def.String
		}

		if strings.ToLower(col.DataType) == "enum" || strings.ToLower(col.DataType) == "set" {
			col.EnumValues = parseEnumValues(col.ColumnType)
		}

		columns = append(columns, col)
	}
	return columns, rows.Err()
}

func introspectFKs(db *sql.DB, schema, tableName string) (map[string]*ForeignKey, error) {
	rows, err := db.Query(`
		SELECT COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
		FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND REFERENCED_TABLE_NAME IS NOT NULL`,
		schema, tableName)
	if err != nil {
		return nil, fmt.Errorf("introspecting FKs for %s: %w", tableName, err)
	}
	defer rows.Close()

	fks := make(map[string]*ForeignKey)
	for rows.Next() {
		var colName, refTable, refCol string
		if err := rows.Scan(&colName, &refTable, &refCol); err != nil {
			return nil, fmt.Errorf("scanning FK for %s: %w", tableName, err)
		}
		fks[colName] = &ForeignKey{
			ReferencedTable:  refTable,
			ReferencedColumn: refCol,
		}
	}
	return fks, rows.Err()
}

func introspectUniqueIndexes(db *sql.DB, schema, tableName string) ([]UniqueIndex, error) {
	rows, err := db.Query(`
		SELECT INDEX_NAME, COLUMN_NAME
		FROM INFORMATION_SCHEMA.STATISTICS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		  AND NON_UNIQUE = 0 AND INDEX_NAME != 'PRIMARY'
		ORDER BY INDEX_NAME, SEQ_IN_INDEX`, schema, tableName)
	if err != nil {
		return nil, fmt.Errorf("introspecting unique indexes for %s: %w", tableName, err)
	}
	defer rows.Close()

	indexMap := make(map[string][]string) // index name -> ordered columns
	var indexOrder []string               // preserve discovery order
	for rows.Next() {
		var idxName, colName string
		if err := rows.Scan(&idxName, &colName); err != nil {
			return nil, fmt.Errorf("scanning unique index for %s: %w", tableName, err)
		}
		if _, seen := indexMap[idxName]; !seen {
			indexOrder = append(indexOrder, idxName)
		}
		indexMap[idxName] = append(indexMap[idxName], colName)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	indexes := make([]UniqueIndex, 0, len(indexOrder))
	for _, name := range indexOrder {
		indexes = append(indexes, UniqueIndex{Name: name, Columns: indexMap[name]})
	}
	return indexes, nil
}

// ApplyReferences merges config-declared references into column metadata.
// This allows columns without actual FK constraints to be treated as FK columns
// for dependency ordering, value generation, and row count scaling.
// refs maps "tableName" -> "columnName" -> "RefTable.RefColumn".
func ApplyReferences(tables map[string]*Table, refs map[string]map[string]string) {
	for tableName, colRefs := range refs {
		t, ok := tables[tableName]
		if !ok {
			continue
		}
		for colName, ref := range colRefs {
			parts := strings.SplitN(ref, ".", 2)
			if len(parts) != 2 {
				continue
			}
			refTable, refCol := parts[0], parts[1]
			for i := range t.Columns {
				if t.Columns[i].Name == colName {
					t.Columns[i].FK = &ForeignKey{
						ReferencedTable:  refTable,
						ReferencedColumn: refCol,
					}
					break
				}
			}
		}
	}
}

