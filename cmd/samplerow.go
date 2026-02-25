package cmd

import (
	"database/sql"
	"fmt"
	"math/rand/v2"
	"regexp"
	"strings"
)

var identRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

// makeSampleRowFunc creates a template function that picks a random row from the
// database. It lazily fetches and caches up to 1000 rows per table+columns
// combination, returning a map[string]any keyed by column name.
//
// Usage in query templates:
//
//	{{with SampleRow "tableName" "col1" "col2"}}
//	SELECT * FROM tableName WHERE col1 = {{.col1}} AND col2 = {{.col2}}
//	{{end}}
func makeSampleRowFunc(db *sql.DB) func(args ...string) (map[string]any, error) {
	cache := make(map[string][]map[string]any)

	return func(args ...string) (map[string]any, error) {
		if len(args) < 2 {
			return nil, fmt.Errorf("SampleRow requires a table name and at least one column")
		}
		table := args[0]
		cols := args[1:]

		// Validate identifiers to prevent SQL injection.
		if !identRe.MatchString(table) {
			return nil, fmt.Errorf("SampleRow: invalid table name %q", table)
		}
		for _, c := range cols {
			if !identRe.MatchString(c) {
				return nil, fmt.Errorf("SampleRow: invalid column name %q", c)
			}
		}

		key := table + ":" + strings.Join(cols, ",")

		if rows, ok := cache[key]; ok && len(rows) > 0 {
			return rows[rand.IntN(len(rows))], nil
		}

		// Fetch sample rows. No ORDER BY RAND() needed — seeded data is
		// already random, and LIMIT 1000 gives sufficient diversity.
		quotedCols := make([]string, len(cols))
		for i, c := range cols {
			quotedCols[i] = "`" + c + "`"
		}
		query := fmt.Sprintf("SELECT %s FROM `%s` LIMIT 1000",
			strings.Join(quotedCols, ", "), table)

		dbRows, err := db.Query(query)
		if err != nil {
			return nil, fmt.Errorf("SampleRow: query failed: %w", err)
		}
		defer dbRows.Close()

		var rows []map[string]any
		for dbRows.Next() {
			values := make([]any, len(cols))
			ptrs := make([]any, len(cols))
			for i := range values {
				ptrs[i] = &values[i]
			}
			if err := dbRows.Scan(ptrs...); err != nil {
				return nil, fmt.Errorf("SampleRow: scan failed: %w", err)
			}
			row := make(map[string]any, len(cols))
			for i, c := range cols {
				// MySQL driver returns strings as []byte; convert for
				// clean template rendering.
				if b, ok := values[i].([]byte); ok {
					row[c] = string(b)
				} else {
					row[c] = values[i]
				}
			}
			rows = append(rows, row)
		}

		if len(rows) == 0 {
			return nil, fmt.Errorf("SampleRow: no rows found in %s", table)
		}

		cache[key] = rows
		return rows[rand.IntN(len(rows))], nil
	}
}
