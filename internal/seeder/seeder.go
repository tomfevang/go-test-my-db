package seeder

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/tomfevang/go-seed-my-db/internal/config"
	"github.com/tomfevang/go-seed-my-db/internal/generator"
	"github.com/tomfevang/go-seed-my-db/internal/introspect"
)

type Config struct {
	DB           *sql.DB
	Schema       string
	Tables       []*introspect.Table // in topological order
	RowsPerTable map[string]int      // pre-computed row count per table
	BatchSize    int
	Workers      int
	Clear        bool
	GenConfig    *config.Config
}

// SeedAll seeds all tables in the configured order.
func SeedAll(cfg Config) error {
	// Disable FK and unique checks for bulk insert performance.
	if _, err := cfg.DB.Exec("SET FOREIGN_KEY_CHECKS=0"); err != nil {
		return fmt.Errorf("disabling FK checks: %w", err)
	}
	if _, err := cfg.DB.Exec("SET UNIQUE_CHECKS=0"); err != nil {
		return fmt.Errorf("disabling unique checks: %w", err)
	}

	fkCache := make(map[string][]any) // "table.column" -> values

	for _, table := range cfg.Tables {
		if cfg.Clear {
			fmt.Printf("[%s] truncating table...\n", table.Name)
			if _, err := cfg.DB.Exec(fmt.Sprintf("TRUNCATE TABLE `%s`", table.Name)); err != nil {
				return fmt.Errorf("truncating %s: %w", table.Name, err)
			}
		}

		// Build FK value map for this table's columns.
		tableFKValues := make(map[string][]any)
		for _, col := range table.Columns {
			if col.FK == nil {
				continue
			}
			cacheKey := col.FK.ReferencedTable + "." + col.FK.ReferencedColumn
			if vals, ok := fkCache[cacheKey]; ok {
				tableFKValues[col.Name] = vals
			} else {
				vals, err := fetchColumnValues(cfg.DB, col.FK.ReferencedTable, col.FK.ReferencedColumn)
				if err != nil {
					return fmt.Errorf("fetching FK values for %s.%s: %w", table.Name, col.Name, err)
				}
				fkCache[cacheKey] = vals
				tableFKValues[col.Name] = vals
			}
		}

		if err := seedTable(cfg, table, tableFKValues); err != nil {
			return fmt.Errorf("seeding %s: %w", table.Name, err)
		}

		// Cache this table's primary key values for downstream FK references.
		for _, col := range table.Columns {
			if col.IsPrimaryKey {
				vals, err := fetchColumnValues(cfg.DB, table.Name, col.Name)
				if err != nil {
					return fmt.Errorf("caching PK values for %s.%s: %w", table.Name, col.Name, err)
				}
				fkCache[table.Name+"."+col.Name] = vals
			}
		}
	}

	// Re-enable checks.
	cfg.DB.Exec("SET FOREIGN_KEY_CHECKS=1")
	cfg.DB.Exec("SET UNIQUE_CHECKS=1")

	return nil
}

func seedTable(cfg Config, table *introspect.Table, fkValues map[string][]any) error {
	// Compute starting values for non-auto-increment integer PKs.
	pkStartValues := make(map[string]int64)
	for _, col := range table.Columns {
		if col.IsPrimaryKey && !col.IsAutoInc && col.IsIntegerType() {
			maxVal, err := fetchMaxPK(cfg.DB, table.Name, col.Name)
			if err != nil {
				return fmt.Errorf("fetching max PK for %s.%s: %w", table.Name, col.Name, err)
			}
			pkStartValues[col.Name] = maxVal + 1
		}
	}

	gen := generator.NewRowGenerator(table, fkValues, cfg.GenConfig, pkStartValues)
	columns := gen.Columns()

	if len(columns) == 0 {
		fmt.Printf("[%s] skipping (no columns to generate)\n", table.Name)
		return nil
	}

	totalRows := cfg.RowsPerTable[table.Name]
	if totalRows <= 0 {
		totalRows = 1000
	}
	batchSize := cfg.BatchSize
	if batchSize > totalRows {
		batchSize = totalRows
	}

	// Build the INSERT prefix: INSERT INTO `table` (`col1`, `col2`, ...) VALUES
	quotedCols := make([]string, len(columns))
	for i, c := range columns {
		quotedCols[i] = "`" + c + "`"
	}
	insertPrefix := fmt.Sprintf("INSERT INTO `%s` (%s) VALUES ", table.Name, strings.Join(quotedCols, ", "))

	// Single row placeholder: (?, ?, ...)
	singleRow := "(" + strings.Repeat("?, ", len(columns)-1) + "?)"

	var inserted atomic.Int64

	// Batch channel for workers.
	type batch struct {
		rows [][]any
	}
	batches := make(chan batch, cfg.Workers*2)

	// Use a context so workers can signal the producer to stop on error.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Worker pool.
	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	var errOnce sync.Once

	for w := 0; w < cfg.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for b := range batches {
				if err := insertBatch(cfg.DB, insertPrefix, singleRow, len(columns), b.rows); err != nil {
					errOnce.Do(func() {
						errCh <- err
						cancel()
					})
					return
				}
				count := inserted.Add(int64(len(b.rows)))
				fmt.Printf("\r[%s] %d / %d rows (%.0f%%)", table.Name, count, totalRows, float64(count)/float64(totalRows)*100)
			}
		}()
	}

	// Generate batches, stopping early if a worker fails.
	remaining := totalRows
	for remaining > 0 {
		size := batchSize
		if size > remaining {
			size = remaining
		}
		rows := make([][]any, size)
		for i := range rows {
			rows[i] = gen.GenerateRow()
		}
		select {
		case batches <- batch{rows: rows}:
		case <-ctx.Done():
			remaining = 0
			continue
		}
		remaining -= size
	}
	close(batches)

	wg.Wait()

	select {
	case err := <-errCh:
		return err
	default:
	}

	fmt.Printf("\r[%s] %d / %d rows (100%%)\n", table.Name, totalRows, totalRows)
	return nil
}

func insertBatch(db *sql.DB, insertPrefix, singleRow string, numCols int, rows [][]any) error {
	placeholders := make([]string, len(rows))
	for i := range placeholders {
		placeholders[i] = singleRow
	}
	query := insertPrefix + strings.Join(placeholders, ", ")

	args := make([]any, 0, len(rows)*numCols)
	for _, row := range rows {
		args = append(args, row...)
	}

	_, err := db.Exec(query, args...)
	return err
}

func fetchMaxPK(db *sql.DB, table, column string) (int64, error) {
	var maxVal sql.NullInt64
	err := db.QueryRow(fmt.Sprintf("SELECT MAX(`%s`) FROM `%s`", column, table)).Scan(&maxVal)
	if err != nil {
		return 0, err
	}
	if !maxVal.Valid {
		return 0, nil // empty table, start from 1
	}
	return maxVal.Int64, nil
}

func fetchColumnValues(db *sql.DB, table, column string) ([]any, error) {
	rows, err := db.Query(fmt.Sprintf("SELECT `%s` FROM `%s`", column, table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var values []any
	for rows.Next() {
		var v any
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		values = append(values, v)
	}
	return values, rows.Err()
}
