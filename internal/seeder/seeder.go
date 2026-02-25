package seeder

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand/v2"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"golang.org/x/term"

	"github.com/tomfevang/go-seed-my-db/internal/config"
	"github.com/tomfevang/go-seed-my-db/internal/generator"
	"github.com/tomfevang/go-seed-my-db/internal/introspect"
)

const barWidth = 30

var isTTY = sync.OnceValue(func() bool {
	return term.IsTerminal(int(os.Stdout.Fd()))
})

// printProgress renders an inline progress bar on TTY, no-op otherwise.
func printProgress(name string, current, total int64) {
	if !isTTY() {
		return
	}
	pct := float64(current) / float64(total)
	filled := int(pct * barWidth)
	if filled > barWidth {
		filled = barWidth
	}
	bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)
	fmt.Printf("\r[%s] %s %d/%d (%.0f%%)", name, bar, current, total, pct*100)
}

// printProgressDone prints the final progress state. On TTY it shows a full
// bar; on non-TTY it prints a single summary line.
func printProgressDone(name string, total int) {
	if isTTY() {
		bar := strings.Repeat("█", barWidth)
		fmt.Printf("\r[%s] %s %d/%d (100%%)\n", name, bar, total, total)
	} else {
		fmt.Printf("[%s] %d rows inserted\n", name, total)
	}
}

type Config struct {
	DB           *sql.DB
	Schema       string
	Tables       []*introspect.Table // in topological order
	RowsPerTable map[string]int      // pre-computed row count per table
	BatchSize    int
	Workers      int
	Clear        bool
	LoadData     bool
	DeferIndexes bool
	GenConfig    *config.Config
	FKSampleSize int // max FK values to cache per column; 0 = unlimited
}

// SeedAll seeds all tables in the configured order.
func SeedAll(cfg Config) error {
	if cfg.LoadData {
		var localInfile int
		if err := cfg.DB.QueryRow("SELECT @@local_infile").Scan(&localInfile); err != nil {
			return fmt.Errorf("checking local_infile: %w", err)
		}
		if localInfile != 1 {
			return fmt.Errorf("LOAD DATA LOCAL INFILE requires the server to have local_infile=ON.\n" +
				"Run: SET GLOBAL local_infile=1; (or add local-infile=1 to my.cnf)")
		}
	}

	// Disable FK and unique checks for bulk insert performance.
	if _, err := cfg.DB.Exec("SET FOREIGN_KEY_CHECKS=0"); err != nil {
		return fmt.Errorf("disabling FK checks: %w", err)
	}
	if _, err := cfg.DB.Exec("SET UNIQUE_CHECKS=0"); err != nil {
		return fmt.Errorf("disabling unique checks: %w", err)
	}

	fkCache := make(map[string][]any) // "table.column" -> values
	lastConsumer := computeLastConsumers(cfg.Tables)

	// Build table map for FK correlation detection.
	tableMap := make(map[string]*introspect.Table, len(cfg.Tables))
	for _, t := range cfg.Tables {
		tableMap[t.Name] = t
	}

	for i, table := range cfg.Tables {
		targetRows := cfg.RowsPerTable[table.Name]
		if targetRows <= 0 {
			targetRows = 1000
		}

		if cfg.Clear {
			fmt.Printf("[%s] truncating table...\n", table.Name)
			if _, err := cfg.DB.Exec(fmt.Sprintf("TRUNCATE TABLE `%s`", table.Name)); err != nil {
				return fmt.Errorf("truncating %s: %w", table.Name, err)
			}
		} else {
			// Incremental: check current row count.
			currentCount, err := countRows(cfg.DB, table.Name)
			if err != nil {
				return fmt.Errorf("counting rows in %s: %w", table.Name, err)
			}
			if currentCount >= targetRows {
				fmt.Printf("[%s] already has %d rows (target %d), skipping\n", table.Name, currentCount, targetRows)
				// Still cache PKs for downstream FK references.
				for _, col := range table.Columns {
					if col.IsPrimaryKey {
						vals, err := fetchColumnValues(cfg.DB, table.Name, col.Name, cfg.FKSampleSize)
						if err != nil {
							return fmt.Errorf("caching PK values for %s.%s: %w", table.Name, col.Name, err)
						}
						fkCache[table.Name+"."+col.Name] = vals
					}
				}
				continue
			}
			cfg.RowsPerTable[table.Name] = targetRows - currentCount
		}

		// Optionally drop secondary indexes before bulk insert.
		var droppedIndexes []SecondaryIndex
		if cfg.DeferIndexes {
			idxs, err := fetchSecondaryIndexes(cfg.DB, cfg.Schema, table.Name)
			if err != nil {
				return fmt.Errorf("fetching indexes for %s: %w", table.Name, err)
			}
			if len(idxs) > 0 {
				// Exclude indexes that are the sole backing index for a FK constraint.
				fkColSets, err := fetchFKColumnSets(cfg.DB, cfg.Schema, table.Name)
				if err != nil {
					return fmt.Errorf("fetching FK constraints for %s: %w", table.Name, err)
				}
				droppable, kept := filterFKBackingIndexes(idxs, fkColSets)
				if len(kept) > 0 {
					fmt.Printf("[%s] keeping %d FK-backing indexes\n", table.Name, len(kept))
				}
				if len(droppable) > 0 {
					fmt.Printf("[%s] dropping %d secondary indexes...\n", table.Name, len(droppable))
					if err := dropSecondaryIndexes(cfg.DB, table.Name, droppable); err != nil {
						return err
					}
					droppedIndexes = droppable
				}
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
				vals, err := fetchColumnValues(cfg.DB, col.FK.ReferencedTable, col.FK.ReferencedColumn, cfg.FKSampleSize)
				if err != nil {
					return fmt.Errorf("fetching FK values for %s.%s: %w", table.Name, col.Name, err)
				}
				fkCache[cacheKey] = vals
				tableFKValues[col.Name] = vals
			}
		}

		// Detect correlated FK columns and build lookup maps.
		correlations := detectFKCorrelations(table, tableMap)
		var fkLookups []generator.FKLookup
		for _, corr := range correlations {
			mapping, err := fetchFKLookup(cfg.DB, corr.parentTable, corr.parentPKCol, corr.parentFKCol)
			if err != nil {
				return fmt.Errorf("fetching FK lookup for %s.%s via %s: %w",
					table.Name, corr.derivedCol, corr.parentTable, err)
			}
			fkLookups = append(fkLookups, generator.FKLookup{
				DerivedColumn: corr.derivedCol,
				DriverColumn:  corr.driverCol,
				Mapping:       mapping,
			})
			fmt.Printf("[%s] correlating %s with %s (via %s.%s)\n",
				table.Name, corr.derivedCol, corr.driverCol, corr.parentTable, corr.parentFKCol)
		}

		// Pre-load existing unique values for incremental seeding.
		var existingUniques map[string][]any
		var existingComposites []generator.ExistingCompositeTuple
		if !cfg.Clear {
			existingUniques, existingComposites, _ = fetchExistingUniques(cfg.DB, table)
		}

		if cfg.LoadData {
			if err := seedTableLoadData(cfg, table, tableFKValues, fkLookups, existingUniques, existingComposites); err != nil {
				// Restore indexes even on seed failure.
				if len(droppedIndexes) > 0 {
					fmt.Printf("[%s] restoring %d secondary indexes after error...\n", table.Name, len(droppedIndexes))
					_ = restoreSecondaryIndexes(cfg.DB, table.Name, droppedIndexes)
				}
				return fmt.Errorf("seeding %s: %w", table.Name, err)
			}
		} else {
			if err := seedTable(cfg, table, tableFKValues, fkLookups, existingUniques, existingComposites); err != nil {
				// Restore indexes even on seed failure.
				if len(droppedIndexes) > 0 {
					fmt.Printf("[%s] restoring %d secondary indexes after error...\n", table.Name, len(droppedIndexes))
					_ = restoreSecondaryIndexes(cfg.DB, table.Name, droppedIndexes)
				}
				return fmt.Errorf("seeding %s: %w", table.Name, err)
			}
		}

		// Restore secondary indexes after bulk insert.
		if len(droppedIndexes) > 0 {
			fmt.Printf("[%s] restoring %d secondary indexes...\n", table.Name, len(droppedIndexes))
			if err := restoreSecondaryIndexes(cfg.DB, table.Name, droppedIndexes); err != nil {
				return err
			}
		}

		// Cache this table's primary key values for downstream FK references.
		for _, col := range table.Columns {
			if col.IsPrimaryKey {
				vals, err := fetchColumnValues(cfg.DB, table.Name, col.Name, cfg.FKSampleSize)
				if err != nil {
					return fmt.Errorf("caching PK values for %s.%s: %w", table.Name, col.Name, err)
				}
				fkCache[table.Name+"."+col.Name] = vals
			}
		}

		// Evict FK cache entries whose last consumer is the current table.
		for key, lastIdx := range lastConsumer {
			if lastIdx == i {
				delete(fkCache, key)
			}
		}
	}

	// Re-enable checks.
	cfg.DB.Exec("SET FOREIGN_KEY_CHECKS=1")
	cfg.DB.Exec("SET UNIQUE_CHECKS=1")

	return nil
}

func seedTable(cfg Config, table *introspect.Table, fkValues map[string][]any, fkLookups []generator.FKLookup, existingUniques map[string][]any, existingComposites []generator.ExistingCompositeTuple) error {
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

	gen, err := generator.NewRowGenerator(table, fkValues, fkLookups, cfg.GenConfig, pkStartValues, existingUniques, existingComposites)
	if err != nil {
		return err
	}
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
				printProgress(table.Name, count, int64(totalRows))
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

	printProgressDone(table.Name, totalRows)
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

func countRows(db *sql.DB, table string) (int, error) {
	var count int
	err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM `%s`", table)).Scan(&count)
	return count, err
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

func fetchColumnValues(db *sql.DB, table, column string, maxSample int) ([]any, error) {
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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return reservoirSample(values, maxSample), nil
}

// reservoirSample returns a random subset of at most maxSample items using
// Algorithm R. When maxSample <= 0 or len(values) <= maxSample, all values
// are returned unchanged.
func reservoirSample(values []any, maxSample int) []any {
	if maxSample <= 0 || len(values) <= maxSample {
		return values
	}
	reservoir := make([]any, maxSample)
	copy(reservoir, values[:maxSample])
	for i := maxSample; i < len(values); i++ {
		j := rand.IntN(i + 1)
		if j < maxSample {
			reservoir[j] = values[i]
		}
	}
	return reservoir
}

// fkCorrelationInfo describes a transitive FK relationship detected during seeding.
// For example, if child has companyId → company.id and voucherId → voucher.id,
// and voucher also has companyId → company.id, then companyId should be derived
// from the voucherId's parent row to ensure realistic data.
type fkCorrelationInfo struct {
	derivedCol  string // column in child table to derive (e.g., companyId)
	driverCol   string // column in child table that drives (e.g., voucherId)
	parentTable string // driver's parent table (e.g., voucher)
	parentPKCol string // PK column in parent (e.g., id)
	parentFKCol string // FK column in parent that maps to derived target (e.g., companyId)
}

// detectFKCorrelations finds FK columns whose values should be derived from
// another FK column's parent row. This ensures that when a table has multiple
// FK columns to related entities, the generated data is consistent.
func detectFKCorrelations(table *introspect.Table, allTables map[string]*introspect.Table) []fkCorrelationInfo {
	// Collect FK columns.
	var fkCols []introspect.Column
	for _, col := range table.Columns {
		if col.FK != nil {
			fkCols = append(fkCols, col)
		}
	}
	if len(fkCols) < 2 {
		return nil
	}

	var result []fkCorrelationInfo
	claimed := make(map[string]bool) // derived columns already assigned

	for _, colA := range fkCols {
		if claimed[colA.Name] {
			continue
		}
		for _, colB := range fkCols {
			if colA.Name == colB.Name || claimed[colA.Name] {
				continue
			}
			// Skip if both reference the exact same table and column.
			if colA.FK.ReferencedTable == colB.FK.ReferencedTable &&
				colA.FK.ReferencedColumn == colB.FK.ReferencedColumn {
				continue
			}

			parentB := allTables[colB.FK.ReferencedTable]
			if parentB == nil {
				continue
			}

			// Does parentB have a FK to colA's referenced table?
			for _, pCol := range parentB.Columns {
				if pCol.FK != nil &&
					pCol.FK.ReferencedTable == colA.FK.ReferencedTable &&
					pCol.FK.ReferencedColumn == colA.FK.ReferencedColumn {
					result = append(result, fkCorrelationInfo{
						derivedCol:  colA.Name,
						driverCol:   colB.Name,
						parentTable: colB.FK.ReferencedTable,
						parentPKCol: colB.FK.ReferencedColumn,
						parentFKCol: pCol.Name,
					})
					claimed[colA.Name] = true
					break
				}
			}
			if claimed[colA.Name] {
				break
			}
		}
	}

	return result
}

// fetchFKLookup queries the parent table to build a mapping from parent PK
// values to the corresponding FK column values.
func fetchFKLookup(db *sql.DB, table, pkCol, fkCol string) (map[any]any, error) {
	rows, err := db.Query(fmt.Sprintf("SELECT `%s`, `%s` FROM `%s`", pkCol, fkCol, table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	lookup := make(map[any]any)
	for rows.Next() {
		var pk, fk any
		if err := rows.Scan(&pk, &fk); err != nil {
			return nil, err
		}
		lookup[pk] = fk
	}
	return lookup, rows.Err()
}

// computeLastConsumers maps each FK cache key ("table.column") to the index
// of the last table in the seeding order that references it. This lets SeedAll
// evict cache entries as soon as they are no longer needed.
func computeLastConsumers(tables []*introspect.Table) map[string]int {
	last := make(map[string]int)
	for i, table := range tables {
		for _, col := range table.Columns {
			if col.FK == nil {
				continue
			}
			key := col.FK.ReferencedTable + "." + col.FK.ReferencedColumn
			last[key] = i
		}
	}
	return last
}

// fetchExistingUniques loads existing values for single-column unique indexes and
// composite unique indexes from the database, for pre-populating unique trackers.
func fetchExistingUniques(db *sql.DB, table *introspect.Table) (map[string][]any, []generator.ExistingCompositeTuple, error) {
	uniques := make(map[string][]any)

	// Single-column unique constraints.
	for _, col := range table.Columns {
		if !col.IsUnique || col.IsPrimaryKey || col.IsAutoInc {
			continue
		}
		vals, err := fetchColumnValues(db, table.Name, col.Name, 0)
		if err != nil {
			return nil, nil, err
		}
		if len(vals) > 0 {
			uniques[col.Name] = vals
		}
	}

	// Composite unique indexes.
	var composites []generator.ExistingCompositeTuple
	for _, idx := range table.UniqueIndexes {
		if len(idx.Columns) < 2 {
			continue
		}
		tuples, err := fetchCompositeTuples(db, table.Name, idx.Columns)
		if err != nil {
			return nil, nil, err
		}
		if len(tuples) > 0 {
			composites = append(composites, generator.ExistingCompositeTuple{
				Columns: idx.Columns,
				Tuples:  tuples,
			})
		}
	}

	return uniques, composites, nil
}

// fetchCompositeTuples queries SELECT DISTINCT col1, col2, ... FROM table.
func fetchCompositeTuples(db *sql.DB, table string, columns []string) ([][]any, error) {
	quoted := make([]string, len(columns))
	for i, c := range columns {
		quoted[i] = "`" + c + "`"
	}
	query := fmt.Sprintf("SELECT DISTINCT %s FROM `%s`", strings.Join(quoted, ", "), table)
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tuples [][]any
	for rows.Next() {
		tuple := make([]any, len(columns))
		ptrs := make([]any, len(columns))
		for i := range tuple {
			ptrs[i] = &tuple[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		tuples = append(tuples, tuple)
	}
	return tuples, rows.Err()
}
