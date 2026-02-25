package seeder

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/tomfevang/go-seed-my-db/internal/generator"
	"github.com/tomfevang/go-seed-my-db/internal/introspect"
)

var handlerCounter atomic.Int64

// csvWriter writes rows as MySQL LOAD DATA TSV format to a writer.
type csvWriter struct {
	w   io.Writer
	buf []byte // reusable buffer for formatting a single row
}

func newCSVWriter(w io.Writer) *csvWriter {
	return &csvWriter{w: w, buf: make([]byte, 0, 4096)}
}

// WriteRow formats a row as tab-separated values terminated by newline.
func (cw *csvWriter) WriteRow(row []any) error {
	cw.buf = cw.buf[:0]
	for i, val := range row {
		if i > 0 {
			cw.buf = append(cw.buf, '\t')
		}
		cw.buf = appendMySQLValue(cw.buf, val)
	}
	cw.buf = append(cw.buf, '\n')
	_, err := cw.w.Write(cw.buf)
	return err
}

// appendMySQLValue appends the LOAD DATA representation of a value to buf.
// NULL → \N, strings/[]byte → escaped, numbers → decimal text, bools → 0/1.
func appendMySQLValue(buf []byte, val any) []byte {
	if val == nil {
		return append(buf, '\\', 'N')
	}
	switch v := val.(type) {
	case string:
		return appendEscapedString(buf, v)
	case []byte:
		return appendEscapedBytes(buf, v)
	case int:
		return appendInt(buf, int64(v))
	case int8:
		return appendInt(buf, int64(v))
	case int16:
		return appendInt(buf, int64(v))
	case int32:
		return appendInt(buf, int64(v))
	case int64:
		return appendInt(buf, v)
	case uint:
		return appendUint(buf, uint64(v))
	case uint8:
		return appendUint(buf, uint64(v))
	case uint16:
		return appendUint(buf, uint64(v))
	case uint32:
		return appendUint(buf, uint64(v))
	case uint64:
		return appendUint(buf, v)
	case float32:
		return appendFloat(buf, float64(v))
	case float64:
		return appendFloat(buf, v)
	case bool:
		if v {
			return append(buf, '1')
		}
		return append(buf, '0')
	case time.Time:
		return appendEscapedString(buf, v.Format("2006-01-02 15:04:05"))
	default:
		return appendEscapedString(buf, fmt.Sprint(v))
	}
}

// appendEscapedString escapes special characters for LOAD DATA and appends to buf.
// Special chars: \t, \n, \r, \\, \0.
func appendEscapedString(buf []byte, s string) []byte {
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\t':
			buf = append(buf, '\\', 't')
		case '\n':
			buf = append(buf, '\\', 'n')
		case '\r':
			buf = append(buf, '\\', 'r')
		case '\\':
			buf = append(buf, '\\', '\\')
		case 0:
			buf = append(buf, '\\', '0')
		default:
			buf = append(buf, s[i])
		}
	}
	return buf
}

// appendEscapedBytes escapes special characters in a byte slice for LOAD DATA.
func appendEscapedBytes(buf []byte, b []byte) []byte {
	for _, c := range b {
		switch c {
		case '\t':
			buf = append(buf, '\\', 't')
		case '\n':
			buf = append(buf, '\\', 'n')
		case '\r':
			buf = append(buf, '\\', 'r')
		case '\\':
			buf = append(buf, '\\', '\\')
		case 0:
			buf = append(buf, '\\', '0')
		default:
			buf = append(buf, c)
		}
	}
	return buf
}

func appendInt(buf []byte, v int64) []byte {
	return fmt.Appendf(buf, "%d", v)
}

func appendUint(buf []byte, v uint64) []byte {
	return fmt.Appendf(buf, "%d", v)
}

func appendFloat(buf []byte, v float64) []byte {
	if v == math.Trunc(v) && !math.IsInf(v, 0) {
		return fmt.Appendf(buf, "%.1f", v)
	}
	return fmt.Appendf(buf, "%g", v)
}

func seedTableLoadData(cfg Config, table *introspect.Table, fkValues map[string][]any, fkLookups []generator.FKLookup, existingUniques map[string][]any, existingComposites []generator.ExistingCompositeTuple) error {
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

	// Build the LOAD DATA statement.
	quotedCols := make([]string, len(columns))
	for i, c := range columns {
		quotedCols[i] = "`" + c + "`"
	}

	var inserted atomic.Int64

	type batch struct {
		rows [][]any
	}
	batches := make(chan batch, cfg.Workers*2)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	var errOnce sync.Once

	for w := 0; w < cfg.Workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for b := range batches {
				if err := loadBatch(cfg.DB, table.Name, quotedCols, b.rows); err != nil {
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

// loadBatch streams rows via io.Pipe to MySQL's LOAD DATA LOCAL INFILE.
func loadBatch(db *sql.DB, tableName string, quotedCols []string, rows [][]any) error {
	pr, pw := io.Pipe()

	name := fmt.Sprintf("batch_%d", handlerCounter.Add(1))
	mysql.RegisterReaderHandler(name, func() io.Reader { return pr })
	defer mysql.DeregisterReaderHandler(name)

	// Write rows in a goroutine — the LOAD DATA statement reads from the pipe.
	go func() {
		cw := newCSVWriter(pw)
		for _, row := range rows {
			if err := cw.WriteRow(row); err != nil {
				pw.CloseWithError(err)
				return
			}
		}
		pw.Close()
	}()

	colList := ""
	for i, c := range quotedCols {
		if i > 0 {
			colList += ", "
		}
		colList += c
	}

	query := fmt.Sprintf(
		"LOAD DATA LOCAL INFILE 'Reader::%s' INTO TABLE `%s` FIELDS TERMINATED BY '\\t' LINES TERMINATED BY '\\n' (%s)",
		name, tableName, colList,
	)

	_, err := db.Exec(query)
	return err
}
