package generator

import (
	"fmt"
	"strings"

	"github.com/tomfevang/go-seed-my-db/internal/introspect"
)

const maxUniqueRetries = 100

// uniqueTracker tracks seen values for a single-column unique constraint.
type uniqueTracker struct {
	seen map[any]bool
}

func newUniqueTracker() *uniqueTracker {
	return &uniqueTracker{seen: make(map[any]bool)}
}

// tryAdd returns true if the value was not previously seen (and records it).
// NULL values always pass (MySQL allows multiple NULLs in unique indexes).
func (ut *uniqueTracker) tryAdd(v any) bool {
	if v == nil {
		return true
	}
	key := fmt.Sprint(v)
	if ut.seen[key] {
		return false
	}
	ut.seen[key] = true
	return true
}

// compositeUniqueTracker tracks seen value tuples for a multi-column unique index.
type compositeUniqueTracker struct {
	colIndices []int
	seen       map[string]bool
}

func newCompositeUniqueTracker(colIndices []int) *compositeUniqueTracker {
	return &compositeUniqueTracker{
		colIndices: colIndices,
		seen:       make(map[string]bool),
	}
}

// check returns true if the row's values at the tracked columns haven't been seen.
// If any tracked column is NULL, uniqueness is satisfied (MySQL semantics).
func (ct *compositeUniqueTracker) check(row []any) bool {
	parts := make([]string, len(ct.colIndices))
	for i, idx := range ct.colIndices {
		if row[idx] == nil {
			return true // NULL in any column → unique is satisfied
		}
		parts[i] = fmt.Sprint(row[idx])
	}
	key := strings.Join(parts, "\x00")
	return !ct.seen[key]
}

// record adds the row's composite key to the seen set.
func (ct *compositeUniqueTracker) record(row []any) {
	parts := make([]string, len(ct.colIndices))
	for i, idx := range ct.colIndices {
		if row[idx] == nil {
			return // don't track rows with NULLs
		}
		parts[i] = fmt.Sprint(row[idx])
	}
	key := strings.Join(parts, "\x00")
	ct.seen[key] = true
}

// initUniqueTracking sets up single-column and composite unique tracking on the RowGenerator.
func (rg *RowGenerator) initUniqueTracking() {
	colIndex := make(map[string]int, len(rg.columns))
	for i, col := range rg.columns {
		colIndex[col.Name] = i
	}

	// Single-column unique constraints
	for i, col := range rg.columns {
		if !col.IsUnique || col.IsPrimaryKey || col.IsAutoInc {
			continue
		}
		rg.wrapSingleUnique(i, col)
	}

	// Composite unique indexes (multi-column only — single-column handled above)
	for _, idx := range rg.table.UniqueIndexes {
		if len(idx.Columns) < 2 {
			continue
		}
		indices := make([]int, 0, len(idx.Columns))
		allFound := true
		for _, colName := range idx.Columns {
			ci, ok := colIndex[colName]
			if !ok {
				// Column not in our generated set (auto-inc, generated, etc.)
				allFound = false
				break
			}
			indices = append(indices, ci)
		}
		if !allFound {
			continue
		}
		rg.compositeUniques = append(rg.compositeUniques, newCompositeUniqueTracker(indices))
	}
}

// wrapSingleUnique wraps a column's generator with retry-based uniqueness enforcement.
// Special case: unique integer columns without FK use a sequential counter.
func (rg *RowGenerator) wrapSingleUnique(idx int, col introspect.Column) {
	// Special case: unique integer non-FK → sequential counter (guaranteed unique)
	if col.IsIntegerType() && col.FK == nil {
		if seq, ok := rg.sequences[col.Name]; ok {
			rg.generators[idx] = func() any {
				return seq.Add(1) - 1
			}
			return
		}
	}

	tracker := newUniqueTracker()
	orig := rg.generators[idx]
	rg.generators[idx] = func() any {
		for attempt := range maxUniqueRetries {
			v := orig()
			if tracker.tryAdd(v) {
				return v
			}
			_ = attempt
		}
		panic(fmt.Sprintf("unique constraint: exhausted %d retries for column %s.%s",
			maxUniqueRetries, rg.table.Name, col.Name))
	}
}
