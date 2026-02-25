package generator

// FKLookup describes how to derive a FK column's value from another FK column
// in the same table. When the driver picks value V, the derived column uses
// Mapping[V] instead of picking independently.
//
// This ensures correlated FKs produce realistic data. For example, if a table
// has companyId and voucherId columns, and the voucher table itself has a
// companyId column, FKLookup ensures the generated companyId matches the
// voucher's actual companyId.
type FKLookup struct {
	DerivedColumn string      // column in this table whose value is derived
	DriverColumn  string      // column in this table that drives the selection
	Mapping       map[any]any // driverValue → derivedValue
}

// applyFKCorrelations wraps generators so that derived FK columns get their
// values from the driver's parent row rather than picking independently.
//
// Column ordering is handled correctly: the first column in table order (for
// each group) triggers generation for the entire group, regardless of whether
// it's the driver or a derived column.
func (rg *RowGenerator) applyFKCorrelations() {
	if len(rg.fkLookups) == 0 {
		return
	}

	// Group lookups by driver column. A single driver can derive multiple columns.
	type corrGroup struct {
		driverCol string
		driverGen func() any             // original generator for the driver column
		lookups   map[string]map[any]any // derivedCol → mapping
		current   map[string]any         // column → value for current row
		ready     bool
	}

	groupsByDriver := make(map[string]*corrGroup)
	derivedToDriver := make(map[string]string) // derivedCol → driverCol

	for _, fl := range rg.fkLookups {
		g, ok := groupsByDriver[fl.DriverColumn]
		if !ok {
			g = &corrGroup{
				driverCol: fl.DriverColumn,
				lookups:   make(map[string]map[any]any),
				current:   make(map[string]any),
			}
			groupsByDriver[fl.DriverColumn] = g
		}
		g.lookups[fl.DerivedColumn] = fl.Mapping
		derivedToDriver[fl.DerivedColumn] = fl.DriverColumn
	}

	// Capture original driver generators.
	for i, col := range rg.columns {
		if g, ok := groupsByDriver[col.Name]; ok {
			g.driverGen = rg.generators[i]
		}
	}

	// Apply generators. The first column in table order (for each group)
	// triggers generation for the whole group, ensuring correct ordering
	// regardless of which column (driver or derived) appears first.
	triggered := make(map[string]bool) // driverCol → has trigger been assigned

	for i, col := range rg.columns {
		colName := col.Name

		// Determine which group this column belongs to.
		var g *corrGroup
		if grp, ok := groupsByDriver[colName]; ok {
			g = grp
		} else if driverName, ok := derivedToDriver[colName]; ok {
			g = groupsByDriver[driverName]
		}
		if g == nil {
			continue
		}

		if !triggered[g.driverCol] {
			// First column in this group — triggers generation for all.
			triggered[g.driverCol] = true
			grp := g // capture for closure
			rg.generators[i] = func() any {
				grp.ready = false

				// Run driver generator to pick the FK value.
				dv := grp.driverGen()
				grp.current[grp.driverCol] = dv

				// Derive all dependent FK values from the lookup.
				for derivedCol, mapping := range grp.lookups {
					if mapped, ok := mapping[dv]; ok {
						grp.current[derivedCol] = mapped
					} else {
						// Driver value not in lookup (shouldn't happen in practice).
						grp.current[derivedCol] = nil
					}
				}

				grp.ready = true
				return grp.current[colName]
			}
		} else {
			// Subsequent column — read from shared state.
			grp := g // capture for closure
			rg.generators[i] = func() any {
				return grp.current[colName]
			}
		}
	}
}
