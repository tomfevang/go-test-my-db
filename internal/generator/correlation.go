package generator

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"strings"
	"text/template"

	"github.com/tomfevang/go-seed-my-db/internal/config"
)

// correlationState holds shared values for a correlated column group.
// The first column's generator populates all values; subsequent columns read from it.
type correlationState struct {
	values map[string]any
	ready  bool
}

// buildCorrelationGenerators overlays correlated generators onto rg.generators.
// Must be called after individual generators are built.
func (rg *RowGenerator) buildCorrelationGenerators() {
	groups := rg.config.GetCorrelations(rg.table.Name)
	if len(groups) == 0 {
		return
	}

	// Build column name -> index in rg.columns
	colIndex := make(map[string]int, len(rg.columns))
	for i, col := range rg.columns {
		colIndex[col.Name] = i
	}

	for _, group := range groups {
		rg.applyCorrelationGroup(group, colIndex)
	}
}

func (rg *RowGenerator) applyCorrelationGroup(group config.CorrelationGroup, colIndex map[string]int) {
	// Resolve column indices
	indices := make([]int, 0, len(group.Columns))
	for _, name := range group.Columns {
		idx, ok := colIndex[name]
		if !ok {
			panic(fmt.Sprintf("correlation: column %q not found in table %s (or is auto-inc/generated)", name, rg.table.Name))
		}
		indices = append(indices, idx)
	}

	state := &correlationState{values: make(map[string]any)}

	// Determine if any column in the group is nullable for all-or-nothing null
	anyNullable := false
	for _, idx := range indices {
		if rg.columns[idx].IsNullable && !rg.columns[idx].IsPrimaryKey {
			anyNullable = true
			break
		}
	}

	// Build the group generator function based on source type
	var generateGroup func()
	switch {
	case group.Source == "address":
		generateGroup = rg.buildAddressGroup(state, group.Columns)
	case group.Source == "person":
		generateGroup = rg.buildPersonGroup(state, group.Columns)
	case group.Source == "latlong":
		generateGroup = rg.buildLatLongGroup(state, group.Columns)
	case group.Source == "template":
		generateGroup = rg.buildTemplateGroup(state, group)
	default:
		panic(fmt.Sprintf("correlation: unknown source %q", group.Source))
	}

	// First column triggers generation; all columns read from state
	for i, idx := range indices {
		colName := group.Columns[i]
		colIdx := idx

		if i == 0 {
			// First column: generate all values, then return own value
			rg.generators[colIdx] = func() any {
				state.ready = false
				if anyNullable && rand.Float64() < 0.1 {
					// All-or-nothing null for the group
					for _, name := range group.Columns {
						state.values[name] = nil
					}
					state.ready = true
					return nil
				}
				generateGroup()
				state.ready = true
				return state.values[colName]
			}
		} else {
			// Subsequent columns: read from state
			rg.generators[colIdx] = func() any {
				if !state.ready {
					panic(fmt.Sprintf("correlation: column %q read before group generated (check column order)", colName))
				}
				return state.values[colName]
			}
		}
	}
}

// buildAddressGroup generates coherent address components from gofakeit.Address().
func (rg *RowGenerator) buildAddressGroup(state *correlationState, columns []string) func() {
	return func() {
		addr := rg.faker.Address()
		for _, col := range columns {
			switch strings.ToLower(col) {
			case "country":
				state.values[col] = addr.Country
			case "state", "province":
				state.values[col] = addr.State
			case "city":
				state.values[col] = addr.City
			case "street", "address", "address_line", "street_address":
				state.values[col] = addr.Street
			case "zip", "zipcode", "zip_code", "postal", "postal_code":
				state.values[col] = addr.Zip
			default:
				state.values[col] = addr.City // fallback
			}
		}
	}
}

// buildPersonGroup generates coherent person data from gofakeit.Person().
func (rg *RowGenerator) buildPersonGroup(state *correlationState, columns []string) func() {
	return func() {
		person := rg.faker.Person()
		for _, col := range columns {
			switch strings.ToLower(col) {
			case "first_name", "firstname":
				state.values[col] = person.FirstName
			case "last_name", "lastname":
				state.values[col] = person.LastName
			case "email":
				state.values[col] = person.Contact.Email
			case "phone":
				state.values[col] = person.Contact.Phone
			default:
				state.values[col] = person.FirstName // fallback
			}
		}
	}
}

// buildLatLongGroup generates a coherent latitude/longitude pair.
func (rg *RowGenerator) buildLatLongGroup(state *correlationState, columns []string) func() {
	return func() {
		lat := rg.faker.Latitude()
		lon := rg.faker.Longitude()
		for _, col := range columns {
			switch strings.ToLower(col) {
			case "latitude", "lat":
				state.values[col] = lat
			case "longitude", "lng", "lon":
				state.values[col] = lon
			default:
				state.values[col] = lat // fallback
			}
		}
	}
}

// buildTemplateGroup generates values using user-defined templates.
// Templates are evaluated in column order; each receives previously generated values.
func (rg *RowGenerator) buildTemplateGroup(state *correlationState, group config.CorrelationGroup) func() {
	// Parse all templates at init
	type parsedCol struct {
		name string
		tmpl *template.Template
	}
	parsed := make([]parsedCol, 0, len(group.Columns))

	fm := rg.funcMap()

	for _, col := range group.Columns {
		tmplStr, ok := group.Template[col]
		if !ok {
			panic(fmt.Sprintf("correlation template: no template for column %q", col))
		}
		t, err := template.New(col).Funcs(fm).Parse(tmplStr)
		if err != nil {
			panic(fmt.Sprintf("correlation template: invalid template for %q: %v", col, err))
		}
		parsed = append(parsed, parsedCol{name: col, tmpl: t})
	}

	var buf bytes.Buffer
	return func() {
		// Clear state for this group
		for k := range state.values {
			delete(state.values, k)
		}
		for _, pc := range parsed {
			buf.Reset()
			if err := pc.tmpl.Execute(&buf, state.values); err != nil {
				panic(fmt.Sprintf("correlation template exec for %q: %v", pc.name, err))
			}
			state.values[pc.name] = buf.String()
		}
	}
}
