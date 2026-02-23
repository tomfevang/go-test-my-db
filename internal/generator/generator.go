package generator

import (
	"bytes"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"strings"
	"sync/atomic"
	"text/template"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/tomfevang/go-seed-my-db/internal/config"
	"github.com/tomfevang/go-seed-my-db/internal/introspect"
)

// NameBasedLabel returns a human-readable label for the name-based heuristic
// that would be applied to the given column, or "" if no heuristic matches.
func NameBasedLabel(col introspect.Column) string {
	name := strings.ToLower(col.Name)
	str := isStringType(col.DataType)
	num := isNumericType(col.DataType)
	date := isDateType(col.DataType)

	switch {
	case str && name == "uuid":
		return "UUID()"
	case str && (name == "email" || strings.HasSuffix(name, "_email")):
		return "Email()"
	case str && (strings.Contains(name, "first_name") || strings.Contains(name, "firstname")):
		return "FirstName()"
	case str && (strings.Contains(name, "last_name") || strings.Contains(name, "lastname")):
		return "LastName()"
	case str && (name == "name" || strings.HasSuffix(name, "_name") || strings.HasPrefix(name, "name_")):
		return "Name()"
	case str && strings.Contains(name, "phone"):
		return "Phone()"
	case str && (strings.Contains(name, "username") || name == "login"):
		return "Username()"
	case str && strings.Contains(name, "password"):
		return "Password()"
	case str && (name == "address" || name == "street" || strings.Contains(name, "address_line") || strings.Contains(name, "street_address")):
		return "Street()"
	case str && name == "city":
		return "City()"
	case str && (name == "state" || name == "province"):
		return "State()"
	case str && (strings.Contains(name, "zip") || strings.Contains(name, "postal")):
		return "Zip()"
	case str && (name == "country" || name == "country_code"):
		return "Country()"
	case str && (strings.Contains(name, "url") || strings.Contains(name, "website") || strings.Contains(name, "homepage")):
		return "URL()"
	case str && (name == "ip" || strings.Contains(name, "ip_address") || name == "ip_addr"):
		return "IPv4Address()"
	case str && (strings.Contains(name, "company") || name == "organization" || name == "org"):
		return "Company()"
	case str && (name == "title" || name == "job_title"):
		return "JobTitle()"
	case str && (name == "description" || name == "bio" || name == "summary" || name == "about"):
		return "Sentence(10)"
	case date && (strings.Contains(name, "created_at") || strings.Contains(name, "updated_at") || strings.Contains(name, "deleted_at")):
		return "DateRange(2020-2025)"
	case date && (name == "date_of_birth" || name == "dob" || name == "birthday" || name == "birthdate"):
		return "DateRange(1950-2005)"
	case num && (strings.Contains(name, "price") || strings.Contains(name, "amount") || strings.Contains(name, "cost") || name == "total" || name == "subtotal"):
		return "Price(1, 1000)"
	case num && (name == "latitude" || name == "lat"):
		return "Latitude()"
	case num && (name == "longitude" || name == "lng" || name == "lon"):
		return "Longitude()"
	case str && (name == "currency" || name == "currency_code"):
		return "CurrencyShort()"
	case str && (name == "color" || name == "colour"):
		return "Color()"
	case str && (name == "avatar" || name == "image_url" || name == "photo_url"):
		return "URL()"
	}
	return ""
}

// DescribeGenerator returns a human-readable description of the generator
// strategy that would be used for the given column.
func DescribeGenerator(col introspect.Column, tableName string, cfg *config.Config) string {
	if col.IsAutoInc {
		return "auto_increment"
	}
	if col.IsGenerated {
		return "generated (skipped)"
	}
	if col.FK != nil {
		return fmt.Sprintf("fk -> %s.%s", col.FK.ReferencedTable, col.FK.ReferencedColumn)
	}
	if col.IsPrimaryKey && !col.IsAutoInc && col.IsIntegerType() {
		return "sequential"
	}
	if len(col.EnumValues) > 0 {
		vals := col.EnumValues
		if len(vals) > 5 {
			vals = vals[:5]
		}
		return fmt.Sprintf("enum: [%s]", strings.Join(vals, ", "))
	}
	if tmpl := cfg.GetTemplate(tableName, col.Name); tmpl != "" {
		return fmt.Sprintf("template: %s", tmpl)
	}
	if label := NameBasedLabel(col); label != "" {
		return fmt.Sprintf("heuristic: %s", label)
	}
	return fmt.Sprintf("type-based: %s", strings.ToLower(col.DataType))
}

// ExistingCompositeTuple holds pre-loaded composite unique index data for incremental seeding.
type ExistingCompositeTuple struct {
	Columns []string
	Tuples  [][]any
}

// RowGenerator produces rows of fake data for a given table.
type RowGenerator struct {
	table              *introspect.Table
	columns            []introspect.Column // columns we actually generate (excludes auto-inc)
	generators         []func() any
	fkValues           map[string][]any // column name -> slice of valid FK values
	compositeUniques   []*compositeUniqueTracker
	faker              *gofakeit.Faker
	config             *config.Config
	sequences          map[string]*atomic.Int64 // column name -> sequence counter for non-auto-inc PKs
	existingUniques    map[string][]any         // column name -> existing values for unique constraint pre-population
	existingComposites []ExistingCompositeTuple  // composite unique index existing tuples
}

// NewRowGenerator creates a generator for the given table.
// fkValues maps column name -> available parent IDs for FK columns.
// pkStartValues maps column name -> starting value for non-auto-inc integer PKs.
// existingUniques maps column name -> existing values for single-column unique constraints (nil to skip).
// existingComposites provides existing tuples for composite unique indexes (nil to skip).
func NewRowGenerator(table *introspect.Table, fkValues map[string][]any, cfg *config.Config, pkStartValues map[string]int64, existingUniques map[string][]any, existingComposites []ExistingCompositeTuple) *RowGenerator {
	seqs := make(map[string]*atomic.Int64, len(pkStartValues))
	for col, start := range pkStartValues {
		seq := &atomic.Int64{}
		seq.Store(start)
		seqs[col] = seq
	}

	rg := &RowGenerator{
		table:              table,
		fkValues:           fkValues,
		faker:              gofakeit.New(0),
		config:             cfg,
		sequences:          seqs,
		existingUniques:    existingUniques,
		existingComposites: existingComposites,
	}

	for _, col := range table.Columns {
		if col.IsAutoInc || col.IsGenerated {
			continue
		}
		rg.columns = append(rg.columns, col)
	}

	rg.generators = make([]func() any, len(rg.columns))
	for i, col := range rg.columns {
		rg.generators[i] = rg.buildGenerator(col)
	}

	rg.buildCorrelationGenerators()
	rg.initUniqueTracking()

	return rg
}

// Columns returns the column names that this generator produces values for.
func (rg *RowGenerator) Columns() []string {
	names := make([]string, len(rg.columns))
	for i, col := range rg.columns {
		names[i] = col.Name
	}
	return names
}

// GenerateRow produces a single row of fake data.
// If composite unique constraints exist, it retries generation on collisions.
func (rg *RowGenerator) GenerateRow() []any {
	for attempt := range maxUniqueRetries {
		row := make([]any, len(rg.generators))
		for i, gen := range rg.generators {
			row[i] = gen()
		}

		if len(rg.compositeUniques) == 0 {
			return row
		}

		if rg.checkCompositeUniques(row) {
			rg.recordCompositeUniques(row)
			return row
		}
		_ = attempt
	}
	panic(fmt.Sprintf("unique constraint: exhausted %d retries for composite unique in table %s",
		maxUniqueRetries, rg.table.Name))
}

func (rg *RowGenerator) checkCompositeUniques(row []any) bool {
	for _, ct := range rg.compositeUniques {
		if !ct.check(row) {
			return false
		}
	}
	return true
}

func (rg *RowGenerator) recordCompositeUniques(row []any) {
	for _, ct := range rg.compositeUniques {
		ct.record(row)
	}
}

// FuncMap builds a template.FuncMap from a Faker instance, exposing all
// gofakeit methods plus helper functions for use in Go templates.
func FuncMap(f *gofakeit.Faker) template.FuncMap {
	fm := template.FuncMap{}

	// Add all public Faker methods via reflection (same as gofakeit internals).
	excluded := map[string]bool{"RandomMapKey": true, "SQL": true, "Template": true}
	v := reflect.ValueOf(f)
	for i := 0; i < v.NumMethod(); i++ {
		name := v.Type().Method(i).Name
		if excluded[name] || v.Type().Method(i).Type.NumOut() == 0 {
			continue
		}
		fm[name] = v.Method(i).Interface()
	}

	// Add the same helper functions gofakeit registers.
	fm["ToUpper"] = strings.ToUpper
	fm["ToLower"] = strings.ToLower
	fm["IntRange"] = func(start, end int) []int {
		n := make([]int, end-start+1)
		for i := range n {
			n[i] = start + i
		}
		return n
	}
	fm["SliceAny"] = func(args ...any) []any { return args }
	fm["SliceString"] = func(args ...string) []string { return args }
	fm["SliceInt"] = func(args ...int) []int { return args }
	fm["SliceUInt"] = func(args ...uint) []uint { return args }
	fm["SliceF32"] = func(args ...float32) []float32 { return args }

	return fm
}

func (rg *RowGenerator) funcMap() template.FuncMap {
	return FuncMap(rg.faker)
}

func (rg *RowGenerator) buildGenerator(col introspect.Column) func() any {
	// FK columns: pick a value from the parent table using distribution.
	if col.FK != nil {
		if vals, ok := rg.fkValues[col.Name]; ok && len(vals) > 0 {
			dist := rg.config.GetDistribution(rg.table.Name, col.Name)
			picker := NewValuePicker(vals, dist)
			return func() any {
				return picker.Pick()
			}
		}
	}

	// Non-auto-increment integer PKs: sequential values.
	if col.IsPrimaryKey && !col.IsAutoInc && col.IsIntegerType() {
		if seq, ok := rg.sequences[col.Name]; ok {
			return func() any {
				return seq.Add(1) - 1
			}
		}
	}

	// Enum/Set columns: pick using distribution.
	if len(col.EnumValues) > 0 {
		dist := rg.config.GetDistribution(rg.table.Name, col.Name)
		enumVals := make([]any, len(col.EnumValues))
		for i, v := range col.EnumValues {
			enumVals[i] = v
		}
		picker := NewValuePicker(enumVals, dist)
		return rg.wrapNullable(col, func() any {
			return picker.Pick()
		})
	}

	// Config template override.
	if tmpl := rg.config.GetTemplate(rg.table.Name, col.Name); tmpl != "" {
		parsed, err := template.New(col.Name).Funcs(rg.funcMap()).Parse(tmpl)
		if err != nil {
			panic(fmt.Sprintf("invalid template for %s.%s: %v", rg.table.Name, col.Name, err))
		}
		var buf bytes.Buffer
		return rg.wrapNullable(col, func() any {
			buf.Reset()
			if err := parsed.Execute(&buf, nil); err != nil {
				panic(fmt.Sprintf("template exec failed for %s.%s: %v", rg.table.Name, col.Name, err))
			}
			return buf.String()
		})
	}

	// Name-based heuristics.
	if gen := rg.nameBasedGenerator(col); gen != nil {
		return rg.wrapNullable(col, gen)
	}

	// Type-based fallback.
	return rg.wrapNullable(col, rg.typeBasedGenerator(col))
}

func (rg *RowGenerator) wrapNullable(col introspect.Column, gen func() any) func() any {
	if col.IsNullable && !col.IsPrimaryKey {
		return func() any {
			if rand.Float64() < 0.1 {
				return nil
			}
			return gen()
		}
	}
	return gen
}

func isStringType(dataType string) bool {
	switch strings.ToLower(dataType) {
	case "varchar", "char", "text", "tinytext", "mediumtext", "longtext":
		return true
	default:
		return false
	}
}

func isNumericType(dataType string) bool {
	switch strings.ToLower(dataType) {
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint",
		"float", "double", "decimal", "numeric":
		return true
	default:
		return false
	}
}

func isDateType(dataType string) bool {
	switch strings.ToLower(dataType) {
	case "date", "datetime", "timestamp":
		return true
	default:
		return false
	}
}

func (rg *RowGenerator) nameBasedGenerator(col introspect.Column) func() any {
	name := strings.ToLower(col.Name)
	str := isStringType(col.DataType)
	num := isNumericType(col.DataType)
	date := isDateType(col.DataType)

	// Order matters: more specific patterns first.
	switch {
	case str && name == "uuid":
		return func() any { return rg.faker.UUID() }
	case str && (name == "email" || strings.HasSuffix(name, "_email")):
		return func() any { return rg.faker.Email() }
	case str && (strings.Contains(name, "first_name") || strings.Contains(name, "firstname")):
		return func() any { return rg.faker.FirstName() }
	case str && (strings.Contains(name, "last_name") || strings.Contains(name, "lastname")):
		return func() any { return rg.faker.LastName() }
	case str && (name == "name" || strings.HasSuffix(name, "_name") || strings.HasPrefix(name, "name_")):
		return func() any { return rg.faker.Name() }
	case str && strings.Contains(name, "phone"):
		return func() any { return rg.faker.Phone() }
	case str && (strings.Contains(name, "username") || name == "login"):
		return func() any { return rg.faker.Username() }
	case str && strings.Contains(name, "password"):
		return func() any { return rg.faker.Password(true, true, true, false, false, 16) }
	case str && (name == "address" || name == "street" || strings.Contains(name, "address_line") || strings.Contains(name, "street_address")):
		return func() any { return rg.faker.Street() }
	case str && name == "city":
		return func() any { return rg.faker.City() }
	case str && (name == "state" || name == "province"):
		return func() any { return rg.faker.State() }
	case str && (strings.Contains(name, "zip") || strings.Contains(name, "postal")):
		return func() any { return rg.faker.Zip() }
	case str && (name == "country" || name == "country_code"):
		return func() any { return rg.faker.Country() }
	case str && (strings.Contains(name, "url") || strings.Contains(name, "website") || strings.Contains(name, "homepage")):
		return func() any { return rg.faker.URL() }
	case str && (name == "ip" || strings.Contains(name, "ip_address") || name == "ip_addr"):
		return func() any { return rg.faker.IPv4Address() }
	case str && (strings.Contains(name, "company") || name == "organization" || name == "org"):
		return func() any { return rg.faker.Company() }
	case str && (name == "title" || name == "job_title"):
		return func() any { return rg.faker.JobTitle() }
	case str && (name == "description" || name == "bio" || name == "summary" || name == "about"):
		return func() any { return rg.faker.Sentence(10) }
	case date && (strings.Contains(name, "created_at") || strings.Contains(name, "updated_at") || strings.Contains(name, "deleted_at")):
		return func() any {
			return rg.faker.DateRange(
				time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC),
			)
		}
	case date && (name == "date_of_birth" || name == "dob" || name == "birthday" || name == "birthdate"):
		return func() any {
			return rg.faker.DateRange(
				time.Date(1950, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2005, 12, 31, 0, 0, 0, 0, time.UTC),
			)
		}
	case num && (strings.Contains(name, "price") || strings.Contains(name, "amount") || strings.Contains(name, "cost") || name == "total" || name == "subtotal"):
		return func() any { return rg.faker.Price(1, 1000) }
	case num && (name == "latitude" || name == "lat"):
		return func() any { return rg.faker.Latitude() }
	case num && (name == "longitude" || name == "lng" || name == "lon"):
		return func() any { return rg.faker.Longitude() }
	case str && (name == "currency" || name == "currency_code"):
		return func() any { return rg.faker.CurrencyShort() }
	case str && (name == "color" || name == "colour"):
		return func() any { return rg.faker.Color() }
	case str && (name == "avatar" || name == "image_url" || name == "photo_url"):
		return func() any { return rg.faker.URL() }
	}

	return nil
}

func (rg *RowGenerator) typeBasedGenerator(col introspect.Column) func() any {
	dt := strings.ToLower(col.DataType)
	ct := strings.ToLower(col.ColumnType)

	switch dt {
	case "tinyint":
		if ct == "tinyint(1)" {
			return func() any { return rg.faker.Bool() }
		}
		if strings.Contains(ct, "unsigned") {
			return func() any { return rand.IntN(256) }
		}
		return func() any { return rand.IntN(256) - 128 }

	case "smallint":
		if strings.Contains(ct, "unsigned") {
			return func() any { return rand.IntN(65536) }
		}
		return func() any { return rand.IntN(65536) - 32768 }

	case "mediumint":
		return func() any { return rand.IntN(16777216) }

	case "int", "integer":
		if strings.Contains(ct, "unsigned") {
			return func() any { return rand.IntN(2147483647) }
		}
		return func() any { return rand.IntN(2147483647) }

	case "bigint":
		return func() any { return rand.Int64N(9223372036854775807) }

	case "float":
		return func() any { return math.Round(rand.Float64()*1000*100) / 100 }

	case "double":
		return func() any { return math.Round(rand.Float64()*10000*100) / 100 }

	case "decimal", "numeric":
		precision := int64(10)
		scale := int64(2)
		if col.Precision != nil {
			precision = *col.Precision
		}
		if col.Scale != nil {
			scale = *col.Scale
		}
		maxVal := math.Pow(10, float64(precision-scale)) - 1
		scaleFactor := math.Pow(10, float64(scale))
		return func() any {
			return math.Round(rand.Float64()*maxVal*scaleFactor) / scaleFactor
		}

	case "varchar", "char":
		length := 20
		if col.MaxLength != nil && *col.MaxLength < int64(length) {
			length = int(*col.MaxLength)
		}
		if length <= 0 {
			length = 1
		}
		return func() any { return rg.faker.LetterN(uint(length)) }

	case "text", "mediumtext", "longtext":
		return func() any { return rg.faker.Paragraph(1, 3, 5, " ") }

	case "tinytext":
		return func() any { return rg.faker.Sentence(5) }

	case "date":
		return func() any {
			return rg.faker.DateRange(
				time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2025, 12, 31, 0, 0, 0, 0, time.UTC),
			).Format("2006-01-02")
		}

	case "datetime", "timestamp":
		return func() any {
			return rg.faker.DateRange(
				time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2025, 12, 31, 23, 59, 59, 0, time.UTC),
			).Format("2006-01-02 15:04:05")
		}

	case "time":
		return func() any {
			return fmt.Sprintf("%02d:%02d:%02d", rand.IntN(24), rand.IntN(60), rand.IntN(60))
		}

	case "year":
		return func() any { return 2000 + rand.IntN(26) }

	case "json":
		return func() any { return "{}" }

	case "blob", "binary", "varbinary", "tinyblob", "mediumblob", "longblob":
		return func() any {
			b := make([]byte, 16)
			for i := range b {
				b[i] = byte(rand.IntN(256))
			}
			return b
		}

	case "bit":
		return func() any { return rand.IntN(2) }

	default:
		// Unknown type: generate a short string.
		return func() any { return rg.faker.LetterN(10) }
	}
}
