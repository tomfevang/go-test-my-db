package generator

import (
	"bytes"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"strings"
	"text/template"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/tomfevang/go-seed-my-db/internal/config"
	"github.com/tomfevang/go-seed-my-db/internal/introspect"
)

// RowGenerator produces rows of fake data for a given table.
type RowGenerator struct {
	table      *introspect.Table
	columns    []introspect.Column // columns we actually generate (excludes auto-inc)
	generators []func() any
	fkValues   map[string][]any // column name -> slice of valid FK values
	uniqueSets map[int]map[any]bool
	faker      *gofakeit.Faker
	config     *config.Config
}

// NewRowGenerator creates a generator for the given table.
// fkValues maps column name -> available parent IDs for FK columns.
func NewRowGenerator(table *introspect.Table, fkValues map[string][]any, cfg *config.Config) *RowGenerator {
	rg := &RowGenerator{
		table:      table,
		fkValues:   fkValues,
		uniqueSets: make(map[int]map[any]bool),
		faker:      gofakeit.New(0),
		config:     cfg,
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
func (rg *RowGenerator) GenerateRow() []any {
	row := make([]any, len(rg.generators))
	for i, gen := range rg.generators {
		row[i] = gen()
	}
	return row
}

// funcMap builds a template.FuncMap from the Faker instance, mirroring
// gofakeit's internal templateFuncMap so we can pre-parse templates.
func (rg *RowGenerator) funcMap() template.FuncMap {
	fm := template.FuncMap{}

	// Add all public Faker methods via reflection (same as gofakeit internals).
	excluded := map[string]bool{"RandomMapKey": true, "SQL": true, "Template": true}
	v := reflect.ValueOf(rg.faker)
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

func (rg *RowGenerator) buildGenerator(col introspect.Column) func() any {
	// FK columns: pick a random value from the parent table.
	if col.FK != nil {
		if vals, ok := rg.fkValues[col.Name]; ok && len(vals) > 0 {
			return func() any {
				return vals[rand.IntN(len(vals))]
			}
		}
	}

	// Enum/Set columns: random pick from parsed values.
	if len(col.EnumValues) > 0 {
		return rg.wrapNullable(col, func() any {
			return col.EnumValues[rand.IntN(len(col.EnumValues))]
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
