package generator

import (
	"testing"

	"github.com/tomfevang/go-seed-my-db/internal/config"
	"github.com/tomfevang/go-seed-my-db/internal/introspect"
)

func TestFKCorrelation_DerivedFromDriver(t *testing.T) {
	// Simulate: child table has companyId → company.id and voucherId → voucher.id
	// voucher table also has companyId → company.id
	// So child.companyId should be derived from the voucher row picked by voucherId.
	table := &introspect.Table{
		Name: "child",
		Columns: []introspect.Column{
			{Name: "companyId", DataType: "int", FK: &introspect.ForeignKey{ReferencedTable: "company", ReferencedColumn: "id"}},
			{Name: "voucherId", DataType: "int", FK: &introspect.ForeignKey{ReferencedTable: "voucher", ReferencedColumn: "id"}},
			{Name: "amount", DataType: "int"},
		},
	}

	// Company IDs: 1, 2, 3
	// Voucher IDs: 10, 20, 30
	// Mapping: voucher 10 → company 1, voucher 20 → company 2, voucher 30 → company 3
	fkValues := map[string][]any{
		"companyId": {int64(1), int64(2), int64(3)},
		"voucherId": {int64(10), int64(20), int64(30)},
	}
	fkLookups := []FKLookup{
		{
			DerivedColumn: "companyId",
			DriverColumn:  "voucherId",
			Mapping: map[any]any{
				int64(10): int64(1),
				int64(20): int64(2),
				int64(30): int64(3),
			},
		},
	}

	cfg := &config.Config{}
	gen, err := NewRowGenerator(table, fkValues, fkLookups, cfg, nil, nil, nil)
	if err != nil {
		t.Fatalf("NewRowGenerator: %v", err)
	}

	// Generate many rows and verify companyId always matches the voucher's company.
	for i := 0; i < 1000; i++ {
		row := gen.GenerateRow()
		companyID := row[0]
		voucherID := row[1]

		expected := fkLookups[0].Mapping[voucherID]
		if companyID != expected {
			t.Fatalf("row %d: companyId=%v but voucherId=%v should map to companyId=%v",
				i, companyID, voucherID, expected)
		}
	}
}

func TestFKCorrelation_DerivedColumnFirst(t *testing.T) {
	// Same as above but the derived column (companyId) comes first in table order.
	// The correlation system must handle this correctly.
	table := &introspect.Table{
		Name: "child",
		Columns: []introspect.Column{
			{Name: "companyId", DataType: "int", FK: &introspect.ForeignKey{ReferencedTable: "company", ReferencedColumn: "id"}},
			{Name: "voucherId", DataType: "int", FK: &introspect.ForeignKey{ReferencedTable: "voucher", ReferencedColumn: "id"}},
		},
	}

	fkValues := map[string][]any{
		"companyId": {int64(1), int64(2)},
		"voucherId": {int64(10), int64(20)},
	}
	fkLookups := []FKLookup{
		{
			DerivedColumn: "companyId",
			DriverColumn:  "voucherId",
			Mapping: map[any]any{
				int64(10): int64(1),
				int64(20): int64(2),
			},
		},
	}

	gen, err := NewRowGenerator(table, fkValues, fkLookups, &config.Config{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("NewRowGenerator: %v", err)
	}

	for i := 0; i < 500; i++ {
		row := gen.GenerateRow()
		companyID := row[0]
		voucherID := row[1]
		expected := fkLookups[0].Mapping[voucherID]
		if companyID != expected {
			t.Fatalf("row %d: companyId=%v but expected %v (voucherId=%v)",
				i, companyID, expected, voucherID)
		}
	}
}

func TestFKCorrelation_DriverColumnFirst(t *testing.T) {
	// Driver column comes first in table order.
	table := &introspect.Table{
		Name: "child",
		Columns: []introspect.Column{
			{Name: "voucherId", DataType: "int", FK: &introspect.ForeignKey{ReferencedTable: "voucher", ReferencedColumn: "id"}},
			{Name: "companyId", DataType: "int", FK: &introspect.ForeignKey{ReferencedTable: "company", ReferencedColumn: "id"}},
		},
	}

	fkValues := map[string][]any{
		"companyId": {int64(1), int64(2)},
		"voucherId": {int64(10), int64(20)},
	}
	fkLookups := []FKLookup{
		{
			DerivedColumn: "companyId",
			DriverColumn:  "voucherId",
			Mapping: map[any]any{
				int64(10): int64(1),
				int64(20): int64(2),
			},
		},
	}

	gen, err := NewRowGenerator(table, fkValues, fkLookups, &config.Config{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("NewRowGenerator: %v", err)
	}

	for i := 0; i < 500; i++ {
		row := gen.GenerateRow()
		voucherID := row[0]
		companyID := row[1]
		expected := fkLookups[0].Mapping[voucherID]
		if companyID != expected {
			t.Fatalf("row %d: companyId=%v but expected %v (voucherId=%v)",
				i, companyID, expected, voucherID)
		}
	}
}

func TestFKCorrelation_MultipleDriverDerived(t *testing.T) {
	// A single driver (voucherId) derives two columns (companyId and employeeId).
	table := &introspect.Table{
		Name: "child",
		Columns: []introspect.Column{
			{Name: "companyId", DataType: "int", FK: &introspect.ForeignKey{ReferencedTable: "company", ReferencedColumn: "id"}},
			{Name: "employeeId", DataType: "int", FK: &introspect.ForeignKey{ReferencedTable: "employee", ReferencedColumn: "id"}},
			{Name: "voucherId", DataType: "int", FK: &introspect.ForeignKey{ReferencedTable: "voucher", ReferencedColumn: "id"}},
		},
	}

	fkValues := map[string][]any{
		"companyId":  {int64(1), int64(2)},
		"employeeId": {int64(100), int64(200)},
		"voucherId":  {int64(10), int64(20)},
	}
	fkLookups := []FKLookup{
		{
			DerivedColumn: "companyId",
			DriverColumn:  "voucherId",
			Mapping: map[any]any{
				int64(10): int64(1),
				int64(20): int64(2),
			},
		},
		{
			DerivedColumn: "employeeId",
			DriverColumn:  "voucherId",
			Mapping: map[any]any{
				int64(10): int64(100),
				int64(20): int64(200),
			},
		},
	}

	gen, err := NewRowGenerator(table, fkValues, fkLookups, &config.Config{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("NewRowGenerator: %v", err)
	}

	for i := 0; i < 500; i++ {
		row := gen.GenerateRow()
		companyID := row[0]
		employeeID := row[1]
		voucherID := row[2]

		expectedCompany := fkLookups[0].Mapping[voucherID]
		expectedEmployee := fkLookups[1].Mapping[voucherID]

		if companyID != expectedCompany {
			t.Fatalf("row %d: companyId=%v but expected %v (voucherId=%v)",
				i, companyID, expectedCompany, voucherID)
		}
		if employeeID != expectedEmployee {
			t.Fatalf("row %d: employeeId=%v but expected %v (voucherId=%v)",
				i, employeeID, expectedEmployee, voucherID)
		}
	}
}

func TestFKCorrelation_NoLookups(t *testing.T) {
	// No FK lookups — should work normally with independent FK picking.
	table := &introspect.Table{
		Name: "child",
		Columns: []introspect.Column{
			{Name: "companyId", DataType: "int", FK: &introspect.ForeignKey{ReferencedTable: "company", ReferencedColumn: "id"}},
			{Name: "amount", DataType: "int"},
		},
	}

	fkValues := map[string][]any{
		"companyId": {int64(1), int64(2), int64(3)},
	}

	gen, err := NewRowGenerator(table, fkValues, nil, &config.Config{}, nil, nil, nil)
	if err != nil {
		t.Fatalf("NewRowGenerator: %v", err)
	}

	for i := 0; i < 100; i++ {
		row := gen.GenerateRow()
		companyID := row[0]
		// Should be one of the valid FK values.
		found := false
		for _, v := range fkValues["companyId"] {
			if companyID == v {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("row %d: companyId=%v not in valid FK values", i, companyID)
		}
	}
}
