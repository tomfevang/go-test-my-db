package seeder

import (
	"testing"

	"github.com/tomfevang/go-seed-my-db/internal/introspect"
)

func TestDetectFKCorrelations_Basic(t *testing.T) {
	// child has companyId → company.id and voucherId → voucher.id
	// voucher has companyId → company.id
	// Expected: companyId derived from voucherId
	company := &introspect.Table{
		Name: "company",
		Columns: []introspect.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true},
		},
	}
	voucher := &introspect.Table{
		Name: "voucher",
		Columns: []introspect.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true},
			{Name: "companyId", DataType: "int", FK: &introspect.ForeignKey{ReferencedTable: "company", ReferencedColumn: "id"}},
		},
	}
	child := &introspect.Table{
		Name: "child",
		Columns: []introspect.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true},
			{Name: "companyId", DataType: "int", FK: &introspect.ForeignKey{ReferencedTable: "company", ReferencedColumn: "id"}},
			{Name: "voucherId", DataType: "int", FK: &introspect.ForeignKey{ReferencedTable: "voucher", ReferencedColumn: "id"}},
		},
	}

	allTables := map[string]*introspect.Table{
		"company": company,
		"voucher": voucher,
		"child":   child,
	}

	correlations := detectFKCorrelations(child, allTables)
	if len(correlations) != 1 {
		t.Fatalf("expected 1 correlation, got %d", len(correlations))
	}

	c := correlations[0]
	if c.derivedCol != "companyId" {
		t.Errorf("derivedCol = %q, want %q", c.derivedCol, "companyId")
	}
	if c.driverCol != "voucherId" {
		t.Errorf("driverCol = %q, want %q", c.driverCol, "voucherId")
	}
	if c.parentTable != "voucher" {
		t.Errorf("parentTable = %q, want %q", c.parentTable, "voucher")
	}
	if c.parentPKCol != "id" {
		t.Errorf("parentPKCol = %q, want %q", c.parentPKCol, "id")
	}
	if c.parentFKCol != "companyId" {
		t.Errorf("parentFKCol = %q, want %q", c.parentFKCol, "companyId")
	}
}

func TestDetectFKCorrelations_NoCorrelation(t *testing.T) {
	// child has companyId → company.id and categoryId → category.id
	// company and category are unrelated — no correlation expected.
	company := &introspect.Table{
		Name: "company",
		Columns: []introspect.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true},
		},
	}
	category := &introspect.Table{
		Name: "category",
		Columns: []introspect.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true},
		},
	}
	child := &introspect.Table{
		Name: "child",
		Columns: []introspect.Column{
			{Name: "companyId", DataType: "int", FK: &introspect.ForeignKey{ReferencedTable: "company", ReferencedColumn: "id"}},
			{Name: "categoryId", DataType: "int", FK: &introspect.ForeignKey{ReferencedTable: "category", ReferencedColumn: "id"}},
		},
	}

	allTables := map[string]*introspect.Table{
		"company":  company,
		"category": category,
		"child":    child,
	}

	correlations := detectFKCorrelations(child, allTables)
	if len(correlations) != 0 {
		t.Fatalf("expected 0 correlations, got %d", len(correlations))
	}
}

func TestDetectFKCorrelations_SingleFK(t *testing.T) {
	// Only one FK column — needs at least 2 for correlation.
	company := &introspect.Table{
		Name: "company",
		Columns: []introspect.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true},
		},
	}
	child := &introspect.Table{
		Name: "child",
		Columns: []introspect.Column{
			{Name: "companyId", DataType: "int", FK: &introspect.ForeignKey{ReferencedTable: "company", ReferencedColumn: "id"}},
		},
	}

	allTables := map[string]*introspect.Table{
		"company": company,
		"child":   child,
	}

	correlations := detectFKCorrelations(child, allTables)
	if len(correlations) != 0 {
		t.Fatalf("expected 0 correlations, got %d", len(correlations))
	}
}

func TestDetectFKCorrelations_SameTarget(t *testing.T) {
	// Two FK columns reference the same table — should NOT correlate.
	company := &introspect.Table{
		Name: "company",
		Columns: []introspect.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true},
		},
	}
	child := &introspect.Table{
		Name: "child",
		Columns: []introspect.Column{
			{Name: "ownerCompanyId", DataType: "int", FK: &introspect.ForeignKey{ReferencedTable: "company", ReferencedColumn: "id"}},
			{Name: "billingCompanyId", DataType: "int", FK: &introspect.ForeignKey{ReferencedTable: "company", ReferencedColumn: "id"}},
		},
	}

	allTables := map[string]*introspect.Table{
		"company": company,
		"child":   child,
	}

	correlations := detectFKCorrelations(child, allTables)
	if len(correlations) != 0 {
		t.Fatalf("expected 0 correlations for same-target FKs, got %d", len(correlations))
	}
}

func TestDetectFKCorrelations_MultipleDerived(t *testing.T) {
	// voucher has both companyId and employeeId
	// child has companyId, employeeId, and voucherId
	// Both companyId and employeeId should be derived from voucherId.
	company := &introspect.Table{
		Name: "company",
		Columns: []introspect.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true},
		},
	}
	employee := &introspect.Table{
		Name: "employee",
		Columns: []introspect.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true},
			{Name: "companyId", DataType: "int", FK: &introspect.ForeignKey{ReferencedTable: "company", ReferencedColumn: "id"}},
		},
	}
	voucher := &introspect.Table{
		Name: "voucher",
		Columns: []introspect.Column{
			{Name: "id", DataType: "int", IsPrimaryKey: true},
			{Name: "companyId", DataType: "int", FK: &introspect.ForeignKey{ReferencedTable: "company", ReferencedColumn: "id"}},
			{Name: "employeeId", DataType: "int", FK: &introspect.ForeignKey{ReferencedTable: "employee", ReferencedColumn: "id"}},
		},
	}
	child := &introspect.Table{
		Name: "child",
		Columns: []introspect.Column{
			{Name: "companyId", DataType: "int", FK: &introspect.ForeignKey{ReferencedTable: "company", ReferencedColumn: "id"}},
			{Name: "employeeId", DataType: "int", FK: &introspect.ForeignKey{ReferencedTable: "employee", ReferencedColumn: "id"}},
			{Name: "voucherId", DataType: "int", FK: &introspect.ForeignKey{ReferencedTable: "voucher", ReferencedColumn: "id"}},
		},
	}

	allTables := map[string]*introspect.Table{
		"company":  company,
		"employee": employee,
		"voucher":  voucher,
		"child":    child,
	}

	correlations := detectFKCorrelations(child, allTables)
	if len(correlations) < 2 {
		t.Fatalf("expected at least 2 correlations, got %d", len(correlations))
	}

	// Both companyId and employeeId should be claimed as derived.
	derived := make(map[string]bool)
	for _, c := range correlations {
		derived[c.derivedCol] = true
	}
	if !derived["companyId"] {
		t.Error("companyId should be derived")
	}
	if !derived["employeeId"] {
		t.Error("employeeId should be derived")
	}
}
