package seeder

import (
	"testing"

	"github.com/tomfevang/go-test-my-db/internal/introspect"
)

func TestReservoirSample_Unbounded(t *testing.T) {
	values := []any{1, 2, 3, 4, 5}
	result := reservoirSample(values, 0)
	if len(result) != len(values) {
		t.Fatalf("expected %d items, got %d", len(values), len(result))
	}
}

func TestReservoirSample_FewerThanLimit(t *testing.T) {
	values := []any{1, 2, 3}
	result := reservoirSample(values, 10)
	if len(result) != len(values) {
		t.Fatalf("expected %d items, got %d", len(values), len(result))
	}
}

func TestReservoirSample_ExactlyLimit(t *testing.T) {
	values := make([]any, 1000)
	for i := range values {
		values[i] = i
	}
	result := reservoirSample(values, 100)
	if len(result) != 100 {
		t.Fatalf("expected 100 items, got %d", len(result))
	}

	// Verify all returned items come from the source.
	sourceSet := make(map[any]bool, len(values))
	for _, v := range values {
		sourceSet[v] = true
	}
	for _, v := range result {
		if !sourceSet[v] {
			t.Fatalf("result contains value %v not in source", v)
		}
	}
}

func TestReservoirSample_NilSlice(t *testing.T) {
	result := reservoirSample(nil, 10)
	if result != nil {
		t.Fatalf("expected nil, got %v", result)
	}
}

func TestComputeLastConsumers_LinearChain(t *testing.T) {
	// A -> B -> C: B references A, C references B
	tables := []*introspect.Table{
		{Name: "A", Columns: []introspect.Column{{Name: "id", IsPrimaryKey: true}}},
		{Name: "B", Columns: []introspect.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "a_id", FK: &introspect.ForeignKey{ReferencedTable: "A", ReferencedColumn: "id"}},
		}},
		{Name: "C", Columns: []introspect.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "b_id", FK: &introspect.ForeignKey{ReferencedTable: "B", ReferencedColumn: "id"}},
		}},
	}

	last := computeLastConsumers(tables)

	// A.id is referenced only by B (index 1)
	if last["A.id"] != 1 {
		t.Errorf("expected A.id last consumer = 1, got %d", last["A.id"])
	}
	// B.id is referenced only by C (index 2)
	if last["B.id"] != 2 {
		t.Errorf("expected B.id last consumer = 2, got %d", last["B.id"])
	}
}

func TestComputeLastConsumers_StarSchema(t *testing.T) {
	// Hub table referenced by multiple fact tables
	tables := []*introspect.Table{
		{Name: "hub", Columns: []introspect.Column{{Name: "id", IsPrimaryKey: true}}},
		{Name: "fact1", Columns: []introspect.Column{
			{Name: "hub_id", FK: &introspect.ForeignKey{ReferencedTable: "hub", ReferencedColumn: "id"}},
		}},
		{Name: "fact2", Columns: []introspect.Column{
			{Name: "hub_id", FK: &introspect.ForeignKey{ReferencedTable: "hub", ReferencedColumn: "id"}},
		}},
		{Name: "fact3", Columns: []introspect.Column{
			{Name: "hub_id", FK: &introspect.ForeignKey{ReferencedTable: "hub", ReferencedColumn: "id"}},
		}},
	}

	last := computeLastConsumers(tables)

	// hub.id should be retained until the last consumer (fact3, index 3)
	if last["hub.id"] != 3 {
		t.Errorf("expected hub.id last consumer = 3, got %d", last["hub.id"])
	}
}

func TestComputeLastConsumers_NoFKs(t *testing.T) {
	tables := []*introspect.Table{
		{Name: "standalone", Columns: []introspect.Column{
			{Name: "id", IsPrimaryKey: true},
			{Name: "name"},
		}},
	}

	last := computeLastConsumers(tables)
	if len(last) != 0 {
		t.Errorf("expected empty map for table with no FKs, got %v", last)
	}
}
