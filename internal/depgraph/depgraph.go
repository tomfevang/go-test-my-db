package depgraph

import (
	"fmt"
	"strings"

	"github.com/tomfevang/go-test-my-db/internal/introspect"
)

// TableRelations holds parent relationship information for each table.
type TableRelations struct {
	// Parents maps each table name to its parent table names (via FK) within the seed set.
	Parents map[string][]string
}

// Resolve takes a map of table name -> Table and returns tables in topological
// order (parents before children). It auto-includes any referenced parent tables
// that are present in allTables but missing from the requested set.
//
// Returns the ordered list of table names, any auto-included parent names,
// table relations, and an error if circular dependencies are detected.
func Resolve(tables map[string]*introspect.Table, allTables map[string]*introspect.Table) ([]string, []string, *TableRelations, error) {
	// Auto-include parent tables that were not explicitly requested.
	var autoIncluded []string
	changed := true
	for changed {
		changed = false
		for _, t := range tables {
			for _, col := range t.Columns {
				if col.FK == nil {
					continue
				}
				ref := col.FK.ReferencedTable
				if ref == t.Name {
					continue // self-referencing FK, skip
				}
				if _, ok := tables[ref]; !ok {
					if parent, exists := allTables[ref]; exists {
						tables[ref] = parent
						autoIncluded = append(autoIncluded, ref)
						changed = true
					}
				}
			}
		}
	}

	// Build adjacency list: edge from parent -> child.
	inDegree := make(map[string]int)
	children := make(map[string][]string)
	for name := range tables {
		inDegree[name] = 0
	}

	for _, t := range tables {
		for _, col := range t.Columns {
			if col.FK == nil {
				continue
			}
			parent := col.FK.ReferencedTable
			if parent == t.Name {
				continue // skip self-references
			}
			if _, ok := tables[parent]; !ok {
				continue // parent not in our set
			}
			children[parent] = append(children[parent], t.Name)
			inDegree[t.Name]++
		}
	}

	// Kahn's algorithm for topological sort.
	var queue []string
	for name, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, name)
		}
	}

	var order []string
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		order = append(order, node)

		for _, child := range children[node] {
			inDegree[child]--
			if inDegree[child] == 0 {
				queue = append(queue, child)
			}
		}
	}

	if len(order) != len(tables) {
		// Find the cycle for a helpful error message.
		cycle := detectCycle(tables)
		return nil, nil, nil, fmt.Errorf("circular foreign key dependency detected: %s", strings.Join(cycle, " -> "))
	}

	// Build parent relationships (child -> deduplicated list of parents).
	parents := make(map[string][]string)
	for _, t := range tables {
		for _, col := range t.Columns {
			if col.FK == nil {
				continue
			}
			p := col.FK.ReferencedTable
			if p == t.Name {
				continue
			}
			if _, ok := tables[p]; !ok {
				continue
			}
			parents[t.Name] = append(parents[t.Name], p)
		}
	}
	// Deduplicate (a table might have multiple FK columns to the same parent).
	for name, ps := range parents {
		seen := make(map[string]bool)
		deduped := ps[:0]
		for _, p := range ps {
			if !seen[p] {
				seen[p] = true
				deduped = append(deduped, p)
			}
		}
		parents[name] = deduped
	}

	return order, autoIncluded, &TableRelations{Parents: parents}, nil
}

func detectCycle(tables map[string]*introspect.Table) []string {
	const (
		white = 0
		gray  = 1
		black = 2
	)

	color := make(map[string]int)
	parent := make(map[string]string)

	for name := range tables {
		color[name] = white
	}

	var cyclePath []string

	var dfs func(node string) bool
	dfs = func(node string) bool {
		color[node] = gray
		t := tables[node]
		for _, col := range t.Columns {
			if col.FK == nil {
				continue
			}
			next := col.FK.ReferencedTable
			if next == node {
				continue
			}
			if _, ok := tables[next]; !ok {
				continue
			}
			if color[next] == gray {
				// Found cycle. Reconstruct path.
				cyclePath = []string{next, node}
				cur := node
				for cur != next {
					cur = parent[cur]
					cyclePath = append(cyclePath, cur)
				}
				// Reverse to get correct order.
				for i, j := 0, len(cyclePath)-1; i < j; i, j = i+1, j-1 {
					cyclePath[i], cyclePath[j] = cyclePath[j], cyclePath[i]
				}
				return true
			}
			if color[next] == white {
				parent[next] = node
				if dfs(next) {
					return true
				}
			}
		}
		color[node] = black
		return false
	}

	for name := range tables {
		if color[name] == white {
			if dfs(name) {
				return cyclePath
			}
		}
	}

	return []string{"(unknown cycle)"}
}
