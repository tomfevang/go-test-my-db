package generator

import (
	"fmt"
	"math"
	"math/rand/v2"
	"sort"

	"github.com/tomfevang/go-seed-my-db/internal/config"
)

// ValuePicker selects values from a slice using a configurable distribution.
type ValuePicker struct {
	values []any
	pick   func() int // returns an index into values
}

// NewValuePicker creates a ValuePicker that selects from values using the given distribution.
// If dist is nil, uniform random selection is used.
func NewValuePicker(values []any, dist *config.DistributionConfig) *ValuePicker {
	if len(values) == 0 {
		panic("NewValuePicker: empty values slice")
	}

	vp := &ValuePicker{values: values}

	if dist == nil {
		vp.pick = func() int { return rand.IntN(len(values)) }
		return vp
	}

	switch dist.Type {
	case "zipf":
		vp.pick = buildZipfPicker(len(values), dist.S)
	case "normal":
		vp.pick = buildNormalPicker(len(values), dist.Mean, dist.StdDev)
	case "weighted":
		vp.pick = buildWeightedPicker(values, dist.Weights)
	default:
		// uniform (including explicit "uniform" or empty string)
		vp.pick = func() int { return rand.IntN(len(values)) }
	}

	return vp
}

// Pick returns a randomly selected value according to the distribution.
func (vp *ValuePicker) Pick() any {
	return vp.values[vp.pick()]
}

// buildZipfPicker creates a picker using Zipf's law: weight[i] = 1/(i+1)^s.
// Values are shuffled so popular values aren't always the lowest indices.
func buildZipfPicker(n int, s float64) func() int {
	if s <= 0 {
		s = 1.0
	}

	// Shuffle a mapping so the "popular" ranks map to random original indices.
	perm := rand.Perm(n)

	// Build CDF: weight[rank] = 1/(rank+1)^s
	cdf := make([]float64, n)
	total := 0.0
	for i := range n {
		w := 1.0 / math.Pow(float64(i+1), s)
		total += w
		cdf[i] = total
	}
	// Normalize
	for i := range cdf {
		cdf[i] /= total
	}

	return func() int {
		r := rand.Float64()
		rank := sort.SearchFloat64s(cdf, r)
		if rank >= n {
			rank = n - 1
		}
		return perm[rank]
	}
}

// buildNormalPicker creates a picker using a normal distribution.
// mean and stddev are fractions of n (e.g. mean=0.5 centers at the middle).
func buildNormalPicker(n int, mean, stddev float64) func() int {
	if mean == 0 {
		mean = 0.5
	}
	if stddev == 0 {
		stddev = 0.15
	}

	return func() int {
		v := rand.NormFloat64()*stddev*float64(n) + mean*float64(n)
		idx := int(v)
		if idx < 0 {
			idx = 0
		}
		if idx >= n {
			idx = n - 1
		}
		return idx
	}
}

// buildWeightedPicker creates a picker using explicit weights per value.
// Values are matched to weights by fmt.Sprint(val) comparison.
func buildWeightedPicker(values []any, weights map[string]float64) func() int {
	n := len(values)

	// Build per-index weights. Unmatched values get weight 1.0.
	w := make([]float64, n)
	for i, v := range values {
		key := fmt.Sprint(v)
		if wt, ok := weights[key]; ok {
			w[i] = wt
		} else {
			w[i] = 1.0
		}
	}

	// Build CDF
	cdf := make([]float64, n)
	total := 0.0
	for i, wt := range w {
		total += wt
		cdf[i] = total
	}
	for i := range cdf {
		cdf[i] /= total
	}

	return func() int {
		r := rand.Float64()
		idx := sort.SearchFloat64s(cdf, r)
		if idx >= n {
			idx = n - 1
		}
		return idx
	}
}
