package ring

import (
	"sort"
	"testing"
)

func TestNodeIndexByDesireSorter(t *testing.T) {
	nodeIndexes := []int32{0, 1, 2, 3, 4}
	nodeIndexToDesire := []int32{10, 5, 8, 20, 3}
	sort.Sort(&nodeIndexByDesireSorter{
		nodeIndexes:       nodeIndexes,
		nodeIndexToDesire: nodeIndexToDesire,
	})
	if nodeIndexes[0] != 3 ||
		nodeIndexes[1] != 0 ||
		nodeIndexes[2] != 2 ||
		nodeIndexes[3] != 1 ||
		nodeIndexes[4] != 4 {
		t.Fatalf("nodeIndexByDesireSorter resulted in %v instead of [3 0 2 1 4]", nodeIndexes)
	}
}
