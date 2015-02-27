package ring

type nodeIndexByDesireSorter struct {
	nodeIndexes       []int32
	nodeIndexToDesire []int32
}

func (sorter *nodeIndexByDesireSorter) Len() int {
	return len(sorter.nodeIndexes)
}

func (sorter *nodeIndexByDesireSorter) Swap(x int, y int) {
	sorter.nodeIndexes[x], sorter.nodeIndexes[y] = sorter.nodeIndexes[y], sorter.nodeIndexes[x]
}

func (sorter *nodeIndexByDesireSorter) Less(x int, y int) bool {
	return sorter.nodeIndexToDesire[sorter.nodeIndexes[x]] > sorter.nodeIndexToDesire[sorter.nodeIndexes[y]]
}
