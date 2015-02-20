package ring

type nodeIndexByDesireSorter struct {
	nodeIndexes      []int32
	nodeIndex2Desire []int32
}

func (sorter *nodeIndexByDesireSorter) Len() int {
	return len(sorter.nodeIndexes)
}

func (sorter *nodeIndexByDesireSorter) Swap(x int, y int) {
	sorter.nodeIndexes[x], sorter.nodeIndexes[y] = sorter.nodeIndexes[y], sorter.nodeIndexes[x]
}

func (sorter *nodeIndexByDesireSorter) Less(x int, y int) bool {
	return sorter.nodeIndex2Desire[sorter.nodeIndexes[x]] > sorter.nodeIndex2Desire[sorter.nodeIndexes[y]]
}
