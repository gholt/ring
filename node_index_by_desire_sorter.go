package ring

type nodeIndexByDesireSorter struct {
	nodeIndexesByDesire             []int32
	nodeIndex2DesiredPartitionCount []int32
}

func (sorter *nodeIndexByDesireSorter) Len() int {
	return len(sorter.nodeIndexesByDesire)
}

func (sorter *nodeIndexByDesireSorter) Swap(x int, y int) {
	sorter.nodeIndexesByDesire[x], sorter.nodeIndexesByDesire[y] = sorter.nodeIndexesByDesire[y], sorter.nodeIndexesByDesire[x]
}

func (sorter *nodeIndexByDesireSorter) Less(x int, y int) bool {
	return sorter.nodeIndex2DesiredPartitionCount[sorter.nodeIndexesByDesire[x]] > sorter.nodeIndex2DesiredPartitionCount[sorter.nodeIndexesByDesire[y]]
}
