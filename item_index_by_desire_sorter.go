package ring

type itemIndexByDesireSorterImpl struct {
	itemIndexesByDesire             []int32
	itemIndex2DesiredPartitionCount []int32
}

func (sorter *itemIndexByDesireSorterImpl) Len() int {
	return len(sorter.itemIndexesByDesire)
}

func (sorter *itemIndexByDesireSorterImpl) Swap(x int, y int) {
	sorter.itemIndexesByDesire[x], sorter.itemIndexesByDesire[y] = sorter.itemIndexesByDesire[y], sorter.itemIndexesByDesire[x]
}

func (sorter *itemIndexByDesireSorterImpl) Less(x int, y int) bool {
	return sorter.itemIndex2DesiredPartitionCount[sorter.itemIndexesByDesire[x]] > sorter.itemIndex2DesiredPartitionCount[sorter.itemIndexesByDesire[y]]
}
