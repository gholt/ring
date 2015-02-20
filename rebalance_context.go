package ring

import (
	"math"
	"sort"
)

type rebalanceContextImpl struct {
	builder                         *Builder
	first                           bool
	nodeIndex2DesiredPartitionCount []int32
	nodeIndexesByDesire             []int32
	nodeIndex2Used                  []bool
	tierCount                       int
	tier2TierSeps                   [][]*tierSeparation
	tier2NodeIndex2TierSep          [][]*tierSeparation
}

func newRebalanceContext(builder *Builder) *rebalanceContextImpl {
	rebalanceContext := &rebalanceContextImpl{builder: builder}
	rebalanceContext.initTierCount()
	rebalanceContext.initNodeDesires()
	rebalanceContext.initTierInfo()
	return rebalanceContext
}

func (rebalanceContext *rebalanceContextImpl) initTierCount() {
	rebalanceContext.tierCount = 0
	for _, node := range rebalanceContext.builder.nodes {
		if !node.Active() {
			continue
		}
		nodeTierCount := len(node.TierValues())
		if nodeTierCount > rebalanceContext.tierCount {
			rebalanceContext.tierCount = nodeTierCount
		}
	}
}

func (rebalanceContext *rebalanceContextImpl) initNodeDesires() {
	totalCapacity := uint64(0)
	for _, node := range rebalanceContext.builder.nodes {
		if node.Active() {
			totalCapacity += (uint64)(node.Capacity())
		}
	}
	nodeIndex2PartitionCount := make([]int32, len(rebalanceContext.builder.nodes))
	rebalanceContext.first = true
	for _, partition2NodeIndex := range rebalanceContext.builder.replica2Partition2NodeIndex {
		for _, nodeIndex := range partition2NodeIndex {
			if nodeIndex >= 0 {
				nodeIndex2PartitionCount[nodeIndex]++
				rebalanceContext.first = false
			}
		}
	}
	rebalanceContext.nodeIndex2DesiredPartitionCount = make([]int32, len(rebalanceContext.builder.nodes))
	allPartitionsCount := len(rebalanceContext.builder.replica2Partition2NodeIndex) * len(rebalanceContext.builder.replica2Partition2NodeIndex[0])
	for nodeIndex, node := range rebalanceContext.builder.nodes {
		if node.Active() {
			rebalanceContext.nodeIndex2DesiredPartitionCount[nodeIndex] = int32(float64(node.Capacity())/float64(totalCapacity)*float64(allPartitionsCount)+0.5) - nodeIndex2PartitionCount[nodeIndex]
		} else {
			rebalanceContext.nodeIndex2DesiredPartitionCount[nodeIndex] = math.MinInt32
		}
	}
	rebalanceContext.nodeIndexesByDesire = make([]int32, 0, len(rebalanceContext.builder.nodes))
	for nodeIndex, node := range rebalanceContext.builder.nodes {
		if node.Active() {
			rebalanceContext.nodeIndexesByDesire = append(rebalanceContext.nodeIndexesByDesire, int32(nodeIndex))
		}
	}
	sort.Sort(&nodeIndexByDesireSorter{
		nodeIndexesByDesire:             rebalanceContext.nodeIndexesByDesire,
		nodeIndex2DesiredPartitionCount: rebalanceContext.nodeIndex2DesiredPartitionCount,
	})
}

func (rebalanceContext *rebalanceContextImpl) initTierInfo() {
	rebalanceContext.tier2NodeIndex2TierSep = make([][]*tierSeparation, rebalanceContext.tierCount)
	rebalanceContext.tier2TierSeps = make([][]*tierSeparation, rebalanceContext.tierCount)
	for tier := 1; tier < rebalanceContext.tierCount; tier++ {
		rebalanceContext.tier2NodeIndex2TierSep[tier] = make([]*tierSeparation, len(rebalanceContext.builder.nodes))
		rebalanceContext.tier2TierSeps[tier] = make([]*tierSeparation, 0)
	}
	for nodeIndex, node := range rebalanceContext.builder.nodes {
		nodeTierValues := node.TierValues()
		for tier := 1; tier < rebalanceContext.tierCount; tier++ {
			var tierSep *tierSeparation
			for _, candidateTierSep := range rebalanceContext.tier2TierSeps[tier] {
				tierSep = candidateTierSep
				for valueIndex := 0; valueIndex < rebalanceContext.tierCount-tier; valueIndex++ {
					value := 0
					if valueIndex+tier < len(nodeTierValues) {
						value = nodeTierValues[valueIndex+tier]
					}
					if tierSep.values[valueIndex] != value {
						tierSep = nil
						break
					}
				}
				if tierSep != nil {
					break
				}
			}
			if tierSep == nil {
				tierSep = &tierSeparation{values: make([]int, rebalanceContext.tierCount-tier), nodeIndexesByDesire: []int32{int32(nodeIndex)}}
				for valueIndex := 0; valueIndex < rebalanceContext.tierCount-tier; valueIndex++ {
					value := 0
					if valueIndex+tier < len(nodeTierValues) {
						value = nodeTierValues[valueIndex+tier]
					}
					tierSep.values[valueIndex] = value
				}
				rebalanceContext.tier2TierSeps[tier] = append(rebalanceContext.tier2TierSeps[tier], tierSep)
			} else {
				tierSep.nodeIndexesByDesire = append(tierSep.nodeIndexesByDesire, int32(nodeIndex))
			}
			rebalanceContext.tier2NodeIndex2TierSep[tier][int32(nodeIndex)] = tierSep
		}
	}
	for tier := 1; tier < rebalanceContext.tierCount; tier++ {
		for _, tierSep := range rebalanceContext.tier2TierSeps[tier] {
			sort.Sort(&nodeIndexByDesireSorter{
				nodeIndexesByDesire:             tierSep.nodeIndexesByDesire,
				nodeIndex2DesiredPartitionCount: rebalanceContext.nodeIndex2DesiredPartitionCount,
			})
		}
	}
}

func (rebalanceContext *rebalanceContextImpl) rebalance() bool {
	if rebalanceContext.first {
		rebalanceContext.firstRebalance()
		return true
	}
	return rebalanceContext.subsequentRebalance()
}

// firstRebalance is much simpler than what we have to do to rebalance existing
// assignments. Here, we just assign each partition in order, giving each
// replica of that partition to the next most-desired node, keeping in mind
// tier separation preferences.
func (rebalanceContext *rebalanceContextImpl) firstRebalance() {
	replicaCount := len(rebalanceContext.builder.replica2Partition2NodeIndex)
	partitionCount := len(rebalanceContext.builder.replica2Partition2NodeIndex[0])
	// We track the other nodes and tiers we've assigned partition replicas to
	// so that we can try to avoid assigning further replicas to similar nodes.
	otherNodeIndexes := make([]int32, replicaCount)
	rebalanceContext.nodeIndex2Used = make([]bool, len(rebalanceContext.builder.nodes))
	tier2OtherTierSeps := make([][]*tierSeparation, rebalanceContext.tierCount)
	for tier := 1; tier < rebalanceContext.tierCount; tier++ {
		tier2OtherTierSeps[tier] = make([]*tierSeparation, replicaCount)
	}
	for partition := 0; partition < partitionCount; partition++ {
		for replica := 0; replica < replicaCount; replica++ {
			if otherNodeIndexes[replica] != -1 {
				rebalanceContext.nodeIndex2Used[otherNodeIndexes[replica]] = false
			}
			otherNodeIndexes[replica] = -1
		}
		for tier := 1; tier < rebalanceContext.tierCount; tier++ {
			for replica := 0; replica < replicaCount; replica++ {
				if tier2OtherTierSeps[tier][replica] != nil {
					tier2OtherTierSeps[tier][replica].used = false
				}
				tier2OtherTierSeps[tier][replica] = nil
			}
		}
		for replica := 0; replica < replicaCount; replica++ {
			nodeIndex := rebalanceContext.bestNodeIndex()
			rebalanceContext.builder.replica2Partition2NodeIndex[replica][partition] = nodeIndex
			rebalanceContext.decrementDesire(nodeIndex)
			rebalanceContext.nodeIndex2Used[nodeIndex] = true
			otherNodeIndexes[replica] = nodeIndex
			for tier := 1; tier < rebalanceContext.tierCount; tier++ {
				tierSep := rebalanceContext.tier2NodeIndex2TierSep[tier][nodeIndex]
				tierSep.used = true
				tier2OtherTierSeps[tier][replica] = tierSep
			}
		}
	}
}

// subsequentRebalance is much more complicated than firstRebalance.
// First we'll reassign any partition replicas assigned to nodes with a
// weight less than 0, as this indicates a deleted node.
// Then we'll attempt to reassign partition replicas that are at extremely
// high risk because they're on the exact same node.
// Next we'll attempt to reassign partition replicas that are at some risk
// because they are currently assigned within the same tier separation.
// Then, we'll attempt to reassign replicas within tiers to achieve better
// distribution, as usually such intra-tier movements are more efficient
// for users of the ring.
// Finally, one last pass will be done to reassign replicas to still
// underweight nodes.
func (rebalanceContext *rebalanceContextImpl) subsequentRebalance() bool {
	replicaCount := len(rebalanceContext.builder.replica2Partition2NodeIndex)
	partitionCount := len(rebalanceContext.builder.replica2Partition2NodeIndex[0])
	// We'll track how many times we can move replicas for a given partition;
	// we want to leave at least half a partition's replicas in place.
	movementsPerPartition := byte(replicaCount / 2)
	if movementsPerPartition < 1 {
		movementsPerPartition = 1
	}
	partition2MovementsLeft := make([]byte, partitionCount)
	for partition := 0; partition < partitionCount; partition++ {
		partition2MovementsLeft[partition] = movementsPerPartition
	}
	altered := false

	// First we'll reassign any partition replicas assigned to nodes with a
	// weight less than 0, as this indicates a deleted node.
	for deletedNodeIndex, deletedNode := range rebalanceContext.builder.nodes {
		if deletedNode.Active() {
			continue
		}
		for replica := 0; replica < replicaCount; replica++ {
			partition2NodeIndex := rebalanceContext.builder.replica2Partition2NodeIndex[replica]
			for partition := 0; partition < partitionCount; partition++ {
				if partition2NodeIndex[partition] != int32(deletedNodeIndex) {
					continue
				}
				// We track the other nodes and tiers we've assigned partition
				// replicas to so that we can try to avoid assigning further
				// replicas to similar nodes.
				otherNodeIndexes := make([]int32, replicaCount)
				rebalanceContext.nodeIndex2Used = make([]bool, len(rebalanceContext.builder.nodes))
				tier2OtherTierSeps := make([][]*tierSeparation, rebalanceContext.tierCount)
				for tier := 1; tier < rebalanceContext.tierCount; tier++ {
					tier2OtherTierSeps[tier] = make([]*tierSeparation, replicaCount)
				}
				for replicaB := 0; replicaB < replicaCount; replicaB++ {
					otherNodeIndexes[replicaB] = rebalanceContext.builder.replica2Partition2NodeIndex[replicaB][partition]
					for tier := 1; tier < rebalanceContext.tierCount; tier++ {
						tierSep := rebalanceContext.tier2NodeIndex2TierSep[tier][otherNodeIndexes[replicaB]]
						tierSep.used = true
						tier2OtherTierSeps[tier][replicaB] = tierSep
					}
				}
				nodeIndex := rebalanceContext.bestNodeIndex()
				partition2NodeIndex[partition] = nodeIndex
				partition2MovementsLeft[partition]--
				altered = true
				rebalanceContext.decrementDesire(nodeIndex)
			}
		}
	}

	// TODO: Then we attempt to reassign at risk partitions. Partitions are
	// considered at risk if they have multiple replicas on the same node or
	// within the same tier separation.

	// TODO: Attempt to reassign replicas within tiers, from innermost tier to
	// outermost, as usually such movements are more efficient for users of the
	// ring. We do this by selecting the most needy node, and then look for
	// overweight nodes in the same tier to steal replicas from.

	// TODO: Lastly, we try to reassign replicas from overweight nodes to
	// underweight ones.
	return altered
}

func (rebalanceContext *rebalanceContextImpl) bestNodeIndex() int32 {
	bestNodeIndex := int32(-1)
	bestNodeDesiredPartitionCount := ^int32(0)
	nodeIndex2DesiredPartitionCount := rebalanceContext.nodeIndex2DesiredPartitionCount
	var tierSep *tierSeparation
	var nodeIndex int32
	tier2TierSeps := rebalanceContext.tier2TierSeps
	for tier := rebalanceContext.tierCount - 1; tier > 0; tier-- {
		// We will go through all tier separations for a tier to get the best
		// node at that tier.
		for _, tierSep = range tier2TierSeps[tier] {
			if !tierSep.used {
				nodeIndex = tierSep.nodeIndexesByDesire[0]
				if bestNodeDesiredPartitionCount < nodeIndex2DesiredPartitionCount[nodeIndex] {
					bestNodeIndex = nodeIndex
					bestNodeDesiredPartitionCount = nodeIndex2DesiredPartitionCount[nodeIndex]
				}
			}
		}
		// If we found a node at this tier, we don't need to check the lower
		// tiers.
		if bestNodeIndex >= 0 {
			return bestNodeIndex
		}
	}
	// If we found no good higher tiered candidates, we'll have to just
	// take the node with the highest desire that hasn't already been
	// selected.
	for _, nodeIndex := range rebalanceContext.nodeIndexesByDesire {
		if !rebalanceContext.nodeIndex2Used[nodeIndex] {
			return nodeIndex
		}
	}
	// If we still found no good candidates, we'll have to just take the
	// node with the highest desire.
	return rebalanceContext.nodeIndexesByDesire[0]
}

func (rebalanceContext *rebalanceContextImpl) decrementDesire(nodeIndex int32) {
	nodeIndex2DesiredPartitionCount := rebalanceContext.nodeIndex2DesiredPartitionCount
	nodeIndexesByDesire := rebalanceContext.nodeIndexesByDesire
	scanDesiredPartitionCount := nodeIndex2DesiredPartitionCount[nodeIndex] - 1
	swapWith := 0
	hi := len(nodeIndexesByDesire)
	mid := 0
	for swapWith < hi {
		mid = (swapWith + hi) / 2
		if nodeIndex2DesiredPartitionCount[nodeIndexesByDesire[mid]] > scanDesiredPartitionCount {
			swapWith = mid + 1
		} else {
			hi = mid
		}
	}
	prev := swapWith
	if prev >= len(nodeIndexesByDesire) {
		prev--
	}
	swapWith--
	for nodeIndexesByDesire[prev] != nodeIndex {
		prev--
	}
	if prev != swapWith {
		nodeIndexesByDesire[prev], nodeIndexesByDesire[swapWith] = nodeIndexesByDesire[swapWith], nodeIndexesByDesire[prev]
	}
	for tier := 1; tier < rebalanceContext.tierCount; tier++ {
		nodeIndexesByDesire := rebalanceContext.tier2NodeIndex2TierSep[tier][nodeIndex].nodeIndexesByDesire
		swapWith = 0
		hi = len(nodeIndexesByDesire)
		mid = 0
		for swapWith < hi {
			mid = (swapWith + hi) / 2
			if nodeIndex2DesiredPartitionCount[nodeIndexesByDesire[mid]] > scanDesiredPartitionCount {
				swapWith = mid + 1
			} else {
				hi = mid
			}
		}
		prev = swapWith
		if prev >= len(nodeIndexesByDesire) {
			prev--
		}
		swapWith--
		for nodeIndexesByDesire[prev] != nodeIndex {
			prev--
		}
		if prev != swapWith {
			nodeIndexesByDesire[prev], nodeIndexesByDesire[swapWith] = nodeIndexesByDesire[swapWith], nodeIndexesByDesire[prev]
		}
	}
	nodeIndex2DesiredPartitionCount[nodeIndex]--
}

type tierSeparation struct {
	values              []int
	nodeIndexesByDesire []int32
	used                bool
}
