package ring

import (
	"math"
	"sort"
)

type rebalanceContext struct {
	builder                *Builder
	first                  bool
	nodeIndex2Desire       []int32
	nodeIndexesByDesire    []int32
	nodeIndex2Used         []bool
	tierCount              int
	tier2TierSeps          [][]*tierSeparation
	tier2NodeIndex2TierSep [][]*tierSeparation
}

func newRebalanceContext(builder *Builder) *rebalanceContext {
	context := &rebalanceContext{builder: builder}
	context.initTierCount()
	context.initNodeDesires()
	context.initTierInfo()
	return context
}

func (context *rebalanceContext) initTierCount() {
	context.tierCount = 0
	for _, node := range context.builder.nodes {
		if !node.Active() {
			continue
		}
		nodeTierCount := len(node.TierValues())
		if nodeTierCount > context.tierCount {
			context.tierCount = nodeTierCount
		}
	}
}

func (context *rebalanceContext) initNodeDesires() {
	totalCapacity := uint64(0)
	for _, node := range context.builder.nodes {
		if node.Active() {
			totalCapacity += (uint64)(node.Capacity())
		}
	}
	nodeIndex2PartitionCount := make([]int32, len(context.builder.nodes))
	context.first = true
	for _, partition2NodeIndex := range context.builder.replica2Partition2NodeIndex {
		for _, nodeIndex := range partition2NodeIndex {
			if nodeIndex >= 0 {
				nodeIndex2PartitionCount[nodeIndex]++
				context.first = false
			}
		}
	}
	context.nodeIndex2Desire = make([]int32, len(context.builder.nodes))
	allPartitionsCount := len(context.builder.replica2Partition2NodeIndex) * len(context.builder.replica2Partition2NodeIndex[0])
	for nodeIndex, node := range context.builder.nodes {
		if node.Active() {
			context.nodeIndex2Desire[nodeIndex] = int32(float64(node.Capacity())/float64(totalCapacity)*float64(allPartitionsCount)+0.5) - nodeIndex2PartitionCount[nodeIndex]
		} else {
			context.nodeIndex2Desire[nodeIndex] = math.MinInt32
		}
	}
	context.nodeIndexesByDesire = make([]int32, 0, len(context.builder.nodes))
	for nodeIndex, node := range context.builder.nodes {
		if node.Active() {
			context.nodeIndexesByDesire = append(context.nodeIndexesByDesire, int32(nodeIndex))
		}
	}
	sort.Sort(&nodeIndexByDesireSorter{
		nodeIndexes:      context.nodeIndexesByDesire,
		nodeIndex2Desire: context.nodeIndex2Desire,
	})
}

func (context *rebalanceContext) initTierInfo() {
	context.tier2NodeIndex2TierSep = make([][]*tierSeparation, context.tierCount)
	context.tier2TierSeps = make([][]*tierSeparation, context.tierCount)
	for tier := 0; tier < context.tierCount; tier++ {
		context.tier2NodeIndex2TierSep[tier] = make([]*tierSeparation, len(context.builder.nodes))
		context.tier2TierSeps[tier] = make([]*tierSeparation, 0)
	}
	for nodeIndex, node := range context.builder.nodes {
		nodeTierValues := node.TierValues()
		for tier := 0; tier < context.tierCount; tier++ {
			var tierSep *tierSeparation
			for _, candidateTierSep := range context.tier2TierSeps[tier] {
				tierSep = candidateTierSep
				for valueIndex := 0; valueIndex < context.tierCount-tier; valueIndex++ {
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
				tierSep = &tierSeparation{values: make([]int, context.tierCount-tier), nodeIndexesByDesire: []int32{int32(nodeIndex)}}
				for valueIndex := 0; valueIndex < context.tierCount-tier; valueIndex++ {
					value := 0
					if valueIndex+tier < len(nodeTierValues) {
						value = nodeTierValues[valueIndex+tier]
					}
					tierSep.values[valueIndex] = value
				}
				context.tier2TierSeps[tier] = append(context.tier2TierSeps[tier], tierSep)
			} else {
				tierSep.nodeIndexesByDesire = append(tierSep.nodeIndexesByDesire, int32(nodeIndex))
			}
			context.tier2NodeIndex2TierSep[tier][int32(nodeIndex)] = tierSep
		}
	}
	for tier := 0; tier < context.tierCount; tier++ {
		for _, tierSep := range context.tier2TierSeps[tier] {
			sort.Sort(&nodeIndexByDesireSorter{
				nodeIndexes:      tierSep.nodeIndexesByDesire,
				nodeIndex2Desire: context.nodeIndex2Desire,
			})
		}
	}
}

func (context *rebalanceContext) rebalance() bool {
	if context.first {
		context.firstRebalance()
		return true
	}
	return context.subsequentRebalance()
}

// firstRebalance is much simpler than what we have to do to rebalance existing
// assignments. Here, we just assign each partition in order, giving each
// replica of that partition to the next most-desired node, keeping in mind
// tier separation preferences.
func (context *rebalanceContext) firstRebalance() {
	replicaCount := len(context.builder.replica2Partition2NodeIndex)
	partitionCount := len(context.builder.replica2Partition2NodeIndex[0])
	// We track the other nodes and tiers we've assigned partition replicas to
	// so that we can try to avoid assigning further replicas to similar nodes.
	otherNodeIndexes := make([]int32, replicaCount)
	context.nodeIndex2Used = make([]bool, len(context.builder.nodes))
	tier2OtherTierSeps := make([][]*tierSeparation, context.tierCount)
	for tier := 0; tier < context.tierCount; tier++ {
		tier2OtherTierSeps[tier] = make([]*tierSeparation, replicaCount)
	}
	for partition := 0; partition < partitionCount; partition++ {
		for replica := 0; replica < replicaCount; replica++ {
			if otherNodeIndexes[replica] != -1 {
				context.nodeIndex2Used[otherNodeIndexes[replica]] = false
			}
			otherNodeIndexes[replica] = -1
		}
		for tier := 0; tier < context.tierCount; tier++ {
			for replica := 0; replica < replicaCount; replica++ {
				if tier2OtherTierSeps[tier][replica] != nil {
					tier2OtherTierSeps[tier][replica].used = false
				}
				tier2OtherTierSeps[tier][replica] = nil
			}
		}
		for replica := 0; replica < replicaCount; replica++ {
			nodeIndex := context.bestNodeIndex()
			context.builder.replica2Partition2NodeIndex[replica][partition] = nodeIndex
			context.decrementDesire(nodeIndex)
			context.nodeIndex2Used[nodeIndex] = true
			otherNodeIndexes[replica] = nodeIndex
			for tier := 0; tier < context.tierCount; tier++ {
				tierSep := context.tier2NodeIndex2TierSep[tier][nodeIndex]
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
func (context *rebalanceContext) subsequentRebalance() bool {
	replicaCount := len(context.builder.replica2Partition2NodeIndex)
	partitionCount := len(context.builder.replica2Partition2NodeIndex[0])
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
	for deletedNodeIndex, deletedNode := range context.builder.nodes {
		if deletedNode.Active() {
			continue
		}
		for replica := 0; replica < replicaCount; replica++ {
			partition2NodeIndex := context.builder.replica2Partition2NodeIndex[replica]
			for partition := 0; partition < partitionCount; partition++ {
				if partition2NodeIndex[partition] != int32(deletedNodeIndex) {
					continue
				}
				// We track the other nodes and tiers we've assigned partition
				// replicas to so that we can try to avoid assigning further
				// replicas to similar nodes.
				otherNodeIndexes := make([]int32, replicaCount)
				context.nodeIndex2Used = make([]bool, len(context.builder.nodes))
				tier2OtherTierSeps := make([][]*tierSeparation, context.tierCount)
				for tier := 0; tier < context.tierCount; tier++ {
					tier2OtherTierSeps[tier] = make([]*tierSeparation, replicaCount)
				}
				for replicaB := 0; replicaB < replicaCount; replicaB++ {
					otherNodeIndexes[replicaB] = context.builder.replica2Partition2NodeIndex[replicaB][partition]
					for tier := 0; tier < context.tierCount; tier++ {
						tierSep := context.tier2NodeIndex2TierSep[tier][otherNodeIndexes[replicaB]]
						tierSep.used = true
						tier2OtherTierSeps[tier][replicaB] = tierSep
					}
				}
				nodeIndex := context.bestNodeIndex()
				partition2NodeIndex[partition] = nodeIndex
				partition2MovementsLeft[partition]--
				altered = true
				context.decrementDesire(nodeIndex)
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

func (context *rebalanceContext) bestNodeIndex() int32 {
	bestNodeIndex := int32(-1)
	bestNodeDesiredPartitionCount := int32(math.MinInt32)
	nodeIndex2Desire := context.nodeIndex2Desire
	var tierSep *tierSeparation
	var nodeIndex int32
	tier2TierSeps := context.tier2TierSeps
	for tier := context.tierCount - 1; tier >= 0; tier-- {
		// We will go through all tier separations for a tier to get the best
		// node at that tier.
		for _, tierSep = range tier2TierSeps[tier] {
			if !tierSep.used {
				nodeIndex = tierSep.nodeIndexesByDesire[0]
				if bestNodeDesiredPartitionCount < nodeIndex2Desire[nodeIndex] {
					bestNodeIndex = nodeIndex
					bestNodeDesiredPartitionCount = nodeIndex2Desire[nodeIndex]
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
	for _, nodeIndex := range context.nodeIndexesByDesire {
		if !context.nodeIndex2Used[nodeIndex] {
			return nodeIndex
		}
	}
	// If we still found no good candidates, we'll have to just take the
	// node with the highest desire.
	return context.nodeIndexesByDesire[0]
}

func (context *rebalanceContext) decrementDesire(nodeIndex int32) {
	nodeIndex2Desire := context.nodeIndex2Desire
	nodeIndexesByDesire := context.nodeIndexesByDesire
	scanDesiredPartitionCount := nodeIndex2Desire[nodeIndex] - 1
	swapWith := 0
	hi := len(nodeIndexesByDesire)
	mid := 0
	for swapWith < hi {
		mid = (swapWith + hi) / 2
		if nodeIndex2Desire[nodeIndexesByDesire[mid]] > scanDesiredPartitionCount {
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
	for tier := 0; tier < context.tierCount; tier++ {
		nodeIndexesByDesire := context.tier2NodeIndex2TierSep[tier][nodeIndex].nodeIndexesByDesire
		swapWith = 0
		hi = len(nodeIndexesByDesire)
		mid = 0
		for swapWith < hi {
			mid = (swapWith + hi) / 2
			if nodeIndex2Desire[nodeIndexesByDesire[mid]] > scanDesiredPartitionCount {
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
	nodeIndex2Desire[nodeIndex]--
}

type tierSeparation struct {
	values              []int
	nodeIndexesByDesire []int32
	used                bool
}
