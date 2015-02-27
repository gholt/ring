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
	context.nodeIndex2Used = make([]bool, len(context.builder.nodes))
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
	replica2Partition2NodeIndex := context.builder.replica2Partition2NodeIndex
	maxReplica := len(replica2Partition2NodeIndex) - 1
	maxPartition := len(replica2Partition2NodeIndex[0]) - 1
	maxTier := context.tierCount - 1
	nodeIndex2Used := context.nodeIndex2Used
	tier2NodeIndex2TierSep := context.tier2NodeIndex2TierSep

	usedNodeIndexes := make([]int32, maxReplica+1)
	tier2UsedTierSeps := make([][]*tierSeparation, maxTier+1)
	for tier := maxTier; tier >= 0; tier-- {
		tier2UsedTierSeps[tier] = make([]*tierSeparation, maxReplica+1)
	}
	for partition := maxPartition; partition >= 0; partition-- {
		// Clear the previous partition's used flags.
		for replica := maxReplica; replica >= 0; replica-- {
			if usedNodeIndexes[replica] != -1 {
				nodeIndex2Used[usedNodeIndexes[replica]] = false
				usedNodeIndexes[replica] = -1
			}
		}
		for tier := maxTier; tier >= 0; tier-- {
			for replica := maxReplica; replica >= 0; replica-- {
				if tier2UsedTierSeps[tier][replica] != nil {
					tier2UsedTierSeps[tier][replica].used = false
				}
				tier2UsedTierSeps[tier][replica] = nil
			}
		}
		// Now assign this partition's replicas.
		for replica := maxReplica; replica >= 0; replica-- {
			nodeIndex := context.bestNodeIndex()
			if nodeIndex < 0 {
				continue
			}
			replica2Partition2NodeIndex[replica][partition] = nodeIndex
			context.changeDesire(nodeIndex, false)
			nodeIndex2Used[nodeIndex] = true
			usedNodeIndexes[replica] = nodeIndex
			for tier := maxTier; tier >= 0; tier-- {
				tierSep := tier2NodeIndex2TierSep[tier][nodeIndex]
				tierSep.used = true
				tier2UsedTierSeps[tier][replica] = tierSep
			}
		}
	}
}

// subsequentRebalance is much more complicated than firstRebalance. It makes
// multiple passes based on different scenarios (deactivated nodes, redundant
// assignments, changing node weights) and reassigns replicas as it can.
func (context *rebalanceContext) subsequentRebalance() bool {
	altered := false
	replica2Partition2NodeIndex := context.builder.replica2Partition2NodeIndex
	maxReplica := len(replica2Partition2NodeIndex) - 1
	maxPartition := len(replica2Partition2NodeIndex[0]) - 1
	maxTier := context.tierCount - 1
	nodes := context.builder.nodes
	nodeIndex2Used := context.nodeIndex2Used
	tier2NodeIndex2TierSep := context.tier2NodeIndex2TierSep

	// Track how many times we can move replicas for a given partition; we want
	// to leave the majority of a partition's replicas in place, if possible.
	movementsPerPartition := byte(maxReplica / 2)
	if movementsPerPartition < 1 {
		movementsPerPartition = 1
	}
	partition2MovementsLeft := make([]byte, maxPartition+1)
	for partition := maxPartition; partition >= 0; partition-- {
		partition2MovementsLeft[partition] = movementsPerPartition
	}

	usedNodeIndexes := make([]int32, maxReplica+1)
	tier2UsedTierSeps := make([][]*tierSeparation, maxTier+1)
	for tier := maxTier; tier >= 0; tier-- {
		tier2UsedTierSeps[tier] = make([]*tierSeparation, maxReplica+1)
	}
	clearUsed := func() {
		for replica := maxReplica; replica >= 0; replica-- {
			if usedNodeIndexes[replica] != -1 {
				nodeIndex2Used[usedNodeIndexes[replica]] = false
				usedNodeIndexes[replica] = -1
			}
		}
		for tier := maxTier; tier >= 0; tier-- {
			for replica := maxReplica; replica >= 0; replica-- {
				if tier2UsedTierSeps[tier][replica] != nil {
					tier2UsedTierSeps[tier][replica].used = false
				}
				tier2UsedTierSeps[tier][replica] = nil
			}
		}
	}
	markUsed := func(partition int) {
		for replica := maxReplica; replica >= 0; replica-- {
			nodeIndex := replica2Partition2NodeIndex[replica][partition]
			usedNodeIndexes[replica] = nodeIndex
			nodeIndex2Used[nodeIndex] = true
			for tier := maxTier; tier >= 0; tier-- {
				tierSep := tier2NodeIndex2TierSep[tier][nodeIndex]
				tierSep.used = true
				tier2UsedTierSeps[tier][replica] = tierSep
			}
		}
	}

	// We'll reassign any partition replicas assigned to nodes not marked
	// active (deleted or failed nodes).
	for deletedNodeIndex, deletedNode := range nodes {
		if deletedNode.Active() {
			continue
		}
		for replica := maxReplica; replica >= 0; replica-- {
			partition2NodeIndex := replica2Partition2NodeIndex[replica]
			for partition := maxPartition; partition >= 0; partition-- {
				if partition2NodeIndex[partition] != int32(deletedNodeIndex) {
					continue
				}
				clearUsed()
				markUsed(partition)
				nodeIndex := context.bestNodeIndex()
				if nodeIndex < 0 {
					continue
				}
				partition2NodeIndex[partition] = nodeIndex
				context.changeDesire(nodeIndex, false)
				partition2MovementsLeft[partition]--
				altered = true
			}
		}
	}

	// Look for replicas assigned to the same node more than once. This
	// shouldn't be a common use case; but if it turns out to be, it might be
	// worthwhile to reassign the worst duplicates first. For example, a
	// partition with only 1 distinct replica node would be fixed before
	// others. Another example, a partition that has two duplicate nodes but
	// one has more replicas than the other, it would be fixed first.
DupLoopPartition:
	for partition := maxPartition; partition >= 0; partition-- {
		if partition2MovementsLeft[partition] < 1 {
			continue
		}
	DupLoopReplica:
		for replica := maxReplica; replica > 0; replica-- {
			for replicaB := replica - 1; replicaB >= 0; replicaB-- {
				if replica2Partition2NodeIndex[replica][partition] == replica2Partition2NodeIndex[replicaB][partition] {
					clearUsed()
					markUsed(partition)
					nodeIndex := context.bestNodeIndex()
					if nodeIndex < 0 {
						continue
					}
					// No sense reassigning a duplicate to another duplicate.
					for replicaC := maxReplica; replicaC >= 0; replicaC-- {
						if nodeIndex == replica2Partition2NodeIndex[replicaC][partition] {
							continue DupLoopReplica
						}
					}
					context.changeDesire(replica2Partition2NodeIndex[replica][partition], true)
					replica2Partition2NodeIndex[replica][partition] = nodeIndex
					context.changeDesire(nodeIndex, false)
					partition2MovementsLeft[partition]--
					altered = true
					if partition2MovementsLeft[partition] < 1 {
						continue DupLoopPartition
					}
				}
			}
		}
	}

	// Look for replicas assigned to the same tier more than once.
	for tier := maxTier; tier >= 0; tier-- {
	DupTierLoopPartition:
		for partition := maxPartition; partition >= 0; partition-- {
			if partition2MovementsLeft[partition] < 1 {
				continue
			}
		DupTierLoopReplica:
			for replica := maxReplica; replica > 0; replica-- {
				for replicaB := replica - 1; replicaB >= 0; replicaB-- {
					if tier2NodeIndex2TierSep[tier][replica2Partition2NodeIndex[replica][partition]] == tier2NodeIndex2TierSep[tier][replica2Partition2NodeIndex[replicaB][partition]] {
						clearUsed()
						markUsed(partition)
						nodeIndex := context.bestNodeIndex()
						if nodeIndex < 0 {
							continue
						}
						// No sense reassigning a duplicate to another duplicate.
						for replicaC := maxReplica; replicaC >= 0; replicaC-- {
							if tier2NodeIndex2TierSep[tier][nodeIndex] == tier2NodeIndex2TierSep[tier][replica2Partition2NodeIndex[replicaC][partition]] {
								continue DupTierLoopReplica
							}
						}
						context.changeDesire(replica2Partition2NodeIndex[replica][partition], true)
						replica2Partition2NodeIndex[replica][partition] = nodeIndex
						context.changeDesire(nodeIndex, false)
						partition2MovementsLeft[partition]--
						altered = true
						if partition2MovementsLeft[partition] < 1 {
							continue DupTierLoopPartition
						}
					}
				}
			}
		}
	}

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
	// If we still found no good candidates...
	return -1
}

func (context *rebalanceContext) changeDesire(nodeIndex int32, increment bool) {
	nodeIndex2Desire := context.nodeIndex2Desire
	nodeIndexesByDesire := context.nodeIndexesByDesire
	scanDesiredPartitionCount := nodeIndex2Desire[nodeIndex]
	if increment {
		scanDesiredPartitionCount++
	} else {
		scanDesiredPartitionCount--
	}
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
	if increment {
		nodeIndex2Desire[nodeIndex]++
	} else {
		nodeIndex2Desire[nodeIndex]--
	}
}

type tierSeparation struct {
	values              []int
	nodeIndexesByDesire []int32
	used                bool
}
