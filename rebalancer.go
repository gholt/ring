package ring

import (
	"math"
	"sort"
)

type rebalancer struct {
	builder                  *Builder
	nodeIndexToDesire        []int32
	nodeIndexesByDesire      []int32
	nodeIndexToUsed          []bool
	tierCount                int
	tierToTierSeps           [][]*tierSeparation
	tierToNodeIndexToTierSep [][]*tierSeparation
}

func newRebalancer(builder *Builder) *rebalancer {
	context := &rebalancer{builder: builder}
	context.initTierCount()
	context.initNodeDesires()
	context.initTierInfo()
	return context
}

func (context *rebalancer) initTierCount() {
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

func (context *rebalancer) initNodeDesires() {
	totalCapacity := uint64(0)
	for _, node := range context.builder.nodes {
		if node.Active() {
			totalCapacity += (uint64)(node.Capacity())
		}
	}
	nodeIndexToPartitionCount := make([]int32, len(context.builder.nodes))
	for _, partitionToNodeIndex := range context.builder.replicaToPartitionToNodeIndex {
		for _, nodeIndex := range partitionToNodeIndex {
			if nodeIndex >= 0 {
				nodeIndexToPartitionCount[nodeIndex]++
			}
		}
	}
	context.nodeIndexToDesire = make([]int32, len(context.builder.nodes))
	allPartitionsCount := len(context.builder.replicaToPartitionToNodeIndex) * len(context.builder.replicaToPartitionToNodeIndex[0])
	for nodeIndex, node := range context.builder.nodes {
		if node.Active() {
			context.nodeIndexToDesire[nodeIndex] = int32(float64(node.Capacity())/float64(totalCapacity)*float64(allPartitionsCount)+0.5) - nodeIndexToPartitionCount[nodeIndex]
		} else {
			context.nodeIndexToDesire[nodeIndex] = math.MinInt32
		}
	}
	context.nodeIndexesByDesire = make([]int32, 0, len(context.builder.nodes))
	for nodeIndex, node := range context.builder.nodes {
		if node.Active() {
			context.nodeIndexesByDesire = append(context.nodeIndexesByDesire, int32(nodeIndex))
		}
	}
	sort.Sort(&nodeIndexByDesireSorter{
		nodeIndexes:       context.nodeIndexesByDesire,
		nodeIndexToDesire: context.nodeIndexToDesire,
	})
	context.nodeIndexToUsed = make([]bool, len(context.builder.nodes))
}

func (context *rebalancer) initTierInfo() {
	context.tierToNodeIndexToTierSep = make([][]*tierSeparation, context.tierCount)
	context.tierToTierSeps = make([][]*tierSeparation, context.tierCount)
	for tier := 0; tier < context.tierCount; tier++ {
		context.tierToNodeIndexToTierSep[tier] = make([]*tierSeparation, len(context.builder.nodes))
		context.tierToTierSeps[tier] = make([]*tierSeparation, 0)
	}
	for nodeIndex, node := range context.builder.nodes {
		nodeTierValues := node.TierValues()
		for tier := 0; tier < context.tierCount; tier++ {
			var tierSep *tierSeparation
			for _, candidateTierSep := range context.tierToTierSeps[tier] {
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
				context.tierToTierSeps[tier] = append(context.tierToTierSeps[tier], tierSep)
			} else {
				tierSep.nodeIndexesByDesire = append(tierSep.nodeIndexesByDesire, int32(nodeIndex))
			}
			context.tierToNodeIndexToTierSep[tier][int32(nodeIndex)] = tierSep
		}
	}
	for tier := 0; tier < context.tierCount; tier++ {
		for _, tierSep := range context.tierToTierSeps[tier] {
			sort.Sort(&nodeIndexByDesireSorter{
				nodeIndexes:       tierSep.nodeIndexesByDesire,
				nodeIndexToDesire: context.nodeIndexToDesire,
			})
		}
	}
}

func (context *rebalancer) rebalance() bool {
	altered := false
	replicaToPartitionToNodeIndex := context.builder.replicaToPartitionToNodeIndex
	maxReplica := len(replicaToPartitionToNodeIndex) - 1
	maxPartition := len(replicaToPartitionToNodeIndex[0]) - 1
	maxTier := context.tierCount - 1
	nodes := context.builder.nodes
	nodeIndexToUsed := context.nodeIndexToUsed
	nodeIndexToDesire := context.nodeIndexToDesire
	tierToNodeIndexToTierSep := context.tierToNodeIndexToTierSep

	// Track how many times we can move replicas for a given partition; we want
	// to leave the majority of a partition's replicas in place, if possible.
	movementsPerPartition := byte(maxReplica / 2)
	if movementsPerPartition < 1 {
		movementsPerPartition = 1
	}
	partitionToMovementsLeft := make([]byte, maxPartition+1)
	for partition := maxPartition; partition >= 0; partition-- {
		partitionToMovementsLeft[partition] = movementsPerPartition
	}

	usedNodeIndexes := make([]int32, maxReplica+1)
	tierToUsedTierSeps := make([][]*tierSeparation, maxTier+1)
	for tier := maxTier; tier >= 0; tier-- {
		tierToUsedTierSeps[tier] = make([]*tierSeparation, maxReplica+1)
	}
	clearUsed := func() {
		for replica := maxReplica; replica >= 0; replica-- {
			if usedNodeIndexes[replica] != -1 {
				nodeIndexToUsed[usedNodeIndexes[replica]] = false
				usedNodeIndexes[replica] = -1
			}
		}
		for tier := maxTier; tier >= 0; tier-- {
			for replica := maxReplica; replica >= 0; replica-- {
				if tierToUsedTierSeps[tier][replica] != nil {
					tierToUsedTierSeps[tier][replica].used = false
				}
				tierToUsedTierSeps[tier][replica] = nil
			}
		}
	}
	markUsed := func(partition int) {
		for replica := maxReplica; replica >= 0; replica-- {
			nodeIndex := replicaToPartitionToNodeIndex[replica][partition]
			if nodeIndex < 0 {
				continue
			}
			usedNodeIndexes[replica] = nodeIndex
			nodeIndexToUsed[nodeIndex] = true
			for tier := maxTier; tier >= 0; tier-- {
				tierSep := tierToNodeIndexToTierSep[tier][nodeIndex]
				tierSep.used = true
				tierToUsedTierSeps[tier][replica] = tierSep
			}
		}
	}

	// Assign any partitions assigned as -1 (can happen with a node removed
	// with the Remove() method).
	for replica := maxReplica; replica >= 0; replica-- {
		partitionToNodeIndex := replicaToPartitionToNodeIndex[replica]
		for partition := maxPartition; partition >= 0; partition-- {
			if partitionToNodeIndex[partition] >= 0 {
				continue
			}
			clearUsed()
			markUsed(partition)
			nodeIndex := context.bestNodeIndex()
			if nodeIndex < 0 {
				nodeIndex = context.nodeIndexesByDesire[0]
			}
			partitionToNodeIndex[partition] = nodeIndex
			context.changeDesire(nodeIndex, false)
			partitionToMovementsLeft[partition]--
			altered = true
		}
	}

	// We'll reassign any partition replicas assigned to nodes not marked
	// active (deleted or failed nodes).
	for deletedNodeIndex, deletedNode := range nodes {
		if deletedNode.Active() {
			continue
		}
		for replica := maxReplica; replica >= 0; replica-- {
			partitionToNodeIndex := replicaToPartitionToNodeIndex[replica]
			for partition := maxPartition; partition >= 0; partition-- {
				if partitionToNodeIndex[partition] != int32(deletedNodeIndex) {
					continue
				}
				clearUsed()
				markUsed(partition)
				nodeIndex := context.bestNodeIndex()
				if nodeIndex < 0 {
					nodeIndex = context.nodeIndexesByDesire[0]
				}
				partitionToNodeIndex[partition] = nodeIndex
				context.changeDesire(nodeIndex, false)
				partitionToMovementsLeft[partition]--
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
		if partitionToMovementsLeft[partition] < 1 {
			continue
		}
	DupLoopReplica:
		for replica := maxReplica; replica > 0; replica-- {
			for replicaB := replica - 1; replicaB >= 0; replicaB-- {
				if replicaToPartitionToNodeIndex[replica][partition] == replicaToPartitionToNodeIndex[replicaB][partition] {
					clearUsed()
					markUsed(partition)
					nodeIndex := context.bestNodeIndex()
					if nodeIndex < 0 || nodeIndexToDesire[nodeIndex] < 1 {
						continue
					}
					// No sense reassigning a duplicate to another duplicate.
					for replicaC := maxReplica; replicaC >= 0; replicaC-- {
						if nodeIndex == replicaToPartitionToNodeIndex[replicaC][partition] {
							continue DupLoopReplica
						}
					}
					context.changeDesire(replicaToPartitionToNodeIndex[replica][partition], true)
					replicaToPartitionToNodeIndex[replica][partition] = nodeIndex
					context.changeDesire(nodeIndex, false)
					partitionToMovementsLeft[partition]--
					altered = true
					if partitionToMovementsLeft[partition] < 1 {
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
			if partitionToMovementsLeft[partition] < 1 {
				continue
			}
		DupTierLoopReplica:
			for replica := maxReplica; replica > 0; replica-- {
				for replicaB := replica - 1; replicaB >= 0; replicaB-- {
					if tierToNodeIndexToTierSep[tier][replicaToPartitionToNodeIndex[replica][partition]] == tierToNodeIndexToTierSep[tier][replicaToPartitionToNodeIndex[replicaB][partition]] {
						clearUsed()
						markUsed(partition)
						nodeIndex := context.bestNodeIndex()
						if nodeIndex < 0 || nodeIndexToDesire[nodeIndex] < 1 {
							continue
						}
						// No sense reassigning a duplicate to another
						// duplicate.
						for replicaC := maxReplica; replicaC >= 0; replicaC-- {
							if tierToNodeIndexToTierSep[tier][nodeIndex] == tierToNodeIndexToTierSep[tier][replicaToPartitionToNodeIndex[replicaC][partition]] {
								continue DupTierLoopReplica
							}
						}
						context.changeDesire(replicaToPartitionToNodeIndex[replica][partition], true)
						replicaToPartitionToNodeIndex[replica][partition] = nodeIndex
						context.changeDesire(nodeIndex, false)
						partitionToMovementsLeft[partition]--
						altered = true
						if partitionToMovementsLeft[partition] < 1 {
							continue DupTierLoopPartition
						}
					}
				}
			}
		}
	}

	// TODO: Attempt to reassign replicas within tiers, from innermost tier to
	// outermost, as usually such movements are more efficient for users of the
	// ring (doesn't span switches, for example). Could be done by selecting
	// the most needy node, and then look for overweight nodes in the same tier
	// to steal replicas from.

	// Lastly, we try to reassign replicas from overweight nodes to underweight
	// ones.
	visited := make([]bool, len(nodes))
OverweightLoop:
	for i := len(context.nodeIndexesByDesire) - 1; i >= 0; i-- {
		overweightNodeIndex := context.nodeIndexesByDesire[i]
		if nodeIndexToDesire[overweightNodeIndex] >= 0 {
			break
		}
		if visited[overweightNodeIndex] || !nodes[overweightNodeIndex].Active() {
			continue
		}
		// First pass to reassign to only underweight nodes.
		for replica := maxReplica; replica >= 0; replica-- {
			partitionToNodeIndex := replicaToPartitionToNodeIndex[replica]
			for partition := maxPartition; partition >= 0; partition-- {
				if partitionToMovementsLeft[partition] < 1 || partitionToNodeIndex[partition] != overweightNodeIndex {
					continue
				}
				clearUsed()
				markUsed(partition)
				nodeIndex := context.bestNodeIndex()
				if nodeIndex < 0 || nodeIndexToDesire[nodeIndex] < 1 {
					continue
				}
				context.changeDesire(overweightNodeIndex, true)
				partitionToNodeIndex[partition] = nodeIndex
				context.changeDesire(nodeIndex, false)
				partitionToMovementsLeft[partition]--
				altered = true
				if nodeIndexToDesire[overweightNodeIndex] >= 0 {
					visited[overweightNodeIndex] = true
					i = len(context.nodeIndexesByDesire)
					continue OverweightLoop
				}
			}
		}
		// Second pass to reassign to any node not as overweight.
		for replica := maxReplica; replica >= 0; replica-- {
			partitionToNodeIndex := replicaToPartitionToNodeIndex[replica]
			for partition := maxPartition; partition >= 0; partition-- {
				if partitionToMovementsLeft[partition] < 1 || partitionToNodeIndex[partition] != overweightNodeIndex {
					continue
				}
				clearUsed()
				markUsed(partition)
				nodeIndex := context.bestNodeIndex()
				if nodeIndex < 0 || nodeIndexToDesire[nodeIndex] <= nodeIndexToDesire[overweightNodeIndex] {
					continue
				}
				context.changeDesire(overweightNodeIndex, true)
				partitionToNodeIndex[partition] = nodeIndex
				context.changeDesire(nodeIndex, false)
				partitionToMovementsLeft[partition]--
				altered = true
				if nodeIndexToDesire[overweightNodeIndex] >= 0 {
					visited[overweightNodeIndex] = true
					i = len(context.nodeIndexesByDesire)
					continue OverweightLoop
				}
			}
		}
		visited[overweightNodeIndex] = true
	}
	return altered
}

func (context *rebalancer) bestNodeIndex() int32 {
	bestNodeIndex := int32(-1)
	bestDesire := int32(math.MinInt32)
	nodeIndexToDesire := context.nodeIndexToDesire
	var tierSep *tierSeparation
	var nodeIndex int32
	tierToTierSeps := context.tierToTierSeps
	for tier := context.tierCount - 1; tier >= 0; tier-- {
		// We will go through all tier separations for a tier to get the best
		// node at that tier.
		for _, tierSep = range tierToTierSeps[tier] {
			if !tierSep.used {
				nodeIndex = tierSep.nodeIndexesByDesire[0]
				if bestDesire < nodeIndexToDesire[nodeIndex] {
					bestNodeIndex = nodeIndex
					bestDesire = nodeIndexToDesire[nodeIndex]
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
		if !context.nodeIndexToUsed[nodeIndex] {
			return nodeIndex
		}
	}
	// If we still found no good candidates...
	return -1
}

func (context *rebalancer) changeDesire(nodeIndex int32, increment bool) {
	nodeIndexToDesire := context.nodeIndexToDesire
	nodeIndexesByDesire := context.nodeIndexesByDesire
	prev := 0
	for nodeIndexesByDesire[prev] != nodeIndex {
		prev++
	}
	newDesire := nodeIndexToDesire[nodeIndex]
	if increment {
		newDesire++
	} else {
		newDesire--
	}
	swapWith := 0
	hi := len(nodeIndexesByDesire)
	mid := 0
	if increment {
		for swapWith < hi {
			mid = (swapWith + hi) / 2
			if nodeIndexToDesire[nodeIndexesByDesire[mid]] >= newDesire {
				swapWith = mid + 1
			} else {
				hi = mid
			}
		}
		if swapWith >= len(nodeIndexesByDesire) {
			swapWith--
		}
	} else {
		for swapWith < hi {
			mid = (swapWith + hi) / 2
			if nodeIndexToDesire[nodeIndexesByDesire[mid]] > newDesire {
				swapWith = mid + 1
			} else {
				hi = mid
			}
		}
		if swapWith > 0 {
			swapWith--
		}
	}
	if prev != swapWith {
		nodeIndexesByDesire[prev], nodeIndexesByDesire[swapWith] = nodeIndexesByDesire[swapWith], nodeIndexesByDesire[prev]
	}
	for tier := 0; tier < context.tierCount; tier++ {
		nodeIndexesByDesire = context.tierToNodeIndexToTierSep[tier][nodeIndex].nodeIndexesByDesire
		prev = 0
		for nodeIndexesByDesire[prev] != nodeIndex {
			prev++
		}
		swapWith = 0
		hi = len(nodeIndexesByDesire)
		mid = 0
		if increment {
			for swapWith < hi {
				mid = (swapWith + hi) / 2
				if nodeIndexToDesire[nodeIndexesByDesire[mid]] >= newDesire {
					swapWith = mid + 1
				} else {
					hi = mid
				}
			}
			if swapWith >= len(nodeIndexesByDesire) {
				swapWith--
			}
		} else {
			for swapWith < hi {
				mid = (swapWith + hi) / 2
				if nodeIndexToDesire[nodeIndexesByDesire[mid]] > newDesire {
					swapWith = mid + 1
				} else {
					hi = mid
				}
			}
			if swapWith > 0 {
				swapWith--
			}
		}
		if prev != swapWith {
			nodeIndexesByDesire[prev], nodeIndexesByDesire[swapWith] = nodeIndexesByDesire[swapWith], nodeIndexesByDesire[prev]
		}
	}
	nodeIndexToDesire[nodeIndex] = newDesire
}

type tierSeparation struct {
	values              []int
	nodeIndexesByDesire []int32
	used                bool
}
