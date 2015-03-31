package ring

import (
	"math"
	"sort"
)

type rebalancer struct {
	builder                  *Builder
	maxReplica               int
	maxPartition             int
	maxTier                  int
	nodeIndexToDesire        []int32
	nodeIndexesByDesire      []int32
	nodeIndexToUsed          []bool
	tierToTierSeps           [][]*tierSeparation
	tierToNodeIndexToTierSep [][]*tierSeparation
	partitionToMovementsLeft []byte
	altered                  bool
	usedNodeIndexes          []int32
	tierToUsedTierSeps       [][]*tierSeparation
}

type tierSeparation struct {
	values              []int32
	nodeIndexesByDesire []int32
	used                bool
}

func newRebalancer(builder *Builder) *rebalancer {
	context := &rebalancer{
		builder:      builder,
		maxReplica:   len(builder.replicaToPartitionToNodeIndex) - 1,
		maxPartition: len(builder.replicaToPartitionToNodeIndex[0]) - 1,
	}
	context.initMaxTier()
	context.initNodeDesires()
	context.initTierInfo()
	context.initMovementsLeft()
	context.usedNodeIndexes = make([]int32, context.maxReplica+1)
	context.tierToUsedTierSeps = make([][]*tierSeparation, context.maxTier+1)
	for tier := context.maxTier; tier >= 0; tier-- {
		context.tierToUsedTierSeps[tier] = make([]*tierSeparation, context.maxReplica+1)
	}
	return context
}

func (context *rebalancer) initMaxTier() {
	context.maxTier = len(context.builder.tiers)
}

func (context *rebalancer) initNodeDesires() {
	totalCapacity := float64(0)
	for _, node := range context.builder.nodes {
		if !node.inactive {
			totalCapacity += (float64)(node.capacity)
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
	allPartitionsCount := float64(len(context.builder.replicaToPartitionToNodeIndex) * len(context.builder.replicaToPartitionToNodeIndex[0]))
	for nodeIndex, node := range context.builder.nodes {
		if node.inactive {
			context.nodeIndexToDesire[nodeIndex] = math.MinInt32
		} else {
			context.nodeIndexToDesire[nodeIndex] = int32(float64(node.capacity)/totalCapacity*allPartitionsCount+0.5) - nodeIndexToPartitionCount[nodeIndex]
		}
	}
	context.nodeIndexesByDesire = make([]int32, len(context.builder.nodes))
	for i := int32(len(context.builder.nodes) - 1); i >= 0; i-- {
		context.nodeIndexesByDesire[i] = i
	}
	sort.Sort(&nodeIndexByDesireSorter{
		nodeIndexes:       context.nodeIndexesByDesire,
		nodeIndexToDesire: context.nodeIndexToDesire,
	})
	context.nodeIndexToUsed = make([]bool, len(context.builder.nodes))
}

func (context *rebalancer) initMovementsLeft() {
	movementsPerPartition := byte(context.maxReplica / 2)
	if movementsPerPartition < 1 {
		movementsPerPartition = 1
	}
	context.partitionToMovementsLeft = make([]byte, context.maxPartition+1)
	for partition := context.maxPartition; partition >= 0; partition-- {
		context.partitionToMovementsLeft[partition] = movementsPerPartition
		for replica := context.maxReplica; replica >= 0; replica-- {
			if context.builder.replicaToPartitionToLastMove[replica][partition] < context.builder.moveWait {
				context.partitionToMovementsLeft[partition]--
			}
		}
	}
}

func (context *rebalancer) initTierInfo() {
	context.tierToNodeIndexToTierSep = make([][]*tierSeparation, context.maxTier+1)
	context.tierToTierSeps = make([][]*tierSeparation, context.maxTier+1)
	for tier := context.maxTier; tier >= 0; tier-- {
		context.tierToNodeIndexToTierSep[tier] = make([]*tierSeparation, len(context.builder.nodes))
		context.tierToTierSeps[tier] = make([]*tierSeparation, 0)
	}
	for nodeIndex, node := range context.builder.nodes {
		nodeTierIndexes := node.tierIndexes
		for tier := 0; tier <= context.maxTier; tier++ {
			var tierSep *tierSeparation
			for _, candidateTierSep := range context.tierToTierSeps[tier] {
				tierSep = candidateTierSep
				for valueIndex := 0; valueIndex <= context.maxTier-tier; valueIndex++ {
					value := int32(0)
					if valueIndex+tier < len(nodeTierIndexes) {
						value = nodeTierIndexes[valueIndex+tier]
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
				tierSep = &tierSeparation{values: make([]int32, context.maxTier-tier+1), nodeIndexesByDesire: []int32{int32(nodeIndex)}}
				for valueIndex := 0; valueIndex <= context.maxTier-tier; valueIndex++ {
					value := int32(0)
					if valueIndex+tier < len(nodeTierIndexes) {
						value = nodeTierIndexes[valueIndex+tier]
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
	for tier := context.maxTier; tier >= 0; tier-- {
		for _, tierSep := range context.tierToTierSeps[tier] {
			sort.Sort(&nodeIndexByDesireSorter{
				nodeIndexes:       tierSep.nodeIndexesByDesire,
				nodeIndexToDesire: context.nodeIndexToDesire,
			})
		}
	}
}

func (context *rebalancer) clearUsed() {
	for replica := context.maxReplica; replica >= 0; replica-- {
		if context.usedNodeIndexes[replica] != -1 {
			context.nodeIndexToUsed[context.usedNodeIndexes[replica]] = false
			context.usedNodeIndexes[replica] = -1
		}
	}
	for tier := context.maxTier; tier >= 0; tier-- {
		for replica := context.maxReplica; replica >= 0; replica-- {
			if context.tierToUsedTierSeps[tier][replica] != nil {
				context.tierToUsedTierSeps[tier][replica].used = false
			}
			context.tierToUsedTierSeps[tier][replica] = nil
		}
	}
}

func (context *rebalancer) markUsed(partition int) {
	for replica := context.maxReplica; replica >= 0; replica-- {
		nodeIndex := context.builder.replicaToPartitionToNodeIndex[replica][partition]
		if nodeIndex < 0 {
			continue
		}
		context.usedNodeIndexes[replica] = nodeIndex
		context.nodeIndexToUsed[nodeIndex] = true
		for tier := context.maxTier; tier >= 0; tier-- {
			tierSep := context.tierToNodeIndexToTierSep[tier][nodeIndex]
			tierSep.used = true
			context.tierToUsedTierSeps[tier][replica] = tierSep
		}
	}
}

func (context *rebalancer) bestNodeIndex() int32 {
	bestNodeIndex := int32(-1)
	bestDesire := int32(math.MinInt32)
	var tierSep *tierSeparation
	var nodeIndex int32
	tierToTierSeps := context.tierToTierSeps
	for tier := context.maxTier; tier >= 0; tier-- {
		// We will go through all tier separations for a tier to get the best
		// node at that tier.
		for _, tierSep = range tierToTierSeps[tier] {
			if !tierSep.used {
				nodeIndex = tierSep.nodeIndexesByDesire[0]
				if bestDesire < context.nodeIndexToDesire[nodeIndex] {
					bestNodeIndex = nodeIndex
					bestDesire = context.nodeIndexToDesire[nodeIndex]
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
	nodeIndexesByDesire := context.nodeIndexesByDesire
	prev := 0
	for nodeIndexesByDesire[prev] != nodeIndex {
		prev++
	}
	newDesire := context.nodeIndexToDesire[nodeIndex]
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
			if context.nodeIndexToDesire[nodeIndexesByDesire[mid]] >= newDesire {
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
			if context.nodeIndexToDesire[nodeIndexesByDesire[mid]] > newDesire {
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
	for tier := 0; tier <= context.maxTier; tier++ {
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
				if context.nodeIndexToDesire[nodeIndexesByDesire[mid]] >= newDesire {
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
				if context.nodeIndexToDesire[nodeIndexesByDesire[mid]] > newDesire {
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
	context.nodeIndexToDesire[nodeIndex] = newDesire
}

func (context *rebalancer) rebalance() bool {
	context.assignUnassigned()
	context.reassignDeactivated()
	context.reassignedSameNodeDups()
	context.reassignedSameTierDups()
	context.reassignOverweighted()
	return context.altered
}

// Assign any partitions assigned as -1 (happens with new ring and can happen
// with a node removed with the Remove() method).
func (context *rebalancer) assignUnassigned() {
	for replica := context.maxReplica; replica >= 0; replica-- {
		partitionToNodeIndex := context.builder.replicaToPartitionToNodeIndex[replica]
		for partition := context.maxPartition; partition >= 0; partition-- {
			if partitionToNodeIndex[partition] >= 0 {
				continue
			}
			context.clearUsed()
			context.markUsed(partition)
			nodeIndex := context.bestNodeIndex()
			if nodeIndex < 0 {
				nodeIndex = context.nodeIndexesByDesire[0]
			}
			partitionToNodeIndex[partition] = nodeIndex
			context.changeDesire(nodeIndex, false)
			context.partitionToMovementsLeft[partition]--
			context.builder.replicaToPartitionToLastMove[replica][partition] = 0
			context.altered = true
		}
	}
}

// We'll reassign any partition replicas assigned to nodes marked inactive
// (deleted or failed nodes).
func (context *rebalancer) reassignDeactivated() {
	for deletedNodeIndex, deletedNode := range context.builder.nodes {
		if !deletedNode.inactive {
			continue
		}
		for replica := context.maxReplica; replica >= 0; replica-- {
			partitionToNodeIndex := context.builder.replicaToPartitionToNodeIndex[replica]
			for partition := context.maxPartition; partition >= 0; partition-- {
				if partitionToNodeIndex[partition] != int32(deletedNodeIndex) {
					continue
				}
				context.clearUsed()
				context.markUsed(partition)
				nodeIndex := context.bestNodeIndex()
				if nodeIndex < 0 {
					nodeIndex = context.nodeIndexesByDesire[0]
				}
				partitionToNodeIndex[partition] = nodeIndex
				context.changeDesire(nodeIndex, false)
				context.partitionToMovementsLeft[partition]--
				context.builder.replicaToPartitionToLastMove[replica][partition] = 0
				context.altered = true
			}
		}
	}
}

// Look for replicas assigned to the same node more than once. This shouldn't
// be a common use case; but if it turns out to be, it might be worthwhile to
// reassign the worst duplicates first. For example, a partition with only 1
// distinct replica node would be fixed before others. Another example, a
// partition that has two duplicate nodes but one has more replicas than the
// other, it would be fixed first.
func (context *rebalancer) reassignedSameNodeDups() {
DupLoopPartition:
	for partition := context.maxPartition; partition >= 0; partition-- {
		if context.partitionToMovementsLeft[partition] < 1 {
			continue
		}
	DupLoopReplica:
		for replica := context.maxReplica; replica > 0; replica-- {
			if context.builder.replicaToPartitionToLastMove[replica][partition] < context.builder.moveWait {
				continue
			}
			for replicaB := replica - 1; replicaB >= 0; replicaB-- {
				if context.builder.replicaToPartitionToNodeIndex[replica][partition] == context.builder.replicaToPartitionToNodeIndex[replicaB][partition] {
					context.clearUsed()
					context.markUsed(partition)
					nodeIndex := context.bestNodeIndex()
					if nodeIndex < 0 || context.nodeIndexToDesire[nodeIndex] < 1 {
						continue
					}
					// No sense reassigning a duplicate to another duplicate.
					for replicaC := context.maxReplica; replicaC >= 0; replicaC-- {
						if nodeIndex == context.builder.replicaToPartitionToNodeIndex[replicaC][partition] {
							continue DupLoopReplica
						}
					}
					context.changeDesire(context.builder.replicaToPartitionToNodeIndex[replica][partition], true)
					context.builder.replicaToPartitionToNodeIndex[replica][partition] = nodeIndex
					context.changeDesire(nodeIndex, false)
					context.partitionToMovementsLeft[partition]--
					context.builder.replicaToPartitionToLastMove[replica][partition] = 0
					context.altered = true
					if context.partitionToMovementsLeft[partition] < 1 {
						continue DupLoopPartition
					}
				}
			}
		}
	}
}

func (context *rebalancer) reassignedSameTierDups() {
	for tier := context.maxTier; tier >= 0; tier-- {
	DupTierLoopPartition:
		for partition := context.maxPartition; partition >= 0; partition-- {
			if context.partitionToMovementsLeft[partition] < 1 {
				continue
			}
		DupTierLoopReplica:
			for replica := context.maxReplica; replica > 0; replica-- {
				if context.builder.replicaToPartitionToLastMove[replica][partition] < context.builder.moveWait {
					continue
				}
				for replicaB := replica - 1; replicaB >= 0; replicaB-- {
					if context.tierToNodeIndexToTierSep[tier][context.builder.replicaToPartitionToNodeIndex[replica][partition]] == context.tierToNodeIndexToTierSep[tier][context.builder.replicaToPartitionToNodeIndex[replicaB][partition]] {
						context.clearUsed()
						context.markUsed(partition)
						nodeIndex := context.bestNodeIndex()
						if nodeIndex < 0 || context.nodeIndexToDesire[nodeIndex] < 1 {
							continue
						}
						// No sense reassigning a duplicate to another
						// duplicate.
						for replicaC := context.maxReplica; replicaC >= 0; replicaC-- {
							if context.tierToNodeIndexToTierSep[tier][nodeIndex] == context.tierToNodeIndexToTierSep[tier][context.builder.replicaToPartitionToNodeIndex[replicaC][partition]] {
								continue DupTierLoopReplica
							}
						}
						context.changeDesire(context.builder.replicaToPartitionToNodeIndex[replica][partition], true)
						context.builder.replicaToPartitionToNodeIndex[replica][partition] = nodeIndex
						context.changeDesire(nodeIndex, false)
						context.partitionToMovementsLeft[partition]--
						context.builder.replicaToPartitionToLastMove[replica][partition] = 0
						context.altered = true
						if context.partitionToMovementsLeft[partition] < 1 {
							continue DupTierLoopPartition
						}
					}
				}
			}
		}
	}
}

// TODO: Attempt to reassign replicas within tiers, from innermost tier to
// outermost, as usually such movements are more efficient for users of the
// ring (doesn't span switches, for example). Could be done by selecting the
// most needy node, and then look for overweight nodes in the same tier to
// steal replicas from.

// Try to reassign replicas from overweight nodes to underweight ones.
func (context *rebalancer) reassignOverweighted() {
	visited := make([]bool, len(context.builder.nodes))
OverweightLoop:
	for i := len(context.nodeIndexesByDesire) - 1; i >= 0; i-- {
		overweightNodeIndex := context.nodeIndexesByDesire[i]
		if context.nodeIndexToDesire[overweightNodeIndex] >= 0 {
			break
		}
		if visited[overweightNodeIndex] || context.builder.nodes[overweightNodeIndex].inactive {
			continue
		}
		// First pass to reassign to only underweight nodes.
		for replica := context.maxReplica; replica >= 0; replica-- {
			partitionToNodeIndex := context.builder.replicaToPartitionToNodeIndex[replica]
			for partition := context.maxPartition; partition >= 0; partition-- {
				if partitionToNodeIndex[partition] != overweightNodeIndex || context.partitionToMovementsLeft[partition] < 1 || context.builder.replicaToPartitionToLastMove[replica][partition] < context.builder.moveWait {
					continue
				}
				context.clearUsed()
				context.markUsed(partition)
				nodeIndex := context.bestNodeIndex()
				if nodeIndex < 0 || context.nodeIndexToDesire[nodeIndex] < 1 {
					continue
				}
				context.changeDesire(overweightNodeIndex, true)
				partitionToNodeIndex[partition] = nodeIndex
				context.changeDesire(nodeIndex, false)
				context.partitionToMovementsLeft[partition]--
				context.builder.replicaToPartitionToLastMove[replica][partition] = 0
				context.altered = true
				if context.nodeIndexToDesire[overweightNodeIndex] >= 0 {
					visited[overweightNodeIndex] = true
					i = len(context.nodeIndexesByDesire)
					continue OverweightLoop
				}
			}
		}
		// Second pass to reassign to any node not as overweight.
		for replica := context.maxReplica; replica >= 0; replica-- {
			partitionToNodeIndex := context.builder.replicaToPartitionToNodeIndex[replica]
			for partition := context.maxPartition; partition >= 0; partition-- {
				if partitionToNodeIndex[partition] != overweightNodeIndex || context.partitionToMovementsLeft[partition] < 1 || context.builder.replicaToPartitionToLastMove[replica][partition] < context.builder.moveWait {
					continue
				}
				context.clearUsed()
				context.markUsed(partition)
				nodeIndex := context.bestNodeIndex()
				if nodeIndex < 0 || context.nodeIndexToDesire[nodeIndex] <= context.nodeIndexToDesire[overweightNodeIndex] {
					continue
				}
				context.changeDesire(overweightNodeIndex, true)
				partitionToNodeIndex[partition] = nodeIndex
				context.changeDesire(nodeIndex, false)
				context.partitionToMovementsLeft[partition]--
				context.builder.replicaToPartitionToLastMove[replica][partition] = 0
				context.altered = true
				if context.nodeIndexToDesire[overweightNodeIndex] >= 0 {
					visited[overweightNodeIndex] = true
					i = len(context.nodeIndexesByDesire)
					continue OverweightLoop
				}
			}
		}
		visited[overweightNodeIndex] = true
	}
}
