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
	rb := &rebalancer{
		builder:      builder,
		maxReplica:   len(builder.replicaToPartitionToNodeIndex) - 1,
		maxPartition: len(builder.replicaToPartitionToNodeIndex[0]) - 1,
	}
	rb.initMaxTier()
	rb.initNodeDesires()
	rb.initTierInfo()
	rb.initMovementsLeft()
	rb.usedNodeIndexes = make([]int32, rb.maxReplica+1)
	rb.tierToUsedTierSeps = make([][]*tierSeparation, rb.maxTier+1)
	for tier := rb.maxTier; tier >= 0; tier-- {
		rb.tierToUsedTierSeps[tier] = make([]*tierSeparation, rb.maxReplica+1)
	}
	return rb
}

func (rb *rebalancer) initMaxTier() {
	rb.maxTier = len(rb.builder.tiers)
}

func (rb *rebalancer) initNodeDesires() {
	totalCapacity := float64(0)
	for _, node := range rb.builder.nodes {
		if !node.inactive {
			totalCapacity += (float64)(node.capacity)
		}
	}
	nodeIndexToPartitionCount := make([]int32, len(rb.builder.nodes))
	for _, partitionToNodeIndex := range rb.builder.replicaToPartitionToNodeIndex {
		for _, nodeIndex := range partitionToNodeIndex {
			if nodeIndex >= 0 {
				nodeIndexToPartitionCount[nodeIndex]++
			}
		}
	}
	rb.nodeIndexToDesire = make([]int32, len(rb.builder.nodes))
	allPartitionsCount := float64(len(rb.builder.replicaToPartitionToNodeIndex) * len(rb.builder.replicaToPartitionToNodeIndex[0]))
	for nodeIndex, node := range rb.builder.nodes {
		if node.inactive {
			rb.nodeIndexToDesire[nodeIndex] = math.MinInt32
		} else {
			rb.nodeIndexToDesire[nodeIndex] = int32(float64(node.capacity)/totalCapacity*allPartitionsCount+0.5) - nodeIndexToPartitionCount[nodeIndex]
		}
	}
	rb.nodeIndexesByDesire = make([]int32, len(rb.builder.nodes))
	for i := int32(len(rb.builder.nodes) - 1); i >= 0; i-- {
		rb.nodeIndexesByDesire[i] = i
	}
	sort.Sort(&nodeIndexByDesireSorter{
		nodeIndexes:       rb.nodeIndexesByDesire,
		nodeIndexToDesire: rb.nodeIndexToDesire,
	})
	rb.nodeIndexToUsed = make([]bool, len(rb.builder.nodes))
}

func (rb *rebalancer) initMovementsLeft() {
	movementsPerPartition := byte(rb.maxReplica / 2)
	if movementsPerPartition < 1 {
		movementsPerPartition = 1
	}
	rb.partitionToMovementsLeft = make([]byte, rb.maxPartition+1)
	for partition := rb.maxPartition; partition >= 0; partition-- {
		rb.partitionToMovementsLeft[partition] = movementsPerPartition
		for replica := rb.maxReplica; replica >= 0; replica-- {
			if rb.builder.replicaToPartitionToLastMove[replica][partition] < rb.builder.moveWait {
				rb.partitionToMovementsLeft[partition]--
			}
		}
	}
}

func (rb *rebalancer) initTierInfo() {
	rb.tierToNodeIndexToTierSep = make([][]*tierSeparation, rb.maxTier+1)
	rb.tierToTierSeps = make([][]*tierSeparation, rb.maxTier+1)
	for tier := rb.maxTier; tier >= 0; tier-- {
		rb.tierToNodeIndexToTierSep[tier] = make([]*tierSeparation, len(rb.builder.nodes))
		rb.tierToTierSeps[tier] = make([]*tierSeparation, 0)
	}
	for nodeIndex, node := range rb.builder.nodes {
		nodeTierIndexes := node.tierIndexes
		for tier := 0; tier <= rb.maxTier; tier++ {
			var tierSep *tierSeparation
			for _, candidateTierSep := range rb.tierToTierSeps[tier] {
				tierSep = candidateTierSep
				for valueIndex := 0; valueIndex <= rb.maxTier-tier; valueIndex++ {
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
				tierSep = &tierSeparation{values: make([]int32, rb.maxTier-tier+1), nodeIndexesByDesire: []int32{int32(nodeIndex)}}
				for valueIndex := 0; valueIndex <= rb.maxTier-tier; valueIndex++ {
					value := int32(0)
					if valueIndex+tier < len(nodeTierIndexes) {
						value = nodeTierIndexes[valueIndex+tier]
					}
					tierSep.values[valueIndex] = value
				}
				rb.tierToTierSeps[tier] = append(rb.tierToTierSeps[tier], tierSep)
			} else {
				tierSep.nodeIndexesByDesire = append(tierSep.nodeIndexesByDesire, int32(nodeIndex))
			}
			rb.tierToNodeIndexToTierSep[tier][int32(nodeIndex)] = tierSep
		}
	}
	for tier := rb.maxTier; tier >= 0; tier-- {
		for _, tierSep := range rb.tierToTierSeps[tier] {
			sort.Sort(&nodeIndexByDesireSorter{
				nodeIndexes:       tierSep.nodeIndexesByDesire,
				nodeIndexToDesire: rb.nodeIndexToDesire,
			})
		}
	}
}

func (rb *rebalancer) clearUsed() {
	for replica := rb.maxReplica; replica >= 0; replica-- {
		if rb.usedNodeIndexes[replica] != -1 {
			rb.nodeIndexToUsed[rb.usedNodeIndexes[replica]] = false
			rb.usedNodeIndexes[replica] = -1
		}
	}
	for tier := rb.maxTier; tier >= 0; tier-- {
		for replica := rb.maxReplica; replica >= 0; replica-- {
			if rb.tierToUsedTierSeps[tier][replica] != nil {
				rb.tierToUsedTierSeps[tier][replica].used = false
			}
			rb.tierToUsedTierSeps[tier][replica] = nil
		}
	}
}

func (rb *rebalancer) markUsed(partition int) {
	for replica := rb.maxReplica; replica >= 0; replica-- {
		nodeIndex := rb.builder.replicaToPartitionToNodeIndex[replica][partition]
		if nodeIndex < 0 {
			continue
		}
		rb.usedNodeIndexes[replica] = nodeIndex
		rb.nodeIndexToUsed[nodeIndex] = true
		for tier := rb.maxTier; tier >= 0; tier-- {
			tierSep := rb.tierToNodeIndexToTierSep[tier][nodeIndex]
			tierSep.used = true
			rb.tierToUsedTierSeps[tier][replica] = tierSep
		}
	}
}

func (rb *rebalancer) bestNodeIndex() int32 {
	bestNodeIndex := int32(-1)
	bestDesire := int32(math.MinInt32)
	var tierSep *tierSeparation
	var nodeIndex int32
	tierToTierSeps := rb.tierToTierSeps
	for tier := rb.maxTier; tier >= 0; tier-- {
		// We will go through all tier separations for a tier to get the best
		// node at that tier.
		for _, tierSep = range tierToTierSeps[tier] {
			if !tierSep.used {
				nodeIndex = tierSep.nodeIndexesByDesire[0]
				if bestDesire < rb.nodeIndexToDesire[nodeIndex] {
					bestNodeIndex = nodeIndex
					bestDesire = rb.nodeIndexToDesire[nodeIndex]
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
	for _, nodeIndex := range rb.nodeIndexesByDesire {
		if !rb.nodeIndexToUsed[nodeIndex] {
			return nodeIndex
		}
	}
	// If we still found no good candidates...
	return -1
}

func (rb *rebalancer) changeDesire(nodeIndex int32, increment bool) {
	nodeIndexesByDesire := rb.nodeIndexesByDesire
	prev := 0
	for nodeIndexesByDesire[prev] != nodeIndex {
		prev++
	}
	newDesire := rb.nodeIndexToDesire[nodeIndex]
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
			if rb.nodeIndexToDesire[nodeIndexesByDesire[mid]] >= newDesire {
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
			if rb.nodeIndexToDesire[nodeIndexesByDesire[mid]] > newDesire {
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
	for tier := 0; tier <= rb.maxTier; tier++ {
		nodeIndexesByDesire = rb.tierToNodeIndexToTierSep[tier][nodeIndex].nodeIndexesByDesire
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
				if rb.nodeIndexToDesire[nodeIndexesByDesire[mid]] >= newDesire {
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
				if rb.nodeIndexToDesire[nodeIndexesByDesire[mid]] > newDesire {
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
	rb.nodeIndexToDesire[nodeIndex] = newDesire
}

func (rb *rebalancer) rebalance() bool {
	rb.assignUnassigned()
	rb.reassignDeactivated()
	rb.reassignedSameNodeDups()
	rb.reassignedSameTierDups()
	rb.reassignOverweighted()
	return rb.altered
}

// Assign any partitions assigned as -1 (happens with new ring and can happen
// with a node removed with the Remove() method).
func (rb *rebalancer) assignUnassigned() {
	for replica := rb.maxReplica; replica >= 0; replica-- {
		partitionToNodeIndex := rb.builder.replicaToPartitionToNodeIndex[replica]
		for partition := rb.maxPartition; partition >= 0; partition-- {
			if partitionToNodeIndex[partition] >= 0 {
				continue
			}
			rb.clearUsed()
			rb.markUsed(partition)
			nodeIndex := rb.bestNodeIndex()
			if nodeIndex < 0 {
				nodeIndex = rb.nodeIndexesByDesire[0]
			}
			partitionToNodeIndex[partition] = nodeIndex
			rb.changeDesire(nodeIndex, false)
			rb.partitionToMovementsLeft[partition]--
			rb.builder.replicaToPartitionToLastMove[replica][partition] = 0
			rb.altered = true
		}
	}
}

// We'll reassign any partition replicas assigned to nodes marked inactive
// (deleted or failed nodes).
func (rb *rebalancer) reassignDeactivated() {
	for deletedNodeIndex, deletedNode := range rb.builder.nodes {
		if !deletedNode.inactive {
			continue
		}
		for replica := rb.maxReplica; replica >= 0; replica-- {
			partitionToNodeIndex := rb.builder.replicaToPartitionToNodeIndex[replica]
			for partition := rb.maxPartition; partition >= 0; partition-- {
				if partitionToNodeIndex[partition] != int32(deletedNodeIndex) {
					continue
				}
				rb.clearUsed()
				rb.markUsed(partition)
				nodeIndex := rb.bestNodeIndex()
				if nodeIndex < 0 {
					nodeIndex = rb.nodeIndexesByDesire[0]
				}
				partitionToNodeIndex[partition] = nodeIndex
				rb.changeDesire(nodeIndex, false)
				rb.partitionToMovementsLeft[partition]--
				rb.builder.replicaToPartitionToLastMove[replica][partition] = 0
				rb.altered = true
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
func (rb *rebalancer) reassignedSameNodeDups() {
DupLoopPartition:
	for partition := rb.maxPartition; partition >= 0; partition-- {
		if rb.partitionToMovementsLeft[partition] < 1 {
			continue
		}
	DupLoopReplica:
		for replica := rb.maxReplica; replica > 0; replica-- {
			if rb.builder.replicaToPartitionToLastMove[replica][partition] < rb.builder.moveWait {
				continue
			}
			for replicaB := replica - 1; replicaB >= 0; replicaB-- {
				if rb.builder.replicaToPartitionToNodeIndex[replica][partition] == rb.builder.replicaToPartitionToNodeIndex[replicaB][partition] {
					rb.clearUsed()
					rb.markUsed(partition)
					nodeIndex := rb.bestNodeIndex()
					if nodeIndex < 0 || rb.nodeIndexToDesire[nodeIndex] < 1 {
						continue
					}
					// No sense reassigning a duplicate to another duplicate.
					for replicaC := rb.maxReplica; replicaC >= 0; replicaC-- {
						if nodeIndex == rb.builder.replicaToPartitionToNodeIndex[replicaC][partition] {
							continue DupLoopReplica
						}
					}
					rb.changeDesire(rb.builder.replicaToPartitionToNodeIndex[replica][partition], true)
					rb.builder.replicaToPartitionToNodeIndex[replica][partition] = nodeIndex
					rb.changeDesire(nodeIndex, false)
					rb.partitionToMovementsLeft[partition]--
					rb.builder.replicaToPartitionToLastMove[replica][partition] = 0
					rb.altered = true
					if rb.partitionToMovementsLeft[partition] < 1 {
						continue DupLoopPartition
					}
				}
			}
		}
	}
}

func (rb *rebalancer) reassignedSameTierDups() {
	for tier := rb.maxTier; tier >= 0; tier-- {
	DupTierLoopPartition:
		for partition := rb.maxPartition; partition >= 0; partition-- {
			if rb.partitionToMovementsLeft[partition] < 1 {
				continue
			}
		DupTierLoopReplica:
			for replica := rb.maxReplica; replica > 0; replica-- {
				if rb.builder.replicaToPartitionToLastMove[replica][partition] < rb.builder.moveWait {
					continue
				}
				for replicaB := replica - 1; replicaB >= 0; replicaB-- {
					if rb.tierToNodeIndexToTierSep[tier][rb.builder.replicaToPartitionToNodeIndex[replica][partition]] == rb.tierToNodeIndexToTierSep[tier][rb.builder.replicaToPartitionToNodeIndex[replicaB][partition]] {
						rb.clearUsed()
						rb.markUsed(partition)
						nodeIndex := rb.bestNodeIndex()
						if nodeIndex < 0 || rb.nodeIndexToDesire[nodeIndex] < 1 {
							continue
						}
						// No sense reassigning a duplicate to another
						// duplicate.
						for replicaC := rb.maxReplica; replicaC >= 0; replicaC-- {
							if rb.tierToNodeIndexToTierSep[tier][nodeIndex] == rb.tierToNodeIndexToTierSep[tier][rb.builder.replicaToPartitionToNodeIndex[replicaC][partition]] {
								continue DupTierLoopReplica
							}
						}
						rb.changeDesire(rb.builder.replicaToPartitionToNodeIndex[replica][partition], true)
						rb.builder.replicaToPartitionToNodeIndex[replica][partition] = nodeIndex
						rb.changeDesire(nodeIndex, false)
						rb.partitionToMovementsLeft[partition]--
						rb.builder.replicaToPartitionToLastMove[replica][partition] = 0
						rb.altered = true
						if rb.partitionToMovementsLeft[partition] < 1 {
							continue DupTierLoopPartition
						}
					}
				}
			}
		}
	}
}

// Consider: Attempt to reassign replicas within tiers, from innermost tier to
// outermost, as usually such movements are more efficient for users of the
// ring (doesn't span switches, for example). Could be done by selecting the
// most needy node, and then look for overweight nodes in the same tier to
// steal replicas from.

// Try to reassign replicas from overweight nodes to underweight ones.
func (rb *rebalancer) reassignOverweighted() {
	visited := make([]bool, len(rb.builder.nodes))
OverweightLoop:
	for i := len(rb.nodeIndexesByDesire) - 1; i >= 0; i-- {
		overweightNodeIndex := rb.nodeIndexesByDesire[i]
		if rb.nodeIndexToDesire[overweightNodeIndex] >= 0 {
			break
		}
		if visited[overweightNodeIndex] || rb.builder.nodes[overweightNodeIndex].inactive {
			continue
		}
		// First pass to reassign to only underweight nodes.
		for replica := rb.maxReplica; replica >= 0; replica-- {
			partitionToNodeIndex := rb.builder.replicaToPartitionToNodeIndex[replica]
			for partition := rb.maxPartition; partition >= 0; partition-- {
				if partitionToNodeIndex[partition] != overweightNodeIndex || rb.partitionToMovementsLeft[partition] < 1 || rb.builder.replicaToPartitionToLastMove[replica][partition] < rb.builder.moveWait {
					continue
				}
				rb.clearUsed()
				rb.markUsed(partition)
				nodeIndex := rb.bestNodeIndex()
				if nodeIndex < 0 || rb.nodeIndexToDesire[nodeIndex] < 1 {
					continue
				}
				rb.changeDesire(overweightNodeIndex, true)
				partitionToNodeIndex[partition] = nodeIndex
				rb.changeDesire(nodeIndex, false)
				rb.partitionToMovementsLeft[partition]--
				rb.builder.replicaToPartitionToLastMove[replica][partition] = 0
				rb.altered = true
				if rb.nodeIndexToDesire[overweightNodeIndex] >= 0 {
					visited[overweightNodeIndex] = true
					i = len(rb.nodeIndexesByDesire)
					continue OverweightLoop
				}
			}
		}
		// Second pass to reassign to any node not as overweight.
		for replica := rb.maxReplica; replica >= 0; replica-- {
			partitionToNodeIndex := rb.builder.replicaToPartitionToNodeIndex[replica]
			for partition := rb.maxPartition; partition >= 0; partition-- {
				if partitionToNodeIndex[partition] != overweightNodeIndex || rb.partitionToMovementsLeft[partition] < 1 || rb.builder.replicaToPartitionToLastMove[replica][partition] < rb.builder.moveWait {
					continue
				}
				rb.clearUsed()
				rb.markUsed(partition)
				nodeIndex := rb.bestNodeIndex()
				if nodeIndex < 0 || rb.nodeIndexToDesire[nodeIndex] <= rb.nodeIndexToDesire[overweightNodeIndex] {
					continue
				}
				rb.changeDesire(overweightNodeIndex, true)
				partitionToNodeIndex[partition] = nodeIndex
				rb.changeDesire(nodeIndex, false)
				rb.partitionToMovementsLeft[partition]--
				rb.builder.replicaToPartitionToLastMove[replica][partition] = 0
				rb.altered = true
				if rb.nodeIndexToDesire[overweightNodeIndex] >= 0 {
					visited[overweightNodeIndex] = true
					i = len(rb.nodeIndexesByDesire)
					continue OverweightLoop
				}
			}
		}
		visited[overweightNodeIndex] = true
	}
}
