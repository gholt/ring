package ring

import (
	"math"
	"sort"
	"time"
)

// rebalancer does the actual work of reassigning replicas to nodes. It is
// separate so that the tracking it uses can just be discarded once complete.
type rebalancer struct {
	builder                  *Builder
	moveWaitInUnits          LastMovedType
	maxReplica               int
	maxPartition             int
	maxTier                  int
	nodeIndexToDesire        []int32
	nodeIndexesByDesire      []NodeIndexType
	nodeIndexToUsed          []bool
	tierToTierSeps           [][]*tierSeparation
	tierToNodeIndexToTierSep [][]*tierSeparation
	partitionToMovementsLeft []byte
	usedNodeIndexes          []NodeIndexType
	tierToUsedTierSeps       [][]*tierSeparation
}

type tierSeparation struct {
	values              []int
	nodeIndexesByDesire []NodeIndexType
	used                bool
}

func newRebalancer(builder *Builder) *rebalancer {
	rb := &rebalancer{
		builder:      builder,
		maxReplica:   len(builder.Ring) - 1,
		maxPartition: len(builder.Ring[0]) - 1,
	}
	if rb.builder.LastMovedUnit == 0 {
		rb.builder.LastMovedUnit = time.Minute
	}
	if rb.builder.MoveWait == 0 {
		rb.builder.MoveWait = time.Hour
	}
	rb.moveWaitInUnits = LastMovedMax
	if int64(builder.MoveWait/builder.LastMovedUnit) < int64(LastMovedMax) {
		rb.moveWaitInUnits = LastMovedType(builder.MoveWait / builder.LastMovedUnit)
	}
	rb.initMaxTier()
	rb.initNodeDesires()
	rb.initTierInfo()
	rb.initMovementsLeft()
	rb.usedNodeIndexes = make([]NodeIndexType, rb.maxReplica+1)
	rb.tierToUsedTierSeps = make([][]*tierSeparation, rb.maxTier+1)
	for tier := rb.maxTier; tier >= 0; tier-- {
		rb.tierToUsedTierSeps[tier] = make([]*tierSeparation, rb.maxReplica+1)
	}
	return rb
}

func (rb *rebalancer) initMaxTier() {
	rb.maxTier = 0
	for _, n := range rb.builder.Nodes {
		if len(n.TierIndexes) > rb.maxTier {
			rb.maxTier = len(n.TierIndexes)
		}
	}
}

func (rb *rebalancer) initNodeDesires() {
	totalCapacity := float64(0)
	for _, node := range rb.builder.Nodes {
		if !node.Disabled {
			totalCapacity += float64(node.Capacity)
		}
	}
	nodeIndexToPartitionCount := make([]int32, len(rb.builder.Nodes))
	for _, partitionToNodeIndex := range rb.builder.Ring {
		for _, nodeIndex := range partitionToNodeIndex {
			if nodeIndex != NodeIndexNil {
				nodeIndexToPartitionCount[nodeIndex]++
			}
		}
	}
	rb.nodeIndexToDesire = make([]int32, len(rb.builder.Nodes))
	allPartitionsCount := float64(len(rb.builder.Ring) * len(rb.builder.Ring[0]))
	for nodeIndex, node := range rb.builder.Nodes {
		if node.Disabled {
			rb.nodeIndexToDesire[nodeIndex] = math.MinInt32
		} else {
			nodeDesire := float64(node.Capacity)/totalCapacity*allPartitionsCount - float64(nodeIndexToPartitionCount[nodeIndex])
			if nodeDesire < math.MinInt32 {
				rb.nodeIndexToDesire[nodeIndex] = math.MinInt32
			} else if nodeDesire > math.MaxInt32 {
				rb.nodeIndexToDesire[nodeIndex] = math.MaxInt32
			} else if nodeDesire < 0 {
				rb.nodeIndexToDesire[nodeIndex] = int32(nodeDesire - 0.5)
			} else {
				rb.nodeIndexToDesire[nodeIndex] = int32(nodeDesire + 0.5)
			}
		}
	}
	rb.nodeIndexesByDesire = make([]NodeIndexType, len(rb.builder.Nodes))
	i := NodeIndexType(len(rb.builder.Nodes) - 1)
	for {
		rb.nodeIndexesByDesire[i] = i
		if i == 0 {
			break
		}
		i--
	}
	sort.Sort(&nodeIndexByDesireSorter{
		nodeIndexes:       rb.nodeIndexesByDesire,
		nodeIndexToDesire: rb.nodeIndexToDesire,
	})
	rb.nodeIndexToUsed = make([]bool, len(rb.builder.Nodes))
}

func (rb *rebalancer) initMovementsLeft() {
	if rb.builder.MovesPerPartition < 1 {
		rb.builder.MovesPerPartition = rb.maxReplica / 2
		if rb.builder.MovesPerPartition < 1 {
			rb.builder.MovesPerPartition = 1
		}
	}
	movementsPerPartition := byte(rb.builder.MovesPerPartition)
	rb.partitionToMovementsLeft = make([]byte, rb.maxPartition+1)
	for partition := rb.maxPartition; partition >= 0; partition-- {
		rb.partitionToMovementsLeft[partition] = movementsPerPartition
		for replica := rb.maxReplica; replica >= 0; replica-- {
			if rb.builder.LastMoved[replica][partition] < rb.moveWaitInUnits {
				if rb.partitionToMovementsLeft[partition] > 0 {
					rb.partitionToMovementsLeft[partition]--
				}
			}
		}
	}
}

func (rb *rebalancer) initTierInfo() {
	rb.tierToNodeIndexToTierSep = make([][]*tierSeparation, rb.maxTier+1)
	rb.tierToTierSeps = make([][]*tierSeparation, rb.maxTier+1)
	for tier := rb.maxTier; tier >= 0; tier-- {
		rb.tierToNodeIndexToTierSep[tier] = make([]*tierSeparation, len(rb.builder.Nodes))
		rb.tierToTierSeps[tier] = make([]*tierSeparation, 0)
	}
	for nodeIndex, node := range rb.builder.Nodes {
		nodeTierIndexes := node.TierIndexes
		for tier := 0; tier <= rb.maxTier; tier++ {
			var tierSep *tierSeparation
			for _, candidateTierSep := range rb.tierToTierSeps[tier] {
				tierSep = candidateTierSep
				for valueIndex := 0; valueIndex <= rb.maxTier-tier; valueIndex++ {
					value := 0
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
				tierSep = &tierSeparation{values: make([]int, rb.maxTier-tier+1), nodeIndexesByDesire: []NodeIndexType{NodeIndexType(nodeIndex)}}
				for valueIndex := 0; valueIndex <= rb.maxTier-tier; valueIndex++ {
					value := 0
					if valueIndex+tier < len(nodeTierIndexes) {
						value = nodeTierIndexes[valueIndex+tier]
					}
					tierSep.values[valueIndex] = value
				}
				rb.tierToTierSeps[tier] = append(rb.tierToTierSeps[tier], tierSep)
			} else {
				tierSep.nodeIndexesByDesire = append(tierSep.nodeIndexesByDesire, NodeIndexType(nodeIndex))
			}
			rb.tierToNodeIndexToTierSep[tier][NodeIndexType(nodeIndex)] = tierSep
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
		if rb.usedNodeIndexes[replica] != NodeIndexNil {
			rb.nodeIndexToUsed[rb.usedNodeIndexes[replica]] = false
			rb.usedNodeIndexes[replica] = NodeIndexNil
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
		nodeIndex := rb.builder.Ring[replica][partition]
		if nodeIndex == NodeIndexNil {
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

func (rb *rebalancer) bestNodeIndex() NodeIndexType {
	bestNodeIndex := NodeIndexNil
	bestDesire := int32(math.MinInt32)
	var tierSep *tierSeparation
	var nodeIndex NodeIndexType
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
		if bestNodeIndex != NodeIndexNil {
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
	return NodeIndexNil
}

func (rb *rebalancer) changeDesire(nodeIndex NodeIndexType, increment bool) {
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

func (rb *rebalancer) rebalance() {
	rb.assignUnassigned()
	rb.reassignDeactivated()
	rb.reassignSameNodeDups()
	rb.reassignSameTierDups()
	rb.reassignOverweight()
}

// Assign any partitions assigned to NodeIndexNil (happens with new ring and
// can happen with a node removed with the Remove() method).
func (rb *rebalancer) assignUnassigned() {
	loop := 1
	bits := int64(0)
	if rb.builder.rnd != noRnd {
		loop = 2
	}
	for ; loop > 0; loop-- {
		for replica := rb.maxReplica; replica >= 0; replica-- {
			partitionToNodeIndex := rb.builder.Ring[replica]
			for partition := rb.maxPartition; partition >= 0; partition-- {
				if partitionToNodeIndex[partition] != NodeIndexNil {
					continue
				}
				if loop == 2 {
					bits >>= 1
					if bits == 0 {
						bits = rb.builder.rnd.Int63()
					}
					if bits&1 == 0 {
						continue
					}
				}
				rb.clearUsed()
				rb.markUsed(partition)
				nodeIndex := rb.bestNodeIndex()
				if nodeIndex == NodeIndexNil {
					nodeIndex = rb.nodeIndexesByDesire[0]
				}
				partitionToNodeIndex[partition] = nodeIndex
				rb.changeDesire(nodeIndex, false)
				if rb.partitionToMovementsLeft[partition] > 0 {
					rb.partitionToMovementsLeft[partition]--
				}
				rb.builder.LastMoved[replica][partition] = 0
			}
		}
	}
}

// We'll reassign any partition replicas assigned to nodes marked disabled
// (deleted or failed nodes).
func (rb *rebalancer) reassignDeactivated() {
	loop := 1
	bits := int64(0)
	if rb.builder.rnd != noRnd {
		loop = 2
	}
	for ; loop > 0; loop-- {
		for deletedNodeIndex, deletedNode := range rb.builder.Nodes {
			if !deletedNode.Disabled {
				continue
			}
			for replica := rb.maxReplica; replica >= 0; replica-- {
				partitionToNodeIndex := rb.builder.Ring[replica]
				for partition := rb.maxPartition; partition >= 0; partition-- {
					if partitionToNodeIndex[partition] != NodeIndexType(deletedNodeIndex) {
						continue
					}
					if loop == 2 {
						bits >>= 1
						if bits == 0 {
							bits = rb.builder.rnd.Int63()
						}
						if bits&1 == 0 {
							continue
						}
					}
					rb.clearUsed()
					rb.markUsed(partition)
					nodeIndex := rb.bestNodeIndex()
					if nodeIndex == NodeIndexNil {
						nodeIndex = rb.nodeIndexesByDesire[0]
					}
					partitionToNodeIndex[partition] = nodeIndex
					rb.changeDesire(nodeIndex, false)
					if rb.partitionToMovementsLeft[partition] > 0 {
						rb.partitionToMovementsLeft[partition]--
					}
					rb.builder.LastMoved[replica][partition] = 0
				}
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
func (rb *rebalancer) reassignSameNodeDups() {
	loop := 1
	bits := int64(0)
	if rb.builder.rnd != noRnd {
		loop = 2
	}
	for ; loop > 0; loop-- {
	DupLoopPartition:
		for partition := rb.maxPartition; partition >= 0; partition-- {
			if rb.partitionToMovementsLeft[partition] == 0 {
				continue
			}
		DupLoopReplica:
			for replica := rb.maxReplica; replica > 0; replica-- {
				if rb.builder.LastMoved[replica][partition] < rb.moveWaitInUnits {
					continue
				}
				for replicaB := replica - 1; replicaB >= 0; replicaB-- {
					if rb.builder.Ring[replica][partition] == rb.builder.Ring[replicaB][partition] {
						if loop == 2 {
							bits >>= 1
							if bits == 0 {
								bits = rb.builder.rnd.Int63()
							}
							if bits&1 == 0 {
								continue
							}
						}
						rb.clearUsed()
						rb.markUsed(partition)
						nodeIndex := rb.bestNodeIndex()
						if nodeIndex == NodeIndexNil || rb.nodeIndexToDesire[nodeIndex] < 1 {
							continue
						}
						// No sense reassigning a duplicate to another duplicate.
						for replicaC := rb.maxReplica; replicaC >= 0; replicaC-- {
							if nodeIndex == rb.builder.Ring[replicaC][partition] {
								continue DupLoopReplica
							}
						}
						rb.changeDesire(rb.builder.Ring[replica][partition], true)
						rb.builder.Ring[replica][partition] = nodeIndex
						rb.changeDesire(nodeIndex, false)
						rb.partitionToMovementsLeft[partition]--
						rb.builder.LastMoved[replica][partition] = 0
						if rb.partitionToMovementsLeft[partition] == 0 {
							continue DupLoopPartition
						}
					}
				}
			}
		}
	}
}

func (rb *rebalancer) reassignSameTierDups() {
	loop := 1
	bits := int64(0)
	if rb.builder.rnd != noRnd {
		loop = 2
	}
	for ; loop > 0; loop-- {
		for tier := rb.maxTier; tier >= 0; tier-- {
		DupTierLoopPartition:
			for partition := rb.maxPartition; partition >= 0; partition-- {
				if rb.partitionToMovementsLeft[partition] == 0 {
					continue
				}
			DupTierLoopReplica:
				for replica := rb.maxReplica; replica > 0; replica-- {
					if rb.builder.LastMoved[replica][partition] < rb.moveWaitInUnits {
						continue
					}
					for replicaB := replica - 1; replicaB >= 0; replicaB-- {
						if rb.tierToNodeIndexToTierSep[tier][rb.builder.Ring[replica][partition]] == rb.tierToNodeIndexToTierSep[tier][rb.builder.Ring[replicaB][partition]] {
							if loop == 2 {
								bits >>= 1
								if bits == 0 {
									bits = rb.builder.rnd.Int63()
								}
								if bits&1 == 0 {
									continue
								}
							}
							rb.clearUsed()
							rb.markUsed(partition)
							nodeIndex := rb.bestNodeIndex()
							if nodeIndex == NodeIndexNil || rb.nodeIndexToDesire[nodeIndex] < 1 {
								continue
							}
							// No sense reassigning a duplicate to another
							// duplicate.
							for replicaC := rb.maxReplica; replicaC >= 0; replicaC-- {
								if rb.tierToNodeIndexToTierSep[tier][nodeIndex] == rb.tierToNodeIndexToTierSep[tier][rb.builder.Ring[replicaC][partition]] {
									continue DupTierLoopReplica
								}
							}
							rb.changeDesire(rb.builder.Ring[replica][partition], true)
							rb.builder.Ring[replica][partition] = nodeIndex
							rb.changeDesire(nodeIndex, false)
							rb.partitionToMovementsLeft[partition]--
							rb.builder.LastMoved[replica][partition] = 0
							if rb.partitionToMovementsLeft[partition] == 0 {
								continue DupTierLoopPartition
							}
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
func (rb *rebalancer) reassignOverweight() {
	visited := make([]bool, len(rb.builder.Nodes))
OverweightLoop:
	for i := len(rb.nodeIndexesByDesire) - 1; i >= 0; i-- {
		overweightNodeIndex := rb.nodeIndexesByDesire[i]
		if rb.nodeIndexToDesire[overweightNodeIndex] >= 0 {
			break
		}
		if visited[overweightNodeIndex] || rb.builder.Nodes[overweightNodeIndex].Disabled {
			continue
		}
		// First pass to reassign to only underweight nodes.
		for replica := rb.maxReplica; replica >= 0; replica-- {
			partitionToNodeIndex := rb.builder.Ring[replica]
			for partition := rb.maxPartition; partition >= 0; partition-- {
				if partitionToNodeIndex[partition] != overweightNodeIndex || rb.partitionToMovementsLeft[partition] == 0 || rb.builder.LastMoved[replica][partition] < rb.moveWaitInUnits {
					continue
				}
				rb.clearUsed()
				rb.markUsed(partition)
				nodeIndex := rb.bestNodeIndex()
				if nodeIndex == NodeIndexNil || rb.nodeIndexToDesire[nodeIndex] < 1 {
					continue
				}
				rb.changeDesire(overweightNodeIndex, true)
				partitionToNodeIndex[partition] = nodeIndex
				rb.changeDesire(nodeIndex, false)
				rb.partitionToMovementsLeft[partition]--
				rb.builder.LastMoved[replica][partition] = 0
				if rb.nodeIndexToDesire[overweightNodeIndex] >= 0 {
					visited[overweightNodeIndex] = true
					i = len(rb.nodeIndexesByDesire)
					continue OverweightLoop
				}
			}
		}
		// Second pass to reassign to any node not as overweight.
		for replica := rb.maxReplica; replica >= 0; replica-- {
			partitionToNodeIndex := rb.builder.Ring[replica]
			for partition := rb.maxPartition; partition >= 0; partition-- {
				if partitionToNodeIndex[partition] != overweightNodeIndex || rb.partitionToMovementsLeft[partition] == 0 || rb.builder.LastMoved[replica][partition] < rb.moveWaitInUnits {
					continue
				}
				rb.clearUsed()
				rb.markUsed(partition)
				nodeIndex := rb.bestNodeIndex()
				if nodeIndex == NodeIndexNil || rb.nodeIndexToDesire[nodeIndex] <= rb.nodeIndexToDesire[overweightNodeIndex] {
					continue
				}
				rb.changeDesire(overweightNodeIndex, true)
				partitionToNodeIndex[partition] = nodeIndex
				rb.changeDesire(nodeIndex, false)
				rb.partitionToMovementsLeft[partition]--
				rb.builder.LastMoved[replica][partition] = 0
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

type nodeIndexByDesireSorter struct {
	nodeIndexes       []NodeIndexType
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
