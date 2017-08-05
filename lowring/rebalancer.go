package lowring

import (
	"math"
	"sort"
	"time"
)

// rebalancer does the actual work of reassigning replicas to nodes. It is
// separate so that the tracking it uses can just be discarded once complete.
type rebalancer struct {
	builder           *Builder
	moveWaitInUnits   LastMovedType
	nodeIndexToDesire []int32
	// Used only for detecting overweight nodes where nodeIndexToDesire would find none.
	nodeIndexToDesireTenths            []int32
	nodeIndexesByDesire                []NodeIndexType
	tierToTierSepsByDesire             [][]*tierSeparation
	tierToNodeIndexToTierSep           [][]*tierSeparation
	partitionToMovementsLeft           []byte
	nodeIndexToReplicaToPartitionCount [][]int
	// These "used" fields are just temporary tracking of what's being used for
	// the current replica assignment being calculated.
	nodeIndexToUsed                   []bool
	replicaToUsedNodeIndex            []NodeIndexType
	tierToReplicaToUsedTierSeparation [][]*tierSeparation
}

// tierSeparation represents a distinct separation at a specific tier level.
type tierSeparation struct {

	// tierIndexes are the TierIndexes (indexes into some list of tier info
	// external to this package) that uniquely identify this tier separation.
	//
	// For example, say we have 8 nodes, assigned to the following Tier0s and Tier1s:
	//
	//  NodeA Tier0A Tier1A
	//  NodeB Tier0A Tier1A
	//  NodeC Tier0B Tier1A
	//  NodeD Tier0B Tier1A
	//  NodeE Tier0A Tier1B
	//  NodeF Tier0A Tier1B
	//  NodeG Tier0B Tier1B
	//  NodeH Tier0B Tier1B
	//
	// There would be two Tier1 separations representing Tier1A and Tier1B.
	// Each of these Tier1 separations would have one value in tierIndexes, the
	// TierIndex that represents either Tier1A or Tier1B.
	//
	// There would be four Tier0 separations representing:
	//
	//  Tier0A-in-Tier1A
	//  Tier0B-in-Tier1A
	//  Tier0A-in-Tier1B
	//  Tier0B-in-Tier1B
	//
	// Each of these Tier0 separations would have two values in tierIndexes,
	// the TierIndexes that represent either Tier0A or Tier0B at tierIndexes[0]
	// and either Tier1A or Tier1B at tierIndexes[1].
	//
	// Extrapolate this to much deeper tier sets and you can see it does get
	// confusing, but tierIndexes[len(tierIndexes)-1] is always the
	// highest/outermost TierIndex and tierIndexes[0] is always the TierIndex
	// for the tier the tierSeparation is directly within.
	//
	// The only place this confusing field is used is within initTierInfo and
	// it is just used while sifting through the nodes to determine which nodes
	// to group together.
	tierIndexes []int

	// nodeIndexesByDesire are all the nodes within the tierSeparation, sorted by desire.
	nodeIndexesByDesire []NodeIndexType

	// desire is the overall desire for this tierSeparation
	desire int32

	// used is temporarily flagged while assigning replicas of a partition to
	// keep from using the same tierSeparation twice.
	used bool
}

func newRebalancer(builder *Builder) *rebalancer {
	rb := &rebalancer{builder: builder}
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
	rb.initNodeInfo()
	rb.initMovementsLeft()
	rb.initTierInfo()
	rb.replicaToUsedNodeIndex = make([]NodeIndexType, rb.builder.ReplicaCount())
	rb.tierToReplicaToUsedTierSeparation = make([][]*tierSeparation, len(rb.tierToTierSepsByDesire))
	for tier := 0; tier < len(rb.tierToReplicaToUsedTierSeparation); tier++ {
		rb.tierToReplicaToUsedTierSeparation[tier] = make([]*tierSeparation, rb.builder.ReplicaCount())
	}
	return rb
}

func (rb *rebalancer) initNodeInfo() {
	totalCapacity := float64(0)
	rb.nodeIndexToReplicaToPartitionCount = make([][]int, len(rb.builder.Nodes))
	for nodeIndex, node := range rb.builder.Nodes {
		if !node.Disabled {
			totalCapacity += float64(node.Capacity)
		}
		rb.nodeIndexToReplicaToPartitionCount[nodeIndex] = make([]int, rb.builder.ReplicaCount())
	}
	nodeIndexToPartitionCount := make([]int32, len(rb.builder.Nodes))
	for replica, partitionToNodeIndex := range rb.builder.Ring {
		for _, nodeIndex := range partitionToNodeIndex {
			if nodeIndex != NodeIndexNil {
				nodeIndexToPartitionCount[nodeIndex]++
				rb.nodeIndexToReplicaToPartitionCount[nodeIndex][replica]++
			}
		}
	}
	rb.nodeIndexToDesire = make([]int32, len(rb.builder.Nodes))
	rb.nodeIndexToDesireTenths = make([]int32, len(rb.builder.Nodes))
	allPartitionsCount := float64(len(rb.builder.Ring) * len(rb.builder.Ring[0]))
	for nodeIndex, node := range rb.builder.Nodes {
		if node.Disabled {
			rb.nodeIndexToDesire[nodeIndex] = math.MinInt32
			rb.nodeIndexToDesireTenths[nodeIndex] = math.MinInt32
		} else {
			nodeDesire := float64(node.Capacity)/totalCapacity*allPartitionsCount - float64(nodeIndexToPartitionCount[nodeIndex])
			if nodeDesire <= math.MinInt32 {
				rb.nodeIndexToDesire[nodeIndex] = math.MinInt32
			} else if nodeDesire >= math.MaxInt32 {
				rb.nodeIndexToDesire[nodeIndex] = math.MaxInt32
			} else if nodeDesire < 0 {
				rb.nodeIndexToDesire[nodeIndex] = int32(nodeDesire - 0.5)
			} else {
				rb.nodeIndexToDesire[nodeIndex] = int32(nodeDesire + 0.5)
			}
			nodeDesireTenths := nodeDesire * 10
			if nodeDesireTenths <= math.MinInt32 {
				rb.nodeIndexToDesireTenths[nodeIndex] = math.MinInt32
			} else if nodeDesireTenths >= math.MaxInt32 {
				rb.nodeIndexToDesireTenths[nodeIndex] = math.MaxInt32
			} else if nodeDesireTenths < 0 {
				rb.nodeIndexToDesireTenths[nodeIndex] = int32(nodeDesireTenths - 0.5)
			} else {
				rb.nodeIndexToDesireTenths[nodeIndex] = int32(nodeDesireTenths + 0.5)
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
	rb.sortNodeIndexesByDesire(rb.nodeIndexesByDesire)
	rb.nodeIndexToUsed = make([]bool, len(rb.builder.Nodes))
}

func (rb *rebalancer) initMovementsLeft() {
	if rb.builder.MovesPerPartition < 1 {
		rb.builder.MovesPerPartition = rb.builder.ReplicaCount() / 2
		if rb.builder.MovesPerPartition < 1 {
			rb.builder.MovesPerPartition = 1
		}
	}
	movementsPerPartition := byte(rb.builder.MovesPerPartition)
	rb.partitionToMovementsLeft = make([]byte, rb.builder.PartitionCount())
	for partition := 0; partition < rb.builder.PartitionCount(); partition++ {
		rb.partitionToMovementsLeft[partition] = movementsPerPartition
		for replica := 0; replica < rb.builder.ReplicaCount(); replica++ {
			if rb.builder.LastMoved[replica][partition] < rb.moveWaitInUnits {
				if rb.partitionToMovementsLeft[partition] > 0 {
					rb.partitionToMovementsLeft[partition]--
				}
			}
		}
	}
}

func (rb *rebalancer) initTierInfo() {
	tierCount := 0
	for _, n := range rb.builder.Nodes {
		if len(n.TierIndexes) > tierCount {
			tierCount = len(n.TierIndexes)
		}
	}
	rb.tierToNodeIndexToTierSep = make([][]*tierSeparation, tierCount)
	rb.tierToTierSepsByDesire = make([][]*tierSeparation, tierCount)
	for tier := 0; tier < tierCount; tier++ {
		rb.tierToNodeIndexToTierSep[tier] = make([]*tierSeparation, len(rb.builder.Nodes))
		rb.tierToTierSepsByDesire[tier] = make([]*tierSeparation, 0)
	}
	for nodeIndex, node := range rb.builder.Nodes {
		nodeTierIndexes := node.TierIndexes
		for tier := 0; tier < tierCount; tier++ {
			var tierSep *tierSeparation
			for _, candidateTierSep := range rb.tierToTierSepsByDesire[tier] {
				tierSep = candidateTierSep
				for tierIndexIndex := 0; tierIndexIndex < tierCount-tier; tierIndexIndex++ {
					tierIndex := 0
					if tierIndexIndex+tier < len(nodeTierIndexes) {
						tierIndex = nodeTierIndexes[tierIndexIndex+tier]
					}
					if tierSep.tierIndexes[tierIndexIndex] != tierIndex {
						tierSep = nil
						break
					}
				}
				if tierSep != nil {
					break
				}
			}
			if tierSep == nil {
				tierSep = &tierSeparation{
					tierIndexes:         make([]int, tierCount-tier),
					nodeIndexesByDesire: []NodeIndexType{NodeIndexType(nodeIndex)},
				}
				if !node.Disabled {
					tierSep.desire = rb.nodeIndexToDesire[nodeIndex]
				}
				for tierIndexIndex := 0; tierIndexIndex < tierCount-tier; tierIndexIndex++ {
					tierIndex := 0
					if tierIndexIndex+tier < len(nodeTierIndexes) {
						tierIndex = nodeTierIndexes[tierIndexIndex+tier]
					}
					tierSep.tierIndexes[tierIndexIndex] = tierIndex
				}
				rb.tierToTierSepsByDesire[tier] = append(rb.tierToTierSepsByDesire[tier], tierSep)
			} else {
				tierSep.nodeIndexesByDesire = append(tierSep.nodeIndexesByDesire, NodeIndexType(nodeIndex))
				if !node.Disabled {
					check := float64(tierSep.desire) + float64(rb.nodeIndexToDesire[nodeIndex])
					if check <= math.MinInt32 {
						tierSep.desire = math.MinInt32
					} else if check >= math.MaxInt32 {
						tierSep.desire = math.MaxInt32
					} else {
						tierSep.desire = int32(check)
					}
				}
			}
			rb.tierToNodeIndexToTierSep[tier][NodeIndexType(nodeIndex)] = tierSep
		}
	}
	for tier := 0; tier < tierCount; tier++ {
		for _, tierSep := range rb.tierToTierSepsByDesire[tier] {
			rb.sortNodeIndexesByDesire(tierSep.nodeIndexesByDesire)
		}
		rb.sortTierSepsByDesire(rb.tierToTierSepsByDesire[tier])
	}
}

func (rb *rebalancer) clearUsed() {
	for replica := 0; replica < rb.builder.ReplicaCount(); replica++ {
		if rb.replicaToUsedNodeIndex[replica] != NodeIndexNil {
			rb.nodeIndexToUsed[rb.replicaToUsedNodeIndex[replica]] = false
			rb.replicaToUsedNodeIndex[replica] = NodeIndexNil
		}
	}
	for tier := 0; tier < len(rb.tierToReplicaToUsedTierSeparation); tier++ {
		for replica := 0; replica < rb.builder.ReplicaCount(); replica++ {
			if rb.tierToReplicaToUsedTierSeparation[tier][replica] != nil {
				rb.tierToReplicaToUsedTierSeparation[tier][replica].used = false
				rb.tierToReplicaToUsedTierSeparation[tier][replica] = nil
			}
		}
	}
}

func (rb *rebalancer) markUsed(partition int, ignoreNodeIndex NodeIndexType) {
	for replica := 0; replica < rb.builder.ReplicaCount(); replica++ {
		nodeIndex := rb.builder.Ring[replica][partition]
		if nodeIndex == NodeIndexNil || nodeIndex == ignoreNodeIndex {
			continue
		}
		rb.replicaToUsedNodeIndex[replica] = nodeIndex
		rb.nodeIndexToUsed[nodeIndex] = true
		for tier := 0; tier < len(rb.tierToReplicaToUsedTierSeparation); tier++ {
			tierSep := rb.tierToNodeIndexToTierSep[tier][nodeIndex]
			tierSep.used = true
			rb.tierToReplicaToUsedTierSeparation[tier][replica] = tierSep
		}
	}
}

func (rb *rebalancer) bestNodeIndex(replica int) NodeIndexType {
	// Work from highest/outermost tier inward.
	for tier := len(rb.tierToTierSepsByDesire) - 1; tier >= 0; tier-- {
		bestNodeIndex := NodeIndexNil
		bestDesire := int32(math.MinInt32)
		bestReplicaToPartitionCount := rb.builder.PartitionCount()
		// We will go through all tier separations for a tier to get the best
		// node at that tier.
		for _, tierSep := range rb.tierToTierSepsByDesire[tier] {
			if !tierSep.used {
				nodeIndex := tierSep.nodeIndexesByDesire[0]
				replicaToPartitionCount := rb.nodeIndexToReplicaToPartitionCount[nodeIndex][replica]
				// If the node has no partitions at this replica level...
				if replicaToPartitionCount == 0 {
					// And more is desired...
					if tierSep.desire > 0 {
						// Just give it to it instead any other node that
						// already has partitions at this replica level.
						if bestReplicaToPartitionCount > 0 {
							bestNodeIndex = nodeIndex
							bestDesire = tierSep.desire
							bestReplicaToPartitionCount = replicaToPartitionCount
						}
					}
				} else if tierSep.desire >= bestDesire && (replicaToPartitionCount <= bestReplicaToPartitionCount || (replicaToPartitionCount < bestReplicaToPartitionCount && tierSep.desire > 0)) {
					bestNodeIndex = tierSep.nodeIndexesByDesire[0]
					bestDesire = tierSep.desire
					bestReplicaToPartitionCount = replicaToPartitionCount
				}
			}
		}
		// If we found a node at this tier, we don't need to check the lower
		// tiers.
		if bestNodeIndex != NodeIndexNil {
			return bestNodeIndex
		}
	}
	// If we found no good higher tiered candidates (or there are no tiers),
	// we'll have to just take the node with the highest desire that hasn't
	// already been selected.
	bestNodeIndex := NodeIndexNil
	bestReplicaToPartitionCount := rb.builder.PartitionCount()
	for _, nodeIndex := range rb.nodeIndexesByDesire {
		if rb.nodeIndexToUsed[nodeIndex] {
			continue
		}
		replicaToPartitionCount := rb.nodeIndexToReplicaToPartitionCount[nodeIndex][replica]
		if replicaToPartitionCount == 0 {
			bestNodeIndex = nodeIndex
			break
		}
		if replicaToPartitionCount < bestReplicaToPartitionCount {
			bestNodeIndex = nodeIndex
			bestReplicaToPartitionCount = replicaToPartitionCount
		}
	}
	return bestNodeIndex
}

func (rb *rebalancer) changeDesire(nodeIndex NodeIndexType, increment bool) {
	newDesire := rb.nodeIndexToDesire[nodeIndex]
	if increment {
		if newDesire >= math.MaxInt32 {
			return
		}
		newDesire++
	} else {
		if newDesire <= math.MinInt32 {
			return
		}
		newDesire--
	}
	rb.swapOrderOfNodeIndexesByDesire(nodeIndex, newDesire, increment, rb.nodeIndexesByDesire)
	for tier := 0; tier < len(rb.tierToNodeIndexToTierSep); tier++ {
		rb.swapOrderOfNodeIndexesByDesire(nodeIndex, newDesire, increment, rb.tierToNodeIndexToTierSep[tier][nodeIndex].nodeIndexesByDesire)
		newTierSepDesire := rb.tierToNodeIndexToTierSep[tier][nodeIndex].desire
		if increment {
			if newTierSepDesire >= math.MaxInt32 {
				continue
			}
			newTierSepDesire++
		} else {
			if newTierSepDesire <= math.MinInt32 {
				continue
			}
			newTierSepDesire--
		}
		rb.swapOrderOfTierSepsByDesire(rb.tierToNodeIndexToTierSep[tier][nodeIndex], newTierSepDesire, increment, rb.tierToTierSepsByDesire[tier])
	}
	rb.nodeIndexToDesire[nodeIndex] = newDesire
	if increment {
		rb.nodeIndexToDesireTenths[nodeIndex] += 10
		for tier := 0; tier < len(rb.tierToNodeIndexToTierSep); tier++ {
			if rb.tierToNodeIndexToTierSep[tier][nodeIndex].desire < math.MaxInt32 {
				rb.tierToNodeIndexToTierSep[tier][nodeIndex].desire++
			}
		}
	} else {
		rb.nodeIndexToDesireTenths[nodeIndex] -= 10
		for tier := 0; tier < len(rb.tierToNodeIndexToTierSep); tier++ {
			if rb.tierToNodeIndexToTierSep[tier][nodeIndex].desire > math.MinInt32 {
				rb.tierToNodeIndexToTierSep[tier][nodeIndex].desire--
			}
		}
	}
}

func (rb *rebalancer) swapOrderOfNodeIndexesByDesire(nodeIndex NodeIndexType, newDesire int32, increment bool, nodeIndexesByDesire []NodeIndexType) {
	// This doesn't actually make the desired change, but keeps the list sorted
	// as if the change had been made. This is because this method is called
	// multiple times for the various lists the node is in. changeDesire will
	// actually update the desire value once all the lists are updated.
	prev := 0
	for nodeIndexesByDesire[prev] != nodeIndex {
		prev++
	}
	// Bisect to find our node to swap with to remain in order.
	swapWith := 0
	if increment {
		hi := prev
		mid := 0
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
		swapWith = prev + 1
		hi := len(nodeIndexesByDesire)
		mid := 0
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
		if rb.builder.rnd != noRnd {
			// Bisect again to find a random node within the range of our new desire to swap with again.
			if increment {
				swapWithAgain := 0
				hi := prev
				mid := 0
				for swapWithAgain < hi {
					mid = (swapWithAgain + hi) / 2
					if rb.nodeIndexToDesire[nodeIndexesByDesire[mid]] > newDesire {
						swapWithAgain = mid + 1
					} else {
						hi = mid
					}
				}
				if swapWithAgain >= len(nodeIndexesByDesire) {
					swapWithAgain--
				}
				if swapWith-swapWithAgain > 1 {
					swapWithAgain = swapWith - rb.builder.rnd.Intn(swapWith-swapWithAgain)
					nodeIndexesByDesire[swapWith], nodeIndexesByDesire[swapWithAgain] = nodeIndexesByDesire[swapWithAgain], nodeIndexesByDesire[swapWith]
				}
			} else {
				swapWithAgain := swapWith + 1
				if swapWithAgain < len(nodeIndexesByDesire) && rb.nodeIndexToDesire[nodeIndexesByDesire[swapWithAgain]] == newDesire {
					hi := len(nodeIndexesByDesire)
					mid := 0
					for swapWithAgain < hi {
						mid = (swapWithAgain + hi) / 2
						if rb.nodeIndexToDesire[nodeIndexesByDesire[mid]] >= newDesire {
							swapWithAgain = mid + 1
						} else {
							hi = mid
						}
					}
					if swapWithAgain > 0 {
						swapWithAgain--
					}
					if swapWithAgain-swapWith > 1 {
						swapWithAgain = swapWith + rb.builder.rnd.Intn(swapWithAgain-swapWith)
						nodeIndexesByDesire[swapWith], nodeIndexesByDesire[swapWithAgain] = nodeIndexesByDesire[swapWithAgain], nodeIndexesByDesire[swapWith]
					}
				}
			}
		}
	}
}

func (rb *rebalancer) swapOrderOfTierSepsByDesire(tierSep *tierSeparation, newDesire int32, increment bool, tierSepsByDesire []*tierSeparation) {
	// This doesn't actually make the desired change, but keeps the list sorted
	// as if the change had been made. This is to keep it's behavior the same
	// as swapOrderOfNodeIndexesByDesire.
	prev := 0
	for tierSepsByDesire[prev] != tierSep {
		prev++
	}
	// Bisect to find our tierSep to swap with to remain in order.
	swapWith := 0
	if increment {
		hi := prev
		mid := 0
		for swapWith < hi {
			mid = (swapWith + hi) / 2
			if tierSepsByDesire[mid].desire >= newDesire {
				swapWith = mid + 1
			} else {
				hi = mid
			}
		}
		if swapWith >= len(tierSepsByDesire) {
			swapWith--
		}
	} else {
		swapWith = prev + 1
		hi := len(tierSepsByDesire)
		mid := 0
		for swapWith < hi {
			mid = (swapWith + hi) / 2
			if tierSepsByDesire[mid].desire > newDesire {
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
		tierSepsByDesire[prev], tierSepsByDesire[swapWith] = tierSepsByDesire[swapWith], tierSepsByDesire[prev]
		if rb.builder.rnd != noRnd {
			// Bisect again to find a random tierSep within the range of our new desire to swap with again.
			if increment {
				swapWithAgain := 0
				hi := prev
				mid := 0
				for swapWithAgain < hi {
					mid = (swapWithAgain + hi) / 2
					if tierSepsByDesire[mid].desire > newDesire {
						swapWithAgain = mid + 1
					} else {
						hi = mid
					}
				}
				if swapWithAgain >= len(tierSepsByDesire) {
					swapWithAgain--
				}
				if swapWith-swapWithAgain > 1 {
					swapWithAgain = swapWith - rb.builder.rnd.Intn(swapWith-swapWithAgain)
					tierSepsByDesire[swapWith], tierSepsByDesire[swapWithAgain] = tierSepsByDesire[swapWithAgain], tierSepsByDesire[swapWith]
				}
			} else {
				swapWithAgain := swapWith + 1
				if swapWithAgain < len(tierSepsByDesire) && tierSepsByDesire[swapWithAgain].desire == newDesire {
					hi := len(tierSepsByDesire)
					mid := 0
					for swapWithAgain < hi {
						mid = (swapWithAgain + hi) / 2
						if tierSepsByDesire[mid].desire >= newDesire {
							swapWithAgain = mid + 1
						} else {
							hi = mid
						}
					}
					if swapWithAgain > 0 {
						swapWithAgain--
					}
					if swapWithAgain-swapWith > 1 {
						swapWithAgain = swapWith + rb.builder.rnd.Intn(swapWithAgain-swapWith)
						tierSepsByDesire[swapWith], tierSepsByDesire[swapWithAgain] = tierSepsByDesire[swapWithAgain], tierSepsByDesire[swapWith]
					}
				}
			}
		}
	}
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
	for partition := 0; partition < rb.builder.PartitionCount(); partition++ {
		for replica := 0; replica < rb.builder.ReplicaCount(); replica++ {
			if rb.builder.Ring[replica][partition] != NodeIndexNil {
				continue
			}
			rb.clearUsed()
			rb.markUsed(partition, NodeIndexNil)
			nodeIndex := rb.bestNodeIndex(replica)
			if nodeIndex == NodeIndexNil {
				nodeIndex = rb.nodeIndexesByDesire[0]
			}
			rb.builder.Ring[replica][partition] = nodeIndex
			rb.nodeIndexToReplicaToPartitionCount[nodeIndex][replica]++
			rb.changeDesire(nodeIndex, false)
			if rb.partitionToMovementsLeft[partition] > 0 {
				rb.partitionToMovementsLeft[partition]--
			}
			rb.builder.LastMoved[replica][partition] = 0
		}
	}
}

// We'll reassign any partition replicas assigned to nodes marked disabled
// (deleted or failed nodes).
func (rb *rebalancer) reassignDeactivated() {
	for deletedNodeIndex, deletedNode := range rb.builder.Nodes {
		if !deletedNode.Disabled {
			continue
		}
		for partition := 0; partition < rb.builder.PartitionCount(); partition++ {
			for replica := 0; replica < rb.builder.ReplicaCount(); replica++ {
				if rb.builder.Ring[replica][partition] != NodeIndexType(deletedNodeIndex) {
					continue
				}
				rb.clearUsed()
				rb.markUsed(partition, NodeIndexType(deletedNodeIndex))
				nodeIndex := rb.bestNodeIndex(replica)
				if nodeIndex == NodeIndexNil {
					nodeIndex = rb.nodeIndexesByDesire[0]
				}
				rb.nodeIndexToReplicaToPartitionCount[deletedNodeIndex][replica]--
				rb.builder.Ring[replica][partition] = nodeIndex
				rb.nodeIndexToReplicaToPartitionCount[nodeIndex][replica]++
				rb.changeDesire(nodeIndex, false)
				if rb.partitionToMovementsLeft[partition] > 0 {
					rb.partitionToMovementsLeft[partition]--
				}
				rb.builder.LastMoved[replica][partition] = 0
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
DupLoopPartition:
	for partition := 0; partition < rb.builder.PartitionCount(); partition++ {
		if rb.partitionToMovementsLeft[partition] == 0 {
			continue
		}
	DupLoopReplica:
		for replica := 0; replica < rb.builder.ReplicaCount(); replica++ {
			if rb.builder.LastMoved[replica][partition] < rb.moveWaitInUnits {
				continue
			}
			for replicaB := replica - 1; replicaB >= 0; replicaB-- {
				if rb.builder.Ring[replica][partition] == rb.builder.Ring[replicaB][partition] {
					rb.clearUsed()
					rb.markUsed(partition, NodeIndexNil)
					nodeIndex := rb.bestNodeIndex(replica)
					if nodeIndex == NodeIndexNil || rb.nodeIndexToDesire[nodeIndex] < 1 {
						continue
					}
					// No sense reassigning a duplicate to another duplicate.
					for replicaC := 0; replicaC < rb.builder.ReplicaCount(); replicaC++ {
						if nodeIndex == rb.builder.Ring[replicaC][partition] {
							continue DupLoopReplica
						}
					}
					rb.nodeIndexToReplicaToPartitionCount[rb.builder.Ring[replica][partition]][replica]--
					rb.changeDesire(rb.builder.Ring[replica][partition], true)
					rb.builder.Ring[replica][partition] = nodeIndex
					rb.nodeIndexToReplicaToPartitionCount[nodeIndex][replica]++
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

func (rb *rebalancer) reassignSameTierDups() {
	// Work from highest/outermost tier inward.
	for tier := len(rb.tierToNodeIndexToTierSep) - 1; tier >= 0; tier-- {
	DupTierLoopPartition:
		for partition := 0; partition < rb.builder.PartitionCount(); partition++ {
			if rb.partitionToMovementsLeft[partition] == 0 {
				continue
			}
		DupTierLoopReplica:
			for replica := 0; replica < rb.builder.ReplicaCount(); replica++ {
				if rb.builder.LastMoved[replica][partition] < rb.moveWaitInUnits {
					continue
				}
				for replicaB := replica - 1; replicaB >= 0; replicaB-- {
					if rb.tierToNodeIndexToTierSep[tier][rb.builder.Ring[replica][partition]] == rb.tierToNodeIndexToTierSep[tier][rb.builder.Ring[replicaB][partition]] {
						rb.clearUsed()
						rb.markUsed(partition, rb.builder.Ring[replica][partition])
						nodeIndex := rb.bestNodeIndex(replica)
						if nodeIndex == NodeIndexNil || rb.nodeIndexToDesire[nodeIndex] < 1 {
							continue
						}
						// No sense reassigning a duplicate to another
						// duplicate.
						for replicaC := 0; replicaC < rb.builder.ReplicaCount(); replicaC++ {
							if rb.tierToNodeIndexToTierSep[tier][nodeIndex] == rb.tierToNodeIndexToTierSep[tier][rb.builder.Ring[replicaC][partition]] {
								continue DupTierLoopReplica
							}
						}
						rb.nodeIndexToReplicaToPartitionCount[rb.builder.Ring[replica][partition]][replica]--
						rb.changeDesire(rb.builder.Ring[replica][partition], true)
						rb.builder.Ring[replica][partition] = nodeIndex
						rb.nodeIndexToReplicaToPartitionCount[nodeIndex][replica]++
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
		if rb.nodeIndexToDesireTenths[overweightNodeIndex] >= 0 {
			break
		}
		if visited[overweightNodeIndex] {
			continue
		}
		// First pass to reassign to only underweight nodes.
		for partition := 0; partition < rb.builder.PartitionCount(); partition++ {
			for replica := 0; replica < rb.builder.ReplicaCount(); replica++ {
				if rb.builder.Ring[replica][partition] != overweightNodeIndex || rb.partitionToMovementsLeft[partition] == 0 || rb.builder.LastMoved[replica][partition] < rb.moveWaitInUnits {
					continue
				}
				rb.clearUsed()
				rb.markUsed(partition, overweightNodeIndex)
				nodeIndex := rb.bestNodeIndex(replica)
				if nodeIndex == NodeIndexNil || rb.nodeIndexToDesire[nodeIndex] <= 0 {
					continue
				}
				rb.nodeIndexToReplicaToPartitionCount[overweightNodeIndex][replica]++
				rb.changeDesire(overweightNodeIndex, true)
				rb.builder.Ring[replica][partition] = nodeIndex
				rb.nodeIndexToReplicaToPartitionCount[nodeIndex][replica]++
				rb.changeDesire(nodeIndex, false)
				rb.partitionToMovementsLeft[partition]--
				rb.builder.LastMoved[replica][partition] = 0
				if rb.nodeIndexToDesireTenths[overweightNodeIndex] >= 0 {
					visited[overweightNodeIndex] = true
					i = len(rb.nodeIndexesByDesire)
					continue OverweightLoop
				}
			}
		}
		// Second pass to reassign to any node not as overweight.
		for partition := 0; partition < rb.builder.PartitionCount(); partition++ {
			for replica := 0; replica < rb.builder.ReplicaCount(); replica++ {
				if rb.builder.Ring[replica][partition] != overweightNodeIndex || rb.partitionToMovementsLeft[partition] == 0 || rb.builder.LastMoved[replica][partition] < rb.moveWaitInUnits {
					continue
				}
				rb.clearUsed()
				rb.markUsed(partition, overweightNodeIndex)
				nodeIndex := rb.bestNodeIndex(replica)
				if nodeIndex == NodeIndexNil || rb.nodeIndexToDesire[nodeIndex] <= rb.nodeIndexToDesire[overweightNodeIndex] {
					continue
				}
				rb.nodeIndexToReplicaToPartitionCount[overweightNodeIndex][replica]++
				rb.changeDesire(overweightNodeIndex, true)
				rb.builder.Ring[replica][partition] = nodeIndex
				rb.nodeIndexToReplicaToPartitionCount[nodeIndex][replica]++
				rb.changeDesire(nodeIndex, false)
				rb.partitionToMovementsLeft[partition]--
				rb.builder.LastMoved[replica][partition] = 0
				if rb.nodeIndexToDesireTenths[overweightNodeIndex] >= 0 {
					visited[overweightNodeIndex] = true
					i = len(rb.nodeIndexesByDesire)
					continue OverweightLoop
				}
			}
		}
		visited[overweightNodeIndex] = true
	}
}

func (rb *rebalancer) sortNodeIndexesByDesire(nodeIndexesByDesire []NodeIndexType) {
	sort.Sort(&nodeIndexByDesireSorter{
		nodeIndexes:       nodeIndexesByDesire,
		nodeIndexToDesire: rb.nodeIndexToDesire,
	})
	if rb.builder.rnd != noRnd {
		// Shuffle each range of same desire.
		lookDesire := int32(math.MaxInt32)
		for {
			iFirst := -1
			var iLast int
			var nodeIndex NodeIndexType
			for iLast, nodeIndex = range nodeIndexesByDesire {
				if rb.nodeIndexToDesire[nodeIndex] == lookDesire && iFirst == -1 {
					iFirst = iLast
				} else if rb.nodeIndexToDesire[nodeIndex] < lookDesire {
					if iFirst == -1 {
						lookDesire = rb.nodeIndexToDesire[nodeIndex]
						iFirst = iLast
					} else {
						iLast--
						break
					}
				}
			}
			if iFirst == -1 {
				break
			}
			if iLast > iFirst {
				n := iLast - iFirst + 1
				for i := 1; i < n; i++ {
					j := rb.builder.rnd.Intn(i + 1)
					nodeIndexesByDesire[iFirst+i], nodeIndexesByDesire[iFirst+j] = nodeIndexesByDesire[iFirst+j], nodeIndexesByDesire[iFirst+i]
				}
			}
			if lookDesire == math.MinInt32 {
				break
			}
			lookDesire--
		}
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

func (rb *rebalancer) sortTierSepsByDesire(tierSepsByDesire []*tierSeparation) {
	sort.Sort(&tierSepByDesireSorter{tierSeps: tierSepsByDesire})
	if rb.builder.rnd != noRnd {
		// Shuffle each range of same desire.
		lookDesire := int32(math.MaxInt32)
		for {
			iFirst := -1
			var iLast int
			var tierSep *tierSeparation
			for iLast, tierSep = range tierSepsByDesire {
				if tierSep.desire == lookDesire && iFirst == -1 {
					iFirst = iLast
				} else if tierSep.desire < lookDesire {
					if iFirst == -1 {
						lookDesire = tierSep.desire
						iFirst = iLast
					} else {
						iLast--
						break
					}
				}
			}
			if iFirst == -1 {
				break
			}
			if iLast > iFirst {
				n := iLast - iFirst + 1
				for i := 1; i < n; i++ {
					j := rb.builder.rnd.Intn(i + 1)
					tierSepsByDesire[iFirst+i], tierSepsByDesire[iFirst+j] = tierSepsByDesire[iFirst+j], tierSepsByDesire[iFirst+i]
				}
			}
			if lookDesire == math.MinInt32 {
				break
			}
			lookDesire--
		}
	}
}

type tierSepByDesireSorter struct {
	tierSeps []*tierSeparation
}

func (sorter *tierSepByDesireSorter) Len() int {
	return len(sorter.tierSeps)
}

func (sorter *tierSepByDesireSorter) Swap(x int, y int) {
	sorter.tierSeps[x], sorter.tierSeps[y] = sorter.tierSeps[y], sorter.tierSeps[x]
}

func (sorter *tierSepByDesireSorter) Less(x int, y int) bool {
	return sorter.tierSeps[x].desire > sorter.tierSeps[y].desire
}
