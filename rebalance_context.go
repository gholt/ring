package ring

import (
	"sort"
)

type rebalanceContextImpl struct {
	ring                            *ringImpl
	first                           bool
	itemIndex2DesiredPartitionCount []int32
	itemIndexesByDesire             []int32
	itemIndex2Used                  []bool
	tierCount                       int
	tier2TierIDs                    [][]*tierIDImpl
	tier2ItemIndex2TierID           [][]*tierIDImpl
}

func newRebalanceContext(ring *ringImpl) *rebalanceContextImpl {
	rebalanceContext := &rebalanceContextImpl{ring: ring}
	rebalanceContext.initTierCount()
	rebalanceContext.initItemIndex2DesiredPartitionCount()
	rebalanceContext.initTier2ItemIndex2TierID()
	return rebalanceContext
}

func (rebalanceContext *rebalanceContextImpl) initTierCount() {
	rebalanceContext.tierCount = 0
	for _, item := range rebalanceContext.ring.items {
		if !item.Active() {
			continue
		}
		itemTierCount := len(item.TierValues())
		if itemTierCount > rebalanceContext.tierCount {
			rebalanceContext.tierCount = itemTierCount
		}
	}
}

func (rebalanceContext *rebalanceContextImpl) initItemIndex2DesiredPartitionCount() {
	totalCapacity := uint64(0)
	for _, item := range rebalanceContext.ring.items {
		if item.Active() {
			totalCapacity += item.Capacity()
		}
	}
	itemIndex2PartitionCount := make([]int32, len(rebalanceContext.ring.items))
	rebalanceContext.first = true
	for _, partition2ItemIndex := range rebalanceContext.ring.replica2Partition2ItemIndex {
		for _, itemIndex := range partition2ItemIndex {
			if itemIndex >= 0 {
				itemIndex2PartitionCount[itemIndex]++
				rebalanceContext.first = false
			}
		}
	}
	rebalanceContext.itemIndex2DesiredPartitionCount = make([]int32, len(rebalanceContext.ring.items))
	allPartitionsCount := len(rebalanceContext.ring.replica2Partition2ItemIndex) * len(rebalanceContext.ring.replica2Partition2ItemIndex[0])
	for itemIndex, item := range rebalanceContext.ring.items {
		if item.Active() {
			rebalanceContext.itemIndex2DesiredPartitionCount[itemIndex] = int32(float64(item.Capacity())/float64(totalCapacity)*float64(allPartitionsCount)+0.5) - itemIndex2PartitionCount[itemIndex]
		} else {
			rebalanceContext.itemIndex2DesiredPartitionCount[itemIndex] = -2147483648
		}
	}
	rebalanceContext.itemIndexesByDesire = make([]int32, 0, len(rebalanceContext.ring.items))
	for itemIndex, item := range rebalanceContext.ring.items {
		if item.Active() {
			rebalanceContext.itemIndexesByDesire = append(rebalanceContext.itemIndexesByDesire, int32(itemIndex))
		}
	}
	sort.Sort(&itemIndexByDesireSorterImpl{
		itemIndexesByDesire:             rebalanceContext.itemIndexesByDesire,
		itemIndex2DesiredPartitionCount: rebalanceContext.itemIndex2DesiredPartitionCount,
	})
}

func (rebalanceContext *rebalanceContextImpl) initTier2ItemIndex2TierID() {
	rebalanceContext.tier2ItemIndex2TierID = make([][]*tierIDImpl, rebalanceContext.tierCount)
	rebalanceContext.tier2TierIDs = make([][]*tierIDImpl, rebalanceContext.tierCount)
	for tier := 1; tier < rebalanceContext.tierCount; tier++ {
		rebalanceContext.tier2ItemIndex2TierID[tier] = make([]*tierIDImpl, len(rebalanceContext.ring.items))
		rebalanceContext.tier2TierIDs[tier] = make([]*tierIDImpl, 0)
	}
	for itemIndex, item := range rebalanceContext.ring.items {
		itemTierValues := item.TierValues()
		for tier := 1; tier < rebalanceContext.tierCount; tier++ {
			var tierID *tierIDImpl
			for _, candidateTierID := range rebalanceContext.tier2TierIDs[tier] {
				tierID = candidateTierID
				for valueIndex := 0; valueIndex < rebalanceContext.tierCount-tier; valueIndex++ {
					value := 0
					if valueIndex+tier < len(itemTierValues) {
						value = itemTierValues[valueIndex+tier]
					}
					if tierID.values[valueIndex] != value {
						tierID = nil
						break
					}
				}
				if tierID != nil {
					break
				}
			}
			if tierID == nil {
				tierID = &tierIDImpl{values: make([]int, rebalanceContext.tierCount-tier), itemIndexesByDesire: []int32{int32(itemIndex)}}
				for valueIndex := 0; valueIndex < rebalanceContext.tierCount-tier; valueIndex++ {
					value := 0
					if valueIndex+tier < len(itemTierValues) {
						value = itemTierValues[valueIndex+tier]
					}
					tierID.values[valueIndex] = value
				}
				rebalanceContext.tier2TierIDs[tier] = append(rebalanceContext.tier2TierIDs[tier], tierID)
			} else {
				tierID.itemIndexesByDesire = append(tierID.itemIndexesByDesire, int32(itemIndex))
			}
			rebalanceContext.tier2ItemIndex2TierID[tier][int32(itemIndex)] = tierID
		}
	}
	for tier := 1; tier < rebalanceContext.tierCount; tier++ {
		for _, tierID := range rebalanceContext.tier2TierIDs[tier] {
			sort.Sort(&itemIndexByDesireSorterImpl{
				itemIndexesByDesire:             tierID.itemIndexesByDesire,
				itemIndex2DesiredPartitionCount: rebalanceContext.itemIndex2DesiredPartitionCount,
			})
		}
	}
}

func (rebalanceContext *rebalanceContextImpl) rebalance() {
	if rebalanceContext.first {
		rebalanceContext.firstRebalance()
	} else {
		rebalanceContext.subsequentRebalance()
	}
}

// firstRebalance is much simpler than what we have to do to rebalance existing
// assignments. Here, we just assign each partition in order, giving each
// replica of that partition to the next most-desired item, keeping in mind
// tier separation preferences.
func (rebalanceContext *rebalanceContextImpl) firstRebalance() {
	replicaCount := len(rebalanceContext.ring.replica2Partition2ItemIndex)
	partitionCount := len(rebalanceContext.ring.replica2Partition2ItemIndex[0])
	// We track the other items and tiers we've assigned partition replicas to
	// so that we can try to avoid assigning further replicas to similar items.
	otherItemIndexes := make([]int32, replicaCount)
	rebalanceContext.itemIndex2Used = make([]bool, len(rebalanceContext.ring.items))
	tier2OtherTierIDs := make([][]*tierIDImpl, rebalanceContext.tierCount)
	for tier := 1; tier < rebalanceContext.tierCount; tier++ {
		tier2OtherTierIDs[tier] = make([]*tierIDImpl, replicaCount)
	}
	for partition := 0; partition < partitionCount; partition++ {
		for replica := 0; replica < replicaCount; replica++ {
			if otherItemIndexes[replica] != -1 {
				rebalanceContext.itemIndex2Used[otherItemIndexes[replica]] = false
			}
			otherItemIndexes[replica] = -1
		}
		for tier := 1; tier < rebalanceContext.tierCount; tier++ {
			for replica := 0; replica < replicaCount; replica++ {
				if tier2OtherTierIDs[tier][replica] != nil {
					tier2OtherTierIDs[tier][replica].used = false
				}
				tier2OtherTierIDs[tier][replica] = nil
			}
		}
		for replica := 0; replica < replicaCount; replica++ {
			itemIndex := rebalanceContext.bestItemIndex()
			rebalanceContext.ring.replica2Partition2ItemIndex[replica][partition] = itemIndex
			rebalanceContext.decrementDesire(itemIndex)
			rebalanceContext.itemIndex2Used[itemIndex] = true
			otherItemIndexes[replica] = itemIndex
			for tier := 1; tier < rebalanceContext.tierCount; tier++ {
				tierID := rebalanceContext.tier2ItemIndex2TierID[tier][itemIndex]
				tierID.used = true
				tier2OtherTierIDs[tier][replica] = tierID
			}
		}
	}
}

// subsequentRebalance is much more complicated than firstRebalance.
// First we'll reassign any partition replicas assigned to items with a
// weight less than 0, as this indicates a deleted item.
// Then we'll attempt to reassign partition replicas that are at extremely
// high risk because they're on the exact same item.
// Next we'll attempt to reassign partition replicas that are at some risk
// because they are currently assigned within the same tier separation.
// Then, we'll attempt to reassign replicas within tiers to achieve better
// distribution, as usually such intra-tier movements are more efficient
// for users of the ring.
// Finally, one last pass will be done to reassign replicas to still
// underweight items.
func (rebalanceContext *rebalanceContextImpl) subsequentRebalance() {
	replicaCount := len(rebalanceContext.ring.replica2Partition2ItemIndex)
	partitionCount := len(rebalanceContext.ring.replica2Partition2ItemIndex[0])
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
	// First we'll reassign any partition replicas assigned to items with a
	// weight less than 0, as this indicates a deleted item.
	for deletedItemIndex, deletedItem := range rebalanceContext.ring.items {
		if deletedItem.Active() {
			continue
		}
		for replica := 0; replica < replicaCount; replica++ {
			partition2ItemIndex := rebalanceContext.ring.replica2Partition2ItemIndex[replica]
			for partition := 0; partition < partitionCount; partition++ {
				if partition2ItemIndex[partition] != int32(deletedItemIndex) {
					continue
				}
				// We track the other items and tiers we've assigned partition
				// replicas to so that we can try to avoid assigning further
				// replicas to similar items.
				otherItemIndexes := make([]int32, replicaCount)
				rebalanceContext.itemIndex2Used = make([]bool, len(rebalanceContext.ring.items))
				tier2OtherTierIDs := make([][]*tierIDImpl, rebalanceContext.tierCount)
				for tier := 1; tier < rebalanceContext.tierCount; tier++ {
					tier2OtherTierIDs[tier] = make([]*tierIDImpl, replicaCount)
				}
				for replicaB := 0; replicaB < replicaCount; replicaB++ {
					otherItemIndexes[replicaB] = rebalanceContext.ring.replica2Partition2ItemIndex[replicaB][partition]
					for tier := 1; tier < rebalanceContext.tierCount; tier++ {
						tierID := rebalanceContext.tier2ItemIndex2TierID[tier][otherItemIndexes[replicaB]]
						tierID.used = true
						tier2OtherTierIDs[tier][replicaB] = tierID
					}
				}
				itemIndex := rebalanceContext.bestItemIndex()
				partition2ItemIndex[partition] = itemIndex
				rebalanceContext.decrementDesire(itemIndex)
			}
		}
	}
}

func (rebalanceContext *rebalanceContextImpl) bestItemIndex() int32 {
	bestItemIndex := int32(-1)
	bestItemDesiredPartitionCount := ^int32(0)
	itemIndex2DesiredPartitionCount := rebalanceContext.itemIndex2DesiredPartitionCount
	var tierID *tierIDImpl
	var itemIndex int32
	tier2TierIDs := rebalanceContext.tier2TierIDs
	for tier := rebalanceContext.tierCount - 1; tier > 0; tier-- {
		// We will go through all tierIDs for a tier to get the
		// best item at that tier.
		for _, tierID = range tier2TierIDs[tier] {
			if !tierID.used {
				itemIndex = tierID.itemIndexesByDesire[0]
				if bestItemDesiredPartitionCount < itemIndex2DesiredPartitionCount[itemIndex] {
					bestItemIndex = itemIndex
					bestItemDesiredPartitionCount = itemIndex2DesiredPartitionCount[itemIndex]
				}
			}
		}
		// If we found an item at this tier, we don't need to check the lower
		// tiers.
		if bestItemIndex >= 0 {
			return bestItemIndex
		}
	}
	// If we found no good higher tiered candidates, we'll have to just
	// take the item with the highest desire that hasn't already been
	// selected.
	for _, itemIndex := range rebalanceContext.itemIndexesByDesire {
		if !rebalanceContext.itemIndex2Used[itemIndex] {
			return itemIndex
		}
	}
	// If we still found no good candidates, we'll have to just take the
	// item with the highest desire.
	return rebalanceContext.itemIndexesByDesire[0]
}

func (rebalanceContext *rebalanceContextImpl) decrementDesire(itemIndex int32) {
	itemIndex2DesiredPartitionCount := rebalanceContext.itemIndex2DesiredPartitionCount
	itemIndexesByDesire := rebalanceContext.itemIndexesByDesire
	scanDesiredPartitionCount := itemIndex2DesiredPartitionCount[itemIndex] - 1
	swapWith := 0
	hi := len(itemIndexesByDesire)
	mid := 0
	for swapWith < hi {
		mid = (swapWith + hi) / 2
		if itemIndex2DesiredPartitionCount[itemIndexesByDesire[mid]] > scanDesiredPartitionCount {
			swapWith = mid + 1
		} else {
			hi = mid
		}
	}
	prev := swapWith
	if prev >= len(itemIndexesByDesire) {
		prev--
	}
	swapWith--
	for itemIndexesByDesire[prev] != itemIndex {
		prev--
	}
	if prev != swapWith {
		itemIndexesByDesire[prev], itemIndexesByDesire[swapWith] = itemIndexesByDesire[swapWith], itemIndexesByDesire[prev]
	}
	for tier := 1; tier < rebalanceContext.tierCount; tier++ {
		itemIndexesByDesire := rebalanceContext.tier2ItemIndex2TierID[tier][itemIndex].itemIndexesByDesire
		swapWith = 0
		hi = len(itemIndexesByDesire)
		mid = 0
		for swapWith < hi {
			mid = (swapWith + hi) / 2
			if itemIndex2DesiredPartitionCount[itemIndexesByDesire[mid]] > scanDesiredPartitionCount {
				swapWith = mid + 1
			} else {
				hi = mid
			}
		}
		prev = swapWith
		if prev >= len(itemIndexesByDesire) {
			prev--
		}
		swapWith--
		for itemIndexesByDesire[prev] != itemIndex {
			prev--
		}
		if prev != swapWith {
			itemIndexesByDesire[prev], itemIndexesByDesire[swapWith] = itemIndexesByDesire[swapWith], itemIndexesByDesire[prev]
		}
	}
	itemIndex2DesiredPartitionCount[itemIndex]--
}

type tierIDImpl struct {
	values              []int
	itemIndexesByDesire []int32
	used                bool
}
