// Package ring contains tools for building and using a consistent hashing ring
// with replicas, automatic partitioning (ring ranges), and keeping replicas of
// the same partitions in as distinct tiered items as possible (tiers might be
// devices, servers, cabinets, rooms, data centers, geographical regions, etc.)
package ring

// Long variable names are used in this code because it is tricky to understand
// and the verbosity can really help.

// 1 << 23 is 8388608 which, with 3 replicas, would use about 100M of memory
const _MAX_PARTITION_COUNT = 8388608

type Ring interface {
	ID() uint64
	NodeID() uint64
	Responsible(partition uint32) bool
	PartitionPower() uint16
}

type MutableRing interface {
	Ring
	ReplicaCount() int
	PartitionCount() int
	// PointsAllowed is the number of percentage points over or under that the
	// ring will try to keep data assignments within. The default is 1 for one
	// percent extra or less data.
	PointsAllowed() int
	SetPointsAllowed(points int)
	ItemCount() int
	Item(itemIndex int) Item
	// Add will add the item to the ring and return its item index.
	Add(item Item) int
	// Rebalance should be called after adding or changing items to reassign
	// partition replicas accordingly.
	Rebalance()
	Stats() *RingStats
}

// Item is a single item assigned to a ring, usually a single device like a
// disk drive.
type Item interface {
	Active() bool
	// Capacity indicates the amount of data that should be assigned to an item
	// relative to other items. It can be in any unit of designation as long as
	// all items use the same designation. Most commonly this is the number of
	// bytes the item can store, but could be based on CPU capacity or another
	// resource if that makes more sense to balance.
	Capacity() uint64
	// Tiers indicate the layout of the item with respect to other items. For
	// example, the lowest tier, tier 0, would be the item itself (e.g.
	// "sdb1"). The next tier might be the server ip, then the power zone the
	// server is in. The number of tiers is flexible, so later an additional
	// tier for geographic region could be added, for example.
	// Here the tier values are represented by ints, presumably as indexes to
	// the actual values stored elsewhere. This is done for speed during
	// rebalancing.
	TierValues() []int
}

type ringImpl struct {
	id                          uint64
	nodeID                      uint64
	partitionPower              uint16
	items                       []Item
	replica2Partition2ItemIndex [][]int32
	pointsAllowed               int
}

func NewRing(replicaCount int) Ring {
	ring := &ringImpl{
		items: make([]Item, 0),
		replica2Partition2ItemIndex: make([][]int32, replicaCount),
		pointsAllowed:               1,
	}
	for replica := 0; replica < replicaCount; replica++ {
		ring.replica2Partition2ItemIndex[replica] = []int32{-1}
	}
	return ring
}

func (ring *ringImpl) ID() uint64 {
	return ring.id
}

func (ring *ringImpl) NodeID() uint64 {
	return ring.nodeID
}

func (ring *ringImpl) Responsible(partition uint32) bool {
	return false
}

func (ring *ringImpl) ReplicaCount() int {
	return len(ring.replica2Partition2ItemIndex)
}

func (ring *ringImpl) PartitionPower() uint16 {
	return ring.partitionPower
}

func (ring *ringImpl) PartitionCount() int {
	return len(ring.replica2Partition2ItemIndex[0])
}

func (ring *ringImpl) PointsAllowed() int {
	return ring.pointsAllowed
}

func (ring *ringImpl) SetPointsAllowed(points int) {
	ring.pointsAllowed = points
}

func (ring *ringImpl) ItemCount() int {
	return len(ring.items)
}

func (ring *ringImpl) Item(itemIndex int) Item {
	return ring.items[itemIndex]
}

func (ring *ringImpl) Add(item Item) int {
	ring.items = append(ring.items, item)
	return len(ring.items) - 1
}

func (ring *ringImpl) Rebalance() {
	ring.resizeIfNeeded()
	newRebalanceContext(ring).rebalance()
}

func (ring *ringImpl) resizeIfNeeded() {
	replicaCount := ring.ReplicaCount()
	// Calculate the partition count needed.
	// Each item is examined to see how much under or over weight it would be
	// and increasing the partition count until the difference is under the
	// points allowed.
	totalCapacity := uint64(0)
	for _, item := range ring.items {
		if item.Active() {
			totalCapacity += item.Capacity()
		}
	}
	partitionCount := len(ring.replica2Partition2ItemIndex[0])
	partitionCountShifts := uint(0)
	pointsAllowed := float64(ring.pointsAllowed) * 0.01
	done := false
	for !done {
		done = true
		for _, item := range ring.items {
			if !item.Active() {
				continue
			}
			desiredPartitionCount := float64(partitionCount) * float64(replicaCount) * (float64(item.Capacity()) / float64(totalCapacity))
			under := (desiredPartitionCount - float64(int(desiredPartitionCount))) / desiredPartitionCount
			over := (float64(int(desiredPartitionCount)+1) - desiredPartitionCount) / desiredPartitionCount
			if under > pointsAllowed || over > pointsAllowed {
				partitionCount <<= 1
				partitionCountShifts++
				if partitionCount >= _MAX_PARTITION_COUNT {
					done = true
					break
				} else {
					done = false
				}
			}
		}
	}
	// Grow the partition2ItemIndex slices if the partition count grew.
	if partitionCount > len(ring.replica2Partition2ItemIndex[0]) {
		for replica := 0; replica < replicaCount; replica++ {
			partition2ItemIndex := make([]int32, partitionCount)
			for partition := 0; partition < partitionCount; partition++ {
				partition2ItemIndex[partition] = ring.replica2Partition2ItemIndex[replica][partition>>partitionCountShifts]
			}
			ring.replica2Partition2ItemIndex[replica] = partition2ItemIndex
		}
	}
}

// RingStats can be obtained with Ring.Stats() and gives information about the
// ring and its health. The MaxUnder and MaxOver values specifically indicate
// how balanced the ring is at this time.
type RingStats struct {
	ReplicaCount      int
	ItemCount         int
	InactiveItemCount int
	PartitionCount    int
	PointsAllowed     int
	TotalCapacity     uint64
	// MaxUnderItemPercentage is the percentage an item is underweight, or has
	// less data assigned to it than its capacity would indicate it desires.
	MaxUnderItemPercentage float64
	MaxUnderItemIndex      int
	// MaxUnderItemPercentage is the percentage an item is overweight, or has
	// more data assigned to it than its capacity would indicate it desires.
	MaxOverItemPercentage float64
	MaxOverItemIndex      int
}

func (ring *ringImpl) Stats() *RingStats {
	stats := &RingStats{
		ReplicaCount:      ring.ReplicaCount(),
		ItemCount:         ring.ItemCount(),
		PartitionCount:    ring.PartitionCount(),
		PointsAllowed:     ring.PointsAllowed(),
		MaxUnderItemIndex: -1,
		MaxOverItemIndex:  -1,
	}
	itemIndex2PartitionCount := make([]int, stats.ItemCount)
	for _, partition2ItemIndex := range ring.replica2Partition2ItemIndex {
		for _, itemIndex := range partition2ItemIndex {
			itemIndex2PartitionCount[itemIndex]++
		}
	}
	for _, item := range ring.items {
		if item.Active() {
			stats.TotalCapacity += item.Capacity()
		} else {
			stats.InactiveItemCount++
		}
	}
	for itemIndex, item := range ring.items {
		if !item.Active() {
			continue
		}
		desiredPartitionCount := float64(item.Capacity()) / float64(stats.TotalCapacity) * float64(stats.PartitionCount) * float64(stats.ReplicaCount)
		actualPartitionCount := float64(itemIndex2PartitionCount[itemIndex])
		if desiredPartitionCount > actualPartitionCount {
			under := 100.0 * (desiredPartitionCount - actualPartitionCount) / desiredPartitionCount
			if under > stats.MaxUnderItemPercentage {
				stats.MaxUnderItemPercentage = under
				stats.MaxUnderItemIndex = itemIndex
			}
		} else if desiredPartitionCount < actualPartitionCount {
			over := 100.0 * (actualPartitionCount - desiredPartitionCount) / desiredPartitionCount
			if over > stats.MaxOverItemPercentage {
				stats.MaxOverItemPercentage = over
				stats.MaxOverItemIndex = itemIndex
			}
		}
	}
	return stats
}
