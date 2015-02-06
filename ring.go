// Package ring contains tools for building and using a consistent hashing ring
// with replicas, automatic partitioning (ring ranges), and keeping replicas of
// the same partitions in as distinct tiered nodes as possible (tiers might be
// devices, servers, cabinets, rooms, data centers, geographical regions, etc.)
package ring

// Long variable names are used in this code because it is tricky to understand
// and the verbosity can really help.

// 1 << 23 is 8388608 which, with 3 replicas, would use about 100M of memory
const _MAX_PARTITION_COUNT = 8388608

type Ring interface {
	// Version can indicate changes in ring data; for example, if a server is
	// currently working with version 123 of ring data and receives requests
	// that are based on a lesser version of ring data, it can ignore those
	// requests or send an "obsoleted" response or something along those lines.
	// Similarly, if the server receives requests for a greater version of ring
	// data, it can ignore those requests or try to obtain a newer ring
	// version.
	Version() uint64
	// PartitionBits is the number of bits that can be used to determine a
	// partition number for the current data in the ring. For example, to
	// convert a uint64 hash value into a partition number you could use
	// hashValue >> (64 - ring.PartitionBits()).
	PartitionBits() uint16
	// LocalNodeID is the identifier of the local node; determines which ring
	// partitions/replicas the local node is responsible for as well as being
	// used to direct message delivery.
	LocalNodeID() uint64
	// Responsible will return true if the local node is considered responsible
	// for a replica of the partition given.
	Responsible(partition uint32) bool
}

type MutableRing interface {
	PartitionCount() int
	ReplicaCount() int
	// PointsAllowed is the number of percentage points over or under that the
	// ring will try to keep data assignments within. The default is 1 for one
	// percent extra or less data.
	PointsAllowed() int
	SetPointsAllowed(points int)
	NodeCount() int
	Node(nodeIndex int) Node
	// Add will add the node to the ring and return its node index.
	Add(node Node) int
	// Rebalance should be called after adding or changing nodes to reassign
	// partition replicas accordingly.
	Rebalance()
	Stats() *RingStats
}

// Node is a single item assigned to a ring, usually a single device like a
// disk drive.
type Node interface {
	Active() bool
	// Capacity indicates the amount of data that should be assigned to a node
	// relative to other nodes. It can be in any unit of designation as long as
	// all nodes use the same designation. Most commonly this is the number of
	// bytes the node can store, but could be based on CPU capacity or another
	// resource if that makes more sense to balance.
	Capacity() uint64
	// Tiers indicate the layout of the node with respect to other nodes. For
	// example, the lowest tier, tier 0, would be the node itself (e.g.
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
	localNodeID                 uint64
	partitionBits               uint16
	nodes                       []Node
	replica2Partition2NodeIndex [][]int32
	pointsAllowed               int
}

func NewRing(replicaCount int) MutableRing {
	ring := &ringImpl{
		nodes: make([]Node, 0),
		replica2Partition2NodeIndex: make([][]int32, replicaCount),
		pointsAllowed:               1,
	}
	for replica := 0; replica < replicaCount; replica++ {
		ring.replica2Partition2NodeIndex[replica] = []int32{-1}
	}
	return ring
}

func (ring *ringImpl) ReplicaCount() int {
	return len(ring.replica2Partition2NodeIndex)
}

func (ring *ringImpl) PartitionCount() int {
	return len(ring.replica2Partition2NodeIndex[0])
}

func (ring *ringImpl) PointsAllowed() int {
	return ring.pointsAllowed
}

func (ring *ringImpl) SetPointsAllowed(points int) {
	ring.pointsAllowed = points
}

func (ring *ringImpl) NodeCount() int {
	return len(ring.nodes)
}

func (ring *ringImpl) Node(nodeIndex int) Node {
	return ring.nodes[nodeIndex]
}

func (ring *ringImpl) Add(node Node) int {
	ring.nodes = append(ring.nodes, node)
	return len(ring.nodes) - 1
}

func (ring *ringImpl) Rebalance() {
	ring.resizeIfNeeded()
	newRebalanceContext(ring).rebalance()
}

func (ring *ringImpl) resizeIfNeeded() {
	replicaCount := ring.ReplicaCount()
	// Calculate the partition count needed.
	// Each node is examined to see how much under or over weight it would be
	// and increasing the partition count until the difference is under the
	// points allowed.
	totalCapacity := uint64(0)
	for _, node := range ring.nodes {
		if node.Active() {
			totalCapacity += node.Capacity()
		}
	}
	partitionCount := len(ring.replica2Partition2NodeIndex[0])
	partitionCountShifts := uint(0)
	pointsAllowed := float64(ring.pointsAllowed) * 0.01
	done := false
	for !done {
		done = true
		for _, node := range ring.nodes {
			if !node.Active() {
				continue
			}
			desiredPartitionCount := float64(partitionCount) * float64(replicaCount) * (float64(node.Capacity()) / float64(totalCapacity))
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
	// Grow the partition2NodeIndex slices if the partition count grew.
	if partitionCount > len(ring.replica2Partition2NodeIndex[0]) {
		for replica := 0; replica < replicaCount; replica++ {
			partition2NodeIndex := make([]int32, partitionCount)
			for partition := 0; partition < partitionCount; partition++ {
				partition2NodeIndex[partition] = ring.replica2Partition2NodeIndex[replica][partition>>partitionCountShifts]
			}
			ring.replica2Partition2NodeIndex[replica] = partition2NodeIndex
		}
	}
}

// RingStats can be obtained with MutableRing.Stats() and gives information
// about the ring and its health. The MaxUnder and MaxOver values specifically
// indicate how balanced the ring is at this time.
type RingStats struct {
	ReplicaCount      int
	NodeCount         int
	InactiveNodeCount int
	PartitionCount    int
	PointsAllowed     int
	TotalCapacity     uint64
	// MaxUnderNodePercentage is the percentage an node is underweight, or has
	// less data assigned to it than its capacity would indicate it desires.
	MaxUnderNodePercentage float64
	MaxUnderNodeIndex      int
	// MaxOverNodePercentage is the percentage an node is overweight, or has
	// more data assigned to it than its capacity would indicate it desires.
	MaxOverNodePercentage float64
	MaxOverNodeIndex      int
}

func (ring *ringImpl) Stats() *RingStats {
	stats := &RingStats{
		ReplicaCount:      ring.ReplicaCount(),
		NodeCount:         ring.NodeCount(),
		PartitionCount:    ring.PartitionCount(),
		PointsAllowed:     ring.PointsAllowed(),
		MaxUnderNodeIndex: -1,
		MaxOverNodeIndex:  -1,
	}
	nodeIndex2PartitionCount := make([]int, stats.NodeCount)
	for _, partition2NodeIndex := range ring.replica2Partition2NodeIndex {
		for _, nodeIndex := range partition2NodeIndex {
			nodeIndex2PartitionCount[nodeIndex]++
		}
	}
	for _, node := range ring.nodes {
		if node.Active() {
			stats.TotalCapacity += node.Capacity()
		} else {
			stats.InactiveNodeCount++
		}
	}
	for nodeIndex, node := range ring.nodes {
		if !node.Active() {
			continue
		}
		desiredPartitionCount := float64(node.Capacity()) / float64(stats.TotalCapacity) * float64(stats.PartitionCount) * float64(stats.ReplicaCount)
		actualPartitionCount := float64(nodeIndex2PartitionCount[nodeIndex])
		if desiredPartitionCount > actualPartitionCount {
			under := 100.0 * (desiredPartitionCount - actualPartitionCount) / desiredPartitionCount
			if under > stats.MaxUnderNodePercentage {
				stats.MaxUnderNodePercentage = under
				stats.MaxUnderNodeIndex = nodeIndex
			}
		} else if desiredPartitionCount < actualPartitionCount {
			over := 100.0 * (actualPartitionCount - desiredPartitionCount) / desiredPartitionCount
			if over > stats.MaxOverNodePercentage {
				stats.MaxOverNodePercentage = over
				stats.MaxOverNodeIndex = nodeIndex
			}
		}
	}
	return stats
}
