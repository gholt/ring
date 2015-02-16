// Package ring contains tools for building and using a consistent hashing ring
// with replicas, automatic partitioning (ring ranges), and keeping replicas of
// the same partitions in as distinct tiered nodes as possible (tiers might be
// devices, servers, cabinets, rooms, data centers, geographical regions, etc.)
// TODO:
//  Actually up version number with changes.
//  Make partitionBits actually work.
//  Indexes should be uint32 and not int32; use 0 for nil node.
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

type ringImpl struct {
	version                     uint64
	localNodeIndex              int32
	partitionBits               uint16
	nodeIDs                     []uint64
	replica2Partition2NodeIndex [][]int32
}

func (ring *ringImpl) Version() uint64 {
	return ring.version
}

func (ring *ringImpl) PartitionBits() uint16 {
	return ring.partitionBits
}

func (ring *ringImpl) LocalNodeID() uint64 {
	return ring.nodeIDs[ring.localNodeIndex]
}

func (ring *ringImpl) Responsible(partition uint32) bool {
	for _, partition2NodeIndex := range ring.replica2Partition2NodeIndex {
		if partition2NodeIndex[partition] == ring.localNodeIndex {
			return true
		}
	}
	return false
}

type RingBuilder interface {
	// Ring returns a Ring instance of the data defined by the RingBuilder.
	// This will cause any pending rebalancing actions to be performed. The
	// Ring returned will be immutable; to obtain updated ring data, Ring()
	// must be called again.
	Ring() Ring
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
	Stats() *RingStats
}

// Node is a single item assigned to a ring, usually a single device like a
// disk drive.
type Node interface {
	// NodeID uniquely identifies this node; this is id is all that is retained
	// when a RingBuilder returns a Ring representation of the data.
	NodeID() uint64
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

type ringBuilderImpl struct {
	version                     uint64
	nodes                       []Node
	replica2Partition2NodeIndex [][]int32
	pointsAllowed               int
}

func NewRingBuilder(replicaCount int) RingBuilder {
	builder := &ringBuilderImpl{
		nodes: make([]Node, 0),
		replica2Partition2NodeIndex: make([][]int32, replicaCount),
		pointsAllowed:               1,
	}
	for replica := 0; replica < replicaCount; replica++ {
		builder.replica2Partition2NodeIndex[replica] = []int32{-1}
	}
	return builder
}

func (builder *ringBuilderImpl) ReplicaCount() int {
	return len(builder.replica2Partition2NodeIndex)
}

func (builder *ringBuilderImpl) PartitionCount() int {
	return len(builder.replica2Partition2NodeIndex[0])
}

func (builder *ringBuilderImpl) PointsAllowed() int {
	return builder.pointsAllowed
}

func (builder *ringBuilderImpl) SetPointsAllowed(points int) {
	builder.pointsAllowed = points
}

func (builder *ringBuilderImpl) NodeCount() int {
	return len(builder.nodes)
}

func (builder *ringBuilderImpl) Node(nodeIndex int) Node {
	return builder.nodes[nodeIndex]
}

func (builder *ringBuilderImpl) Add(node Node) int {
	builder.nodes = append(builder.nodes, node)
	return len(builder.nodes) - 1
}

func (builder *ringBuilderImpl) Ring() Ring {
	builder.resizeIfNeeded()
	newRebalanceContext(builder).rebalance()
	nodeIDs := make([]uint64, len(builder.nodes))
	for i := 0; i < len(nodeIDs); i++ {
		nodeIDs[i] = builder.nodes[i].NodeID()
	}
	replica2Partition2NodeIndex := make([][]int32, len(builder.replica2Partition2NodeIndex))
	for i := 0; i < len(replica2Partition2NodeIndex); i++ {
		replica2Partition2NodeIndex[i] = make([]int32, len(builder.replica2Partition2NodeIndex[i]))
		copy(replica2Partition2NodeIndex[i], builder.replica2Partition2NodeIndex[i])
	}
	return &ringImpl{
		version:                     builder.version,
		partitionBits:               1, // TODO
		nodeIDs:                     nodeIDs,
		replica2Partition2NodeIndex: replica2Partition2NodeIndex,
	}
}

func (builder *ringBuilderImpl) resizeIfNeeded() {
	replicaCount := builder.ReplicaCount()
	// Calculate the partition count needed.
	// Each node is examined to see how much under or over weight it would be
	// and increasing the partition count until the difference is under the
	// points allowed.
	totalCapacity := uint64(0)
	for _, node := range builder.nodes {
		if node.Active() {
			totalCapacity += node.Capacity()
		}
	}
	partitionCount := len(builder.replica2Partition2NodeIndex[0])
	partitionCountShifts := uint(0)
	pointsAllowed := float64(builder.pointsAllowed) * 0.01
	done := false
	for !done {
		done = true
		for _, node := range builder.nodes {
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
	if partitionCount > len(builder.replica2Partition2NodeIndex[0]) {
		for replica := 0; replica < replicaCount; replica++ {
			partition2NodeIndex := make([]int32, partitionCount)
			for partition := 0; partition < partitionCount; partition++ {
				partition2NodeIndex[partition] = builder.replica2Partition2NodeIndex[replica][partition>>partitionCountShifts]
			}
			builder.replica2Partition2NodeIndex[replica] = partition2NodeIndex
		}
	}
}

// RingStats can be obtained with RingBuilder.Stats() and gives information
// about the builder and its health. The MaxUnder and MaxOver values specifically
// indicate how balanced the builder is at this time.
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

func (builder *ringBuilderImpl) Stats() *RingStats {
	stats := &RingStats{
		ReplicaCount:      builder.ReplicaCount(),
		NodeCount:         builder.NodeCount(),
		PartitionCount:    builder.PartitionCount(),
		PointsAllowed:     builder.PointsAllowed(),
		MaxUnderNodeIndex: -1,
		MaxOverNodeIndex:  -1,
	}
	nodeIndex2PartitionCount := make([]int, stats.NodeCount)
	for _, partition2NodeIndex := range builder.replica2Partition2NodeIndex {
		for _, nodeIndex := range partition2NodeIndex {
			nodeIndex2PartitionCount[nodeIndex]++
		}
	}
	for _, node := range builder.nodes {
		if node.Active() {
			stats.TotalCapacity += node.Capacity()
		} else {
			stats.InactiveNodeCount++
		}
	}
	for nodeIndex, node := range builder.nodes {
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
