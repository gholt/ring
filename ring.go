// Package ring contains tools for building and using a consistent hashing ring
// with replicas, automatic partitioning (ring ranges), and keeping replicas of
// the same partitions in as distinct tiered nodes as possible (tiers might be
// devices, servers, cabinets, rooms, data centers, geographical regions, etc.)
package ring

import "time"

// Long variable names are used in this code because it is tricky to understand
// and the verbosity can really help.

// 1 << 23 is 8388608 which, with 3 replicas, would use about 100M of memory
const _MAX_PARTITION_COUNT = 8388608

type Ring interface {
	// Version can indicate changes in ring data; for example, if a server is
	// currently working with one version of ring data and receives requests
	// that are based on a lesser version of ring data, it can ignore those
	// requests or send an "obsoleted" response or something along those lines.
	// Similarly, if the server receives requests for a greater version of ring
	// data, it can ignore those requests or try to obtain a newer ring
	// version.
	Version() int64
	// PartitionBits is the number of bits that can be used to determine a
	// partition number for the current data in the ring. For example, to
	// convert a uint64 hash value into a partition number you could use
	// hashValue >> (64 - ring.PartitionBits()).
	PartitionBits() uint16
	ReplicaCount() int
	// LocalNodeID is the identifier of the local node; determines which ring
	// partitions/replicas the local node is responsible for as well as being
	// used to direct message delivery.
	LocalNodeID() uint64
	// Responsible will return true if the local node is considered responsible
	// for a replica of the partition given.
	Responsible(partition uint32) bool
}

type ringImpl struct {
	version                     int64
	localNodeIndex              int32
	partitionBits               uint16
	nodeIDs                     []uint64
	replica2Partition2NodeIndex [][]int32
}

func (ring *ringImpl) Version() int64 {
	return ring.version
}

func (ring *ringImpl) PartitionBits() uint16 {
	return ring.partitionBits
}

func (ring *ringImpl) ReplicaCount() int {
	return len(ring.replica2Partition2NodeIndex)
}

func (ring *ringImpl) LocalNodeID() uint64 {
	if ring.localNodeIndex == 0 {
		return 0
	}
	return ring.nodeIDs[ring.localNodeIndex]
}

func (ring *ringImpl) Responsible(partition uint32) bool {
	if ring.localNodeIndex == 0 {
		return false
	}
	for _, partition2NodeIndex := range ring.replica2Partition2NodeIndex {
		if partition2NodeIndex[partition] == ring.localNodeIndex {
			return true
		}
	}
	return false
}

// Node is a single item assigned to a ring, usually a single device like a
// disk drive.
type Node interface {
	// NodeID uniquely identifies this node; this is id is all that is retained
	// when a Builder returns a Ring representation of the data.
	NodeID() uint64
	Active() bool
	// Capacity indicates the amount of data that should be assigned to a node
	// relative to other nodes. It can be in any unit of designation as long as
	// all nodes use the same designation. Most commonly this is the number of
	// gigabytes the node can store, but could be based on CPU capacity or
	// another resource if that makes more sense to balance.
	Capacity() uint32
	// Tiers indicate the layout of the node with respect to other nodes. For
	// example, the lowest tier, tier 0, would be the node itself (e.g. sdb1).
	// The next tier might be the server ip, then the power zone the server is
	// in. The number of tiers is flexible, so later an additional tier for
	// geographic region could be added, for example.
	// Here the tier values are represented by ints, presumably as indexes to
	// the actual values stored elsewhere. This is done for speed during
	// rebalancing.
	TierValues() []int
}

type Builder struct {
	version                     int64
	nodes                       []Node
	partitionBits               uint16
	replica2Partition2NodeIndex [][]int32
	pointsAllowed               int
}

func NewBuilder(replicaCount int) *Builder {
	b := &Builder{
		nodes: make([]Node, 0),
		replica2Partition2NodeIndex: make([][]int32, replicaCount),
		pointsAllowed:               1,
	}
	for replica := 0; replica < replicaCount; replica++ {
		b.replica2Partition2NodeIndex[replica] = []int32{-1}
	}
	return b
}

// PointsAllowed is the number of percentage points over or under that the ring
// will try to keep data assignments within. The default is 1 for one percent
// extra or less data.
func (b *Builder) PointsAllowed() int {
	return b.pointsAllowed
}

func (b *Builder) SetPointsAllowed(points int) {
	b.pointsAllowed = points
}

func (b *Builder) NodeCount() int {
	return len(b.nodes)
}

func (b *Builder) Node(nodeIndex int) Node {
	return b.nodes[nodeIndex]
}

// Add will add the node to the builder's list and return its index in that
// list.
func (b *Builder) Add(node Node) int {
	b.nodes = append(b.nodes, node)
	return len(b.nodes) - 1
}

// Ring returns a Ring instance of the data defined by the builder. This will
// cause any pending rebalancing actions to be performed. The Ring returned
// will be immutable; to obtain updated ring data, Ring() must be called again.
// The localNodeID is so the Ring instance can provide local responsibility
// information; you can give 0 if you don't intended to use those features.
func (b *Builder) Ring(localNodeID uint64) Ring {
	if b.resizeIfNeeded() {
		b.version = time.Now().UnixNano()
	}
	if newRebalanceContext(b).rebalance() {
		b.version = time.Now().UnixNano()
	}
	localNodeIndex := int32(0)
	nodeIDs := make([]uint64, len(b.nodes))
	for i := 0; i < len(nodeIDs); i++ {
		nodeIDs[i] = b.nodes[i].NodeID()
		if nodeIDs[i] == localNodeID {
			localNodeIndex = int32(i)
		}
	}
	replica2Partition2NodeIndex := make([][]int32, len(b.replica2Partition2NodeIndex))
	for i := 0; i < len(replica2Partition2NodeIndex); i++ {
		replica2Partition2NodeIndex[i] = make([]int32, len(b.replica2Partition2NodeIndex[i]))
		copy(replica2Partition2NodeIndex[i], b.replica2Partition2NodeIndex[i])
	}
	return &ringImpl{
		version:                     b.version,
		localNodeIndex:              localNodeIndex,
		partitionBits:               b.partitionBits,
		nodeIDs:                     nodeIDs,
		replica2Partition2NodeIndex: replica2Partition2NodeIndex,
	}
}

func (b *Builder) resizeIfNeeded() bool {
	replicaCount := len(b.replica2Partition2NodeIndex)
	// Calculate the partition count needed.
	// Each node is examined to see how much under or over weight it would be
	// and increasing the partition count until the difference is under the
	// points allowed.
	totalCapacity := uint64(0)
	for _, node := range b.nodes {
		if node.Active() {
			totalCapacity += (uint64)(node.Capacity())
		}
	}
	partitionCount := len(b.replica2Partition2NodeIndex[0])
	pointsAllowed := float64(b.pointsAllowed) * 0.01
	done := false
	for !done {
		done = true
		for _, node := range b.nodes {
			if !node.Active() {
				continue
			}
			desiredPartitionCount := float64(partitionCount) * float64(replicaCount) * (float64(node.Capacity()) / float64(totalCapacity))
			under := (desiredPartitionCount - float64(int(desiredPartitionCount))) / desiredPartitionCount
			over := (float64(int(desiredPartitionCount)+1) - desiredPartitionCount) / desiredPartitionCount
			if under > pointsAllowed || over > pointsAllowed {
				partitionCount <<= 1
				b.partitionBits++
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
	if partitionCount > len(b.replica2Partition2NodeIndex[0]) {
		for replica := 0; replica < replicaCount; replica++ {
			partition2NodeIndex := make([]int32, partitionCount)
			for partition := 0; partition < partitionCount; partition++ {
				partition2NodeIndex[partition] = b.replica2Partition2NodeIndex[replica][partition>>b.partitionBits]
			}
			b.replica2Partition2NodeIndex[replica] = partition2NodeIndex
		}
		return true
	}
	// Shrinking the partition2NodeIndex slices doesn't happen because it would
	// normally cause more data movements than it's worth. Perhaps in the
	// future we can add detection of cases when shrinking makes sense.
	return false
}

type BuilderStats struct {
	ReplicaCount      int
	NodeCount         int
	InactiveNodeCount int
	PartitionBits     uint16
	PartitionCount    int
	PointsAllowed     int
	TotalCapacity     uint64
	// MaxUnderNodePercentage is the percentage a node is underweight, or has
	// less data assigned to it than its capacity would indicate it desires.
	MaxUnderNodePercentage float64
	MaxUnderNodeIndex      int
	// MaxOverNodePercentage is the percentage a node is overweight, or has
	// more data assigned to it than its capacity would indicate it desires.
	MaxOverNodePercentage float64
	MaxOverNodeIndex      int
}

// Stats gives information about the builder and its health; note that this
// will call the Ring method and so could cause rebalancing. The MaxUnder and
// MaxOver values specifically indicate how balanced the builder is at this
// time.
func (b *Builder) Stats() *BuilderStats {
	ring := b.Ring(0)
	stats := &BuilderStats{
		ReplicaCount:      ring.ReplicaCount(),
		NodeCount:         b.NodeCount(),
		PartitionBits:     ring.PartitionBits(),
		PartitionCount:    1 << ring.PartitionBits(),
		PointsAllowed:     b.PointsAllowed(),
		MaxUnderNodeIndex: -1,
		MaxOverNodeIndex:  -1,
	}
	nodeIndex2PartitionCount := make([]int, stats.NodeCount)
	for _, partition2NodeIndex := range b.replica2Partition2NodeIndex {
		for _, nodeIndex := range partition2NodeIndex {
			nodeIndex2PartitionCount[nodeIndex]++
		}
	}
	for _, node := range b.nodes {
		if node.Active() {
			stats.TotalCapacity += (uint64)(node.Capacity())
		} else {
			stats.InactiveNodeCount++
		}
	}
	for nodeIndex, node := range b.nodes {
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
