// Package ring contains tools for building and using a consistent hashing ring
// with replicas, automatic partitioning (ring ranges), and keeping replicas of
// the same partitions in as distinct tiered nodes as possible (tiers might be
// devices, servers, cabinets, rooms, data centers, geographical regions, etc.)
//
// It also contains tools for using a ring as a messaging hub, easing
// communication between nodes in the ring.
package ring

type Ring struct {
	version                       int64
	localNodeIndex                int32
	partitionBitCount             uint16
	nodes                         []*Node
	replicaToPartitionToNodeIndex [][]int32
}

// Version can indicate changes in ring data; for example, if a server is
// currently working with one version of ring data and receives requests that
// are based on a lesser version of ring data, it can ignore those requests or
// send an "obsoleted" response or something along those lines. Similarly, if
// the server receives requests for a greater version of ring data, it can
// ignore those requests or try to obtain a newer ring version.
func (ring *Ring) Version() int64 {
	return ring.version
}

// PartitionBitCount is the number of bits that can be used to determine a
// partition number for the current data in the ring. For example, to convert a
// uint64 hash value into a partition number you could use hashValue >> (64 -
// ring.PartitionBitCount()).
func (ring *Ring) PartitionBitCount() uint16 {
	return ring.partitionBitCount
}

func (ring *Ring) ReplicaCount() int {
	return len(ring.replicaToPartitionToNodeIndex)
}

// Nodes returns a list of nodes referenced by the ring.
func (ring *Ring) Nodes() []*Node {
	nodes := make([]*Node, len(ring.nodes))
	copy(nodes, ring.nodes)
	return nodes
}

func (ring *Ring) Node(id uint64) *Node {
	for _, node := range ring.nodes {
		if node.ID == id {
			return node
		}
	}
	return nil
}

// LocalNode contains the information for the local node; determining which
// ring partitions/replicas the local node is responsible for as well as being
// used to direct message delivery. If this instance of the ring has no local
// node information, nil will be returned.
func (ring *Ring) LocalNode() *Node {
	if ring.localNodeIndex == -1 {
		return nil
	}
	return ring.nodes[ring.localNodeIndex]
}

// Responsible will return true if the local node is considered responsible for
// a replica of the partition given.
func (ring *Ring) Responsible(partition uint32) bool {
	if ring.localNodeIndex == -1 {
		return false
	}
	for _, partitionToNodeIndex := range ring.replicaToPartitionToNodeIndex {
		if partitionToNodeIndex[partition] == ring.localNodeIndex {
			return true
		}
	}
	return false
}

// ResponsibleNodes will return a list of nodes for considered responsible for
// the replicas of the partition given.
func (ring *Ring) ResponsibleNodes(partition uint32) []*Node {
	nodes := make([]*Node, ring.ReplicaCount())
	for replica, partitionToNodeIndex := range ring.replicaToPartitionToNodeIndex {
		nodes[replica] = ring.nodes[partitionToNodeIndex[partition]]
	}
	return nodes
}

// Node is a single item assigned to a ring, usually a single device like a
// disk drive.
type Node struct {
	// NodeID uniquely identifies this node; it must be non-zero as zero is
	// used to indicate "no node".
	ID       uint64
	Inactive bool
	// Capacity indicates the amount of data that should be assigned to a node
	// relative to other nodes. It can be in any unit of designation as long as
	// all nodes use the same designation. Most commonly this is the number of
	// gigabytes the node can store, but could be based on CPU capacity or
	// another resource if that makes more sense to balance.
	Capacity uint32
	// Tiers indicate the layout of the node with respect to other nodes. For
	// example, the lowest tier, tier 0, might be the server ip (where each
	// node represents a drive on that server). The next tier, 1, might then be
	// the power zone the server is in. The number of tiers is flexible, so
	// later an additional tier for geographic region could be added.
	// Here the tier values are represented by ints, presumably as indexes to
	// the actual values stored elsewhere. This is done for speed during
	// rebalancing.
	TierValues []int
	// Address gives the location information for the node; probably something
	// like an ip:port.
	Address string
	// Meta is additional information for the node; not defined or used by the
	// builder or ring directly.
	Meta string
}

type RingStats struct {
	ReplicaCount      int
	NodeCount         int
	InactiveNodeCount int
	PartitionBitCount uint16
	PartitionCount    int
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

// Stats gives information about the ring and its health; the MaxUnder and
// MaxOver values specifically indicate how balanced the ring is.
func (ring *Ring) Stats() *RingStats {
	stats := &RingStats{
		ReplicaCount:      ring.ReplicaCount(),
		NodeCount:         len(ring.nodes),
		PartitionBitCount: ring.PartitionBitCount(),
		PartitionCount:    1 << ring.PartitionBitCount(),
		MaxUnderNodeIndex: -1,
		MaxOverNodeIndex:  -1,
	}
	nodeIndexToPartitionCount := make([]int, stats.NodeCount)
	for _, partitionToNodeIndex := range ring.replicaToPartitionToNodeIndex {
		for _, nodeIndex := range partitionToNodeIndex {
			nodeIndexToPartitionCount[nodeIndex]++
		}
	}
	for _, node := range ring.nodes {
		if node.Inactive {
			stats.InactiveNodeCount++
		} else {
			stats.TotalCapacity += (uint64)(node.Capacity)
		}
	}
	for nodeIndex, node := range ring.nodes {
		if node.Inactive {
			continue
		}
		desiredPartitionCount := float64(node.Capacity) / float64(stats.TotalCapacity) * float64(stats.PartitionCount) * float64(stats.ReplicaCount)
		actualPartitionCount := float64(nodeIndexToPartitionCount[nodeIndex])
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
