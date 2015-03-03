// Package ring contains tools for building and using a consistent hashing ring
// with replicas, automatic partitioning (ring ranges), and keeping replicas of
// the same partitions in as distinct tiered nodes as possible (tiers might be
// devices, servers, cabinets, rooms, data centers, geographical regions, etc.)
//
// It also contains tools for using a ring as a messaging hub, easing
// communication between nodes in the ring.
package ring

type Ring interface {
	// Version can indicate changes in ring data; for example, if a server is
	// currently working with one version of ring data and receives requests
	// that are based on a lesser version of ring data, it can ignore those
	// requests or send an "obsoleted" response or something along those lines.
	// Similarly, if the server receives requests for a greater version of ring
	// data, it can ignore those requests or try to obtain a newer ring
	// version.
	Version() int64
	// PartitionBitCount is the number of bits that can be used to determine a
	// partition number for the current data in the ring. For example, to
	// convert a uint64 hash value into a partition number you could use
	// hashValue >> (64 - ring.PartitionBitCount()).
	PartitionBitCount() uint16
	ReplicaCount() int
	// Nodes returns a list of nodes referenced by the ring.
	Nodes() []Node
	// LocalNode contains the information for the local node; determining which
	// ring partitions/replicas the local node is responsible for as well as
	// being used to direct message delivery. If this instance of the ring has
	// no local node information, nil will be returned.
	LocalNode() Node
	// Responsible will return true if the local node is considered responsible
	// for a replica of the partition given.
	Responsible(partition uint32) bool
	// ResponsibleNodes will return a list of nodes for considered responsible
	// for the replicas of the partition given.
	ResponsibleNodes(partition uint32) []Node
}

type ringImpl struct {
	version                       int64
	localNodeIndex                int32
	partitionBitCount             uint16
	nodes                         []Node
	replicaToPartitionToNodeIndex [][]int32
}

func (ring *ringImpl) Version() int64 {
	return ring.version
}

func (ring *ringImpl) PartitionBitCount() uint16 {
	return ring.partitionBitCount
}

func (ring *ringImpl) ReplicaCount() int {
	return len(ring.replicaToPartitionToNodeIndex)
}

func (ring *ringImpl) Nodes() []Node {
	nodes := make([]Node, len(ring.nodes))
	copy(nodes, ring.nodes)
	return nodes
}

func (ring *ringImpl) LocalNode() Node {
	if ring.localNodeIndex == -1 {
		return nil
	}
	return ring.nodes[ring.localNodeIndex]
}

func (ring *ringImpl) Responsible(partition uint32) bool {
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

func (ring *ringImpl) ResponsibleNodes(partition uint32) []Node {
	nodes := make([]Node, ring.ReplicaCount())
	for replica, partitionToNodeIndex := range ring.replicaToPartitionToNodeIndex {
		nodes[replica] = ring.nodes[partitionToNodeIndex[partition]]
	}
	return nodes
}

// Node is a single item assigned to a ring, usually a single device like a
// disk drive.
type Node interface {
	// NodeID uniquely identifies this node; this id is all that is retained
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
	// example, the lowest tier, tier 0, might be the server ip (where each
	// node represents a drive on that server). The next tier, 1, might then be
	// the power zone the server is in. The number of tiers is flexible, so
	// later an additional tier for geographic region could be added.
	// Here the tier values are represented by ints, presumably as indexes to
	// the actual values stored elsewhere. This is done for speed during
	// rebalancing.
	TierValues() []int
	// Address gives the location information for the node; probably something
	// like an ip:port.
	Address() string
}
