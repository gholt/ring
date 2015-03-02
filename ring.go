// Package ring contains tools for building and using a consistent hashing ring
// with replicas, automatic partitioning (ring ranges), and keeping replicas of
// the same partitions in as distinct tiered nodes as possible (tiers might be
// devices, servers, cabinets, rooms, data centers, geographical regions, etc.)
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
	version                       int64
	localNodeIndex                int32
	partitionBits                 uint16
	nodeIDs                       []uint64
	replicaToPartitionToNodeIndex [][]int32
}

func (ring *ringImpl) Version() int64 {
	return ring.version
}

func (ring *ringImpl) PartitionBits() uint16 {
	return ring.partitionBits
}

func (ring *ringImpl) ReplicaCount() int {
	return len(ring.replicaToPartitionToNodeIndex)
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
	for _, partitionToNodeIndex := range ring.replicaToPartitionToNodeIndex {
		if partitionToNodeIndex[partition] == ring.localNodeIndex {
			return true
		}
	}
	return false
}
