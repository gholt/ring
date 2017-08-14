package ring

import (
	"github.com/gholt/holdme"
	"github.com/gholt/ring/lowring"
)

type Node struct {
	ring                *Ring
	index               int
	info                string
	capacity            int
	replicaToPartitions [][]uint32
}

func (n *Node) Info() string {
	return n.info
}

func (n *Node) Capacity() int {
	return n.capacity
}

func (n *Node) Group() *Group {
	return n.ring.groups[n.ring.nodeToGroup[n.index]]
}

func (n *Node) fillReplicaToPartitions() {
	if n.replicaToPartitions == nil {
		replicaCount := len(n.ring.replicaToPartitionToNode)
		partitionCount := uint32(len(n.ring.replicaToPartitionToNode[0]))
		n.replicaToPartitions = make([][]uint32, replicaCount)
		for replica := 0; replica < replicaCount; replica++ {
			partitions := []uint32{}
			partitionToNode := n.ring.replicaToPartitionToNode[replica]
			for partition := uint32(0); partition < partitionCount; partition++ {
				if partitionToNode[partition] == lowring.Node(n.index) {
					partitions = append(partitions, partition)
				}
			}
			n.replicaToPartitions[replica] = partitions
		}
	}
}

func (n *Node) Partitions() []int {
	n.fillReplicaToPartitions()
	var partitions holdme.OrderedIntsNoDups
	for _, parts := range n.replicaToPartitions {
		for _, partition := range parts {
			partitions.Add(int(partition))
		}
	}
	return partitions
}

func (n *Node) PartitionsForReplica(replica int) []int {
	n.fillReplicaToPartitions()
	partitions := make([]int, len(n.replicaToPartitions[replica]))
	for i, partition := range n.replicaToPartitions[replica] {
		partitions[i] = int(partition)
	}
	return partitions
}

func (n *Node) Responsible(key int) int {
	partition := key % len(n.ring.replicaToPartitionToNode[0])
	for replica, partitionToNode := range n.ring.replicaToPartitionToNode {
		if int(partitionToNode[partition]) == n.index {
			return replica
		}
	}
	return -1
}

func (n *Node) ResponsibleForReplicaPartition(replica, partition int) bool {
	for _, partitionToNode := range n.ring.replicaToPartitionToNode {
		if int(partitionToNode[partition]) == n.index {
			return true
		}
	}
	return false
}
