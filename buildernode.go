package ring

import "github.com/gholt/holdme"

type BuilderNode struct {
	builder *Builder
	index   int
	info    string
}

func (n *BuilderNode) Info() string {
	return n.info
}

func (n *BuilderNode) SetInfo(v string) {
	n.info = v
}

func (n *BuilderNode) Capacity() int {
	return n.builder.ring.NodeToCapacity[n.index]
}

func (n *BuilderNode) SetCapacity(v int) {
	n.builder.ring.NodeToCapacity[n.index] = v
}

func (n *BuilderNode) Group() *BuilderGroup {
	return n.builder.groups[n.builder.ring.NodeToGroup[n.index]]
}

func (n *BuilderNode) SetGroup(group *BuilderGroup) {
	n.builder.ring.NodeToGroup[n.index] = group.index
}

func (n *BuilderNode) Partitions() []int {
	n.builder.ring.FillReplicaToNodeToPartitions()
	var partitions holdme.OrderedIntsNoDups
	for _, nodeToPartitions := range n.builder.ring.ReplicaToNodeToPartitions {
		for _, partition := range nodeToPartitions[n.index] {
			partitions.Add(int(partition))
		}
	}
	return partitions
}

func (n *BuilderNode) PartitionsForReplica(replica int) []int {
	n.builder.ring.FillReplicaToNodeToPartitions()
	partitions := make([]int, len(n.builder.ring.ReplicaToNodeToPartitions[replica][n.index]))
	for i, partition := range n.builder.ring.ReplicaToNodeToPartitions[replica][n.index] {
		partitions[i] = int(partition)
	}
	return partitions
}

func (n *BuilderNode) Responsible(key int) int {
	partition := key % len(n.builder.ring.ReplicaToPartitionToNode[0])
	for replica, partitionToNode := range n.builder.ring.ReplicaToPartitionToNode {
		if int(partitionToNode[partition]) == n.index {
			return replica
		}
	}
	return -1
}

func (n *BuilderNode) ResponsibleForReplicaPartition(replica, partition int) bool {
	for _, partitionToNode := range n.builder.ring.ReplicaToPartitionToNode {
		if int(partitionToNode[partition]) == n.index {
			return true
		}
	}
	return false
}

func (n *BuilderNode) Assign(replica, partition int) {
	n.builder.Assign(replica, partition, n)
}
