package ring

import "github.com/gholt/holdme"

// BuilderNode is a node within a builder; a node represents a single
// assignment target of replicas of partitions, such as a single disk in a
// distributed storage system.
type BuilderNode struct {
	builder *Builder
	index   int
	info    string
}

// Info returns the user-defined info string; this info is not used directly by
// the builder.
func (n *BuilderNode) Info() string {
	return n.info
}

// SetInfo sets the user-defined info string; this info is not used directly by
// the builder.
func (n *BuilderNode) SetInfo(v string) {
	n.info = v
}

// Capacity specifies, relative to other nodes, how many assignments the node
// should have.
func (n *BuilderNode) Capacity() int {
	return n.builder.ring.NodeToCapacity[n.index]
}

// SetCapacity specifies, relative to other nodes, how many assignments the
// node should have.
func (n *BuilderNode) SetCapacity(v int) {
	n.builder.ring.NodeToCapacity[n.index] = v
}

// Group returns the parent group of the node; it may return nil if there is no
// parent group.
func (n *BuilderNode) Group() *BuilderGroup {
	return n.builder.groups[n.builder.ring.NodeToGroup[n.index]]
}

// SetGroup sets the parent group of the node; it may be set to nil to have no
// parent group.
func (n *BuilderNode) SetGroup(group *BuilderGroup) {
	n.builder.ring.NodeToGroup[n.index] = group.index
}

// Partitions returns the list of partitions assigned to this node; the list
// will be in ascending order with no duplicates.
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

// ReplicaPartitions returns the list of partitions assigned to this node for
// the given replica; the list will be in ascending order with no duplicates.
func (n *BuilderNode) ReplicaPartitions(replica int) []int {
	n.builder.ring.FillReplicaToNodeToPartitions()
	partitions := make([]int, len(n.builder.ring.ReplicaToNodeToPartitions[replica][n.index]))
	for i, partition := range n.builder.ring.ReplicaToNodeToPartitions[replica][n.index] {
		partitions[i] = int(partition)
	}
	return partitions
}

// Responsible returns the replica number this node is responsible for with
// respect to the key given; will return -1 if this node is not responsible for
// any replica for the key.
func (n *BuilderNode) Responsible(key int) int {
	partition := key % len(n.builder.ring.ReplicaToPartitionToNode[0])
	for replica, partitionToNode := range n.builder.ring.ReplicaToPartitionToNode {
		if int(partitionToNode[partition]) == n.index {
			return replica
		}
	}
	return -1
}

// ResponsibleForReplicaPartition returns true if this node is reponsible for
// the specific replica and partition given.
func (n *BuilderNode) ResponsibleForReplicaPartition(replica, partition int) bool {
	for _, partitionToNode := range n.builder.ring.ReplicaToPartitionToNode {
		if int(partitionToNode[partition]) == n.index {
			return true
		}
	}
	return false
}

// Assign will override the current builder's assignment and set a specific
// replica of a partition to this specific node. This is mostly just useful for
// testing, as future calls to Rebalance may move this assignment.
func (n *BuilderNode) Assign(replica, partition int) {
	n.builder.Assign(replica, partition, n)
}
