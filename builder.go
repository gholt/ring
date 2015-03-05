package ring

import "time"

type Builder struct {
	version                       int64
	nodes                         []Node
	partitionBitCount             uint16
	replicaToPartitionToNodeIndex [][]int32
	pointsAllowed                 int
	maxPartitionBitCount          uint16
}

func NewBuilder(replicaCount int) *Builder {
	b := &Builder{
		nodes:                         make([]Node, 0),
		partitionBitCount:             1,
		replicaToPartitionToNodeIndex: make([][]int32, replicaCount),
		pointsAllowed:                 1,
		// 1 << 23 is 8388608 which, with 3 replicas, would use about 100M of
		// memory.
		maxPartitionBitCount: 23,
	}
	for replica := 0; replica < replicaCount; replica++ {
		b.replicaToPartitionToNodeIndex[replica] = []int32{-1, -1}
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

func (b *Builder) MaxPartitionBitCount() uint16 {
	return b.maxPartitionBitCount
}

func (b *Builder) SetMaxPartitionBitCount(count uint16) {
	b.maxPartitionBitCount = count
}

func (b *Builder) Add(n Node) {
	b.nodes = append(b.nodes, n)
}

// Remove will remove the node from the list of nodes for this builder/ring.
// Note that this can be relatively expensive as all nodes that had been added
// after the removed node had been originally added will have their internal
// indexes shifted down one and all the replica-to-partition-to-node indexing
// will have to be updated, as well as clearing any assignments that were to
// the removed node. Normally it is better to just leave a "dead" node in place
// and simply set it as not Active().
func (b *Builder) Remove(nodeID uint64) {
	for i, node := range b.nodes {
		if node.NodeID() == nodeID {
			copy(b.nodes[i:], b.nodes[i+1:])
			b.nodes = b.nodes[:len(b.nodes)-1]
			for _, partitionToNodeIndex := range b.replicaToPartitionToNodeIndex {
				for j := len(partitionToNodeIndex) - 1; j >= 0; j-- {
					if partitionToNodeIndex[j] == int32(i) {
						partitionToNodeIndex[j] = -1
					} else if partitionToNodeIndex[j] > int32(i) {
						partitionToNodeIndex[j]--
					}
				}
			}
			break
		}
	}
}

func (b *Builder) Node(nodeID uint64) Node {
	for _, node := range b.nodes {
		if node.NodeID() == nodeID {
			return node
		}
	}
	return nil
}

// Ring returns a Ring instance of the data defined by the builder. This will
// cause any pending rebalancing actions to be performed. The Ring returned
// will be immutable; to obtain updated ring data, Ring() must be called again.
// The localNodeID is so the Ring instance can provide local responsibility
// information; you can give 0 if you don't intend to use those features.
func (b *Builder) Ring(localNodeID uint64) Ring {
	if b.resizeIfNeeded() {
		b.version = time.Now().UnixNano()
	}
	if newRebalancer(b).rebalance() {
		b.version = time.Now().UnixNano()
	}
	localNodeIndex := int32(-1)
	nodes := make([]Node, len(b.nodes))
	copy(nodes, b.nodes)
	for i, node := range nodes {
		if node.NodeID() == localNodeID {
			localNodeIndex = int32(i)
		}
	}
	replicaToPartitionToNodeIndex := make([][]int32, len(b.replicaToPartitionToNodeIndex))
	for i := 0; i < len(replicaToPartitionToNodeIndex); i++ {
		replicaToPartitionToNodeIndex[i] = make([]int32, len(b.replicaToPartitionToNodeIndex[i]))
		copy(replicaToPartitionToNodeIndex[i], b.replicaToPartitionToNodeIndex[i])
	}
	return &ringImpl{
		version:           b.version,
		localNodeIndex:    localNodeIndex,
		partitionBitCount: b.partitionBitCount,
		nodes:             nodes,
		replicaToPartitionToNodeIndex: replicaToPartitionToNodeIndex,
	}
}

func (b *Builder) resizeIfNeeded() bool {
	if b.partitionBitCount >= b.maxPartitionBitCount {
		return false
	}
	replicaCount := len(b.replicaToPartitionToNodeIndex)
	// Calculate the partition count needed.
	// Each node is examined to see how much under or overweight it would be
	// and increasing the partition count until the difference is under the
	// points allowed.
	totalCapacity := uint64(0)
	for _, node := range b.nodes {
		if node.Active() {
			totalCapacity += (uint64)(node.Capacity())
		}
	}
	partitionCount := len(b.replicaToPartitionToNodeIndex[0])
	partitionBitCount := b.partitionBitCount
	pointsAllowed := float64(b.pointsAllowed) * 0.01
	for _, node := range b.nodes {
		if !node.Active() {
			continue
		}
		desiredPartitionCount := float64(partitionCount) * float64(replicaCount) * (float64(node.Capacity()) / float64(totalCapacity))
		under := (desiredPartitionCount - float64(int(desiredPartitionCount))) / desiredPartitionCount
		over := float64(0)
		if desiredPartitionCount > float64(int(desiredPartitionCount)) {
			over = (float64(int(desiredPartitionCount)+1) - desiredPartitionCount) / desiredPartitionCount
		}
		if under > pointsAllowed || over > pointsAllowed {
			partitionCount <<= 1
			partitionBitCount++
			if partitionBitCount == b.maxPartitionBitCount {
				break
			}
		}
	}
	// Grow the partitionToNodeIndex slices if the partition count grew.
	if partitionCount > len(b.replicaToPartitionToNodeIndex[0]) {
		shift := partitionBitCount - b.partitionBitCount
		for replica := 0; replica < replicaCount; replica++ {
			partitionToNodeIndex := make([]int32, partitionCount)
			for partition := 0; partition < partitionCount; partition++ {
				partitionToNodeIndex[partition] = b.replicaToPartitionToNodeIndex[replica][partition>>shift]
			}
			b.replicaToPartitionToNodeIndex[replica] = partitionToNodeIndex
		}
		b.partitionBitCount = partitionBitCount
		return true
	}
	// TODO: Shrinking the partitionToNodeIndex slices doesn't happen because
	// it would normally cause more data movements than it's worth. Perhaps in
	// the future we can add detection of cases when shrinking makes sense.
	return false
}
