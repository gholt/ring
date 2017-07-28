package ring

import (
	"math"
	"time"
)

// Node is a target of a ring assignment. For example, it might represent a
// disk drive in a large storage cluster.
type Node struct {
	// Disabled is whether this node can currently be assigned replicas.
	// See Capacity below for a bit more information.
	Disabled bool
	// Capacity indicates how many replicas this node should be assigned; it is
	// relative to other nodes' capacities.
	// 0 indicates the node should not have any assignments, but if it already
	// has some and they cannot yet be moved due to some restrictions, that is
	// okay for now.
	// 0 capacity differs from Disabled in that Disabled means that no
	// assignments can be made, overriding the restrictions.
	// 0 capacity might be used to "drain" a node for maintenance once enough
	// rebalances clear it of any assignments.
	Capacity uint32
	// TierIndexes specify which tiers a Node is within.
	// Tiers indicate the layout of the node with respect to other nodes. For
	// example, the lowest tier, tier 0, might be the server (where each node
	// represents a drive on that server). The next tier, 1, might then be the
	// power zone the server is in. The number of tiers is flexible, so later
	// an additional tier for geographic region could be added.
	// Different replicas of a given partition are attempted to be assigned to
	// distinct tiers at each level.
	// If a tier index is less than 0, it will be assumed to be 0.
	// If a tier index does not exist for a node (but exists for some other
	// node) it will be assumed to be 0.
	// See the package documentation Example (Tiers) for in-depth code
	// discussion to better understand tiers.
	TierIndexes []uint32
}

// Builder encapsulates the information needed to best balance ring assignments.
type Builder struct {
	Ring
	// Nodes are the targets of each replica assignment.
	Nodes []*Node
	// LastMovedBase is the base time for the LastMoved delta values.
	LastMovedBase time.Time
	// LastMoved is the number of minutes since LastMovedBase that replicas were
	// last moved; LastMoved[replica][partition] = minutes.
	LastMoved [][]uint16
	// MoveWait is the number of minutes that must elapse before a replica can
	// be moved again.
	// 0 will be treated as the default, which currently is 60 minutes.
	MoveWait uint16
	// PointsAllowed is the number of percentage points over or under that the
	// Builder will try to keep replica assignments within.
	// 0 will be treated as the default, which currently is 1 for one percent
	// extra or fewer replicas per node.
	PointsAllowed byte
	// MaxPartitionCount is the maximum number of partitions the builder should
	// grow to, capping the memory usage.
	// Estimate replicas*MaxPartitionCount*6 bytes of maximum memory usage.
	// Each Ring entry is 4 bytes (int32) and each LastMoved entry is 2 bytes
	// (uint16).
	// Values less than 2 will be treated as the default, which currently is
	// 8388608 and would, with 3 replicas, use about 150M of memory at max
	// size.
	MaxPartitionCount int
}

// Rebalance updates the b.Ring replica assignments, if needed and possible.
func (b *Builder) Rebalance() {
	if b.MoveWait == 0 {
		b.MoveWait = 60 // MoveWait default of 60 minutes.
	}
	validNodes := false
	for _, n := range b.Nodes {
		if !n.Disabled {
			validNodes = true
		}
	}
	if !validNodes {
		return
	}
	if len(b.Ring) == 0 {
		b.Ring = Ring{[]int32{-1, -1}}
		b.LastMoved = [][]uint16{{math.MaxUint16, math.MaxUint16}}
	}
	newBase := time.Now()
	d := int(newBase.Sub(b.LastMovedBase) / time.Minute)
	if d > 0 {
		var d16 uint16 = math.MaxUint16
		if d < math.MaxUint16 {
			d16 = uint16(d)
		}
		b.AddLastMoved(d16)
		b.LastMovedBase = newBase
	}
	b.growIfNeeded()
	newRebalancer(b).rebalance()
	nodes := make([]*Node, len(b.Nodes))
	copy(nodes, b.Nodes)
	r := make(Ring, len(b.Ring))
	for i := 0; i < len(b.Ring); i++ {
		r[i] = make([]int32, len(b.Ring[i]))
		copy(r[i], b.Ring[i])
	}
}

// ChangeReplicaCount will add or remove replicas so that len(b.Ring) == count.
// If replicas are added they will be unassigned: b.Ring[newReplica][*] = -1.
func (b *Builder) ChangeReplicaCount(count int) {
	if count < 1 {
		count = 1
	}
	if len(b.Ring) == 0 {
		b.Ring = Ring{[]int32{-1, -1}}
		b.LastMoved = [][]uint16{{math.MaxUint16, math.MaxUint16}}
	}
	if count < len(b.Ring) {
		b.Ring = b.Ring[:count]
		b.LastMoved = b.LastMoved[:count]
	} else if count > len(b.Ring) {
		partitionCount := len(b.Ring[0])
		for count > len(b.Ring) {
			newPartitionToNodeIndex := make([]int32, partitionCount)
			newPartitionToLastMove := make([]uint16, partitionCount)
			for i := 0; i < partitionCount; i++ {
				newPartitionToNodeIndex[i] = -1
				newPartitionToLastMove[i] = math.MaxUint16
			}
			b.Ring = append(b.Ring, newPartitionToNodeIndex)
			b.LastMoved = append(b.LastMoved, newPartitionToLastMove)
		}
	}
}

// AddLastMoved shifts forward the LastMoved records by the number of minutes
// given. This is done when the LastMovedBase is updated but can also be useful
// in testing, as the ring algorithms will not reassign replicas more often
// than once per MoveWait minutes in order to let reassignments take effect
// before moving the same replica yet again.
func (b *Builder) AddLastMoved(minutes uint16) {
	for _, partitionToLastMoved := range b.LastMoved {
		for partition := len(partitionToLastMoved) - 1; partition >= 0; partition-- {
			if math.MaxUint16-partitionToLastMoved[partition] < minutes {
				partitionToLastMoved[partition] = math.MaxUint16
			} else {
				partitionToLastMoved[partition] += minutes
			}
		}
	}
}

// RemoveNode will remove the node from the Builder.
//
// Note that this can be relatively expensive as all nodes that had been added
// after the removed node had been originally added will have their indexes
// shifted down one and many of the replica-to-partition-to-node assignments
// will have to be updated, as well as clearing any assignments that were to
// the removed node.
//
// Depending on the use case, it might be better to just leave a "dead" node in
// place and simply set it as inactive.
func (b *Builder) RemoveNode(nodeIndex int32) {
	if nodeIndex < 0 || int(nodeIndex) >= len(b.Nodes) {
		return
	}
	copy(b.Nodes[nodeIndex:], b.Nodes[nodeIndex+1:])
	b.Nodes = b.Nodes[:len(b.Nodes)-1]
	for _, partitionToNodeIndex := range b.Ring {
		for j := len(partitionToNodeIndex) - 1; j >= 0; j-- {
			if partitionToNodeIndex[j] == nodeIndex {
				partitionToNodeIndex[j] = -1
			} else if partitionToNodeIndex[j] > nodeIndex {
				partitionToNodeIndex[j]--
			}
		}
	}
}

func (b *Builder) growIfNeeded() {
	// Consider: Shrinking the partitionToNodeIndex slices doesn't happen
	// because it would normally cause more data movements than it's worth.
	// Perhaps in the future we can add detection of cases when shrinking makes
	// sense. This does mean if you change the MaxPartitionCount and the
	// current partition count is already over that, it will not reduce at this
	// time.
	if b.MaxPartitionCount < 2 {
		b.MaxPartitionCount = 8388608 // MaxPartitionCount default of 8388608 partitions.
	}
	if b.PartitionCount() >= b.MaxPartitionCount {
		return
	}
	replicaCount := len(b.Ring)
	// Calculate the partition count needed.
	// Each node is examined to see how much under or overweight it would be
	// and increasing the partition count until the difference is under the
	// points allowed.
	totalCapacity := uint64(0)
	for _, n := range b.Nodes {
		if !n.Disabled {
			totalCapacity += (uint64)(n.Capacity)
		}
	}
	partitionCount := len(b.Ring[0])
	doublings := uint(0)
	if b.PointsAllowed == 0 {
		b.PointsAllowed = 1 // PointsAllowed default of 1 percent.
	}
	pointsAllowed := float64(b.PointsAllowed) * 0.01
	for _, n := range b.Nodes {
		if n.Disabled {
			continue
		}
		desiredPartitionCount := float64(partitionCount) * float64(replicaCount) * (float64(n.Capacity) / float64(totalCapacity))
		under := (desiredPartitionCount - float64(int(desiredPartitionCount))) / desiredPartitionCount
		over := float64(0)
		if desiredPartitionCount > float64(int(desiredPartitionCount)) {
			over = (float64(int(desiredPartitionCount)+1) - desiredPartitionCount) / desiredPartitionCount
		}
		if under > pointsAllowed || over > pointsAllowed {
			partitionCount *= 2
			doublings++
			if partitionCount == b.MaxPartitionCount {
				break
			} else if partitionCount > b.MaxPartitionCount {
				partitionCount /= 2
				doublings--
				break
			}
		}
	}
	// Grow the partitionToNodeIndex slices if the partition count grew.
	if partitionCount > len(b.Ring[0]) {
		for replica := 0; replica < replicaCount; replica++ {
			partitionToNodeIndex := make([]int32, partitionCount)
			partitionToLastMoved := make([]uint16, partitionCount)
			for partition := 0; partition < partitionCount; partition++ {
				partitionToNodeIndex[partition] = b.Ring[replica][partition>>doublings]
				partitionToLastMoved[partition] = b.LastMoved[replica][partition>>doublings]
			}
			b.Ring[replica] = partitionToNodeIndex
			b.LastMoved[replica] = partitionToLastMoved
		}
	}
}
