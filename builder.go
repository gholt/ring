package ring

import (
	"math/rand"
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
	//
	// Values less than 1 are treated as 0 and indicate the node should not
	// have any assignments, but if it already has some and they cannot yet be
	// moved due to some restrictions, that is okay for now.
	//
	// 0 capacity differs from Disabled in that Disabled means that no
	// assignments can be made, overriding the restrictions.
	//
	// 0 capacity might be used to "drain" a node for maintenance once enough
	// rebalances clear it of any assignments.
	Capacity int

	// TierIndexes specify which tiers a Node is within.
	//
	// Tiers indicate the layout of the node with respect to other nodes. For
	// example, the lowest tier, tier 0, might be the server (where each node
	// represents a drive on that server). The next tier, 1, might then be the
	// power zone the server is in. The number of tiers is flexible, so later
	// an additional tier for geographic region could be added.
	//
	// Different replicas of a given partition are attempted to be assigned to
	// distinct tiers at each level.
	//
	// If a tier index is less than 0, it will be assumed to be 0.
	//
	// If a tier index does not exist for a node (but exists for some other
	// node) it will be assumed to be 0.
	//
	// See the package documentation Example (Tiers) for in-depth code
	// discussion to better understand tiers.
	TierIndexes []int
}

// Builder encapsulates the information needed to best balance ring assignments.
type Builder struct {
	Ring

	// Nodes are the targets of each replica assignment.
	//
	// You can add nodes to this slice directly but if you wish to remove
	// nodes, use the Builder's RemoveNode method. Indexes to this slice are
	// used elsewhere and need to be updated if they change. Currently there is
	// no method to help with completely changing the order of Nodes. If you
	// really want to do that, you will have to update the Builder's Ring
	// assignments yourself.
	Nodes []*Node

	// LastMovedBase is the base time for the LastMoved delta values.
	LastMovedBase time.Time

	// LastMoved is the number of LastMovedUnits since LastMovedBase that
	// replicas were last moved; LastMoved[replica][partition] = elapsed.
	// This is used to restrict moving these replicas again.
	LastMoved [][]LastMovedType

	// LastMovedUnit is the duration that LastMoved values are measured in.
	//
	// 0 will be treated as the default, which currently is time.Minute.
	LastMovedUnit time.Duration

	// MoveWait is how much time that must elapse before a replica can be moved
	// again.
	//
	// 0 will be treated as the default, which currently is time.Hour.
	MoveWait time.Duration

	// MovesPerPartition is the highest number of replicas for a partition that
	// can be in motion within the MoveWait window.
	//
	// For full copy replicas, you probably want this set to no more than half
	// the replica count. For erasure coding, you probably want this set to no
	// more than "m", the maximum number of failure chunks.
	//
	// 0 (or less) will be treated as the default, which currently is
	// int(replicas/2), but at least 1.
	MovesPerPartition int

	// PointsAllowed is the number of percentage points over or under that the
	// Builder will try to keep replica assignments within. For example, if set
	// to 1% and a node should receive 100 replica assignments, the Builder
	// will target 99-101 assignments to that node. This is not guaranteed,
	// just a target.
	//
	// This setting will affect how quickly the partition count will increase.
	// If you feel your use case can handle an over/under of 10%, that could
	// end up with smaller rings if memory is more of a concern. However, just
	// setting the MaxPartitionCount is probably a better approach in such a
	// case.
	//
	// 0 will be treated as the default, which currently is 1 for one percent
	// extra or fewer replicas per node.
	PointsAllowed int

	// MaxPartitionCount is the maximum number of partitions the builder should
	// grow to. This keeps the rebalancer from running too long trying to
	// achieve desired balances and also caps memory usage.
	//
	// 0 (or less) will be treated as the default, which currently is 8388608.
	MaxPartitionCount int

	// rnd is used to add a some shuffling to the rebalancing algorithms. This
	// can help keep nodes from mirroring each other too much. Mirroring
	// meaning that two nodes share more partitions than they really should, so
	// if both fail at the same time more partitions lose multiple replicas
	// than they really should.
	//
	// If nil, this will be set to rand.New(rand.NewSource(0)) -- yes, this
	// means it's the same seed always, which is fine. We want the shuffling to
	// help with distribution, but providing repeatable results for users is
	// also beneficial.
	//
	// It can be set to noRnd to skip the shuffling, useful for tests comparing
	// shuffling versus not.
	rnd *rand.Rand
}

var noRnd = &rand.Rand{}

// Rebalance updates the b.Ring replica assignments, if needed and possible.
func (b *Builder) Rebalance() {
	validNodes := false
	for _, n := range b.Nodes {
		if !n.Disabled {
			validNodes = true
		}
	}
	if !validNodes {
		return
	}
	if b.rnd == nil {
		b.rnd = rand.New(rand.NewSource(0))
	}
	if b.LastMovedUnit == 0 {
		b.LastMovedUnit = time.Minute
	}
	if b.MoveWait == 0 {
		b.MoveWait = time.Hour
	}
	if len(b.Ring) == 0 {
		b.Ring = Ring{[]NodeIndexType{NodeIndexNil}}
		b.LastMoved = [][]LastMovedType{{LastMovedMax}}
	}
	newBase := time.Now()
	b.ShiftLastMoved(newBase.Sub(b.LastMovedBase))
	b.growIfNeeded()
	newRebalancer(b).rebalance()
}

// SetReplicaCount will add or remove replicas so that len(b.Ring) == count.
// If replicas are added they will be unassigned: b.Ring[newReplica][*] = NodeIndexNil
func (b *Builder) SetReplicaCount(count int) {
	if count < 1 {
		count = 1
	}
	if len(b.Ring) == 0 {
		b.Ring = Ring{[]NodeIndexType{NodeIndexNil}}
		b.LastMoved = [][]LastMovedType{{LastMovedMax}}
	}
	if count < len(b.Ring) {
		b.Ring = b.Ring[:count]
		b.LastMoved = b.LastMoved[:count]
	} else if count > len(b.Ring) {
		partitionCount := len(b.Ring[0])
		for count > len(b.Ring) {
			newPartitionToNodeIndex := make([]NodeIndexType, partitionCount)
			newPartitionToLastMove := make([]LastMovedType, partitionCount)
			for i := 0; i < partitionCount; i++ {
				newPartitionToNodeIndex[i] = NodeIndexNil
				newPartitionToLastMove[i] = LastMovedMax
			}
			b.Ring = append(b.Ring, newPartitionToNodeIndex)
			b.LastMoved = append(b.LastMoved, newPartitionToLastMove)
		}
	}
}

// ShiftLastMoved shifts the LastMoved records by the duration given, as if
// that much more time has passed since all replicas of all partitions have
// been moved.
//
// This is done automatically when Rebalance is called and therefore
// LastMovedBase is updated but can also be useful in testing, as the ring
// algorithms will restrict the movement of replicas based on this information.
func (b *Builder) ShiftLastMoved(d time.Duration) {
	if b.LastMovedUnit == 0 {
		b.LastMovedUnit = time.Minute
	}
	units := LastMovedType(LastMovedMax)
	if int64(d/b.LastMovedUnit) < int64(LastMovedMax) {
		units = LastMovedType(d / b.LastMovedUnit)
	}
	for _, partitionToLastMoved := range b.LastMoved {
		for partition := len(partitionToLastMoved) - 1; partition >= 0; partition-- {
			if LastMovedMax-partitionToLastMoved[partition] < units {
				partitionToLastMoved[partition] = LastMovedMax
			} else {
				partitionToLastMoved[partition] += units
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
// place and simply set it as disabled.
func (b *Builder) RemoveNode(nodeIndex NodeIndexType) {
	if nodeIndex == NodeIndexNil || int(nodeIndex) >= len(b.Nodes) {
		return
	}
	copy(b.Nodes[nodeIndex:], b.Nodes[nodeIndex+1:])
	b.Nodes = b.Nodes[:len(b.Nodes)-1]
	for _, partitionToNodeIndex := range b.Ring {
		for j := len(partitionToNodeIndex) - 1; j >= 0; j-- {
			if partitionToNodeIndex[j] == nodeIndex {
				partitionToNodeIndex[j] = NodeIndexNil
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
	// sense. Perhaps providing a way to do multiple rebalances over time to
	// distribute partitions where halving wouldn't move anything on the
	// shrinking rebalance.
	//
	// The current restriction of no shrinking does mean that if you change the
	// MaxPartitionCount and the current partition count is already over that,
	// it will not reduce at this time.
	if b.MaxPartitionCount < 1 {
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
	totalCapacity := float64(0)
	for _, n := range b.Nodes {
		if !n.Disabled {
			totalCapacity += float64(n.Capacity)
		}
	}
	partitionCount := len(b.Ring[0])
	doublings := uint(0)
	if b.PointsAllowed == 0 {
		b.PointsAllowed = 1 // PointsAllowed default of 1 percent.
	}
	pointsAllowed := float64(b.PointsAllowed) * 0.01
	keepTrying := true
	for keepTrying && partitionCount < b.MaxPartitionCount {
		keepTrying = false
		for _, n := range b.Nodes {
			if n.Disabled {
				continue
			}
			desiredPartitionCount := float64(partitionCount) * float64(replicaCount) * (float64(n.Capacity) / totalCapacity)
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
				keepTrying = true
			}
		}
	}
	// Grow the partitionToNodeIndex slices if the partition count grew.
	if partitionCount > len(b.Ring[0]) {
		for replica := 0; replica < replicaCount; replica++ {
			partitionToNodeIndex := make([]NodeIndexType, partitionCount)
			partitionToLastMoved := make([]LastMovedType, partitionCount)
			for partition := 0; partition < partitionCount; partition++ {
				partitionToNodeIndex[partition] = b.Ring[replica][partition>>doublings]
				partitionToLastMoved[partition] = b.LastMoved[replica][partition>>doublings]
			}
			b.Ring[replica] = partitionToNodeIndex
			b.LastMoved[replica] = partitionToLastMoved
		}
	}
}

// Stats gives an overview of the state and health of the Builder.
// It is returned by the Builder.Stats() method.
type Stats struct {
	ReplicaCount      int
	EnabledNodeCount  int
	DisabledNodeCount int
	PartitionCount    int
	UnassignedCount   int
	EnabledCapacity   float64
	DisabledCapacity  float64
	// MaxUnderNodePercentage is the percentage a node is underweight, or has
	// less data assigned to it than its capacity would indicate it desires.
	MaxUnderNodePercentage float64
	MaxUnderNodeIndex      NodeIndexType
	// MaxOverNodePercentage is the percentage a node is overweight, or has
	// more data assigned to it than its capacity would indicate it desires.
	MaxOverNodePercentage float64
	MaxOverNodeIndex      NodeIndexType
}

// Stats gives an overview of the state and health of the Builder.
func (b *Builder) Stats() *Stats {
	stats := &Stats{
		ReplicaCount:   b.ReplicaCount(),
		PartitionCount: b.PartitionCount(),
	}
	nodeIndexToPartitionCount := make([]int, len(b.Nodes))
	for _, partitionToNodeIndex := range b.Ring {
		for _, nodeIndex := range partitionToNodeIndex {
			if nodeIndex == NodeIndexNil {
				stats.UnassignedCount++
			} else {
				nodeIndexToPartitionCount[nodeIndex]++
			}
		}
	}
	for _, n := range b.Nodes {
		if n.Disabled {
			stats.DisabledNodeCount++
			if n.Capacity > 0 {
				stats.DisabledCapacity += float64(n.Capacity)
			}
		} else {
			stats.EnabledNodeCount++
			if n.Capacity > 0 {
				stats.EnabledCapacity += float64(n.Capacity)
			}
		}
	}
	for nodeIndex, n := range b.Nodes {
		if n.Disabled {
			continue
		}
		desiredPartitionCount := float64(n.Capacity) / stats.EnabledCapacity * float64(stats.PartitionCount) * float64(stats.ReplicaCount)
		actualPartitionCount := float64(nodeIndexToPartitionCount[nodeIndex])
		if desiredPartitionCount > actualPartitionCount {
			under := 100.0 * (desiredPartitionCount - actualPartitionCount) / desiredPartitionCount
			if under > stats.MaxUnderNodePercentage {
				stats.MaxUnderNodePercentage = under
				stats.MaxUnderNodeIndex = NodeIndexType(nodeIndex)
			}
		} else if desiredPartitionCount < actualPartitionCount {
			over := 100.0 * (actualPartitionCount - desiredPartitionCount) / desiredPartitionCount
			if over > stats.MaxOverNodePercentage {
				stats.MaxOverNodePercentage = over
				stats.MaxOverNodeIndex = NodeIndexType(nodeIndex)
			}
		}
	}
	return stats
}
