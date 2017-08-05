package lowring

import (
	"math/rand"
	"time"
)

type Node struct {
	Disabled bool
	// Capacity less than zero is treated as zero.
	Capacity int
	// TierIndexes less than zero are treated as zero, as are missing tiers.
	TierIndexes []int
}

type Builder struct {
	Ring

	// Nodes can be appended directly but RemoveNode should be used for
	// removals. Reordering isn't supported at all.
	Nodes []*Node

	// LastMovedBase is the base time for the LastMoved delta values.
	LastMovedBase time.Time

	// LastMoved is the number of LastMovedUnits since LastMovedBase that
	// replicas were last moved.
	//
	// LastMoved[replica][partition] * LastMovedUnit = elapsed since
	// LastMovedBase.
	LastMoved [][]LastMovedType

	// LastMovedUnit less than or equal to zero will be treated as the default,
	// which currently is time.Minute.
	LastMovedUnit time.Duration

	// MoveWait less than or equal to 0 will be treated as the default, which
	// currently is time.Hour.
	MoveWait time.Duration

	// MovesPerPartition less than or equal to zero will be treated as the
	// default, which currently is int(replicas/2), but at least 1.
	MovesPerPartition int

	// PointsAllowed less than or equal to zero will be treated as the default,
	// which currently is 1 for one percent extra or fewer replicas per node.
	PointsAllowed int

	// MaxPartitionCount less than or equal to zero will be treated as the
	// default, which currently is 8388608.
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

// SetReplicaCount makes len(b.Ring) = count.
//
// If replicas are added they will be unassigned: b.Ring[newReplica][*] = NodeIndexNil.
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
// This is done automatically when Rebalance is called (and therefore
// LastMovedBase is updated) but can also be useful in testing, as the ring
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

func (b *Builder) Equal(b2 interface{}) bool {
	return b == b2
}

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
