package ring

// Ring stores the assignments of replicas of partitions to node indexes;
// Ring[replica][partition] = node index.
//
// All Ring[replica] slices will be the same length.
//
// A negative int32 value indicates no node assignment.
//
// Usually these are generated and maintained by the Builder.
type Ring [][]int32

// ReplicaCount is a convenience method for len(r).
func (r Ring) ReplicaCount() int {
	return len(r)
}

// PartitionCount is a convenience method for len(r[0]) if len(r) > 0 else 0.
func (r Ring) PartitionCount() int {
	if len(r) > 0 {
		return len(r[0])
	}
	return 0
}

// RingEqual is true if the size and all the assignments of the given ring are the
// same as this ring.
//
// Note: This only can compare the node index value assignments. If the indexes
// point to different node information, that is outside the Ring itself. For
// example, it is possible you could remove a node from a builder, add another
// node, rebalance, and end up with the same index values assigned.
func (r Ring) RingEqual(r2 Ring) bool {
	if len(r) != len(r2) {
		return false
	}
	for replica := range r {
		partitionToNodeIndex := r[replica]
		partitionToNodeIndex2 := r2[replica]
		if len(partitionToNodeIndex) != len(partitionToNodeIndex2) {
			return false
		}
		for partition := range partitionToNodeIndex {
			if partitionToNodeIndex[partition] != partitionToNodeIndex2[partition] {
				return false
			}
		}
	}
	return true
}

// RingDuplicate returns a duplicate of the ring; a deep copy of all the
// replica assignments.
func (r Ring) RingDuplicate() Ring {
	r2 := make(Ring, len(r))
	for replica := range r {
		r2[replica] = make([]int32, len(r[replica]))
		copy(r2[replica], r[replica])
	}
	return r2
}
