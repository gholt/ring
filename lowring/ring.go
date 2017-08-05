package lowring

// Ring stores the assignments of replicas of partitions to node indexes;
// Ring[replica][partition] = node index.
//
// All Ring[replica] slices will be the same length.
//
// A NodeIndexNil value indicates no node assignment.
type Ring [][]NodeIndexType

func (r Ring) ReplicaCount() int {
	return len(r)
}

func (r Ring) PartitionCount() int {
	if len(r) > 0 {
		return len(r[0])
	}
	return 0
}

func (r Ring) Equal(r2 interface{}) bool {
	r2t, ok := r2.(Ring)
	if !ok {
		return false
	}
	if len(r) != len(r2t) {
		return false
	}
	for replica := range r {
		partitionToNodeIndex := r[replica]
		partitionToNodeIndex2 := r2t[replica]
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

func (r Ring) Copy() Ring {
	r2 := make(Ring, len(r))
	for replica := range r {
		r2[replica] = make([]NodeIndexType, len(r[replica]))
		copy(r2[replica], r[replica])
	}
	return r2
}
