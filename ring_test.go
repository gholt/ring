package ring

import "testing"

func TestRingVersion(t *testing.T) {
	v := (&ringImpl{version: 1}).Version()
	if v != 1 {
		t.Fatalf("Version() gave %d instead of 1", v)
	}
}

func TestRingPartitionBitCount(t *testing.T) {
	v := (&ringImpl{partitionBitCount: 1}).PartitionBitCount()
	if v != 1 {
		t.Fatalf("PartitionBitCount() gave %d instead of 1", v)
	}
}

func TestRingReplicaCount(t *testing.T) {
	v := (&ringImpl{replicaToPartitionToNodeIndex: make([][]int32, 3)}).ReplicaCount()
	if v != 3 {
		t.Fatalf("ReplicaCount() gave %d instead of 3", v)
	}
}

func TestRingLocalNodeID(t *testing.T) {
	v := (&ringImpl{localNodeIndex: -1}).LocalNodeID()
	if v != 0 {
		t.Fatalf("LocalNodeID() gave %d instead of 0", v)
	}
	v = (&ringImpl{localNodeIndex: 0, nodeIDs: []uint64{123, 456, 789}}).LocalNodeID()
	if v != 123 {
		t.Fatalf("LocalNodeID() gave %d instead of 0", v)
	}
	v = (&ringImpl{localNodeIndex: 1, nodeIDs: []uint64{123, 456, 789}}).LocalNodeID()
	if v != 456 {
		t.Fatalf("LocalNodeID() gave %d instead of 0", v)
	}
}

func TestRingResponsible(t *testing.T) {
	v := (&ringImpl{localNodeIndex: -1}).Responsible(123)
	if v {
		t.Fatal("Responsible(123) gave true instead of false")
	}
	d := make([][]int32, 3)
	d[0] = []int32{0, 1, 2}
	d[1] = []int32{3, 4, 5}
	d[2] = []int32{6, 7, 8}
	v = (&ringImpl{localNodeIndex: 0, replicaToPartitionToNodeIndex: d}).Responsible(0)
	if !v {
		t.Fatal("Responsible(0) gave false instead of true")
	}
	v = (&ringImpl{localNodeIndex: 0, replicaToPartitionToNodeIndex: d}).Responsible(1)
	if v {
		t.Fatal("Responsible(1) gave true instead of false")
	}
}

func TestRingResponsibleIDs(t *testing.T) {
	d := make([][]int32, 3)
	d[0] = []int32{0, 1, 2}
	d[1] = []int32{3, 4, 5}
	d[2] = []int32{6, 7, 8}
	v := (&ringImpl{nodeIDs: []uint64{10, 11, 12, 13, 14, 15, 16, 17, 18}, replicaToPartitionToNodeIndex: d}).ResponsibleIDs(0)
	if len(v) != 3 || v[0] != 10 || v[1] != 13 || v[2] != 16 {
		t.Fatalf("ResponsibleIDs(0) gave %v instead of [10 13 16]", v)
	}
	v = (&ringImpl{nodeIDs: []uint64{10, 11, 12, 13, 14, 15, 16, 17, 18}, replicaToPartitionToNodeIndex: d}).ResponsibleIDs(2)
	if len(v) != 3 || v[0] != 12 || v[1] != 15 || v[2] != 18 {
		t.Fatalf("ResponsibleIDs(2) gave %v instead of [12 15 18]", v)
	}
}
