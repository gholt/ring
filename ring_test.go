package ring

import "testing"

type testNode struct {
	id uint64
}

func (n *testNode) NodeID() uint64 {
	return n.id
}

func (n *testNode) Active() bool {
	return true
}

func (n *testNode) Capacity() uint32 {
	return 1
}

func (n *testNode) TierValues() []int {
	return nil
}

func (n *testNode) Address() string {
	return ""
}

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

func TestRingLocalNode(t *testing.T) {
	v := (&ringImpl{localNodeIndex: -1}).LocalNode()
	if v != nil {
		t.Fatalf("LocalNode() gave %v instead of nil", v)
	}
	v = (&ringImpl{localNodeIndex: 0, nodes: []Node{&testNode{id: 123}, &testNode{456}, &testNode{789}}}).LocalNode()
	if v.NodeID() != 123 {
		t.Fatalf("LocalNode() gave %v instead of 0", v)
	}
	v = (&ringImpl{localNodeIndex: 1, nodes: []Node{&testNode{id: 123}, &testNode{id: 456}, &testNode{id: 789}}}).LocalNode()
	if v.NodeID() != 456 {
		t.Fatalf("LocalNode() gave %v instead of 0", v)
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
	v := (&ringImpl{nodes: []Node{&testNode{id: 10}, &testNode{id: 11}, &testNode{id: 12}, &testNode{id: 13}, &testNode{id: 14}, &testNode{id: 15}, &testNode{id: 16}, &testNode{id: 17}, &testNode{id: 18}}, replicaToPartitionToNodeIndex: d}).ResponsibleNodes(0)
	if len(v) != 3 || v[0].NodeID() != 10 || v[1].NodeID() != 13 || v[2].NodeID() != 16 {
		t.Fatalf("ResponsibleNodes(0) gave %v instead of [10 13 16]", v)
	}
	v = (&ringImpl{nodes: []Node{&testNode{id: 10}, &testNode{id: 11}, &testNode{id: 12}, &testNode{id: 13}, &testNode{id: 14}, &testNode{id: 15}, &testNode{id: 16}, &testNode{id: 17}, &testNode{id: 18}}, replicaToPartitionToNodeIndex: d}).ResponsibleNodes(2)
	if len(v) != 3 || v[0].NodeID() != 12 || v[1].NodeID() != 15 || v[2].NodeID() != 18 {
		t.Fatalf("ResponsibleNodes(2) gave %v instead of [12 15 18]", v)
	}
}
