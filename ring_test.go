package ring

import "testing"

type testNode struct {
	id         uint64
	inactive   bool
	capacity   uint32
	tierValues []int
	address    string
}

func (node *testNode) NodeID() uint64 {
	return node.id
}

func (node *testNode) Active() bool {
	return !node.inactive
}

func (node *testNode) Capacity() uint32 {
	return node.capacity
}

func (node *testNode) TierValues() []int {
	return node.tierValues
}

func (node *testNode) Address() string {
	return node.address
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

func TestRingNodes(t *testing.T) {
	v := (&ringImpl{nodes: []Node{&testNode{id: 1}, &testNode{id: 2}}}).Nodes()
	if len(v) != 2 {
		t.Fatalf("Nodes() gave %d entries instead of 2", len(v))
	}
	if v[0].NodeID() != 1 || v[1].NodeID() != 2 {
		t.Fatalf("Nodes() gave [%v, %v] instead of [&{1}, &{2}]", v[0], v[1])
	}
}

func TestRingNode(t *testing.T) {
	v := (&ringImpl{nodes: []Node{&testNode{id: 1}, &testNode{id: 2}}}).Node(1)
	if v.NodeID() != 1 {
		t.Fatalf("Nodes() gave %v instead of &{1}", v)
	}
	v = (&ringImpl{nodes: []Node{&testNode{id: 1}, &testNode{id: 2}}}).Node(2)
	if v.NodeID() != 2 {
		t.Fatalf("Nodes() gave %v instead of &{2}", v)
	}
	v = (&ringImpl{nodes: []Node{&testNode{id: 1}, &testNode{id: 2}}}).Node(3)
	if v != nil {
		t.Fatalf("Nodes() gave %v instead of nil", v)
	}
}

func TestRingLocalNode(t *testing.T) {
	v := (&ringImpl{localNodeIndex: -1}).LocalNode()
	if v != nil {
		t.Fatalf("LocalNode() gave %v instead of nil", v)
	}
	v = (&ringImpl{localNodeIndex: 0, nodes: []Node{&testNode{id: 123}, &testNode{id: 456}, &testNode{id: 789}}}).LocalNode()
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

func TestRingStats(t *testing.T) {
	s := (&ringImpl{
		partitionBitCount: 2,
		nodes: []Node{
			&testNode{id: 0, capacity: 100},
			&testNode{id: 1, capacity: 101},
			&testNode{id: 2, capacity: 102},
			&testNode{id: 3, capacity: 103},
			&testNode{id: 4, capacity: 104},
			&testNode{id: 5, inactive: true},
		},
		replicaToPartitionToNodeIndex: [][]int32{
			[]int32{0, 1, 2, 3},
			[]int32{4, 0, 1, 2},
			[]int32{3, 4, 0, 1},
		},
	}).Stats()
	if s.ReplicaCount != 3 {
		t.Fatalf("RingStats gave ReplicaCount of %d instead of 3", s.ReplicaCount)
	}
	if s.NodeCount != 6 {
		t.Fatalf("RingStats gave NodeCount of %d instead of 6", s.NodeCount)
	}
	if s.InactiveNodeCount != 1 {
		t.Fatalf("RingStats gave InactiveNodeCount of %d instead of 1", s.InactiveNodeCount)
	}
	if s.PartitionBitCount != 2 {
		t.Fatalf("RingStats gave PartitionBitCount of %d instead of 2", s.PartitionBitCount)
	}
	if s.PartitionCount != 4 {
		t.Fatalf("RingStats gave PartitionCount of %d instead of 4", s.PartitionCount)
	}
	if s.TotalCapacity != 510 {
		t.Fatalf("RingStats gave TotalCapacity of %d instead of 510", s.TotalCapacity)
	}
	// Node id 4 should be most underweight.
	d := float64(104.0) / 510.0 * 4 * 3
	v := float64(100.0) * (d - 2) / d
	if s.MaxUnderNodePercentage != v {
		t.Fatalf("RingStats gave MaxUnderNodePercentage of %v instead of %v", s.MaxUnderNodePercentage, v)
	}
	// Node id 0 should be most overweight.
	d = float64(100.0) / 510.0 * 4 * 3
	v = float64(100.0) * (3 - d) / d
	if s.MaxOverNodePercentage != v {
		t.Fatalf("RingStats gave MaxOverNodePercentage of %v instead of %v", s.MaxOverNodePercentage, v)
	}
}
