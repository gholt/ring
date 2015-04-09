package ring

import (
	"bytes"
	"testing"
)

func TestRingVersion(t *testing.T) {
	v := (&ring{version: 1}).Version()
	if v != 1 {
		t.Fatalf("Version() gave %d instead of 1", v)
	}
}

func TestRingPartitionBitCount(t *testing.T) {
	v := (&ring{partitionBitCount: 1}).PartitionBitCount()
	if v != 1 {
		t.Fatalf("PartitionBitCount() gave %d instead of 1", v)
	}
}

func TestRingReplicaCount(t *testing.T) {
	v := (&ring{replicaToPartitionToNodeIndex: make([][]int32, 3)}).ReplicaCount()
	if v != 3 {
		t.Fatalf("ReplicaCount() gave %d instead of 3", v)
	}
}

func TestRingNodes(t *testing.T) {
	v := (&ring{nodes: []*node{&node{id: 1}, &node{id: 2}}}).Nodes()
	if len(v) != 2 {
		t.Fatalf("Nodes() gave %d entries instead of 2", len(v))
	}
	if v[0].ID() != 1 || v[1].ID() != 2 {
		t.Fatalf("Nodes() gave [%v, %v] instead of [&{1}, &{2}]", v[0], v[1])
	}
}

func TestRingNode(t *testing.T) {
	v := (&ring{nodes: []*node{&node{id: 1}, &node{id: 2}}}).Node(1)
	if v.ID() != 1 {
		t.Fatalf("Nodes() gave %v instead of &{1}", v)
	}
	v = (&ring{nodes: []*node{&node{id: 1}, &node{id: 2}}}).Node(2)
	if v.ID() != 2 {
		t.Fatalf("Nodes() gave %v instead of &{2}", v)
	}
	v = (&ring{nodes: []*node{&node{id: 1}, &node{id: 2}}}).Node(3)
	if v != nil {
		t.Fatalf("Nodes() gave %v instead of nil", v)
	}
}

func TestRingLocalNode(t *testing.T) {
	v := (&ring{localNodeIndex: -1}).LocalNode()
	if v != nil {
		t.Fatalf("LocalNode() gave %v instead of nil", v)
	}
	v = (&ring{localNodeIndex: 0, nodes: []*node{&node{id: 123}, &node{id: 456}, &node{id: 789}}}).LocalNode()
	if v.ID() != 123 {
		t.Fatalf("LocalNode() gave %v instead of 0", v)
	}
	v = (&ring{localNodeIndex: 1, nodes: []*node{&node{id: 123}, &node{id: 456}, &node{id: 789}}}).LocalNode()
	if v.ID() != 456 {
		t.Fatalf("LocalNode() gave %v instead of 0", v)
	}
}

func TestRingResponsible(t *testing.T) {
	v := (&ring{localNodeIndex: -1}).Responsible(123)
	if v {
		t.Fatal("Responsible(123) gave true instead of false")
	}
	d := make([][]int32, 3)
	d[0] = []int32{0, 1, 2}
	d[1] = []int32{3, 4, 5}
	d[2] = []int32{6, 7, 8}
	v = (&ring{localNodeIndex: 0, replicaToPartitionToNodeIndex: d}).Responsible(0)
	if !v {
		t.Fatal("Responsible(0) gave false instead of true")
	}
	v = (&ring{localNodeIndex: 0, replicaToPartitionToNodeIndex: d}).Responsible(1)
	if v {
		t.Fatal("Responsible(1) gave true instead of false")
	}
}

func TestRingResponsibleIDs(t *testing.T) {
	d := make([][]int32, 3)
	d[0] = []int32{0, 1, 2}
	d[1] = []int32{3, 4, 5}
	d[2] = []int32{6, 7, 8}
	v := (&ring{nodes: []*node{&node{id: 10}, &node{id: 11}, &node{id: 12}, &node{id: 13}, &node{id: 14}, &node{id: 15}, &node{id: 16}, &node{id: 17}, &node{id: 18}}, replicaToPartitionToNodeIndex: d}).ResponsibleNodes(0)
	if len(v) != 3 || v[0].ID() != 10 || v[1].ID() != 13 || v[2].ID() != 16 {
		t.Fatalf("ResponsibleNodes(0) gave %v instead of [10 13 16]", v)
	}
	v = (&ring{nodes: []*node{&node{id: 10}, &node{id: 11}, &node{id: 12}, &node{id: 13}, &node{id: 14}, &node{id: 15}, &node{id: 16}, &node{id: 17}, &node{id: 18}}, replicaToPartitionToNodeIndex: d}).ResponsibleNodes(2)
	if len(v) != 3 || v[0].ID() != 12 || v[1].ID() != 15 || v[2].ID() != 18 {
		t.Fatalf("ResponsibleNodes(2) gave %v instead of [12 15 18]", v)
	}
}

func TestRingPersistence(t *testing.T) {
	b := NewBuilder()
	b.SetReplicaCount(3)
	b.AddNode(true, 1, []string{"server1", "zone1"}, []string{"1.2.3.4:56789"}, "Meta One")
	b.AddNode(true, 1, []string{"server2", "zone1"}, []string{"1.2.3.5:56789", "1.2.3.5:9876"}, "Meta Four")
	b.AddNode(false, 0, []string{"server3", "zone1"}, []string{"1.2.3.6:56789"}, "Meta Three")
	r := b.Ring().(*ring)
	buf := bytes.NewBuffer(make([]byte, 0, 65536))
	err := r.Persist(buf)
	if err != nil {
		t.Fatal(err)
	}
	r2i, err := LoadRing(bytes.NewBuffer(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	r2 := r2i.(*ring)
	if r2.version != r.version {
		t.Fatalf("%v != %v", r2.version, r.version)
	}
	if len(r2.nodes) != len(r.nodes) {
		t.Fatalf("%v != %v", len(r2.nodes), len(r.nodes))
	}
	for i := 0; i < len(r2.nodes); i++ {
		if r2.nodes[i].id != r.nodes[i].id {
			t.Fatalf("%v != %v", r2.nodes[i].id, r.nodes[i].id)
		}
		if r2.nodes[i].capacity != r.nodes[i].capacity {
			t.Fatalf("%v != %v", r2.nodes[i].capacity, r.nodes[i].capacity)
		}
		if len(r2.nodes[i].tierIndexes) != len(r.nodes[i].tierIndexes) {
			t.Fatalf("%v != %v", len(r2.nodes[i].tierIndexes), len(r.nodes[i].tierIndexes))
		}
		for j := 0; j < len(r2.nodes[i].tierIndexes); j++ {
			if r2.nodes[i].tierIndexes[j] != r.nodes[i].tierIndexes[j] {
				t.Fatalf("%v != %v", r2.nodes[i].tierIndexes[j], r.nodes[i].tierIndexes[j])
			}
		}
		if len(r2.nodes[i].addresses) != len(r.nodes[i].addresses) {
			t.Fatalf("%v != %v", len(r2.nodes[i].addresses), len(r.nodes[i].addresses))
		}
		for j := 0; j < len(r2.nodes[i].addresses); j++ {
			if r2.nodes[i].addresses[j] != r.nodes[i].addresses[j] {
				t.Fatalf("%v != %v", r2.nodes[i].addresses[j], r.nodes[i].addresses[j])
			}
		}
		if r2.nodes[i].meta != r.nodes[i].meta {
			t.Fatalf("%v != %v", r2.nodes[i].meta, r.nodes[i].meta)
		}
	}
	if r2.partitionBitCount != r.partitionBitCount {
		t.Fatalf("%v != %v", r2.partitionBitCount, r.partitionBitCount)
	}
	if len(r2.replicaToPartitionToNodeIndex) != len(r.replicaToPartitionToNodeIndex) {
		t.Fatalf("%v != %v", len(r2.replicaToPartitionToNodeIndex), len(r.replicaToPartitionToNodeIndex))
	}
	for i := 0; i < len(r2.replicaToPartitionToNodeIndex); i++ {
		if len(r2.replicaToPartitionToNodeIndex[i]) != len(r.replicaToPartitionToNodeIndex[i]) {
			t.Fatalf("%v != %v", len(r2.replicaToPartitionToNodeIndex[i]), len(r.replicaToPartitionToNodeIndex[i]))
		}
		for j := 0; j < len(r2.replicaToPartitionToNodeIndex[i]); j++ {
			if r2.replicaToPartitionToNodeIndex[i][j] != r.replicaToPartitionToNodeIndex[i][j] {
				t.Fatalf("%v != %v", r2.replicaToPartitionToNodeIndex[i][j], r.replicaToPartitionToNodeIndex[i][j])
			}
		}
	}
}

func TestRingStats(t *testing.T) {
	s := (&ring{
		partitionBitCount: 2,
		nodes: []*node{
			&node{id: 0, capacity: 100},
			&node{id: 1, capacity: 101},
			&node{id: 2, capacity: 102},
			&node{id: 3, capacity: 103},
			&node{id: 4, capacity: 104},
			&node{id: 5, inactive: true},
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
