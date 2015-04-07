package ring

import (
	"bytes"
	"math"
	"testing"
)

func TestNewBuilder(t *testing.T) {
	b := NewBuilder()
	b.SetReplicaCount(3)
	b.AddNode(true, 1, nil, nil, "")
	pa := b.PointsAllowed()
	if pa != 1 {
		t.Fatalf("NewBuilder's PointsAllowed was %d not 1", pa)
	}
	b.SetPointsAllowed(10)
	pa = b.PointsAllowed()
	if pa != 10 {
		t.Fatalf("NewBuilder's PointsAllowed was %d not 10", pa)
	}
	rc := b.Ring().ReplicaCount()
	if rc != 3 {
		t.Fatalf("NewBuilder's ReplicaCount was %d not 3", rc)
	}
	u16 := b.Ring().PartitionBitCount()
	if u16 != 1 {
		t.Fatalf("NewBuilder's PartitionBitCount was %d not 1", u16)
	}
	n := b.Ring().Nodes()
	if len(n) != 1 {
		t.Fatalf("NewBuilder's Nodes count was %d not 1", len(n))
	}
}

func TestBuilderPersistence(t *testing.T) {
	b := NewBuilder()
	b.SetReplicaCount(3)
	b.AddNode(true, 1, []string{"server1", "zone1"}, []string{"1.2.3.4:56789"}, "Meta One")
	b.AddNode(true, 1, []string{"server2", "zone1"}, []string{"1.2.3.5:56789", "1.2.3.5:9876"}, "Meta Four")
	b.AddNode(false, 0, []string{"server3", "zone1"}, []string{"1.2.3.6:56789"}, "Meta Three")
	b.Ring()
	buf := bytes.NewBuffer(make([]byte, 0, 65536))
	err := b.Persist(buf)
	if err != nil {
		t.Fatal(err)
	}
	b2, err := LoadBuilder(bytes.NewBuffer(buf.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if b2.version != b.version {
		t.Fatalf("%v != %v", b2.version, b.version)
	}
	if len(b2.nodes) != len(b.nodes) {
		t.Fatalf("%v != %v", len(b2.nodes), len(b.nodes))
	}
	for i := 0; i < len(b2.nodes); i++ {
		if b2.nodes[i].id != b.nodes[i].id {
			t.Fatalf("%v != %v", b2.nodes[i].id, b.nodes[i].id)
		}
		if b2.nodes[i].capacity != b.nodes[i].capacity {
			t.Fatalf("%v != %v", b2.nodes[i].capacity, b.nodes[i].capacity)
		}
		if len(b2.nodes[i].tierIndexes) != len(b.nodes[i].tierIndexes) {
			t.Fatalf("%v != %v", len(b2.nodes[i].tierIndexes), len(b.nodes[i].tierIndexes))
		}
		for j := 0; j < len(b2.nodes[i].tierIndexes); j++ {
			if b2.nodes[i].tierIndexes[j] != b.nodes[i].tierIndexes[j] {
				t.Fatalf("%v != %v", b2.nodes[i].tierIndexes[j], b.nodes[i].tierIndexes[j])
			}
		}
		if len(b2.nodes[i].addresses) != len(b.nodes[i].addresses) {
			t.Fatalf("%v != %v", len(b2.nodes[i].addresses), len(b.nodes[i].addresses))
		}
		for j := 0; j < len(b2.nodes[i].addresses); j++ {
			if b2.nodes[i].addresses[j] != b.nodes[i].addresses[j] {
				t.Fatalf("%v != %v", b2.nodes[i].addresses[j], b.nodes[i].addresses[j])
			}
		}
		if b2.nodes[i].meta != b.nodes[i].meta {
			t.Fatalf("%v != %v", b2.nodes[i].meta, b.nodes[i].meta)
		}
	}
	if b2.partitionBitCount != b.partitionBitCount {
		t.Fatalf("%v != %v", b2.partitionBitCount, b.partitionBitCount)
	}
	if len(b2.replicaToPartitionToNodeIndex) != len(b.replicaToPartitionToNodeIndex) {
		t.Fatalf("%v != %v", len(b2.replicaToPartitionToNodeIndex), len(b.replicaToPartitionToNodeIndex))
	}
	for i := 0; i < len(b2.replicaToPartitionToNodeIndex); i++ {
		if len(b2.replicaToPartitionToNodeIndex[i]) != len(b.replicaToPartitionToNodeIndex[i]) {
			t.Fatalf("%v != %v", len(b2.replicaToPartitionToNodeIndex[i]), len(b.replicaToPartitionToNodeIndex[i]))
		}
		for j := 0; j < len(b2.replicaToPartitionToNodeIndex[i]); j++ {
			if b2.replicaToPartitionToNodeIndex[i][j] != b.replicaToPartitionToNodeIndex[i][j] {
				t.Fatalf("%v != %v", b2.replicaToPartitionToNodeIndex[i][j], b.replicaToPartitionToNodeIndex[i][j])
			}
		}
	}
	if len(b2.replicaToPartitionToLastMove) != len(b.replicaToPartitionToLastMove) {
		t.Fatalf("%v != %v", len(b2.replicaToPartitionToLastMove), len(b.replicaToPartitionToLastMove))
	}
	for i := 0; i < len(b2.replicaToPartitionToLastMove); i++ {
		if len(b2.replicaToPartitionToLastMove[i]) != len(b.replicaToPartitionToLastMove[i]) {
			t.Fatalf("%v != %v", len(b2.replicaToPartitionToLastMove[i]), len(b.replicaToPartitionToLastMove[i]))
		}
		for j := 0; j < len(b2.replicaToPartitionToLastMove[i]); j++ {
			if b2.replicaToPartitionToLastMove[i][j] != b.replicaToPartitionToLastMove[i][j] {
				t.Fatalf("%v != %v", b2.replicaToPartitionToLastMove[i][j], b.replicaToPartitionToLastMove[i][j])
			}
		}
	}
	if b2.pointsAllowed != b.pointsAllowed {
		t.Fatalf("%v != %v", b2.pointsAllowed, b.pointsAllowed)
	}
	if b2.maxPartitionBitCount != b.maxPartitionBitCount {
		t.Fatalf("%v != %v", b2.maxPartitionBitCount, b.maxPartitionBitCount)
	}
	if b2.moveWait != b.moveWait {
		t.Fatalf("%v != %v", b2.moveWait, b.moveWait)
	}
}

func TestBuilderLoadGarbage(t *testing.T) {
	b, err := LoadBuilder(bytes.NewBuffer([]byte{
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
		0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
	}))
	if err == nil {
		t.Fatal("")
	}
	if b != nil {
		t.Fatal("")
	}
}

func TestBuilderAddRemoveNodes(t *testing.T) {
	b := NewBuilder()
	b.SetReplicaCount(3)
	nA := b.AddNode(true, 1, nil, nil, "")
	nB := b.AddNode(true, 1, nil, nil, "")
	r := b.Ring()
	n := r.Nodes()
	if len(n) != 2 {
		t.Fatalf("Ring had %d nodes instead of 2", len(n))
	}
	b.RemoveNode(nA.ID())
	r = b.Ring()
	n = r.Nodes()
	if len(n) != 1 {
		t.Fatalf("Ring had %d nodes instead of 1", len(n))
	}
	pc := uint32(1) << r.PartitionBitCount()
	for p := uint32(0); p < pc; p++ {
		n = r.ResponsibleNodes(p)
		if len(n) != 3 {
			t.Fatalf("Supposed to get 3 replicas, got %d", len(n))
		}
		if n[0].ID() != nB.ID() ||
			n[1].ID() != nB.ID() ||
			n[2].ID() != nB.ID() {
			t.Fatalf("Supposed only have node id:2 and got %#v %#v %#v", n[0], n[1], n[2])
		}
	}
}

func TestBuilderNodeLookup(t *testing.T) {
	b := NewBuilder()
	b.SetReplicaCount(3)
	nA := b.AddNode(true, 1, nil, nil, "")
	nB := b.AddNode(true, 1, nil, nil, "")
	n := b.Node(nA.ID())
	if n.ID() != nA.ID() {
		t.Fatalf("Node lookup should've given id:1 but instead gave %#v", n)
	}
	n = b.Node(nB.ID())
	if n.ID() != nB.ID() {
		t.Fatalf("Node lookup should've given id:2 but instead gave %#v", n)
	}
	n = b.Node(84)
	if n != nil {
		t.Fatalf("Node lookup should've given nil but instead gave %#v", n)
	}
}

func TestBuilderRing(t *testing.T) {
	b := NewBuilder()
	b.SetReplicaCount(3)
	nA := b.AddNode(true, 1, nil, nil, "")
	b.AddNode(true, 1, nil, nil, "")
	r := b.Ring()
	n := r.LocalNode()
	if n != nil {
		t.Fatalf("Ring() should've returned an unbound ring; instead LocalNode gave %#v", n)
	}
	r.SetLocalNode(nA.ID())
	n = r.LocalNode()
	if n == nil || n.ID() != nA.ID() {
		t.Fatalf("SetLocalNode(nA.ID()) should've bound the ring to %#v; instead LocalNode gave %#v", nA, n)
	}
	pbc := r.PartitionBitCount()
	if pbc != 1 {
		t.Fatalf("Ring's PartitionBitCount was %d and should've been 1", pbc)
	}
	// Make sure a new Ring call doesn't alter the previous Ring.
	b.AddNode(true, 3, nil, nil, "")
	r2 := b.Ring()
	r2.SetLocalNode(nA.ID())
	pbc = r2.PartitionBitCount()
	if pbc == 1 {
		t.Fatalf("Ring2's PartitionBitCount should not have been 1")
	}
	pbc = r.PartitionBitCount()
	if pbc != 1 {
		t.Fatalf("Ring's PartitionBitCount was %d and should've been 1", pbc)
	}
	ns := r2.Nodes()
	if len(ns) != 3 {
		t.Fatalf("Ring2 should've had 3 nodes; instead had %d", len(ns))
	}
	ns = r.Nodes()
	if len(ns) != 2 {
		t.Fatalf("Ring should've had 2 nodes; instead had %d", len(ns))
	}
}

func TestBuilderResizeIfNeeded(t *testing.T) {
	b := NewBuilder()
	b.SetReplicaCount(3)
	b.AddNode(true, 1, nil, nil, "")
	b.AddNode(true, 1, nil, nil, "")
	r := b.Ring()
	pbc := r.PartitionBitCount()
	if pbc != 1 {
		t.Fatalf("Ring's PartitionBitCount was %d and should've been 1", pbc)
	}
	nC := b.AddNode(false, 3, nil, nil, "")
	r = b.Ring()
	pbc = r.PartitionBitCount()
	if pbc != 1 {
		t.Fatalf("Ring's PartitionBitCount was %d and should've been 1", pbc)
	}
	nC.SetActive(true)
	r = b.Ring()
	pbc = r.PartitionBitCount()
	if pbc != 4 {
		t.Fatalf("Ring's PartitionBitCount was %d and should've been 4", pbc)
	}
	// Test that shrinking does not happen (at least for now).
	b.RemoveNode(nC.ID())
	r = b.Ring()
	pbc = r.PartitionBitCount()
	if pbc != 4 {
		t.Fatalf("Ring's PartitionBitCount was %d and should've been 4", pbc)
	}
	// Test partition count cap.
	pbc = b.MaxPartitionBitCount()
	if pbc != 23 {
		t.Fatalf("Expected the default max partition bit count to be 23; it was %d", pbc)
	}
	b.SetMaxPartitionBitCount(6)
	pbc = b.MaxPartitionBitCount()
	if pbc != 6 {
		t.Fatalf("Expected the max partition bit count to be saved as 6; instead it was %d", pbc)
	}
	for i := 4; i < 14; i++ {
		b.AddNode(true, uint32(i), nil, nil, "")
	}
	r = b.Ring()
	pbc = r.PartitionBitCount()
	if pbc != 6 {
		t.Fatalf("Ring's PartitionBitCount was %d and should've been 6", pbc)
	}
	// Just exercises the "already at max" short-circuit.
	b.AddNode(true, 14, nil, nil, "")
	r = b.Ring()
	pbc = r.PartitionBitCount()
	if pbc != 6 {
		t.Fatalf("Ring's PartitionBitCount was %d and should've been 6", pbc)
	}
}

func TestBuilderMinimizeTiers(t *testing.T) {
	b := NewBuilder()
	n := b.AddNode(true, 1, []string{"one"}, nil, "")
	b.AddNode(true, 1, []string{"two"}, nil, "")
	b.minimizeTiers()
	if len(b.tiers) != 1 {
		t.Fatal("")
	}
	if len(b.tiers[0]) != 3 {
		t.Fatal("")
	}
	if b.tiers[0][0] != "" {
		t.Fatal("")
	}
	if b.tiers[0][1] != "one" {
		t.Fatal("")
	}
	if b.tiers[0][2] != "two" {
		t.Fatal("")
	}
	b.RemoveNode(n.ID())
	b.minimizeTiers()
	if len(b.tiers) != 1 {
		t.Fatal("")
	}
	if len(b.tiers[0]) != 2 {
		t.Fatal("")
	}
	if b.tiers[0][0] != "" {
		t.Fatal("")
	}
	if b.tiers[0][1] != "two" {
		t.Fatal("")
	}
}

func TestBuilderLowerReplicaCount(t *testing.T) {
	b := NewBuilder()
	b.SetReplicaCount(3)
	b.AddNode(true, 1, nil, nil, "")
	b.AddNode(true, 1, nil, nil, "")
	b.AddNode(true, 1, nil, nil, "")
	b.Ring()
	// ring ends up:
	// 0 2
	// 1 1
	// 2 0
	if len(b.replicaToPartitionToNodeIndex) != 3 {
		t.Fatal(len(b.replicaToPartitionToNodeIndex))
	}
	if len(b.replicaToPartitionToNodeIndex[0]) != 2 {
		t.Fatal(len(b.replicaToPartitionToNodeIndex[0]))
	}
	if b.replicaToPartitionToNodeIndex[0][0] != 0 {
		t.Fatal(b.replicaToPartitionToNodeIndex[0][0])
	}
	if b.replicaToPartitionToNodeIndex[0][1] != 2 {
		t.Fatal(b.replicaToPartitionToNodeIndex[0][1])
	}
	if b.replicaToPartitionToNodeIndex[1][0] != 1 {
		t.Fatal(b.replicaToPartitionToNodeIndex[1][0])
	}
	if b.replicaToPartitionToNodeIndex[1][1] != 1 {
		t.Fatal(b.replicaToPartitionToNodeIndex[1][1])
	}
	if b.replicaToPartitionToNodeIndex[2][0] != 2 {
		t.Fatal(b.replicaToPartitionToNodeIndex[2][0])
	}
	if b.replicaToPartitionToNodeIndex[2][1] != 0 {
		t.Fatal(b.replicaToPartitionToNodeIndex[2][1])
	}
	// dropping the replica count should just drop the last replicas so:
	// 0 2
	// 1 1
	b.SetReplicaCount(2)
	if len(b.replicaToPartitionToNodeIndex) != 2 {
		t.Fatal(len(b.replicaToPartitionToNodeIndex))
	}
	if len(b.replicaToPartitionToNodeIndex[0]) != 2 {
		t.Fatal(len(b.replicaToPartitionToNodeIndex[0]))
	}
	if b.replicaToPartitionToNodeIndex[0][0] != 0 {
		t.Fatal(b.replicaToPartitionToNodeIndex[0][0])
	}
	if b.replicaToPartitionToNodeIndex[0][1] != 2 {
		t.Fatal(b.replicaToPartitionToNodeIndex[0][1])
	}
	if b.replicaToPartitionToNodeIndex[1][0] != 1 {
		t.Fatal(b.replicaToPartitionToNodeIndex[1][0])
	}
	if b.replicaToPartitionToNodeIndex[1][1] != 1 {
		t.Fatal(b.replicaToPartitionToNodeIndex[1][1])
	}
	// Just to show that now we have 2 replicas but 3 nodes and that the
	// partition count has to jump up to try to keep good balance.
	b.PretendElapsed(math.MaxUint16)
	b.Ring()
	b.SetReplicaCount(2)
	if len(b.replicaToPartitionToNodeIndex) != 2 {
		t.Fatal(len(b.replicaToPartitionToNodeIndex))
	}
	if len(b.replicaToPartitionToNodeIndex[0]) <= 2 {
		t.Fatal(len(b.replicaToPartitionToNodeIndex[0]))
	}
}
