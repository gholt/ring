package ring

import (
	"bytes"
	"testing"
)

func TestNewBuilder(t *testing.T) {
	b := NewBuilder(3)
	b.Add(&Node{ID: 1})
	pa := b.PointsAllowed()
	if pa != 1 {
		t.Fatalf("NewBuilder's PointsAllowed was %d not 1", pa)
	}
	b.SetPointsAllowed(10)
	pa = b.PointsAllowed()
	if pa != 10 {
		t.Fatalf("NewBuilder's PointsAllowed was %d not 10", pa)
	}
	rc := b.Ring(0).ReplicaCount()
	if rc != 3 {
		t.Fatalf("NewBuilder's ReplicaCount was %d not 3", rc)
	}
	u16 := b.Ring(0).PartitionBitCount()
	if u16 != 1 {
		t.Fatalf("NewBuilder's PartitionBitCount was %d not 1", u16)
	}
	n := b.Ring(0).Nodes()
	if len(n) != 1 {
		t.Fatalf("NewBuilder's Nodes count was %d not 1", len(n))
	}
}

func TestBuilderPersistence(t *testing.T) {
	b := NewBuilder(3)
	b.Add(&Node{ID: 1, Capacity: 1})
	b.Add(&Node{ID: 2, Capacity: 1})
	b.Ring(0)
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
		if b2.nodes[i].ID != b.nodes[i].ID {
			t.Fatalf("%v != %v", b2.nodes[i].ID, b.nodes[i].ID)
		}
		if b2.nodes[i].Capacity != b.nodes[i].Capacity {
			t.Fatalf("%v != %v", b2.nodes[i].Capacity, b.nodes[i].Capacity)
		}
		if len(b2.nodes[i].TierValues) != len(b.nodes[i].TierValues) {
			t.Fatalf("%v != %v", len(b2.nodes[i].TierValues), len(b.nodes[i].TierValues))
		}
		for j := 0; j < len(b2.nodes[i].TierValues); j++ {
			if b2.nodes[i].TierValues[j] != b.nodes[i].TierValues[j] {
				t.Fatalf("%v != %v", b2.nodes[i].TierValues[j], b.nodes[i].TierValues[j])
			}
		}
		if len(b2.nodes[i].Addresses) != len(b.nodes[i].Addresses) {
			t.Fatalf("%v != %v", len(b2.nodes[i].Addresses), len(b.nodes[i].Addresses))
		}
		for j := 0; j < len(b2.nodes[i].Addresses); j++ {
			if b2.nodes[i].Addresses[j] != b.nodes[i].Addresses[j] {
				t.Fatalf("%v != %v", b2.nodes[i].Addresses[j], b.nodes[i].Addresses[j])
			}
		}
		if b2.nodes[i].Meta != b.nodes[i].Meta {
			t.Fatalf("%v != %v", b2.nodes[i].Meta, b.nodes[i].Meta)
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

func TestBuilderAddRemoveNodes(t *testing.T) {
	b := NewBuilder(3)
	b.Add(&Node{ID: 1, Capacity: 1})
	b.Add(&Node{ID: 2, Capacity: 1})
	r := b.Ring(0)
	n := r.Nodes()
	if len(n) != 2 {
		t.Fatalf("Ring had %d nodes instead of 2", len(n))
	}
	b.Remove(1)
	r = b.Ring(0)
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
		if n[0].ID != 2 ||
			n[1].ID != 2 ||
			n[2].ID != 2 {
			t.Fatalf("Supposed only have node id:2 and got %#v %#v %#v", n[0], n[1], n[2])
		}
	}
}

func TestBuilderNodeLookup(t *testing.T) {
	b := NewBuilder(3)
	b.Add(&Node{ID: 1, Capacity: 1})
	b.Add(&Node{ID: 2, Capacity: 1})
	n := b.Node(1)
	if n.ID != 1 {
		t.Fatalf("Node lookup should've given id:1 but instead gave %#v", n)
	}
	n = b.Node(2)
	if n.ID != 2 {
		t.Fatalf("Node lookup should've given id:2 but instead gave %#v", n)
	}
	n = b.Node(84)
	if n != nil {
		t.Fatalf("Node lookup should've given nil but instead gave %#v", n)
	}
}

func TestBuilderRing(t *testing.T) {
	b := NewBuilder(3)
	b.Add(&Node{ID: 1, Capacity: 1})
	b.Add(&Node{ID: 2, Capacity: 1})
	r := b.Ring(0)
	n := r.LocalNode()
	if n != nil {
		t.Fatalf("Ring(0) should've returned an unbound ring; instead LocalNode gave %#v", n)
	}
	r = b.Ring(1)
	n = r.LocalNode()
	if n.ID != 1 {
		t.Fatalf("Ring(1) should've returned a ring bound to id:1; instead LocalNode gave %#v", n)
	}
	pbc := r.PartitionBitCount()
	if pbc != 1 {
		t.Fatalf("Ring's PartitionBitCount was %d and should've been 1", pbc)
	}
	// Make sure a new Ring call doesn't alter the previous Ring.
	b.Add(&Node{ID: 3, Capacity: 3})
	r2 := b.Ring(1)
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
	b := NewBuilder(3)
	b.Add(&Node{ID: 1, Capacity: 1})
	b.Add(&Node{ID: 2, Capacity: 1})
	r := b.Ring(0)
	pbc := r.PartitionBitCount()
	if pbc != 1 {
		t.Fatalf("Ring's PartitionBitCount was %d and should've been 1", pbc)
	}
	b.Add(&Node{ID: 3, Inactive: true, Capacity: 3})
	r = b.Ring(0)
	pbc = r.PartitionBitCount()
	if pbc != 1 {
		t.Fatalf("Ring's PartitionBitCount was %d and should've been 1", pbc)
	}
	b.Node(3).Inactive = false
	r = b.Ring(0)
	pbc = r.PartitionBitCount()
	if pbc != 4 {
		t.Fatalf("Ring's PartitionBitCount was %d and should've been 4", pbc)
	}
	// Test that shrinking does not happen (at least for now).
	b.Remove(3)
	r = b.Ring(0)
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
		b.Add(&Node{ID: uint64(i), Capacity: uint32(i)})
	}
	r = b.Ring(0)
	pbc = r.PartitionBitCount()
	if pbc != 6 {
		t.Fatalf("Ring's PartitionBitCount was %d and should've been 6", pbc)
	}
	// Just exercises the "already at max" short-circuit.
	b.Add(&Node{ID: 14, Capacity: 14})
	r = b.Ring(0)
	pbc = r.PartitionBitCount()
	if pbc != 6 {
		t.Fatalf("Ring's PartitionBitCount was %d and should've been 6", pbc)
	}
}
