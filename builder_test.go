package ring

import "testing"

func TestNewBuilder(t *testing.T) {
	b := NewBuilder(3)
	b.Add(&testNode{})
	i := b.PointsAllowed()
	if i != 1 {
		t.Fatalf("NewBuilder's PointsAllowed was %d not 1", i)
	}
	b.SetPointsAllowed(10)
	i = b.PointsAllowed()
	if i != 10 {
		t.Fatalf("NewBuilder's PointsAllowed was %d not 10", i)
	}
	i = b.Ring(0).ReplicaCount()
	if i != 3 {
		t.Fatalf("NewBuilder's ReplicaCount was %d not 3", i)
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

func TestBuilderAddRemoveNodes(t *testing.T) {
	b := NewBuilder(3)
	b.Add(&testNode{id: 1, capacity: 1})
	b.Add(&testNode{id: 2, capacity: 1})
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
		if n[0].NodeID() != 2 ||
			n[1].NodeID() != 2 ||
			n[2].NodeID() != 2 {
			t.Fatalf("Supposed only have node id:2 and got %#v %#v %#v", n[0], n[1], n[2])
		}
	}
}

func TestBuilderNodeLookup(t *testing.T) {
	b := NewBuilder(3)
	b.Add(&testNode{id: 1, capacity: 1})
	b.Add(&testNode{id: 2, capacity: 1})
	n := b.Node(1)
	if n.NodeID() != 1 {
		t.Fatalf("Node lookup should've given id:1 but instead gave %#v", n)
	}
	n = b.Node(2)
	if n.NodeID() != 2 {
		t.Fatalf("Node lookup should've given id:2 but instead gave %#v", n)
	}
	n = b.Node(84)
	if n != nil {
		t.Fatalf("Node lookup should've given nil but instead gave %#v", n)
	}
}

func TestBuilderRing(t *testing.T) {
	b := NewBuilder(3)
	b.Add(&testNode{id: 1, capacity: 1})
	b.Add(&testNode{id: 2, capacity: 1})
	r := b.Ring(0)
	n := r.LocalNode()
	if n != nil {
		t.Fatalf("Ring(0) should've returned an unbound ring; instead LocalNode gave %#v", n)
	}
	r = b.Ring(1)
	n = r.LocalNode()
	if n.NodeID() != 1 {
		t.Fatalf("Ring(1) should've returned a ring bound to id:1; instead LocalNode gave %#v", n)
	}
	pbc := r.PartitionBitCount()
	if pbc != 1 {
		t.Fatalf("Ring's PartitionBitCount was %d and should've been 1", pbc)
	}
	// Make sure a new Ring call doesn't alter the previous Ring.
	b.Add(&testNode{id: 3, capacity: 3})
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
	b.Add(&testNode{id: 1, capacity: 1})
	b.Add(&testNode{id: 2, capacity: 1})
	r := b.Ring(0)
	pbc := r.PartitionBitCount()
	if pbc != 1 {
		t.Fatalf("Ring's PartitionBitCount was %d and should've been 1", pbc)
	}
	b.Add(&testNode{id: 3, capacity: 3, inactive: true})
	r = b.Ring(0)
	pbc = r.PartitionBitCount()
	if pbc != 1 {
		t.Fatalf("Ring's PartitionBitCount was %d and should've been 1", pbc)
	}
	b.Node(3).(*testNode).inactive = false
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
		b.Add(&testNode{id: uint64(i), capacity: uint32(i)})
	}
	r = b.Ring(0)
	pbc = r.PartitionBitCount()
	if pbc != 6 {
		t.Fatalf("Ring's PartitionBitCount was %d and should've been 6", pbc)
	}
	// Just exercises the "already at max" short-circuit.
	b.Add(&testNode{id: 14, capacity: 14})
	r = b.Ring(0)
	pbc = r.PartitionBitCount()
	if pbc != 6 {
		t.Fatalf("Ring's PartitionBitCount was %d and should've been 6", pbc)
	}
}
