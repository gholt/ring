package ring_test

import (
	"bytes"
	"fmt"

	"github.com/gholt/ring"
)

func Example_builderMarshal() {
	// Build something to marshal...
	b := ring.NewBuilder()
	b.SetReplicaCount(2)
	n1 := b.AddNode()
	n1.SetCapacity(1)
	n1.SetInfo("Node One")
	n2 := b.AddNode()
	n2.SetCapacity(1)
	n2.SetInfo("Node Two")
	b.Rebalance()
	// And marshal it...
	var buf bytes.Buffer
	if err := b.Marshal(&buf); err != nil {
		panic(err)
	}
	fmt.Println(len(buf.Bytes()), "bytes written")
	fmt.Println(string(buf.Bytes()[:71]), "...")
	// Note that even though the beginning is just JSON, there is trailing
	// binary for the larger data sets that JSON just isn't suited for.

	// Output:
	// 466 bytes written
	// {"MarshalVersion":0,"NodeIndexType":16,"LastMovedType":16,"Rebalanced": ...
}

func ExampleUnmarshalBuilder() {
	// Build something to marshal...
	b1 := ring.NewBuilder()
	b1.SetReplicaCount(2)
	n1 := b1.AddNode()
	n1.SetCapacity(1)
	n1.SetInfo("Node One")
	n2 := b1.AddNode()
	n2.SetCapacity(1)
	n2.SetInfo("Node Two")
	b1.Rebalance()
	// And marshal the builder...
	var buf bytes.Buffer
	if err := b1.Marshal(&buf); err != nil {
		panic(err)
	}
	// So we can show how to unmarshal it...
	b2, err := ring.UnmarshalBuilder(&buf)
	if err != nil {
		panic(err)
	}
	// And just do some checks to show they're the same...
	if !b1.Rebalanced().Equal(b2.Rebalanced()) {
		panic("")
	}
	if b1.MaxNodeCount() != b2.MaxNodeCount() {
		panic("")
	}
	if b1.ReplicaCount() != b2.ReplicaCount() {
		panic("")
	}
	if b1.PartitionCount() != b2.PartitionCount() {
		panic("")
	}
	if b1.LastMovedUnit() != b2.LastMovedUnit() {
		panic("")
	}
	if b1.MoveWait() != b2.MoveWait() {
		panic("")
	}
	if b1.MovesPerPartition() != b2.MovesPerPartition() {
		panic("")
	}
	if b1.PointsAllowed() != b2.PointsAllowed() {
		panic("")
	}
	if b1.MaxPartitionCount() != b2.MaxPartitionCount() {
		panic("")
	}
	ns1 := b1.Nodes()
	ns2 := b2.Nodes()
	if len(ns1) != len(ns2) {
		panic("")
	}
	for i := 0; i < len(ns1); i++ {
		if ns1[i].Disabled() != ns2[i].Disabled() {
			panic("")
		}
		if ns1[i].Capacity() != ns2[i].Capacity() {
			panic("")
		}
		t1 := ns1[i].Tiers()
		t2 := ns2[i].Tiers()
		if len(t1) != len(t2) {
			panic("")
		}
		for j := 0; j < len(t1); j++ {
			if t1[j] != t2[j] {
				panic("")
			}
		}
		if ns1[i].Info() != ns2[i].Info() {
			panic("")
		}
	}
	// And compare their rings for equality...
	r1 := b1.Ring()
	r2 := b2.Ring()
	if !r1.Rebalanced().Equal(r2.Rebalanced()) {
		panic("")
	}
	if r1.ReplicaCount() != r2.ReplicaCount() {
		panic("")
	}
	if r1.PartitionCount() != r2.PartitionCount() {
		panic("")
	}
	compareNodeSlices := func(ns1, ns2 []ring.Node) {
		if len(ns1) != len(ns2) {
			panic("")
		}
		for i := 0; i < len(ns1); i++ {
			if ns1[i].Disabled() != ns2[i].Disabled() {
				panic("")
			}
			if ns1[i].Capacity() != ns2[i].Capacity() {
				panic("")
			}
			t1 := ns1[i].Tiers()
			t2 := ns2[i].Tiers()
			if len(t1) != len(t2) {
				panic("")
			}
			for j := 0; j < len(t1); j++ {
				if t1[j] != t2[j] {
					panic("")
				}
			}
			if ns1[i].Info() != ns2[i].Info() {
				panic("")
			}
		}
	}
	compareNodeSlices(r1.Nodes(), r2.Nodes())
	for partition := 0; partition < r1.PartitionCount(); partition++ {
		compareNodeSlices(r1.KeyNodes(partition), r2.KeyNodes(partition))
	}
	fmt.Println("They look the same!")
	// Output:
	// They look the same!
}
