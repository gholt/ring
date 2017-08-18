package ring_test

import (
	"bytes"
	"fmt"

	"github.com/gholt/ring"
)

func ExampleBuilder_Marshal() {
	// Build something to marshal...
	b := ring.NewBuilder(2)
	b.AddNode("Node One", 1, nil)
	b.AddNode("Node Two", 1, nil)
	b.Rebalance()
	// And marshal it...
	var buf bytes.Buffer
	if err := b.Marshal(&buf); err != nil {
		panic(err)
	}
	fmt.Println(len(buf.Bytes()), "bytes written")
	fmt.Println(string(buf.Bytes()[:241]), "...")
	// Note that even though the beginning is just JSON, there is trailing
	// binary for the larger data sets that JSON just isn't suited for.

	// Output:
	// 333 bytes written
	// {"MarshalVersion":0,"NodeType":16,"ReplicaCount":2,"PartitionCount":2,"Nodes":[{"Info":"Node One","Capacity":1,"Group":0},{"Info":"Node Two","Capacity":1,"Group":0}],"Groups":[{"Info":"","Parent":0}],"MaxPartitionCount":8388608,"Rebalanced": ...
}

func ExampleUnmarshalBuilder() {
	// Build something to marshal...
	b1 := ring.NewBuilder(2)
	b1.AddNode("Node One", 1, nil)
	b1.AddNode("Node Two", 1, nil)
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
	if b1.ReplicaCount() != b2.ReplicaCount() {
		panic(fmt.Sprintln(b1.ReplicaCount(), b2.ReplicaCount()))
	}
	if b1.PartitionCount() != b2.PartitionCount() {
		panic("")
	}
	if b1.ReassignmentWait() != b2.ReassignmentWait() {
		panic("")
	}
	if b1.MaxReplicaReassignableCount() != b2.MaxReplicaReassignableCount() {
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
		if ns1[i].Capacity() != ns2[i].Capacity() {
			panic("")
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
	compareNodeSlices := func(ns1, ns2 []*ring.Node) {
		if len(ns1) != len(ns2) {
			panic("")
		}
		for i := 0; i < len(ns1); i++ {
			if ns1[i].Capacity() != ns2[i].Capacity() {
				panic("")
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
