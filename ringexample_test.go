package ring_test

import (
	"bytes"
	"fmt"

	"github.com/gholt/ring"
)

func ExampleRing_Marshal() {
	// Build a ring to marshal...
	b := ring.NewBuilder(2)
	b.AddNode("Node One", 1, nil)
	b.AddNode("Node Two", 1, nil)
	b.Rebalance()
	r := b.Ring()
	// And marshal it...
	var buf bytes.Buffer
	if err := r.Marshal(&buf); err != nil {
		panic(err)
	}
	fmt.Println(len(buf.Bytes()), "bytes written")
	fmt.Println(string(buf.Bytes()[:213]), "...")
	// Note that even though the beginning is just JSON, there is trailing
	// binary for the larger data sets that JSON just isn't suited for.

	// Output:
	// 243 bytes written
	// {"MarshalVersion":0,"NodeType":16,"ReplicaCount":2,"PartitionCount":2,"Nodes":[{"Info":"Node One","Capacity":1,"Group":0},{"Info":"Node Two","Capacity":1,"Group":0}],"Groups":[{"Info":"","Parent":0}],"Rebalanced": ...
}

func ExampleUnmarshal() {
	// Build a ring to marshal...
	b := ring.NewBuilder(2)
	b.AddNode("Node One", 1, nil)
	b.AddNode("Node Two", 1, nil)
	b.Rebalance()
	r1 := b.Ring()
	// And marshal it...
	var buf bytes.Buffer
	if err := r1.Marshal(&buf); err != nil {
		panic(err)
	}
	// So we can show how to unmarshal it...
	r2, err := ring.Unmarshal(&buf)
	if err != nil {
		panic(err)
	}
	// And just do some checks to show they're the same...
	if !r1.Rebalanced().Equal(r2.Rebalanced()) {
		panic("")
	}
	if r1.ReplicaCount() != r2.ReplicaCount() {
		panic(fmt.Sprint(r1.ReplicaCount(), r2.ReplicaCount()))
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
