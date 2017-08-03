package ring_test

import (
	"fmt"

	"github.com/gholt/ring"
)

func ExampleRing_overview() {
	nodes := []string{"FirstNode", "SecondNode"}
	replicaToPartitionToNodeIndex_aka_Ring := ring.Ring{[]ring.NodeIndexType{0, 1}, []ring.NodeIndexType{1, ring.NodeIndexNil}}
	for replica, partitionToNodeIndex := range replicaToPartitionToNodeIndex_aka_Ring {
		for partition, nodeIndex := range partitionToNodeIndex {
			fmt.Printf("Replica %d of Partition %d is ", replica, partition)
			if nodeIndex == ring.NodeIndexNil {
				fmt.Println("unassigned")
			} else {
				fmt.Printf("assigned to %s\n", nodes[nodeIndex])
			}
		}
	}
	// Output:
	// Replica 0 of Partition 0 is assigned to FirstNode
	// Replica 0 of Partition 1 is assigned to SecondNode
	// Replica 1 of Partition 0 is assigned to SecondNode
	// Replica 1 of Partition 1 is unassigned
}

func ExampleRing_PartitionCount() {
	fmt.Println(ring.Ring{
		[]ring.NodeIndexType{0, 1, 0},
		[]ring.NodeIndexType{1, 0, 1},
	}.PartitionCount())
	// Output: 3
}

func ExampleRing_ReplicaCount() {
	fmt.Println(ring.Ring{
		[]ring.NodeIndexType{0, 1, 1},
		[]ring.NodeIndexType{1, 0, 1},
	}.ReplicaCount())
	// Output: 2
}

func ExampleRing_RingEqual() {
	r1 := ring.Ring{
		[]ring.NodeIndexType{0, 1, 1},
		[]ring.NodeIndexType{1, 0, 1},
	}
	r2 := ring.Ring{
		[]ring.NodeIndexType{0, 1, 1},
		[]ring.NodeIndexType{1, 0, 1},
	}
	r3 := ring.Ring{
		[]ring.NodeIndexType{1, 0, 0},
		[]ring.NodeIndexType{0, 1, 0},
	}
	fmt.Println(r1.RingEqual(r2), r1.RingEqual(r3), r2.RingEqual(r1), r2.RingEqual(r3), r3.RingEqual(r1), r3.RingEqual(r2))
	fmt.Println(r1.RingEqual(r1))
	// Output:
	// true false true false false false
	// true
}

func ExampleRing_RingDuplicate() {
	builder := ring.Builder{Nodes: []*ring.Node{{Capacity: 1}, {Capacity: 1}}}
	builder.Rebalance()
	ring1 := builder.RingDuplicate()
	fmt.Println(ring1.RingEqual(builder.Ring))
	builder.Nodes = append(builder.Nodes, &ring.Node{Capacity: 1})
	builder.ShiftLastMoved(builder.MoveWait * 2)
	builder.Rebalance()
	fmt.Println(ring1.RingEqual(builder.Ring))
	// Output:
	// true
	// false
}
