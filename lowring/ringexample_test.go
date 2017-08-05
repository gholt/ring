package lowring_test

import (
	"fmt"

	"github.com/gholt/ring/lowring"
)

func ExampleRing_overview() {
	nodes := []string{"FirstNode", "SecondNode"}
	replicaToPartitionToNodeIndex_aka_Ring := lowring.Ring{[]lowring.NodeIndexType{0, 1}, []lowring.NodeIndexType{1, lowring.NodeIndexNil}}
	for replica, partitionToNodeIndex := range replicaToPartitionToNodeIndex_aka_Ring {
		for partition, nodeIndex := range partitionToNodeIndex {
			fmt.Printf("Replica %d of Partition %d is ", replica, partition)
			if nodeIndex == lowring.NodeIndexNil {
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
	fmt.Println(lowring.Ring{
		[]lowring.NodeIndexType{0, 1, 0},
		[]lowring.NodeIndexType{1, 0, 1},
	}.PartitionCount())
	// Output: 3
}

func ExampleRing_ReplicaCount() {
	fmt.Println(lowring.Ring{
		[]lowring.NodeIndexType{0, 1, 1},
		[]lowring.NodeIndexType{1, 0, 1},
	}.ReplicaCount())
	// Output: 2
}

func ExampleRing_Equal() {
	r1 := lowring.Ring{
		[]lowring.NodeIndexType{0, 1, 1},
		[]lowring.NodeIndexType{1, 0, 1},
	}
	r2 := lowring.Ring{
		[]lowring.NodeIndexType{0, 1, 1},
		[]lowring.NodeIndexType{1, 0, 1},
	}
	r3 := lowring.Ring{
		[]lowring.NodeIndexType{1, 0, 0},
		[]lowring.NodeIndexType{0, 1, 0},
	}
	fmt.Println(r1.Equal(r2), r1.Equal(r3), r2.Equal(r1), r2.Equal(r3), r3.Equal(r1), r3.Equal(r2))
	fmt.Println(r1.Equal(r1))
	// Output:
	// true false true false false false
	// true
}

func ExampleRing_Copy() {
	builder := lowring.Builder{Nodes: []*lowring.Node{{Capacity: 1}, {Capacity: 1}}}
	builder.Rebalance()
	ring1 := builder.Copy()
	fmt.Println(ring1.Equal(builder.Ring))
	builder.Nodes = append(builder.Nodes, &lowring.Node{Capacity: 1})
	builder.ShiftLastMoved(builder.MoveWait * 2)
	builder.Rebalance()
	fmt.Println(ring1.Equal(builder.Ring))
	// Output:
	// true
	// false
}
