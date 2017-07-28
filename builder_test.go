package ring_test

import (
	"fmt"

	"github.com/gholt/ring"
)

func ExampleBuilder_overview() {
	builder := ring.Builder{
		Nodes: []*ring.Node{
			{Capacity: 1},
			{Capacity: 1},
		},
	}
	builder.ChangeReplicaCount(2)
	builder.Rebalance()
	for partition := 0; partition < builder.PartitionCount(); partition++ {
		for replica := 0; replica < builder.ReplicaCount(); replica++ {
			fmt.Printf("Replica %d of Partition %d is assigned to Node %d\n", replica, partition, builder.Ring[replica][partition])
		}
	}
	// Output:
	// Replica 0 of Partition 0 is assigned to Node 0
	// Replica 1 of Partition 0 is assigned to Node 1
	// Replica 0 of Partition 1 is assigned to Node 1
	// Replica 1 of Partition 1 is assigned to Node 0
}

func ExampleBuilder_AddLastMoved_showingHowTheRestrictionWorks() {
	builder := ring.Builder{
		Nodes: []*ring.Node{
			{Capacity: 1},
			{Capacity: 1},
			{Capacity: 1},
		},
	}
	builder.ChangeReplicaCount(2)
	builder.Rebalance()
	printRing := func() {
		for _, partitionToNodeIndex := range builder.Ring {
			fmt.Printf("%v\n", partitionToNodeIndex)
		}
	}
	fmt.Println("Here are the initial assignments:")
	printRing()
	fmt.Println("Let's change the capacity of a node and rebalance...")
	builder.Nodes[0].Capacity = 2
	builder.Rebalance()
	fmt.Println("Note they haven't moved, even though we changed one node's capacity:")
	printRing()
	fmt.Println("So we'll \"pretend\" some time has passed and rebalance...")
	builder.AddLastMoved(builder.MoveWait + 1)
	builder.Rebalance()
	fmt.Println("Now reassignments have occurred:")
	printRing()
	// Output:
	// Here are the initial assignments:
	// [0 1 1 2 0 0 1 2 2 0 1 1 2 0 0 1]
	// [2 2 0 1 1 2 0 0 1 2 2 0 1 1 2 0]
	// Let's change the capacity of a node and rebalance...
	// Note they haven't moved, even though we changed one node's capacity:
	// [0 1 1 2 0 0 1 2 2 0 1 1 2 0 0 1]
	// [2 2 0 1 1 2 0 0 1 2 2 0 1 1 2 0]
	// So we'll "pretend" some time has passed and rebalance...
	// Now reassignments have occurred:
	// [0 1 1 2 0 0 1 2 2 0 1 1 2 0 0 1]
	// [2 0 0 0 1 2 0 0 0 2 0 0 0 1 2 0]
}

func ExampleBuilder_ChangeReplicaCount() {
	builder := ring.Builder{
		Nodes: []*ring.Node{
			{Capacity: 1},
			{Capacity: 1},
		},
	}
	builder.Rebalance()
	printRing := func() {
		for _, partitionToNodeIndex := range builder.Ring {
			fmt.Printf("%v\n", partitionToNodeIndex)
		}
	}
	fmt.Println("We start with a basic one replica ring:")
	printRing()
	fmt.Println("And add a replica...")
	builder.ChangeReplicaCount(2)
	fmt.Println("Note the new replicas are not assigned yet:")
	printRing()
	fmt.Println("So we rebalance...")
	builder.Rebalance()
	fmt.Println("And now they are assigned:")
	printRing()
	fmt.Println("Let's change back to one replica...")
	builder.ChangeReplicaCount(1)
	fmt.Println("And see that the second one has been removed:")
	printRing()
	// Output:
	// We start with a basic one replica ring:
	// [1 0]
	// And add a replica...
	// Note the new replicas are not assigned yet:
	// [1 0]
	// [-1 -1]
	// So we rebalance...
	// And now they are assigned:
	// [1 0]
	// [0 1]
	// Let's change back to one replica...
	// And see that the second one has been removed:
	// [1 0]
}

func ExampleBuilder_Rebalance_inDepth() {
	builder := ring.Builder{
		Nodes: []*ring.Node{
			{Capacity: 1},
			{Capacity: 1},
		},
	}
	builder.Rebalance()
	printRing := func() {
		for _, partitionToNodeIndex := range builder.Ring {
			fmt.Printf("%v\n", partitionToNodeIndex)
		}
	}
	fmt.Println("Here are the initial assignments:")
	printRing()
	fmt.Println("Let's triple the capacity of a node and rebalance...")
	builder.Nodes[0].Capacity = 3
	builder.AddLastMoved(builder.MoveWait + 1) // Pretend time has passed
	builder.Rebalance()
	fmt.Println("Note that node now has three times the assignments, and that the partition count grew:")
	printRing()
	fmt.Println("Let's add another node, with a capacity of 2 to make things difficult, and rebalance...")
	builder.Nodes = append(builder.Nodes, &ring.Node{Capacity: 2})
	builder.AddLastMoved(builder.MoveWait + 1) // Pretend time has passed
	builder.Rebalance()
	printRing()
	fmt.Println("Node 0 now has eight assignments, node 1 has three, and node 2 has five.")
	sidestepGoVet := `The partition count grows just until it gets close to balanced, using builder.PointsAllowed (default +-1%) as a helper.
The capacities of the nodes were 3, 1, and 2; meaning 50%, 16.67%, and 33.33% of the assignments.
They got 50%, 18.75%, and 31.25%, which isn't too far off, but is usually much better with more nodes.
Let's try doubling the nodes and see how the balance is then...`
	fmt.Println(sidestepGoVet)
	builder.Nodes = append(builder.Nodes, &ring.Node{Capacity: 3}, &ring.Node{Capacity: 1}, &ring.Node{Capacity: 2})
	builder.AddLastMoved(builder.MoveWait + 1) // Pretend time has passed
	builder.Rebalance()
	assignments := make([]int, len(builder.Nodes))
	for _, partitionToNodeIndex := range builder.Ring {
		for _, nodeIndex := range partitionToNodeIndex {
			assignments[nodeIndex]++
		}
	}
	totalCapacity := 0
	for _, node := range builder.Nodes {
		totalCapacity += int(node.Capacity)
	}
	for nodeIndex, count := range assignments {
		fmt.Printf("Node %d wanted %.02f%% and got %.02f%%\n", nodeIndex, float64(builder.Nodes[nodeIndex].Capacity)/float64(totalCapacity)*100, float64(count)/float64(len(builder.Ring[0]))*100)
	}
	// Output:
	// Here are the initial assignments:
	// [1 0]
	// Let's triple the capacity of a node and rebalance...
	// Note that node now has three times the assignments, and that the partition count grew:
	// [1 0 0 0]
	// Let's add another node, with a capacity of 2 to make things difficult, and rebalance...
	// [1 1 1 2 0 0 0 0 0 0 0 0 2 2 2 2]
	// Node 0 now has eight assignments, node 1 has three, and node 2 has five.
	// The partition count grows just until it gets close to balanced, using builder.PointsAllowed (default +-1%) as a helper.
	// The capacities of the nodes were 3, 1, and 2; meaning 50%, 16.67%, and 33.33% of the assignments.
	// They got 50%, 18.75%, and 31.25%, which isn't too far off, but is usually much better with more nodes.
	// Let's try doubling the nodes and see how the balance is then...
	// Node 0 wanted 25.00% and got 25.00%
	// Node 1 wanted 8.33% and got 8.20%
	// Node 2 wanted 16.67% and got 16.80%
	// Node 3 wanted 25.00% and got 25.00%
	// Node 4 wanted 8.33% and got 8.20%
	// Node 5 wanted 16.67% and got 16.80%
}

func ExampleBuilder_RemoveNode() {
	builder := ring.Builder{
		Nodes: []*ring.Node{
			{Capacity: 1},
			{Capacity: 1},
			{Capacity: 1},
			{Capacity: 1},
		},
	}
	builder.Rebalance()
	printRing := func() {
		for _, partitionToNodeIndex := range builder.Ring {
			fmt.Printf("%v\n", partitionToNodeIndex)
		}
	}
	fmt.Println("Here are the initial assignments:")
	printRing()
	builder.RemoveNode(2)
	fmt.Println("And now the assignments after removing a node:")
	printRing()
	// Output:
	// Here are the initial assignments:
	// [1 2 3 0]
	// And now the assignments after removing a node:
	// [1 -1 2 0]
}
