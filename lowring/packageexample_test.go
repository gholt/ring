package lowring_test

import (
	"fmt"
	"hash/fnv"

	"github.com/gholt/ring/lowring"
)

func Example_tiers() {
	fmt.Println("Tiers can be confusing, so let's work with a detailed example.")
	fmt.Println("We are going to have two servers, each with two disk drives.")
	fmt.Println("The disk drives are going to represented by the nodes themselves.")
	fmt.Println("The servers will be represented by a tier.")
	fmt.Println("By defining a tier, we can tell the Builder to build rings with replicas assigned as far apart as possible, while keeping the whole Ring in balance.")
	fmt.Println("So let's define and build our ring; we'll use two replicas to start with...")
	nodeNames := []string{"firstDisk", "secondDisk", "firstDisk", "secondDisk"}
	tierNames := [][]string{
		{"serverOne", "serverTwo"}, // Tier 0 = servers
	}
	serverTier := 0
	builder := lowring.Builder{
		Nodes: []*lowring.Node{
			{Capacity: 1, TierIndexes: []int{0}}, // firstDisk, serverOne
			{Capacity: 1, TierIndexes: []int{0}}, // secondDisk, serverOne
			{Capacity: 1, TierIndexes: []int{1}}, // firstDisk, serverTwo
			{Capacity: 1, TierIndexes: []int{1}}, // secondDisk, serverTwo
		},
	}
	builder.SetReplicaCount(2)
	builder.Rebalance()
	fmt.Println("Here are the node assignments:")
	//     [2 0]
	//     [1 3]
	for _, partitionToNodeIndex := range builder.Ring {
		fmt.Printf("    %v\n", partitionToNodeIndex)
	}
	fmt.Println("And here are the descriptive replica assigments for partition 0:")
	//     Replica 0 assigned to firstDisk of serverTwo
	//     Replica 1 assigned to secondDisk of serverOne
	partition := 0
	for replica := 0; replica < builder.ReplicaCount(); replica++ {
		fmt.Printf("    Replica %d assigned to %s of %s\n", replica,
			nodeNames[builder.Ring[replica][partition]],
			tierNames[serverTier][builder.Nodes[builder.Ring[replica][partition]].TierIndexes[serverTier]])
	}
	fmt.Println("And here are the replica assigments for partition 1:")
	//     Replica 0 assigned to firstDisk of serverOne
	//     Replica 1 assigned to secondDisk of serverTwo
	partition = 1
	for replica := 0; replica < builder.ReplicaCount(); replica++ {
		fmt.Printf("    Replica %d assigned to %s of %s\n", replica,
			nodeNames[builder.Ring[replica][partition]],
			tierNames[serverTier][builder.Nodes[builder.Ring[replica][partition]].TierIndexes[serverTier]])
	}
	fmt.Println("Note that it assigned each replica of a partition to a distinct server.")
	fmt.Println("If the node name (disk name) happened to be the same it wouldn't matter since they are on different servers.")
	fmt.Println("Let's up the replica count to 3, where we know it will have to assign multiple replicas to a single server...")
	// Reset the ring, so it will completely rebalance freshly.
	builder.Ring = nil
	builder.LastMoved = nil
	builder.SetReplicaCount(3)
	builder.Rebalance()
	fmt.Println("And now here are the replica assigments for partition 0:")
	//     Replica 0 assigned to secondDisk of serverOne
	//     Replica 1 assigned to firstDisk of serverTwo
	//     Replica 2 assigned to secondDisk of serverTwo
	partition = 0
	for replica := 0; replica < builder.ReplicaCount(); replica++ {
		fmt.Printf("    Replica %d assigned to %s of %s\n", replica,
			nodeNames[builder.Ring[replica][partition]],
			tierNames[serverTier][builder.Nodes[builder.Ring[replica][partition]].TierIndexes[serverTier]])
	}
	fmt.Println("So now it ended up using serverTwo twice, but note that it made sure to pick distinct drives at least.")
	fmt.Println("Let's get more complicated and define another tier, will call it the region tier.")
	regionTier := len(tierNames)
	tierNames = append(tierNames, []string{"east", "central", "west"})
	fmt.Println("And we'll assign our first two servers to the east region, and add two more servers in the central region, and even two more servers in the west region.")
	for _, n := range builder.Nodes {
		n.TierIndexes = append(n.TierIndexes, 0) // east region
	}
	nodeNames = append(nodeNames, "firstDisk", "secondDisk", "firstDisk", "secondDisk")
	builder.Nodes = append(builder.Nodes, []*lowring.Node{
		{Capacity: 1, TierIndexes: []int{0, 1}}, // firstDisk, serverOne, central
		{Capacity: 1, TierIndexes: []int{0, 1}}, // secondDisk, serverOne, central
		{Capacity: 1, TierIndexes: []int{1, 1}}, // firstDisk, serverTwo, central
		{Capacity: 1, TierIndexes: []int{1, 1}}, // secondDisk, serverTwo, central
	}...)
	nodeNames = append(nodeNames, "firstDisk", "secondDisk", "firstDisk", "secondDisk")
	builder.Nodes = append(builder.Nodes, []*lowring.Node{
		{Capacity: 1, TierIndexes: []int{0, 2}}, // firstDisk, serverOne, west
		{Capacity: 1, TierIndexes: []int{0, 2}}, // secondDisk, serverOne, west
		{Capacity: 1, TierIndexes: []int{1, 2}}, // firstDisk, serverTwo, west
		{Capacity: 1, TierIndexes: []int{1, 2}}, // secondDisk, serverTwo, west
	}...)
	// Reset the ring, so it will completely rebalance freshly.
	builder.Ring = nil
	builder.LastMoved = nil
	builder.SetReplicaCount(3)
	builder.Rebalance()
	fmt.Println("And now here are the replica assigments for partition 0:")
	//     Replica 0 assigned to secondDisk of serverOne in the west region
	//     Replica 1 assigned to secondDisk of serverTwo in the central region
	//     Replica 2 assigned to secondDisk of serverOne in the east region
	partition = 0
	for replica := 0; replica < builder.ReplicaCount(); replica++ {
		fmt.Printf("    Replica %d assigned to %s of %s in the %s region\n", replica,
			nodeNames[builder.Ring[replica][partition]],
			tierNames[serverTier][builder.Nodes[builder.Ring[replica][partition]].TierIndexes[serverTier]],
			tierNames[regionTier][builder.Nodes[builder.Ring[replica][partition]].TierIndexes[regionTier]])
	}
	fmt.Println("So now you can see it assigned replicas in distinct regions before worrying about the lower tiers.")
	// Output:
	// Tiers can be confusing, so let's work with a detailed example.
	// We are going to have two servers, each with two disk drives.
	// The disk drives are going to represented by the nodes themselves.
	// The servers will be represented by a tier.
	// By defining a tier, we can tell the Builder to build rings with replicas assigned as far apart as possible, while keeping the whole Ring in balance.
	// So let's define and build our ring; we'll use two replicas to start with...
	// Here are the node assignments:
	//     [2 0]
	//     [1 3]
	// And here are the descriptive replica assigments for partition 0:
	//     Replica 0 assigned to firstDisk of serverTwo
	//     Replica 1 assigned to secondDisk of serverOne
	// And here are the replica assigments for partition 1:
	//     Replica 0 assigned to firstDisk of serverOne
	//     Replica 1 assigned to secondDisk of serverTwo
	// Note that it assigned each replica of a partition to a distinct server.
	// If the node name (disk name) happened to be the same it wouldn't matter since they are on different servers.
	// Let's up the replica count to 3, where we know it will have to assign multiple replicas to a single server...
	// And now here are the replica assigments for partition 0:
	//     Replica 0 assigned to secondDisk of serverOne
	//     Replica 1 assigned to firstDisk of serverTwo
	//     Replica 2 assigned to secondDisk of serverTwo
	// So now it ended up using serverTwo twice, but note that it made sure to pick distinct drives at least.
	// Let's get more complicated and define another tier, will call it the region tier.
	// And we'll assign our first two servers to the east region, and add two more servers in the central region, and even two more servers in the west region.
	// And now here are the replica assigments for partition 0:
	//     Replica 0 assigned to secondDisk of serverOne in the west region
	//     Replica 1 assigned to secondDisk of serverTwo in the central region
	//     Replica 2 assigned to secondDisk of serverOne in the east region
	// So now you can see it assigned replicas in distinct regions before worrying about the lower tiers.
}

// If you need to update this example, be sure to update
// https://github.com/gholt/ring/blob/master/BASIC_HASH_RING.md as well.

func Example_fromBasicHashRingDocument() {
	// This is the code from the end of
	// https://github.com/gholt/ring/blob/master/BASIC_HASH_RING.md and is
	// included here to ensure it runs properly.
	Hash := func(x int) uint64 {
		hasher := fnv.New64a()
		hasher.Write([]byte(fmt.Sprintf("%d", x)))
		return hasher.Sum64()
	}

	const ITEMS = 1000000
	const NODES = 100

	b := lowring.Builder{}
	for n := 0; n < NODES; n++ {
		b.Nodes = append(b.Nodes, &lowring.Node{Capacity: 1})
	}
	b.Rebalance()
	ring1 := b.Ring.Copy()

	partitionCount1 := uint64(ring1.PartitionCount())
	countPerNode := make([]int, NODES)
	for i := 0; i < ITEMS; i++ {
		n := ring1[0][Hash(i)%partitionCount1]
		countPerNode[n]++
	}
	min := ITEMS
	max := 0
	for n := 0; n < NODES; n++ {
		if countPerNode[n] < min {
			min = countPerNode[n]
		}
		if countPerNode[n] > max {
			max = countPerNode[n]
		}
	}
	t := ITEMS / NODES
	fmt.Printf("%d to %d assigments per node, target was %d.\n", min, max, t)
	fmt.Printf("That's %.02f%% under and %.02f%% over.\n",
		float64(t-min)/float64(t)*100, float64(max-t)/float64(t)*100)

	b.Nodes = append(b.Nodes, &lowring.Node{Capacity: 1})
	b.ShiftLastMoved(b.MoveWait * 2)
	b.Rebalance()
	ring2 := b.Ring.Copy()

	partitionCount2 := uint64(ring2.PartitionCount())
	moved := 0
	for i := 0; i < ITEMS; i++ {
		h := Hash(i)
		n1 := ring1[0][h%partitionCount1]
		n2 := ring2[0][h%partitionCount2]
		if n1 != n2 {
			moved++
		}
	}
	fmt.Printf("%d items moved, %.02f%%.\n",
		moved, float64(moved)/float64(ITEMS)*100)
	// Output:
	// 9768 to 10260 assigments per node, target was 10000.
	// That's 2.32% under and 2.60% over.
	// 10061 items moved, 1.01%.
}
