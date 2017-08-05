package ring_test

import (
	"fmt"

	"github.com/gholt/ring"
)

func Example_overview() {
	// We'll create a new builder for a ring with three replicas:
	builder := ring.NewBuilder()
	builder.SetReplicaCount(3)
	// And we'll add eight nodes we'll label A-H:
	for n := 'A'; n <= 'H'; n++ {
		node := builder.AddNode()
		node.SetCapacity(1)
		node.SetInfo(string(n))
	}
	// Generate the ring:
	builder.Rebalance()
	rring := builder.Ring()
	// Print out the ring assignments: partitions horizontally, replicas vertically:
	// `  P0 P1 P2 P3 P4 P5 P6 P7
	// R0  C  F  B  G  A  E  D  H
	// R1  H  D  G  B  E  F  A  C
	// R2  E  A  H  C  D  B  F  G
	fmt.Print("` ")
	for partition := 0; partition < rring.PartitionCount(); partition++ {
		fmt.Print(fmt.Sprintf(" P%d", partition))
	}
	fmt.Println()
	for replica := 0; replica < rring.ReplicaCount(); replica++ {
		fmt.Print(fmt.Sprintf("R%d", replica))
		for partition := 0; partition < rring.PartitionCount(); partition++ {
			fmt.Print("  " + rring.KeyNodes(partition)[replica].Info())
		}
		fmt.Println()
	}
	// Output:
	// `  P0 P1 P2 P3 P4 P5 P6 P7
	// R0  C  F  B  G  A  E  D  H
	// R1  H  D  G  B  E  F  A  C
	// R2  E  A  H  C  D  B  F  G
}

func Example_tiers() {
	fmt.Println("Tiers can be confusing, so let's work with a detailed example.")
	fmt.Println("We are going to have two servers, each with two disk drives.")
	fmt.Println("The disk drives are going to represented by the nodes themselves.")
	fmt.Println("The servers will be represented by a tier.")
	fmt.Println("By defining a tier, we can tell the Builder to build rings with replicas assigned as far apart as possible, while keeping the whole Ring in balance.")
	fmt.Println("So let's define and build our ring; we'll use two replicas to start with...")
	builder := ring.NewBuilder()
	builder.SetReplicaCount(2)
	for _, server := range []string{"ServerA", "ServerB"} {
		for _, disk := range []string{"1stDisk", "2ndDisk"} {
			node := builder.AddNode()
			node.SetInfo(disk)
			node.SetTier(0, server)
			node.SetCapacity(1)
		}
	}
	builder.Rebalance()
	rring := builder.Ring()
	printRing := func(rring ring.Ring) {
		fmt.Println("Here are the ring assignments: partitions horizontally, replicas vertically:")
		fmt.Print("` ")
		for partition := 0; partition < rring.PartitionCount(); partition++ {
			fmt.Print(fmt.Sprintf("  -------P%d------", partition))
		}
		fmt.Println()
		for replica := 0; replica < rring.ReplicaCount(); replica++ {
			fmt.Print(fmt.Sprintf("R%d", replica))
			for partition := 0; partition < rring.PartitionCount(); partition++ {
				node := rring.KeyNodes(partition)[replica]
				fmt.Print("  " + node.Tier(0) + ":" + node.Info())
			}
			fmt.Println()
		}
	}
	printRing(rring)
	// `   -------P0------  -------P1------
	// R0  ServerB:1stDisk  ServerA:1stDisk
	// R1  ServerA:2ndDisk  ServerB:2ndDisk
	fmt.Println("Note that it assigned each replica of a partition to a distinct server.")
	fmt.Println("If the node info (disk name) happened to be the same it wouldn't matter since they are on different servers.")
	fmt.Println()
	fmt.Println("Let's up the replica count to 3, where we know it will have to assign multiple replicas to a single server...")
	builder = ring.NewBuilder()
	builder.SetReplicaCount(3)
	for _, server := range []string{"ServerA", "ServerB"} {
		for _, disk := range []string{"1stDisk", "2ndDisk"} {
			node := builder.AddNode()
			node.SetInfo(disk)
			node.SetTier(0, server)
			node.SetCapacity(1)
		}
	}
	builder.Rebalance()
	rring = builder.Ring()
	printRing(rring)
	// `   -------P0------  -------P1------  -------P2------  -------P3------
	// R0  ServerB:1stDisk  ServerB:2ndDisk  ServerA:2ndDisk  ServerA:1stDisk
	// R1  ServerA:2ndDisk  ServerA:1stDisk  ServerB:2ndDisk  ServerB:1stDisk
	// R2  ServerA:1stDisk  ServerA:2ndDisk  ServerB:1stDisk  ServerB:2ndDisk
	fmt.Println("So now it ended up using servers twice within the same partition, but note that it made sure to pick distinct drives for each replica at least.")
	fmt.Println()
	fmt.Println("Let's get more complicated and define another tier, will call it the region tier.")
	fmt.Println("And we'll assign our first two servers to the East region, and add two more servers in the Cent region, and even two more servers in the West region.")
	builder = ring.NewBuilder()
	builder.SetReplicaCount(3)
	for _, region := range []string{"East", "Cent", "West"} {
		for _, server := range []string{"ServerA", "ServerB"} {
			for _, disk := range []string{"1stDisk", "2ndDisk"} {
				node := builder.AddNode()
				node.SetInfo(disk)
				node.SetTier(0, server)
				node.SetTier(1, region)
				node.SetCapacity(1)
			}
		}
	}
	builder.Rebalance()
	rring = builder.Ring()
	fmt.Println("Here are the ring assignments: partitions horizontally, replicas vertically:")
	// `   ---------P0---------  ---------P1---------  ---------P2---------  ---------P3---------
	// R0  Cent:ServerB:2ndDisk  East:ServerA:1stDisk  West:ServerA:2ndDisk  Cent:ServerB:1stDisk
	// R1  West:ServerA:1stDisk  Cent:ServerA:2ndDisk  East:ServerA:2ndDisk  West:ServerB:2ndDisk
	// R2  East:ServerB:1stDisk  West:ServerB:1stDisk  Cent:ServerA:1stDisk  East:ServerB:2ndDisk
	fmt.Print("` ")
	for partition := 0; partition < rring.PartitionCount(); partition++ {
		fmt.Print(fmt.Sprintf("  ---------P%d---------", partition))
	}
	fmt.Println()
	for replica := 0; replica < rring.ReplicaCount(); replica++ {
		fmt.Print(fmt.Sprintf("R%d", replica))
		for partition := 0; partition < rring.PartitionCount(); partition++ {
			node := rring.KeyNodes(partition)[replica]
			fmt.Print("  " + node.Tier(1) + ":" + node.Tier(0) + ":" + node.Info())
		}
		fmt.Println()
	}
	fmt.Println("So now you can see it assigned replicas in distinct regions before worrying about the lower tiers.")

	// Output:
	// Tiers can be confusing, so let's work with a detailed example.
	// We are going to have two servers, each with two disk drives.
	// The disk drives are going to represented by the nodes themselves.
	// The servers will be represented by a tier.
	// By defining a tier, we can tell the Builder to build rings with replicas assigned as far apart as possible, while keeping the whole Ring in balance.
	// So let's define and build our ring; we'll use two replicas to start with...
	// Here are the ring assignments: partitions horizontally, replicas vertically:
	// `   -------P0------  -------P1------
	// R0  ServerB:1stDisk  ServerA:1stDisk
	// R1  ServerA:2ndDisk  ServerB:2ndDisk
	// Note that it assigned each replica of a partition to a distinct server.
	// If the node info (disk name) happened to be the same it wouldn't matter since they are on different servers.
	//
	// Let's up the replica count to 3, where we know it will have to assign multiple replicas to a single server...
	// Here are the ring assignments: partitions horizontally, replicas vertically:
	// `   -------P0------  -------P1------  -------P2------  -------P3------
	// R0  ServerB:1stDisk  ServerB:2ndDisk  ServerA:2ndDisk  ServerA:1stDisk
	// R1  ServerA:2ndDisk  ServerA:1stDisk  ServerB:2ndDisk  ServerB:1stDisk
	// R2  ServerA:1stDisk  ServerA:2ndDisk  ServerB:1stDisk  ServerB:2ndDisk
	// So now it ended up using servers twice within the same partition, but note that it made sure to pick distinct drives for each replica at least.
	//
	// Let's get more complicated and define another tier, will call it the region tier.
	// And we'll assign our first two servers to the East region, and add two more servers in the Cent region, and even two more servers in the West region.
	// Here are the ring assignments: partitions horizontally, replicas vertically:
	// `   ---------P0---------  ---------P1---------  ---------P2---------  ---------P3---------
	// R0  Cent:ServerB:2ndDisk  East:ServerA:1stDisk  West:ServerA:2ndDisk  Cent:ServerB:1stDisk
	// R1  West:ServerA:1stDisk  Cent:ServerA:2ndDisk  East:ServerA:2ndDisk  West:ServerB:2ndDisk
	// R2  East:ServerB:1stDisk  West:ServerB:1stDisk  Cent:ServerA:1stDisk  East:ServerB:2ndDisk
	// So now you can see it assigned replicas in distinct regions before worrying about the lower tiers.
}
