package ring_test

import (
	"fmt"

	"github.com/gholt/ring"
)

func Example_overview() {
	// We'll create a new builder for a ring with three replicas:
	builder := ring.NewBuilder(3)
	// And we'll add four nodes we'll label ABCD:
	for n := 'A'; n <= 'D'; n++ {
		builder.AddNode(string(n), 1, nil)
	}
	// Generate the ring:
	builder.Rebalance()
	rring := builder.Ring()
	// Print out the ring assignments: partitions horizontally, replicas vertically:
	// `  P0 P1 P2 P3
	// R0  A  B  C  D
	// R1  B  A  D  C
	// R2  D  C  A  B
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
	// `  P0 P1 P2 P3
	// R0  A  B  C  D
	// R1  B  A  D  C
	// R2  D  C  A  B
}

func Example_groupTiers() {
	fmt.Println("Group tiers can be confusing, so let's work with a detailed example.")
	fmt.Println("We are going to have two servers, each with two disk drives.")
	fmt.Println("The disk drives are going to be represented by the nodes themselves.")
	fmt.Println("The servers will be represented by groups.")
	fmt.Println("By defining groups, we can tell the Builder to build rings with replicas assigned as far apart as possible, while keeping the whole Ring in balance.")
	fmt.Println("So let's define and build our ring; we'll use two replicas to start with...")
	builder := ring.NewBuilder(2)
	builder.SetMaxPartitionCount(2) // Just to keep the output simpler
	for _, server := range []string{"ServerA", "ServerB"} {
		group := builder.AddGroup(server, nil)
		for _, disk := range []string{"1stDisk", "2ndDisk"} {
			builder.AddNode(disk, 1, group)
		}
	}
	builder.Rebalance()
	rring := builder.Ring()
	printRing := func(rring *ring.Ring) {
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
				fmt.Print("  " + node.Group().Info() + ":" + node.Info())
			}
			fmt.Println()
		}
	}
	printRing(rring)
	// `   -------P0------  -------P1------
	// R0  ServerA:1stDisk  ServerB:2ndDisk
	// R1  ServerB:1stDisk  ServerA:2ndDisk
	fmt.Println("Note that it assigned each replica of a partition to a distinct server.")
	fmt.Println("The node info (disk names) happened to be the same but it doesn't matter since they are on different servers.")
	fmt.Println()
	fmt.Println("Let's up the replica count to 3, where we know it will have to assign multiple replicas to a single server...")
	builder = ring.NewBuilder(3)
	builder.SetMaxPartitionCount(4) // Just to keep the output simpler
	for _, server := range []string{"ServerA", "ServerB"} {
		group := builder.AddGroup(server, nil)
		for _, disk := range []string{"1stDisk", "2ndDisk"} {
			builder.AddNode(disk, 1, group)
		}
	}
	builder.Rebalance()
	rring = builder.Ring()
	printRing(rring)
	// `   -------P0------  -------P1------  -------P2------  -------P3------
	// R0  ServerA:1stDisk  ServerB:2ndDisk  ServerA:2ndDisk  ServerB:1stDisk
	// R1  ServerB:1stDisk  ServerA:1stDisk  ServerB:2ndDisk  ServerA:2ndDisk
	// R2  ServerA:2ndDisk  ServerB:1stDisk  ServerA:1stDisk  ServerB:2ndDisk
	fmt.Println("So now it ended up using servers twice within the same partition, but note that it made sure to pick distinct drives for each replica at least.")
	fmt.Println()
	fmt.Println("Let's get more complicated and define another tier of groups; we'll call it the region tier.")
	fmt.Println("To do this, we simply create the new region groups and set them as the parents of the server groups.")
	fmt.Println("We'll assign our first two servers to the East region, and add two more servers in the Cent region, and even two more servers in the West region.")
	builder = ring.NewBuilder(3)
	builder.SetMaxPartitionCount(4) // Just to keep the output simpler
	for _, region := range []string{"East", "Cent", "West"} {
		regionGroup := builder.AddGroup(region, nil)
		for _, server := range []string{"ServerA", "ServerB"} {
			serverGroup := builder.AddGroup(server, regionGroup)
			for _, disk := range []string{"1stDisk", "2ndDisk"} {
				builder.AddNode(disk, 1, serverGroup)
			}
		}
	}
	builder.Rebalance()
	rring = builder.Ring()
	fmt.Println("Here are the ring assignments: partitions horizontally, replicas vertically:")
	// `   ---------P0---------  ---------P1---------  ---------P2---------  ---------P3---------
	// R0  East:ServerA:1stDisk  Cent:ServerB:2ndDisk  West:ServerB:2ndDisk  East:ServerB:1stDisk
	// R1  Cent:ServerA:1stDisk  East:ServerA:2ndDisk  Cent:ServerA:2ndDisk  West:ServerB:1stDisk
	// R2  West:ServerA:1stDisk  West:ServerA:2ndDisk  East:ServerB:2ndDisk  Cent:ServerB:1stDisk
	fmt.Print("` ")
	for partition := 0; partition < rring.PartitionCount(); partition++ {
		fmt.Print(fmt.Sprintf("  ---------P%d---------", partition))
	}
	fmt.Println()
	for replica := 0; replica < rring.ReplicaCount(); replica++ {
		fmt.Print(fmt.Sprintf("R%d", replica))
		for partition := 0; partition < rring.PartitionCount(); partition++ {
			node := rring.KeyNodes(partition)[replica]
			fmt.Print("  " + node.Group().Parent().Info() + ":" + node.Group().Info() + ":" + node.Info())
		}
		fmt.Println()
	}
	fmt.Println("So now you can see it assigned replicas in distinct regions before worrying about the lower tiers.")

	// Output:
	// Group tiers can be confusing, so let's work with a detailed example.
	// We are going to have two servers, each with two disk drives.
	// The disk drives are going to be represented by the nodes themselves.
	// The servers will be represented by groups.
	// By defining groups, we can tell the Builder to build rings with replicas assigned as far apart as possible, while keeping the whole Ring in balance.
	// So let's define and build our ring; we'll use two replicas to start with...
	// Here are the ring assignments: partitions horizontally, replicas vertically:
	// `   -------P0------  -------P1------
	// R0  ServerA:1stDisk  ServerB:2ndDisk
	// R1  ServerB:1stDisk  ServerA:2ndDisk
	// Note that it assigned each replica of a partition to a distinct server.
	// The node info (disk names) happened to be the same but it doesn't matter since they are on different servers.
	//
	// Let's up the replica count to 3, where we know it will have to assign multiple replicas to a single server...
	// Here are the ring assignments: partitions horizontally, replicas vertically:
	// `   -------P0------  -------P1------  -------P2------  -------P3------
	// R0  ServerA:1stDisk  ServerB:2ndDisk  ServerA:2ndDisk  ServerB:1stDisk
	// R1  ServerB:1stDisk  ServerA:1stDisk  ServerB:2ndDisk  ServerA:2ndDisk
	// R2  ServerA:2ndDisk  ServerB:1stDisk  ServerA:1stDisk  ServerB:2ndDisk
	// So now it ended up using servers twice within the same partition, but note that it made sure to pick distinct drives for each replica at least.
	//
	// Let's get more complicated and define another tier of groups; we'll call it the region tier.
	// To do this, we simply create the new region groups and set them as the parents of the server groups.
	// We'll assign our first two servers to the East region, and add two more servers in the Cent region, and even two more servers in the West region.
	// Here are the ring assignments: partitions horizontally, replicas vertically:
	// `   ---------P0---------  ---------P1---------  ---------P2---------  ---------P3---------
	// R0  East:ServerA:1stDisk  Cent:ServerB:2ndDisk  West:ServerB:2ndDisk  East:ServerB:1stDisk
	// R1  Cent:ServerA:1stDisk  East:ServerA:2ndDisk  Cent:ServerA:2ndDisk  West:ServerB:1stDisk
	// R2  West:ServerA:1stDisk  West:ServerA:2ndDisk  East:ServerB:2ndDisk  Cent:ServerB:1stDisk
	// So now you can see it assigned replicas in distinct regions before worrying about the lower tiers.
}
