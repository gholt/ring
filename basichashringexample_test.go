package ring_test

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"time"

	"github.com/gholt/ring/lowring"
)

// See https://github.com/gholt/ring/blob/master/BASIC_HASH_RING.md

func Example_fromBasicHashRingDocument() {
	hash := func(x int) uint64 {
		hasher := fnv.New64a()
		hasher.Write([]byte(fmt.Sprintf("%d", x)))
		return hasher.Sum64()
	}
	randIntn := rand.New(rand.NewSource(0)).Intn

	const ITEMS = 1000000
	const NODES = 100

	r := lowring.New(1)
	for n := 0; n < NODES; n++ {
		r.AddNode(1, 0)
	}
	r.Rebalance(randIntn)
	// Copy the essential ring data
	ring1 := make([][]lowring.Node, len(r.ReplicaToPartitionToNode))
	for replica, partitionToNode := range r.ReplicaToPartitionToNode {
		ring1[replica] = make([]lowring.Node, len(partitionToNode))
		copy(ring1[replica], partitionToNode)
	}

	partitionCount1 := uint64(len(ring1[0]))
	countPerNode := make([]int, NODES)
	for i := 0; i < ITEMS; i++ {
		n := ring1[0][hash(i)%partitionCount1]
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
	fmt.Printf("%d to %d assignments per node, target was %d.\n", min, max, t)
	fmt.Printf("That's %.02f%% under and %.02f%% over.\n",
		float64(t-min)/float64(t)*100, float64(max-t)/float64(t)*100)

	r.AddNode(1, 0)
	// Reset wait time restrictions
	r.Rebalanced = r.Rebalanced.Add(-(time.Duration(r.ReassignmentWait) * time.Minute))
	r.Rebalance(randIntn)
	// Copy the essential ring data
	ring2 := make([][]lowring.Node, len(r.ReplicaToPartitionToNode))
	for replica, partitionToNode := range r.ReplicaToPartitionToNode {
		ring2[replica] = make([]lowring.Node, len(partitionToNode))
		copy(ring2[replica], partitionToNode)
	}

	partitionCount2 := uint64(len(ring2[0]))
	moved := 0
	for i := 0; i < ITEMS; i++ {
		h := hash(i)
		n1 := ring1[0][h%partitionCount1]
		n2 := ring2[0][h%partitionCount2]
		if n1 != n2 {
			moved++
		}
	}
	fmt.Printf("%d items moved, %.02f%%.\n",
		moved, float64(moved)/float64(ITEMS)*100)

	// Output:
	// 9554 to 10289 assignments per node, target was 10000.
	// That's 4.46% under and 2.89% over.
	// 9815 items moved, 0.98%.
}
