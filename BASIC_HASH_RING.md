# Basic Hash Ring

> Copyright See AUTHORS. All rights reserved.  

Here I will describe a basic consistent hashing ring, as often implemented
elsewhere. Lastly, I'll do a quick comparison to the results from this
library's partition ring, though a fuller explanation of the tradeoffs can be
found in [Partition Ring vs. Hash Ring](PARTITION_RING_VS_HASH_RING.md).

"Consistent Hashing" is a term used to describe a process where data (or work
of some sort) is distributed using a hashing algorithm to determine its
location. Using only the hash of the ID of the data, you can determine exactly
where that data should be. This map of hashes to locations is usually termed a
"ring".

Probably the simplest hash is just a modulus of the ID. For instance, if all
IDs are numbers and you have two machines you wish to distribute data to, you
could just put all odd numbered IDs on one machine and even numbered IDs on the
other. Assuming you have a balanced number of odd and even IDs, and a balanced
data size per ID, your data would be balanced between the two machines.

Since data IDs are often textual names and not numbers, like paths for files or
URLs, it makes sense to use a "real" hashing algorithm to convert the names to
numbers first. Using MD5 for instance, the hash of the name 'mom.png' is
'4559a12e3e8da7c2186250c2f292e3af' and the hash of 'dad.png' is
'096edcc4107e9e18d6a03a43b3853bea'. Now, using the modulus, we can place
'mom.jpg' on the odd machine and 'dad.png' on the even one. Another benefit of
using a hashing algorithm like MD5 is that the resulting hashes have a known
even distribution, meaning your IDs will be evenly distributed without worrying
about keeping the ID values themselves evenly distributed.

> For further discussion on picking a hashing algorithm, there's an
[interesting read on StackExchange](http://programmers.stackexchange.com/questions/49550/which-hashing-algorithm-is-best-for-uniqueness-and-speed).

For ease of implementation here, I'm going to use Go's built in FNV hash
library. First, here's code to examine a ring using modulus distribution:

```go
package main

import (
    "fmt"
    "hash/fnv"
)

func Hash(item int) uint64 {
    hasher := fnv.New64a()
    hasher.Write([]byte(fmt.Sprintf("%d", item)))
    return hasher.Sum64()
}

const ITEMS = 1000000
const NODES = 100

func main() {
    countPerNode := make([]int, NODES)
    for i := 0; i < ITEMS; i++ {
        countPerNode[int(Hash(i)%NODES)]++
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
}
```

```
9780 to 10215 assigments per node, target was 10000.
That's 2.20% under and 2.15% over.
```

That's pretty good balance, as expected. But what happens when we add a node?
We'll test out the theory by adding the following code to the main function.
The new code just compares the node assignments for each item for 100 nodes and
101 nodes, counting up all the items that have changed nodes:

```go
moved := 0
for i := 0; i < ITEMS; i++ {
    hasher := fnv.New64a()
    hasher.Write([]byte(fmt.Sprintf("%d", i)))
    x := hasher.Sum64()
    if int(x%NODES) != int(x%(NODES+1)) {
        moved++
    }
}
fmt.Printf("%d items moved, %.02f%%.\n",
    moved, float64(moved)/float64(ITEMS)*100)
```

```
990214 items moved, 99.02%.
```

Well, that's not good at all. Over 99% of the items have changed nodes. If this
was a real cluster with real data, practically all the data would have to be
moved just because we added one node.

That's where consistent hashing comes in. Instead of using modulus, we give the
nodes hashes too, making them responsible for ranges of the hash space. Adding
a new node will just take a some of the hash space from another node, rather
than shifting all the data around.

As an example, imagine the hash space is 0-9 and we have two nodes. If we hash
Node A's ID and get, say, 3, then Node A would be responsible for the hash
space from 0 to 3. Let's say Node B gets hashed to 9; that'd give it the 4-9
hash space. That means Node A will have just four hash values assigned and Node
B will have six; this isn't balanced, but that's okay for now.

The benefit this gives us is when we add another node, Node C. Let's say it
hashes to 6, that would assign it the hash space from 4-6. So it took 3 values
away from Node B. Now, Node A still has four values, Node B has just three now,
and Node C took over three. But only three changed assignments, rather than
most the values as with modulus.

With that example, I picked the final value 9 for Node B to simplify things,
but if you imagine it as 8 instead, then the 9 hash space has no Node at or
above its value. In that case, the assignment wraps around to the lowest Node,
Node A in this case. That's where the ring gets its name from, as it can be
visualized as a ring upon which nodes lie. Perhaps this visualization can help:

![](BASIC_HASH_RING.png)

Now I'm going to provide code for a very simple hash ring. This ring will
consist of a sorted list of node hashes and a map from node hash back to the
node ID. To discover on which node an item should be, we search for the item's
hash in the sorted list looking for the closest node hash greater than or equal
to our item's hash, then use the map to get the node ID. The output will be as
before, showing the under/over and movements when adding a node.

```go
package main

import (
    "fmt"
    "hash/fnv"
    "sort"
)

func Hash(x int) uint64 {
    hasher := fnv.New64a()
    hasher.Write([]byte(fmt.Sprintf("%d", x)))
    return hasher.Sum64()
}

const ITEMS = 1000000
const NODES = 100

type nodeSlice []uint64

func (ns nodeSlice) Len() int               { return len(ns) }
func (ns nodeSlice) Swap(a int, b int)      { ns[a], ns[b] = ns[b], ns[a] }
func (ns nodeSlice) Less(a int, b int) bool { return ns[a] < ns[b] }

func main() {
    ring := make([]uint64, NODES)
    hashesToNode := make(map[uint64]int, NODES)
    for n := 0; n < NODES; n++ {
        h := Hash(n)
        ring[n] = h
        hashesToNode[h] = n
    }
    sort.Sort(nodeSlice(ring))

    countPerNode := make([]int, NODES)
    for i := 0; i < ITEMS; i++ {
        h := Hash(i)
        x := sort.Search(len(ring), func(x int) bool { return ring[x] >= h })
        if x >= len(ring) {
            x = 0
        }
        countPerNode[hashesToNode[ring[x]]]++
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

    ring2 := make([]uint64, NODES+1)
    copy(ring2, ring)
    hashesToNode2 := make(map[uint64]int, NODES+1)
    for k, v := range hashesToNode {
        hashesToNode2[k] = v
    }
    h := Hash(NODES)
    ring2[NODES] = h
    hashesToNode2[h] = NODES
    sort.Sort(nodeSlice(ring2))

    moved := 0
    for i := 0; i < ITEMS; i++ {
        h := Hash(i)
        x := sort.Search(len(ring), func(x int) bool { return ring[x] >= h })
        if x >= len(ring) {
            x = 0
        }
        x2 := sort.Search(len(ring2), func(x int) bool { return ring2[x] >= h })
        if x2 >= len(ring2) {
            x2 = 0
        }
        if hashesToNode[ring[x]] != hashesToNode2[ring2[x2]] {
            moved++
        }
    }
    fmt.Printf("%d items moved, %.02f%%.\n",
        moved, float64(moved)/float64(ITEMS)*100)
}
```

```
1 to 659651 assigments per node, target was 10000.
That's 99.99% under and 6496.51% over.
240855 items moved, 24.09%.
```

That's horrible balance, at least one node is way overworked and at least one
other has practically nothing. But the movements after adding a node is a lot
better. We increased the number of nodes by 1% and about 24% of the items
moved; not great, but a whole lot better than 99%.

To improve the algorithm, we can add the concept of _virtual nodes_. Virtual
nodes are a set of hashes that all map to a single actual node. Imagining the
ring once again, it's as if we placed multiple points on the ring all labelled
with the the same node, and then repeated that multiple point assignment with
the rest of the nodes. You end up with many more node hashes in the ring, but
the effect is that more and smaller portions are assigned, smoothing out the
ranges.

I'm going to modify our last program to include 1,000 virtual nodes per actual
node. I'll list the entire program for clarity:

```go
package main

import (
    "fmt"
    "hash/fnv"
    "sort"
)

func Hash(x int) uint64 {
    hasher := fnv.New64a()
    hasher.Write([]byte(fmt.Sprintf("%d", x)))
    return hasher.Sum64()
}

const ITEMS = 1000000
const NODES = 100
const VIRTUAL_NODES_PER_NODE = 1000

type nodeSlice []uint64

func (ns nodeSlice) Len() int               { return len(ns) }
func (ns nodeSlice) Swap(a int, b int)      { ns[a], ns[b] = ns[b], ns[a] }
func (ns nodeSlice) Less(a int, b int) bool { return ns[a] < ns[b] }

func main() {
    ring := make([]uint64, NODES*VIRTUAL_NODES_PER_NODE)
    hashesToNode := make(map[uint64]int, NODES*VIRTUAL_NODES_PER_NODE)
    for p, n := 0, 0; n < NODES; n++ {
        for v := 0; v < VIRTUAL_NODES_PER_NODE; v++ {
            h := Hash(n*1000000 + v)
            ring[p] = h
            p++
            hashesToNode[h] = n
        }
    }
    sort.Sort(nodeSlice(ring))

    countPerNode := make([]int, NODES)
    for i := 0; i < ITEMS; i++ {
        h := Hash(i)
        x := sort.Search(len(ring), func(x int) bool { return ring[x] >= h })
        if x >= len(ring) {
            x = 0
        }
        countPerNode[hashesToNode[ring[x]]]++
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

    ring2 := make([]uint64, (NODES+1)*VIRTUAL_NODES_PER_NODE)
    copy(ring2, ring)
    hashesToNode2 := make(map[uint64]int, (NODES+1)*VIRTUAL_NODES_PER_NODE)
    for k, v := range hashesToNode {
        hashesToNode2[k] = v
    }
    for p, v := NODES*VIRTUAL_NODES_PER_NODE, 0; v < VIRTUAL_NODES_PER_NODE; v++ {
        h := Hash(NODES*1000000 + v)
        ring2[p] = h
        p++
        hashesToNode2[h] = NODES
    }
    sort.Sort(nodeSlice(ring2))

    moved := 0
    for i := 0; i < ITEMS; i++ {
        h := Hash(i)
        x := sort.Search(len(ring), func(x int) bool { return ring[x] >= h })
        if x >= len(ring) {
            x = 0
        }
        x2 := sort.Search(len(ring2), func(x int) bool { return ring2[x] >= h })
        if x2 >= len(ring2) {
            x2 = 0
        }
        if hashesToNode[ring[x]] != hashesToNode2[ring2[x2]] {
            moved++
        }
    }
    fmt.Printf("%d items moved, %.02f%%.\n",
        moved, float64(moved)/float64(ITEMS)*100)
}
```

```
2920 to 27557 assigments per node, target was 10000.
That's 70.80% under and 175.57% over.
10279 items moved, 1.03%.
```

The balance has become much better and though it might not be as good as we
like, even more virtual nodes can continue to help with that. The number of
items moved is impressive though; now when we added one more node, only about
1% of the items moved.

So that explains why consistent hashing is such a popular concept with
distributed systems. However, I should note that this library does not use a
hash ring exactly, it uses a partition ring. A partition ring is quite similar
in function but the implementation differs; see [Partition Ring vs. Hash
Ring](PARTITION_RING_VS_HASH_RING.md) for more information on the tradeoffs. I
just wanted to use this page to describe a hash ring as it is the more common
algorithm in use. For a peek into the performance of the partition ring
algorithm, here is the above code translated for this library:

```go
package main

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"time"

	"github.com/gholt/ring/lowring"
)

func main() {
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
}
```

```
9554 to 10289 assignments per node, target was 10000.
That's 4.46% under and 2.89% over.
9815 items moved, 0.98%.
```

The under/over is much, much better. Again, there are more tradeoffs between
partition rings and hash rings, but see [Partition Ring vs. Hash
Ring](PARTITION_RING_VS_HASH_RING.md) for more information.

I also ran each sample program 100 times by just making a new main function
that called the original 100 times. The first hash ring with no virtual nodes
took 46.277s, the second hash ring with virtual nodes took 1m5.711s, and the
partition ring from this library took 36.244s. Granted, this isn't exactly the
best benchmark; ideally a benchmark would measure creation speeds separately
from modification speeds separately from lookup speeds, etc. But it gives a
general idea.

> TODO(GLH): Need to redo these tests because this library is much slower than
> it was since I improved the placement calculations and haven't yet optimized
> for speed again.

There are also other more complex topics with rings, like node weights (or
capacities) where you'd give nodes different amounts of items based on their
weight, and replicas (or copies or backup nodes) where you'd assign items to
distinct secondary nodes, among other features. But I think this is enough for
one page.

> Other interesting ideas in this space:  
> [Jump consistent hashing](http://arxiv.org/abs/1406.2294) - [dgryski implementation](https://github.com/dgryski/go-jump) also [dgryski shared key-value store](https://github.com/dgryski/go-shardedkv)  
> [Multi-probe consistent hashing](http://arxiv.org/pdf/1505.00062.pdf) - [dgryski implementation](https://github.com/dgryski/go-mpchash)  
> [GreenCHT replication scheme](http://storageconference.us/2015/Papers/16.Zhao.pdf)
