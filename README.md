# Ring
## Development Repository

**Experimental: No stable version of this package yet exists; it is still in
early development.**

> _If you're not entirely sure what consistent hashing is, reading [Basic Hash
> Ring](BASIC_HASH_RING.md) might help._

Package ring contains tools for building and using a consistent hashing ring
with replicas, automatic partitioning (ring ranges), and keeping replicas of
the same partitions in as distinct tiered nodes as possible (tiers might be
devices, servers, cabinets, rooms, data centers, geographical regions, etc.)

Here's a quick example of building a ring and discovering what items are
assigned to what nodes:

```go
package main

import (
    "fmt"
    "hash/fnv"

    "github.com/gholt/ring"
)

func main() {
    // Note that we're ignoring errors for the purpose of a shorter example.
    // The 64 indicates how many bits can be used in a uint64 for node IDs;
    // 64 is fine unless you have a specific use case.
    builder := ring.NewBuilder(64)
    // (active, capacity, no tiers, no addresses, meta, no conf)
    builder.AddNode(true, 1, nil, nil, "NodeA", nil)
    builder.AddNode(true, 1, nil, nil, "NodeB", nil)
    builder.AddNode(true, 1, nil, nil, "NodeC", nil)
    // This rebalances if necessary and provides a usable Ring instance.
    ring := builder.Ring()
    // This value indicates how many bits are in use for determining ring
    // partitions.
    partitionBitCount := ring.PartitionBitCount()
    for _, item := range []string{"First", "Second", "Third"} {
        // We're using fnv hashing here, but you can use whatever you like.
        // We don't actually recommend fnv, but it's useful for this example.
        hasher := fnv.New64a()
        hasher.Write([]byte(item))
        partition := uint32(hasher.Sum64() >> (64 - partitionBitCount))
        // We can just grab the first node since this example just uses one
        // replica. See Builder.SetReplicaCount for more information.
        node := ring.ResponsibleNodes(partition)[0]
        fmt.Printf("%s is handled by %v\n", item, node.Meta())
    }
}
```

The output would be:

```
First is handled by NodeC
Second is handled by NodeB
Third is handled by NodeB
```

[API Documentation](http://godoc.org/github.com/gholt/ring)  
[Basic Hash Ring](BASIC_HASH_RING.md)  
[Partition Ring vs. Hash Ring](PARTITION_RING_VS_HASH_RING.md)

> Other interesting ideas in this space:  
> [Jump consistent hashing](http://arxiv.org/abs/1406.2294) - [dgryski implementation](https://github.com/dgryski/go-jump) also [dgryski shared key-value store](https://github.com/dgryski/go-shardedkv)  
> [Multi-probe consistent hashing](http://arxiv.org/pdf/1505.00062.pdf) - [dgryski implementation](https://github.com/dgryski/go-mpchash)  
> [GreenCHT replication scheme](http://storageconference.us/2015/Papers/16.Zhao.pdf)

This is the latest development area for the package.  
Eventually a stable version of the package will be established but, for now,
all things about this package are subject to change.

> Copyright See AUTHORS. All rights reserved.  
> Use of this source code is governed by a BSD-style  
> license that can be found in the LICENSE file.
