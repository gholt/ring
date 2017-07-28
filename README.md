# Ring
## Development Repository

**Experimental: No stable version of this package yet exists; it is still in
early development.**

Package ring provides a way to distribute replicas of partitioned items to
nodes.

An example would be a distributed storage system, storing duplicate copies of
each file on different drives, servers, or even data centers based on the
assignments given by the Ring.

> _If you're not entirely sure what consistent hashing is, reading [Basic Hash
> Ring](BASIC_HASH_RING.md) might help._

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
