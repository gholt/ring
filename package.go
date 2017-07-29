// Package ring provides a way to distribute replicas of partitioned items to
// nodes.
//
// An example would be a distributed storage system, storing duplicate copies
// of each file on different drives, servers, or even data centers based on the
// assignments given by the Ring.
//
// See https://github.com/gholt/ring/blob/master/BASIC_HASH_RING.md for a
// introduction to consistent hashing and hashing rings.
//
// This package tries to be as simple and concise as possible, which is still
// pretty complex considering the subject matter. For example, nodes and tiers
// are just represented by indexes -- no names, IPs, etc. -- it is left to the
// user of the package to maintain such metadata.
//
// This package also aims for speed, which fights against the previous goal.
// Building and maintaining rings can be resource intensive, and optimizations
// can greatly reduce this impact. But optimizations often add complexity and
// reduce flexibility.
//
// Terms Used With This Package
//
// Node: A single unit within a distributed system. For example, a server or a
// single drive within a server.
//
// Partition: A numeric value from a range of values. These partitions are
// assigned to nodes to indicate each node's responsibilities, such as which
// data to store or which requests to process. Mapping these data items or
// requests to partitions is usually done by hashing the name or some other
// identifier to obtain a number and then using the modulus operator with the
// overall partition count.
//
// Ring: Stores the assignments of replicas of partitions to nodes.
//
// Builder: A program to build and maintain a ring.
//
// Capacity: The relative size of a node as compared to other nodes. For
// example, the amount of disk space available on the node.
//
// Desire: The number of additional, or fewer, partitions a node would like to
// have assigned in order to reach a balance with the rest of the nodes in a
// ring.
//
// Tier: Represents the relationship of nodes to one another. For example, a
// geographic tier might have two values, east and west, and each node would be
// associated with one of those regions. There can be multiple levels of tiers,
// such as disk, server, zone, datacenter, region, etc. See the Example (Tiers)
// for a more in-depth code discussion.
//
// Last Moved: The record of when a given replica of a partition was last
// reassigned to a different node. The builder uses this information restrict
// future movements of that replica and of the other replicas for that
// partition. For example, it might only move 2 of 5 replicas of a partition
// within an hour, unless absolutely necessary, such as a failed node.
//
// What About Other Distributed Ring Algorithms
//
// Each has their strengths and weaknesses. This package uses more memory than
// many other ring implementations and it is slower to create and modify the
// ring. But, it is usually much faster to use once loaded and provides precise
// node placements.
// https://github.com/gholt/ring/blob/master/PARTITION_RING_VS_HASH_RING.md has
// more discussion on the trade offs.
//
// Other interesting ideas in this space:
//
// Amazon's original 2007 Dynamo paper -
// http://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf
//
// Jump consistent hashing - http://arxiv.org/abs/1406.2294 - dgryski
// implementation https://github.com/dgryski/go-jump - also dgryski shared
// key-value store https://github.com/dgryski/go-shardedkv
//
// Multi-probe consistent hashing http://arxiv.org/pdf/1505.00062.pdf - dgryski
// implementation https://github.com/dgryski/go-mpchash
//
// GreenCHT replication scheme
// http://storageconference.us/2015/Papers/16.Zhao.pdf
//
// Examples For This Package
//
// Below are examples for overall usage of this package, and more in-depth
// discussions as well.
package ring
