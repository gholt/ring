package ring_test

import (
	"fmt"
	"hash/fnv"

	"github.com/gholt/ring"
)

// This will be an in-depth implementation of using the ring package. We will
// be building a distributed object storage system where object names will be
// mapped to disks. There will be multiple disks per server, and multiple
// servers per zone.

// First, let's define our BuilderDisk, representing a single disk in the
// cluster and is what the ring package will be mapping assignments to.

// We want all the fields to be private because we don't want users of our new
// package to accidentally alter anything. This complicates our code, but
// simplifies things for users of our new storage package, and would be a
// common use case.

type BuilderDisk struct {
	// We'll define our specific StorageBuilder later. We need a reference to
	// it in each node so we notify it of changes and to resolve things like
	// tier names.
	storageBuilder *StorageBuilder
	// This is the actual ring.Node the ring.Builder will need to work with,
	// and contains whether the disk is Disabled or not, the Capacity, and the
	// list of indexes that define what tiers (server, zone) the disk is in.
	node ring.Node
	// The ip:port where the disk can be reached.
	addr string
	// The name of the disk on the server.
	name string
}

// Now we map public methods to the fields.

func (bd *BuilderDisk) Disabled() bool {
	return bd.node.Disabled
}

func (bd *BuilderDisk) SetDisabled(v bool) {
	bd.node.Disabled = v
}

func (bd *BuilderDisk) Capacity() int {
	return bd.node.Capacity
}

func (bd *BuilderDisk) SetCapacity(v int) {
	bd.node.Capacity = v
}

func (bd *BuilderDisk) Tiers() []string {
	tiers := make([]string, len(bd.node.TierIndexes))
	for i, ti := range bd.node.TierIndexes {
		tiers[i] = bd.storageBuilder.tierIndexToName[ti]
	}
	return tiers
}

func (bd *BuilderDisk) SetTier(tier int, name string) {
	for tier >= len(bd.node.TierIndexes) {
		bd.node.TierIndexes = append(bd.node.TierIndexes, 0)
	}
	if bd.storageBuilder.tierNameToIndex == nil {
		bd.storageBuilder.tierNameToIndex = map[string]int{}
	}
	if ti, ok := bd.storageBuilder.tierNameToIndex[name]; ok {
		bd.node.TierIndexes[tier] = ti
	} else {
		ti = len(bd.storageBuilder.tierIndexToName)
		bd.storageBuilder.tierIndexToName = append(bd.storageBuilder.tierIndexToName, name)
		bd.storageBuilder.tierNameToIndex[name] = ti
		bd.node.TierIndexes[tier] = ti
	}
}

func (bd *BuilderDisk) Addr() string {
	return bd.addr
}

func (bd *BuilderDisk) SetAddr(v string) {
	bd.addr = v
}

func (bd *BuilderDisk) Name() string {
	return bd.name
}

func (bd *BuilderDisk) SetName(v string) {
	bd.name = v
}

// Now, let's define the StorageBuilder.

type StorageBuilder struct {
	// This is the actual builder that will do all the reblancing work.
	builder ring.Builder
	// These are all the disks in the cluster, we have to map them to the
	// ring.Builder's nodes.
	disks []*BuilderDisk
	// These are to give the tiers human readable names, like Server34 and
	// Zone5 or whatever is desired.
	tierIndexToName []string
	tierNameToIndex map[string]int
}

func (sb *StorageBuilder) AddDisk() *BuilderDisk {
	bd := &BuilderDisk{storageBuilder: sb}
	sb.disks = append(sb.disks, bd)
	sb.builder.Nodes = append(sb.builder.Nodes, &bd.node)
	return bd
}

func (sb *StorageBuilder) ChangeReplicaCount(count int) {
	sb.builder.ChangeReplicaCount(count)
}

// We'll skip removing disks, listing all disks, reading the replica count,
// partition count, changing last moved, etc. but those kind of methods would
// normally exist. The important point is that any modifications need to keep
// the ring.Builder's nodes in sync.

// We do want to give a useful ring to use, so let's provide that method. We'll
// define the StorageRing later, but it will be an immutable copy of the state
// of things at the time of StorageRing() call.

func (sb *StorageBuilder) StorageRing() *StorageRing {
	sb.builder.Rebalance()
	storageRing := &StorageRing{ring: sb.builder.RingDuplicate()}
	storageRing.disks = make([]*StorageDisk, len(sb.disks))
	for i, d := range sb.disks {
		storageRing.disks[i] = &StorageDisk{
			storageRing: storageRing,
			disabled:    d.node.Disabled,
			capacity:    d.node.Capacity,
			tierIndexes: d.node.TierIndexes,
			addr:        d.addr,
			name:        d.name,
		}
	}
	storageRing.tierIndexToName = make([]string, len(sb.tierIndexToName))
	copy(storageRing.tierIndexToName, sb.tierIndexToName)
	return storageRing
}

// Now let's define the StorageDisk and StorageRing.
// These are completely immutable structs, as you don't want users of the ring
// to have to constantly check if things moved, replica counts changed, etc.
// Instead, those sort of changes would be propagated by distributing a new
// ring.

type StorageDisk struct {
	storageRing *StorageRing
	disabled    bool
	capacity    int
	tierIndexes []int
	addr        string
	name        string
}

func (sd *StorageDisk) Disabled() bool {
	return sd.disabled
}

func (sd *StorageDisk) Capacity() int {
	return sd.capacity
}

func (sd *StorageDisk) Tiers() []string {
	tiers := make([]string, len(sd.tierIndexes))
	for i, ti := range sd.tierIndexes {
		tiers[i] = sd.storageRing.tierIndexToName[ti]
	}
	return tiers
}

func (sd *StorageDisk) Addr() string {
	return sd.addr
}

func (sd *StorageDisk) Name() string {
	return sd.name
}

type StorageRing struct {
	ring            ring.Ring
	disks           []*StorageDisk
	tierIndexToName []string
}

// This is the main lookup method. You give it an object name and it gives you
// the disks you should store it on.
// We're going to use fnv for the hashing here, for convenience, but you'd
// probably pick your favorite here, something better like blake2.

func (sr *StorageRing) DisksFor(objectName string) []*StorageDisk {
	hasher := fnv.New64a()
	hasher.Write([]byte(objectName))
	partition := hasher.Sum64() % uint64(sr.ring.PartitionCount())
	disks := make([]*StorageDisk, sr.ring.ReplicaCount())
	for replica := sr.ring.ReplicaCount() - 1; replica >= 0; replica-- {
		disks[replica] = sr.disks[sr.ring[replica][partition]]
	}
	return disks
}

// Now let's actually use all this stuff.

func Example_storageUseCase() {
	sb := &StorageBuilder{}
	sb.ChangeReplicaCount(3)
	// We're going to add a bunch of disks, two per server, four servers per
	// zone, and five zones.
	serverNumber := 0
	for _, zone := range []string{"ZoneA", "ZoneB", "ZoneC", "ZoneD", "ZoneE"} {
		for i := 0; i < 4; i++ {
			serverNumber++
			for _, disk := range []string{"sda1", "sdb1"} {
				bd := sb.AddDisk()
				// We're going to vary the capacities for a bit more work on
				// the rebalancer. This would usually represent the amount of
				// space, say, in gigabytes, that each disk has.
				bd.SetCapacity(100 + 100*i)
				bd.SetName(disk)
				bd.SetAddr(fmt.Sprintf("10.1.1.%d", serverNumber))
				bd.SetTier(0, fmt.Sprintf("Server%d", serverNumber))
				bd.SetTier(1, zone)
			}
		}
	}
	// Now let's get a useful ring and look up an object.
	sr := sb.StorageRing()
	objectName := "my test object"
	for replica, disk := range sr.DisksFor(objectName) {
		fmt.Printf("Replica %d of %q is on %s/%s which is on %s in %s\n", replica, objectName, disk.addr, disk.name, disk.Tiers()[0], disk.Tiers()[1])
	}
	// Replica 0 of "my test object" is on 10.1.1.10/sda1 which is on Server10 in ZoneC
	// Replica 1 of "my test object" is on 10.1.1.20/sda1 which is on Server20 in ZoneE
	// Replica 2 of "my test object" is on 10.1.1.8/sdb1 which is on Server8 in ZoneB

	// Output:
	// Replica 0 of "my test object" is on 10.1.1.10/sda1 which is on Server10 in ZoneC
	// Replica 1 of "my test object" is on 10.1.1.20/sda1 which is on Server20 in ZoneE
	// Replica 2 of "my test object" is on 10.1.1.8/sdb1 which is on Server8 in ZoneB
}
