// Package ring contains tools for building and using a consistent hashing ring
// with replicas, automatic partitioning (ring ranges), and keeping replicas of
// the same partitions in as distinct tiered nodes as possible (tiers might be
// devices, servers, cabinets, rooms, data centers, geographical regions, etc.)
//
// It also contains tools for using a ring as a messaging hub, easing
// communication between nodes in the ring.
package ring

import (
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

// RINGVERSION is the ring file format version written to and checked for in
// the ring file header. If the on disk format of the ring changes this version
// should be incremented.
const RINGVERSION = "RINGv00000000001"

// Ring is the immutable snapshot of data assignments to nodes.
//
// The immutable characteristic is important, as it means code that uses a Ring
// can make calculations around its attributes and not worry about those
// attributes changing from call to call. For example, the PartitionBitCount
// can be used to generate a list of partitions that are then used with
// ResponsibleNodes without worrying the PartitionBitCount changed during the
// calculations. Changes to the Ring are done by the Builder, which will
// generate a new immutable Ring instance to be distributed to the users of the
// older Ring instance. There is one exception, however; the LocalNode
// attribute is mutable as its function is to represent the local user of that
// Ring instance.
//
// Note that with several methods the partition value is not bounds checked; an
// invalid partition will cause a panic. This behavior is for speed reasons, as
// bounds checking every call would be wasteful in most use cases that already
// guarantee proper bounding. You could easily create a BoundedRing that wraps
// a Ring instance to provide such bounding if you really need it; but
// considering most uses of partitions involve bit shifting based on the
// PartitionBitCount, such bounding doesn't seem worth it.
type Ring interface {
	// Version is the time.Now().UnixNano() of when the Ring data was
	// established.
	//
	// Version can indicate changes in ring data; for example, if a server is
	// currently working with one version of ring data and receives requests
	// that are based on a lesser version of ring data, it can ignore those
	// requests or send an "obsoleted" response or something along those lines.
	// Similarly, if the server receives requests for a greater version of ring
	// data, it can ignore those requests or try to obtain a newer ring
	// version.
	Version() int64
	// Conf returns the raw encoded global configuration. This configuration
	// data isn't used by the ring itself, but can be useful in storing
	// configuration data for users of the ring.
	Conf() []byte
	// Node returns the node instance identified, if there is one.
	Node(nodeID uint64) Node
	// Nodes returns a NodeSlice of the nodes the Ring references.
	Nodes() NodeSlice
	// Tiers returns the tier values in use at each level. Note that an empty
	// string is always an available value at any level, although it is not
	// returned from this method.
	Tiers() [][]string
	// PartitionBitCount is the number of bits that can be used to determine a
	// partition number for the current data in the ring. For example, to
	// convert a uint64 hash value into a partition number you could use
	// hashValue >> (64 - ring.PartitionBitCount()). The PartitionBitCount also
	// indicates how many partitions the Ring has; for example, a value of 16
	// would indicate 2**16 or 65,536 partitions.
	PartitionBitCount() uint16
	// ReplicaCount specifies how many replicas the Ring has.
	ReplicaCount() int
	// LocalNode returns the node the ring is locally bound to, if any. This
	// local node binding is used by things such as MsgRing to know what items
	// are bound for the local instance or need to be sent to remote ones, etc.
	LocalNode() Node
	// SetLocalNode sets the node the ring is locally bound to, if any. This
	// local node binding is used by things such as MsgRing to know what items
	// are bound for the local instance or need to be sent to remote ones, etc.
	SetLocalNode(nodeID uint64)
	// Responsible will return true if LocalNode is set and one of the
	// partition's replicas is assigned to that local node.
	//
	// Note that the partition value is not bounds checked; an invalid
	// partition will cause a panic. See the documentation for the Ring
	// interface itself for further discussion.
	Responsible(partition uint32) bool
	// ResponsibleReplica will return the replica index >= 0 if LocalNode is
	// set and one of the partition's replicas is assigned to that local node;
	// it will return -1 if LocalNode is not responsible for the partition.
	//
	// Note that the partition value is not bounds checked; an invalid
	// partition will cause a panic. See the documentation for the Ring
	// interface itself for further discussion.
	ResponsibleReplica(partition uint32) int
	// ResponsibleNodes will return the list of nodes that are responsible for
	// the replicas of the partition.
	//
	// Note that the partition value is not bounds checked; an invalid
	// partition will cause a panic. See the documentation for the Ring
	// interface itself for further discussion.
	ResponsibleNodes(partition uint32) NodeSlice
	// Stats gives information about the ring and its health; the MaxUnder and
	// MaxOver values specifically indicate how balanced the ring is.
	Stats() *Stats
	// Persist saves the Ring state to the given Writer for later reloading via
	// the LoadRing method.
	Persist(w io.Writer) error
}

type tierBase struct {
	tiers [][]string
}

type ring struct {
	tierBase
	version                       int64
	conf                          []byte
	localNodeIndex                int32
	partitionBitCount             uint16
	nodes                         []*node
	replicaToPartitionToNodeIndex [][]int32
}

// LoadRing creates a new Ring instance based on the persisted data from the
// Reader (presumably previously saved with the Ring.Persist method).
func LoadRing(rd io.Reader) (Ring, error) {
	// CONSIDER: This code uses binary.Read which incurs fleeting allocations;
	// these could be reduced by creating a buffer upfront and using
	// binary.Put* calls instead.
	gr, err := gzip.NewReader(rd)
	if err != nil {
		return nil, err
	}
	defer gr.Close() // does not close the underlying reader
	header := make([]byte, 16)
	_, err = io.ReadFull(gr, header)
	if err != nil {
		return nil, err
	}
	if string(header) != RINGVERSION {
		return nil, fmt.Errorf("unknown header %s", string(header))
	}
	r := &ring{}
	err = binary.Read(gr, binary.BigEndian, &r.version)
	if err != nil {
		return nil, err
	}
	var confbytes int32
	err = binary.Read(gr, binary.BigEndian, &confbytes)
	if err != nil {
		return nil, err
	}
	r.conf = make([]byte, confbytes)
	_, err = io.ReadFull(gr, r.conf)
	if err != nil {
		return nil, err
	}
	err = binary.Read(gr, binary.BigEndian, &r.localNodeIndex)
	if err != nil {
		return nil, err
	}
	err = binary.Read(gr, binary.BigEndian, &r.partitionBitCount)
	if err != nil {
		return nil, err
	}
	var vint32 int32
	err = binary.Read(gr, binary.BigEndian, &vint32)
	if err != nil {
		return nil, err
	}
	r.tiers = make([][]string, vint32)
	for i := int32(0); i < vint32; i++ {
		var vvint32 int32
		err = binary.Read(gr, binary.BigEndian, &vvint32)
		if err != nil {
			return nil, err
		}
		r.tiers[i] = make([]string, vvint32)
		for j := int32(0); j < vvint32; j++ {
			var vvvint32 int32
			err = binary.Read(gr, binary.BigEndian, &vvvint32)
			if err != nil {
				return nil, err
			}
			byts := make([]byte, vvvint32)
			_, err = io.ReadFull(gr, byts)
			if err != nil {
				return nil, err
			}
			r.tiers[i][j] = string(byts)
		}
	}
	err = binary.Read(gr, binary.BigEndian, &vint32)
	if err != nil {
		return nil, err
	}
	r.nodes = make([]*node, vint32)
	for i := int32(0); i < vint32; i++ {
		r.nodes[i] = &node{tierBase: &r.tierBase}
		err = binary.Read(gr, binary.BigEndian, &r.nodes[i].id)
		if err != nil {
			return nil, err
		}
		tf := byte(0)
		err = binary.Read(gr, binary.BigEndian, &tf)
		if err != nil {
			return nil, err
		}
		if tf == 1 {
			r.nodes[i].inactive = true
		}
		err = binary.Read(gr, binary.BigEndian, &r.nodes[i].capacity)
		if err != nil {
			return nil, err
		}
		var vvint32 int32
		err = binary.Read(gr, binary.BigEndian, &vvint32)
		if err != nil {
			return nil, err
		}
		r.nodes[i].tierIndexes = make([]int32, vvint32)
		for j := int32(0); j < vvint32; j++ {
			err = binary.Read(gr, binary.BigEndian, &r.nodes[i].tierIndexes[j])
			if err != nil {
				return nil, err
			}
		}
		err = binary.Read(gr, binary.BigEndian, &vvint32)
		if err != nil {
			return nil, err
		}
		r.nodes[i].addresses = make([]string, vvint32)
		for j := int32(0); j < vvint32; j++ {
			var vvvint32 int32
			err = binary.Read(gr, binary.BigEndian, &vvvint32)
			if err != nil {
				return nil, err
			}
			byts := make([]byte, vvvint32)
			_, err = io.ReadFull(gr, byts)
			if err != nil {
				return nil, err
			}
			r.nodes[i].addresses[j] = string(byts)
		}
		err = binary.Read(gr, binary.BigEndian, &vvint32)
		if err != nil {
			return nil, err
		}
		byts := make([]byte, vvint32)
		_, err = io.ReadFull(gr, byts)
		if err != nil {
			return nil, err
		}
		r.nodes[i].meta = string(byts)
		var cbytes int32
		err = binary.Read(gr, binary.BigEndian, &cbytes)
		if err != nil {
			return nil, err
		}
		r.nodes[i].conf = make([]byte, cbytes)
		_, err = io.ReadFull(gr, r.nodes[i].conf)
		if err != nil {
			return nil, err
		}
	}
	err = binary.Read(gr, binary.BigEndian, &vint32)
	if err != nil {
		return nil, err
	}
	r.replicaToPartitionToNodeIndex = make([][]int32, vint32)
	for i := int32(0); i < vint32; i++ {
		var vvint32 int32
		err = binary.Read(gr, binary.BigEndian, &vvint32)
		if err != nil {
			return nil, err
		}
		r.replicaToPartitionToNodeIndex[i] = make([]int32, vvint32)
		err = binary.Read(gr, binary.BigEndian, r.replicaToPartitionToNodeIndex[i])
	}
	return r, nil
}

func (r *ring) Persist(w io.Writer) error {
	// CONSIDER: This code uses binary.Write which incurs fleeting allocations;
	// these could be reduced by creating a buffer upfront and using
	// binary.Put* calls instead.
	gw := gzip.NewWriter(w)
	defer gw.Close() // does not close the underlying writer
	_, err := gw.Write([]byte(RINGVERSION))
	if err != nil {
		return err
	}
	err = binary.Write(gw, binary.BigEndian, r.version)
	if err != nil {
		return err
	}
	if len(r.conf) > math.MaxInt32 {
		return fmt.Errorf("%d conf bytes is too large; max is %d", len(r.conf), math.MaxInt32)
	}
	err = binary.Write(gw, binary.BigEndian, int32(len(r.conf)))
	if err != nil {
		return err
	}
	_, err = gw.Write(r.conf)
	if err != nil {
		return err
	}
	err = binary.Write(gw, binary.BigEndian, r.localNodeIndex)
	if err != nil {
		return err
	}
	err = binary.Write(gw, binary.BigEndian, r.partitionBitCount)
	if err != nil {
		return err
	}
	if len(r.tiers) > math.MaxInt32 {
		return fmt.Errorf("%d number of tiers is too large; max is %d", len(r.tiers), math.MaxInt32)
	}
	err = binary.Write(gw, binary.BigEndian, int32(len(r.tiers)))
	if err != nil {
		return err
	}
	for _, tier := range r.tiers {
		if len(tier) > math.MaxInt32 {
			return fmt.Errorf("%d number of tier positions is too large; max is %d", len(tier), math.MaxInt32)
		}
		err = binary.Write(gw, binary.BigEndian, int32(len(tier)))
		if err != nil {
			return err
		}
		for _, name := range tier {
			byts := []byte(name)
			if len(byts) > math.MaxInt32 {
				return fmt.Errorf("%d name length is too large; max is %d", len(byts), math.MaxInt32)
			}
			err = binary.Write(gw, binary.BigEndian, int32(len(byts)))
			if err != nil {
				return err
			}
			_, err = gw.Write(byts)
			if err != nil {
				return err
			}
		}
	}
	if len(r.nodes) > math.MaxInt32 {
		return fmt.Errorf("%d number of nodes is too large; max is %d", len(r.nodes), math.MaxInt32)
	}
	err = binary.Write(gw, binary.BigEndian, int32(len(r.nodes)))
	if err != nil {
		return err
	}
	for _, n := range r.nodes {
		err = binary.Write(gw, binary.BigEndian, n.id)
		if err != nil {
			return err
		}
		tf := byte(0)
		if n.inactive {
			tf = 1
		}
		err = binary.Write(gw, binary.BigEndian, tf)
		if err != nil {
			return err
		}
		err = binary.Write(gw, binary.BigEndian, n.capacity)
		if err != nil {
			return err
		}
		if len(n.tierIndexes) > math.MaxInt32 {
			return fmt.Errorf("%d tier positions is too large; max is %d", len(n.tierIndexes), math.MaxInt32)
		}
		err = binary.Write(gw, binary.BigEndian, int32(len(n.tierIndexes)))
		if err != nil {
			return err
		}
		for _, v := range n.tierIndexes {
			err = binary.Write(gw, binary.BigEndian, v)
			if err != nil {
				return err
			}
		}
		if len(n.tierIndexes) > math.MaxInt32 {
			return fmt.Errorf("%d addresses is too large; max is %d", len(n.tierIndexes), math.MaxInt32)
		}
		err = binary.Write(gw, binary.BigEndian, int32(len(n.addresses)))
		if err != nil {
			return err
		}
		for _, address := range n.addresses {
			byts := []byte(address)
			if len(byts) > math.MaxInt32 {
				return fmt.Errorf("%d address length is too large; max is %d", len(byts), math.MaxInt32)
			}
			err = binary.Write(gw, binary.BigEndian, int32(len(byts)))
			if err != nil {
				return err
			}
			_, err = gw.Write(byts)
			if err != nil {
				return err
			}
		}
		byts := []byte(n.meta)
		if len(byts) > math.MaxInt32 {
			return fmt.Errorf("%d meta length is too large; max is %d", len(byts), math.MaxInt32)
		}
		err = binary.Write(gw, binary.BigEndian, int32(len(byts)))
		if err != nil {
			return err
		}
		_, err = gw.Write(byts)
		if err != nil {
			return err
		}
		if len(n.conf) > math.MaxInt32 {
			return fmt.Errorf("%d conf length is too large; max is %d", len(n.conf), math.MaxInt32)
		}
		err = binary.Write(gw, binary.BigEndian, int32(len(n.conf)))
		if err != nil {
			return err
		}
		_, err = gw.Write(n.conf)
		if err != nil {
			return err
		}

	}
	if len(r.replicaToPartitionToNodeIndex) > math.MaxInt32 {
		return fmt.Errorf("%d replica count is too large; max is %d", len(r.replicaToPartitionToNodeIndex), math.MaxInt32)
	}
	err = binary.Write(gw, binary.BigEndian, int32(len(r.replicaToPartitionToNodeIndex)))
	if err != nil {
		return err
	}
	for _, partitionToNodeIndex := range r.replicaToPartitionToNodeIndex {
		if len(partitionToNodeIndex) > math.MaxInt32 {
			return fmt.Errorf("%d partition count is too large; max is %d", len(partitionToNodeIndex), math.MaxInt32)
		}
		err = binary.Write(gw, binary.BigEndian, int32(len(partitionToNodeIndex)))
		if err != nil {
			return err
		}
		err = binary.Write(gw, binary.BigEndian, partitionToNodeIndex)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *ring) Version() int64 {
	return r.version
}

func (r *ring) Conf() []byte {
	return r.conf
}

func (r *ring) PartitionBitCount() uint16 {
	return r.partitionBitCount
}

func (r *ring) ReplicaCount() int {
	return len(r.replicaToPartitionToNodeIndex)
}

func (r *ring) Nodes() NodeSlice {
	nodes := make(NodeSlice, len(r.nodes))
	for i := len(nodes) - 1; i >= 0; i-- {
		nodes[i] = r.nodes[i]
	}
	return nodes
}

func (r *ring) Node(id uint64) Node {
	for _, n := range r.nodes {
		if n.id == id {
			return n
		}
	}
	return nil
}

func (r *ring) Tiers() [][]string {
	rv := make([][]string, len(r.tiers))
	for i, t := range r.tiers {
		rv[i] = make([]string, len(t)-1)
		copy(rv[i], t[1:])
	}
	return rv
}

func (r *ring) LocalNode() Node {
	if r.localNodeIndex == -1 {
		return nil
	}
	return r.nodes[r.localNodeIndex]
}

func (r *ring) SetLocalNode(id uint64) {
	r.localNodeIndex = -1
	for i, n := range r.nodes {
		if n.id == id {
			r.localNodeIndex = int32(i)
			break
		}
	}
}

func (r *ring) Responsible(partition uint32) bool {
	if r.localNodeIndex == -1 {
		return false
	}
	for _, partitionToNodeIndex := range r.replicaToPartitionToNodeIndex {
		if partitionToNodeIndex[partition] == r.localNodeIndex {
			return true
		}
	}
	return false
}

func (r *ring) ResponsibleReplica(partition uint32) int {
	if r.localNodeIndex == -1 {
		return -1
	}
	for index, partitionToNodeIndex := range r.replicaToPartitionToNodeIndex {
		if partitionToNodeIndex[partition] == r.localNodeIndex {
			return index
		}
	}
	return -1
}

func (r *ring) ResponsibleNodes(partition uint32) NodeSlice {
	nodes := make(NodeSlice, r.ReplicaCount())
	for replica, partitionToNodeIndex := range r.replicaToPartitionToNodeIndex {
		nodes[replica] = r.nodes[partitionToNodeIndex[partition]]
	}
	return nodes
}

// Stats gives an overview of the state and health of a Ring. It is returned by
// the Ring.Stats() method.
type Stats struct {
	ReplicaCount      int
	NodeCount         int
	InactiveNodeCount int
	PartitionBitCount uint16
	PartitionCount    int
	TotalCapacity     uint64
	// MaxUnderNodePercentage is the percentage a node is underweight, or has
	// less data assigned to it than its capacity would indicate it desires.
	MaxUnderNodePercentage float64
	MaxUnderNodeID         uint64
	// MaxOverNodePercentage is the percentage a node is overweight, or has
	// more data assigned to it than its capacity would indicate it desires.
	MaxOverNodePercentage float64
	MaxOverNodeID         uint64
}

func (r *ring) Stats() *Stats {
	stats := &Stats{
		ReplicaCount:      r.ReplicaCount(),
		NodeCount:         len(r.nodes),
		PartitionBitCount: r.PartitionBitCount(),
		PartitionCount:    1 << r.PartitionBitCount(),
		MaxUnderNodeID:    0,
		MaxOverNodeID:     0,
	}
	nodeIndexToPartitionCount := make([]int, stats.NodeCount)
	for _, partitionToNodeIndex := range r.replicaToPartitionToNodeIndex {
		for _, nodeIndex := range partitionToNodeIndex {
			nodeIndexToPartitionCount[nodeIndex]++
		}
	}
	for _, n := range r.nodes {
		if n.inactive {
			stats.InactiveNodeCount++
		} else {
			stats.TotalCapacity += (uint64)(n.capacity)
		}
	}
	for nodeIndex, n := range r.nodes {
		if n.inactive {
			continue
		}
		desiredPartitionCount := float64(n.capacity) / float64(stats.TotalCapacity) * float64(stats.PartitionCount) * float64(stats.ReplicaCount)
		actualPartitionCount := float64(nodeIndexToPartitionCount[nodeIndex])
		if desiredPartitionCount > actualPartitionCount {
			under := 100.0 * (desiredPartitionCount - actualPartitionCount) / desiredPartitionCount
			if under > stats.MaxUnderNodePercentage {
				stats.MaxUnderNodePercentage = under
				stats.MaxUnderNodeID = n.id
			}
		} else if desiredPartitionCount < actualPartitionCount {
			over := 100.0 * (actualPartitionCount - desiredPartitionCount) / desiredPartitionCount
			if over > stats.MaxOverNodePercentage {
				stats.MaxOverNodePercentage = over
				stats.MaxOverNodeID = n.id
			}
		}
	}
	return stats
}
