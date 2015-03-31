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
)

type Ring interface {
	Version() int64
	Node(id uint64) Node
	Nodes() NodeSlice
	PartitionBitCount() uint16
	ReplicaCount() int
	LocalNode() Node
	SetLocalNode(id uint64)
	Responsible(partition uint32) bool
	ResponsibleNodes(partition uint32) NodeSlice
	Stats() *RingStats
	Persist(w io.Writer) error
}

type tierBase struct {
	tiers [][]string
}

type ring struct {
	tierBase
	version int64
	// TODO: Need to be able to set this (ring gets transferred to another node
	// and they need to re-local-node it).
	localNodeIndex                int32
	partitionBitCount             uint16
	nodes                         []*node
	replicaToPartitionToNodeIndex [][]int32
}

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
	if string(header) != "RINGv00000000001" {
		return nil, fmt.Errorf("unknown header %s", string(header))
	}
	r := &ring{}
	err = binary.Read(gr, binary.BigEndian, &r.version)
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
	_, err := gw.Write([]byte("RINGv00000000001"))
	if err != nil {
		return err
	}
	err = binary.Write(gw, binary.BigEndian, r.version)
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
	err = binary.Write(gw, binary.BigEndian, int32(len(r.tiers)))
	if err != nil {
		return err
	}
	for _, tier := range r.tiers {
		err = binary.Write(gw, binary.BigEndian, int32(len(tier)))
		if err != nil {
			return err
		}
		for _, name := range tier {
			byts := []byte(name)
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
		err = binary.Write(gw, binary.BigEndian, int32(len(n.addresses)))
		if err != nil {
			return err
		}
		for _, address := range n.addresses {
			byts := []byte(address)
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
		err = binary.Write(gw, binary.BigEndian, int32(len(byts)))
		if err != nil {
			return err
		}
		_, err = gw.Write(byts)
		if err != nil {
			return err
		}
	}
	err = binary.Write(gw, binary.BigEndian, int32(len(r.replicaToPartitionToNodeIndex)))
	if err != nil {
		return err
	}
	for _, partitionToNodeIndex := range r.replicaToPartitionToNodeIndex {
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

// Version can indicate changes in ring data; for example, if a server is
// currently working with one version of ring data and receives requests that
// are based on a lesser version of ring data, it can ignore those requests or
// send an "obsoleted" response or something along those lines. Similarly, if
// the server receives requests for a greater version of ring data, it can
// ignore those requests or try to obtain a newer ring version.
func (r *ring) Version() int64 {
	return r.version
}

// PartitionBitCount is the number of bits that can be used to determine a
// partition number for the current data in the ring. For example, to convert a
// uint64 hash value into a partition number you could use hashValue >> (64 -
// ring.PartitionBitCount()).
func (r *ring) PartitionBitCount() uint16 {
	return r.partitionBitCount
}

func (r *ring) ReplicaCount() int {
	return len(r.replicaToPartitionToNodeIndex)
}

// Nodes returns a list of nodes referenced by the ring.
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

// LocalNode contains the information for the local node; determining which
// ring partitions/replicas the local node is responsible for as well as being
// used to direct message delivery. If this instance of the ring has no local
// node information, nil will be returned.
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

// Responsible will return true if the local node is considered responsible for
// a replica of the partition given.
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

// ResponsibleNodes will return a list of nodes for considered responsible for
// the replicas of the partition given.
func (r *ring) ResponsibleNodes(partition uint32) NodeSlice {
	nodes := make(NodeSlice, r.ReplicaCount())
	for replica, partitionToNodeIndex := range r.replicaToPartitionToNodeIndex {
		nodes[replica] = r.nodes[partitionToNodeIndex[partition]]
	}
	return nodes
}

type RingStats struct {
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

// Stats gives information about the ring and its health; the MaxUnder and
// MaxOver values specifically indicate how balanced the ring is.
func (r *ring) Stats() *RingStats {
	stats := &RingStats{
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
