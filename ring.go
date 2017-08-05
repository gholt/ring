package ring

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"time"
	"unsafe"

	"github.com/gholt/ring/lowring"
)

// Ring stores the assignments of replicas of partitions to node indexes.
// Usually these are generated and maintained by the Builder.
type Ring interface {

	// Rebalanced is the time the ring was rebalanced.
	Rebalanced() time.Time

	// ReplicaCount is the number of replicas for each partition.
	ReplicaCount() int

	// PartitionCount indicates how split the key space is.
	PartitionCount() int

	// Nodes returns all the defined nodes for this ring, some of which may be
	// disabled.
	Nodes() []Node

	// KeyNodes returns the nodes assigned to a key; the slice will be
	// ReplicaCount in length. Usually keys are created by hashing some
	// identifier of the data, such as the name of an object to be stored in an
	// object storage system.
	//
	// The key % PartitionCount() determines which partition the key belongs
	// to, so if you'd like to map the node assignments for all partitions you
	// can easily do that with:
	//
	//  for key := 0; key < ring.PartititionCount(); key++ {
	//      nodes := ring.KeyNodes(key)
	//      ...
	//  }
	KeyNodes(key int) []Node

	// Marshal will persist the data from this ring to the writer. You can read
	// it back with the UnmarshalRing function.
	Marshal(w io.Writer) error
}

type rring struct {
	rebalanced time.Time
	nodes      []*node
	ring       lowring.Ring
}

func (r *rring) Rebalanced() time.Time {
	return r.rebalanced
}

func (r *rring) ReplicaCount() int {
	return r.ring.ReplicaCount()
}

func (r *rring) PartitionCount() int {
	return r.ring.PartitionCount()
}

func (r *rring) Nodes() []Node {
	nodes := make([]Node, len(r.nodes))
	for i, n := range r.nodes {
		nodes[i] = n
	}
	return nodes
}

func (r *rring) KeyNodes(key int) []Node {
	partition := key % r.ring.PartitionCount()
	nodes := make([]Node, r.ring.ReplicaCount())
	for i, partitionToNodeIndex := range r.ring {
		nodes[i] = r.nodes[partitionToNodeIndex[partition]]
	}
	return nodes
}

type ringJSON struct {
	MarshalVersion int
	NodeIndexType  int
	Rebalanced     int64
	ReplicaCount   int
	PartitionCount int
	Nodes          []*ringNodeJSON
}

type ringNodeJSON struct {
	Disabled bool
	Capacity int
	Tiers    []string
	Info     string
}

func (r *rring) Marshal(w io.Writer) error {
	var nit lowring.NodeIndexType
	j := &ringJSON{
		MarshalVersion: 0,
		NodeIndexType:  int(unsafe.Sizeof(nit)) * 8,
		Rebalanced:     r.rebalanced.UnixNano(),
		ReplicaCount:   r.ReplicaCount(),
		PartitionCount: r.PartitionCount(),
		Nodes:          make([]*ringNodeJSON, len(r.nodes)),
	}
	for i, n := range r.nodes {
		j.Nodes[i] = &ringNodeJSON{
			Disabled: n.disabled,
			Capacity: n.capacity,
			Tiers:    n.tiers,
			Info:     n.info,
		}
	}
	if err := json.NewEncoder(w).Encode(j); err != nil {
		return err
	}
	// This 0 byte is written as a preface to the raw ring data and will let
	// the unmarshaler get past any trailing whitespace, newlines, etc. that
	// the JSON encoder may or may not have written.
	if _, err := w.Write([]byte{0}); err != nil {
		return err
	}
	for _, partitionToNodeIndex := range r.ring {
		if err := binary.Write(w, binary.LittleEndian, partitionToNodeIndex); err != nil {
			return err
		}
	}
	return nil
}

// UnmarshalRing will return a ring created from data stored in the reader. You
// can persist a ring to a writer with the ring's Marshal method.
func UnmarshalRing(r io.Reader) (Ring, error) {
	var nit lowring.NodeIndexType
	j := &ringJSON{}
	jsonDecoder := json.NewDecoder(r)
	if err := jsonDecoder.Decode(j); err != nil {
		return nil, err
	}
	r = io.MultiReader(jsonDecoder.Buffered(), r)
	// These byte reads are to get past any trailing whitespace, newlines, etc.
	// the JSON encoder may or may not have written. When marshalling we
	// preface the raw ring data with a 0 byte.
	b0 := []byte{0}
	for {
		if n, err := r.Read(b0); err != nil {
			return nil, err
		} else if n == 0 {
			b0[0] = 1
		}
		if b0[0] == 0 {
			break
		}
	}
	if j.MarshalVersion != 0 {
		return nil, fmt.Errorf("unable to interpret data with MarshalVersion %d", j.MarshalVersion)
	}
	if j.NodeIndexType != int(unsafe.Sizeof(nit))*8 {
		return nil, fmt.Errorf("ring data does not match compiled ring format. NodeIndexType is %d bits in the data and %d bits compiled.", j.NodeIndexType, int(unsafe.Sizeof(nit))*8)
	}
	rv := &rring{
		rebalanced: time.Unix(0, j.Rebalanced),
		nodes:      make([]*node, len(j.Nodes)),
		ring:       make(lowring.Ring, j.ReplicaCount),
	}
	for i, jn := range j.Nodes {
		rn := &node{}
		rn.disabled = jn.Disabled
		rn.capacity = jn.Capacity
		rn.tiers = jn.Tiers
		rn.info = jn.Info
		rv.nodes[i] = rn
	}
	for replica := 0; replica < j.ReplicaCount; replica++ {
		rv.ring[replica] = make([]lowring.NodeIndexType, j.PartitionCount)
		if err := binary.Read(r, binary.LittleEndian, rv.ring[replica]); err != nil {
			return nil, err
		}
	}
	return rv, nil
}
