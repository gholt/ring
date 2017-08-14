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

type Ring struct {
	nodes                    []*Node
	groups                   []*Group
	nodeToGroup              []int
	groupToGroup             []int
	replicaToPartitionToNode [][]lowring.Node
	rebalanced               time.Time
}

func (r *Ring) Nodes() []*Node {
	nodes := make([]*Node, len(r.nodes))
	copy(nodes, r.nodes)
	return nodes
}

func (r *Ring) Groups() []*Group {
	groups := make([]*Group, len(r.groups))
	copy(groups, r.groups)
	return groups
}

func (r *Ring) ReplicaCount() int {
	return len(r.replicaToPartitionToNode)
}

func (r *Ring) PartitionCount() int {
	return len(r.replicaToPartitionToNode[0])
}

func (r *Ring) AssignmentCount() int {
	return len(r.replicaToPartitionToNode) * len(r.replicaToPartitionToNode[0])
}

func (r *Ring) Rebalanced() time.Time {
	return r.rebalanced
}

func (r *Ring) KeyNodes(key int) []*Node {
	nodes := make([]*Node, 0, len(r.replicaToPartitionToNode))
	partition := key % len(r.replicaToPartitionToNode[0])
	for _, partitionToNode := range r.replicaToPartitionToNode {
		nodes = append(nodes, r.nodes[partitionToNode[partition]])
	}
	return nodes
}

func (r *Ring) ResponsibleForReplicaPartition(replica, partition int) *Node {
	return r.nodes[r.replicaToPartitionToNode[replica][partition]]
}

type ringJSON struct {
	MarshalVersion int
	NodeType       int
	ReplicaCount   int
	PartitionCount int
	Nodes          []*ringNodeJSON
	Groups         []*ringGroupJSON
	Rebalanced     int64
}

type ringNodeJSON struct {
	Info     string
	Capacity int
	Group    int
}

type ringGroupJSON struct {
	Info   string
	Parent int
}

func (r *Ring) Marshal(w io.Writer) error {
	var nodeType lowring.Node
	j := &ringJSON{
		MarshalVersion: 0,
		NodeType:       int(unsafe.Sizeof(nodeType)) * 8,
		ReplicaCount:   len(r.replicaToPartitionToNode),
		PartitionCount: len(r.replicaToPartitionToNode[0]),
		Nodes:          make([]*ringNodeJSON, len(r.nodes)),
		Groups:         make([]*ringGroupJSON, len(r.groups)),
		Rebalanced:     r.rebalanced.UnixNano(),
	}
	for i, n := range r.nodes {
		j.Nodes[i] = &ringNodeJSON{
			Info:     n.info,
			Capacity: n.capacity,
			Group:    r.nodeToGroup[n.index],
		}
	}
	for i, g := range r.groups {
		j.Groups[i] = &ringGroupJSON{
			Info:   g.info,
			Parent: r.groupToGroup[g.index],
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
	for _, partitionToNode := range r.replicaToPartitionToNode {
		if err := binary.Write(w, binary.LittleEndian, partitionToNode); err != nil {
			return err
		}
	}
	return nil
}

func Unmarshal(r io.Reader) (*Ring, error) {
	var nodeType lowring.Node
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
	if j.NodeType != int(unsafe.Sizeof(nodeType))*8 {
		return nil, fmt.Errorf("ring data does not match compiled ring format. NodeType is %d bits in the data and %d bits compiled.", j.NodeType, int(unsafe.Sizeof(nodeType))*8)
	}
	rv := &Ring{
		nodes:  make([]*Node, len(j.Nodes)),
		groups: make([]*Group, len(j.Groups)),
		replicaToPartitionToNode: make([][]lowring.Node, j.ReplicaCount),
	}
	rv.nodeToGroup = make([]int, len(j.Nodes))
	rv.groupToGroup = make([]int, len(j.Groups))
	rv.rebalanced = time.Unix(0, j.Rebalanced)
	for i, jn := range j.Nodes {
		rv.nodes[i] = &Node{ring: rv, index: i, info: jn.Info, capacity: jn.Capacity}
		rv.nodeToGroup[i] = jn.Group
	}
	for i, jg := range j.Groups {
		rv.groups[i] = &Group{ring: rv, index: i, info: jg.Info}
		rv.groupToGroup[i] = jg.Parent
	}
	rv.replicaToPartitionToNode = make([][]lowring.Node, j.ReplicaCount)
	for replica := 0; replica < j.ReplicaCount; replica++ {
		rv.replicaToPartitionToNode[replica] = make([]lowring.Node, j.PartitionCount)
		if err := binary.Read(r, binary.LittleEndian, rv.replicaToPartitionToNode[replica]); err != nil {
			return nil, err
		}
	}
	return rv, nil
}
