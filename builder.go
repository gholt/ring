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

// Builder encapsulates the information needed to best balance ring assignments.
type Builder interface {

	// Rebalanced is the time Rebalance was last called.
	Rebalanced() time.Time

	// Nodes are the targets of each replica assignment.
	Nodes() []BuilderNode

	// AddNode adds a new node to the builder and returns it; may return nil if
	// no more nodes may be added. It will not be disabled, but will have a
	// capacity of 0, no tiers, and an empty info string.
	AddNode() BuilderNode

	// RemoveNode will remove the node from the Builder.
	//
	// Note that this can be relatively expensive as it will have to update all
	// information that pointed to nodes added after the removed node had been
	// originally added.
	//
	// Depending on the use case, it might be better to just leave a "dead"
	// node in place and simply set it as disabled.
	RemoveNode(n BuilderNode)

	// MaxNodeCount returns the maximum number of nodes allowed.
	MaxNodeCount() int

	// ReplicaCount returns the number of replicas per partition.
	ReplicaCount() int

	// SetReplicaCount will add or remove replicas; if replicas are added they
	// will be unassigned and require a Rebalance to become assigned.
	SetReplicaCount(v int)

	// PartitionCount indicates how split the key space is.
	PartitionCount() int

	// LastMovedUnit is the duration that time is measured in for recording
	// when replica assignments were last moved.
	LastMovedUnit() time.Duration

	// SetLastMovedUnit defines the duration that time is measured in for
	// recording when replica assignments were last moved. The default is
	// time.Minute as is usually fine.
	//
	// If you decide to tweak this, you should understand the underlying code
	// well, and the constraints in play.
	SetLastMovedUnit(v time.Duration)

	// MoveWait is how much time must elapse before a replica can be moved
	// again.
	MoveWait() time.Duration

	// SetMoveWait defines how much time must elapse before a replica can be
	// moved again. The default is time.Hour.
	SetMoveWait(v time.Duration)

	// MovesPerPartition is how many replicas of a given partition can be
	// reassigned with the MoveWait window.
	MovesPerPartition() int

	// SetMovesPerPartition defines how many replicas of a given partition can
	// be reassigned with the MoveWait window.
	//
	// For full copy replicas, you probably want this set to no more than half
	// the replica count. For erasure coding, you probably want this set to no
	// more than "m", the maximum number of failure chunks.
	//
	// 0 (or less) will be treated as the default, which currently is
	// int(replicas/2), but at least 1.
	SetMovesPerPartition(v int)

	// PointsAllowed is the number of percentage points over or under that the
	// Builder will try to keep replica assignments within.
	PointsAllowed() int

	// SetPointsAllowed defines the number of percentage points over or under
	// that the Builder will try to keep replica assignments within. For
	// example, if set to 1% and a node should receive 100 replica assignments,
	// the Builder will target 99-101 assignments to that node. This is not
	// guaranteed, just a target.
	//
	// This setting will affect how quickly the partition count will increase.
	// If you feel your use case can handle an over/under of 10%, that could
	// end up with smaller rings if memory is more of a concern. However, just
	// setting the MaxPartitionCount is probably a better approach in such a
	// case.
	//
	// 0 (or less) will be treated as the default, which currently is 1 for one
	// percent extra or fewer replicas per node.
	SetPointsAllowed(v int)

	// MaxPartitionCount is the maximum number of partitions the builder can
	// grow to.
	MaxPartitionCount() int

	// MaxPartitionCount is the maximum number of partitions the builder should
	// grow to. This keeps the rebalancer from running too long trying to
	// achieve desired balances and also caps memory usage.
	//
	// 0 (or less) will be treated as the default, which currently is 8388608.
	SetMaxPartitionCount(v int)

	// Rebalance updates the replica assignments, if needed and possible.
	Rebalance()

	// Ring returns an immutable copy of the replica assignments, the nodes
	// list, and any other information needed to have a usable ring. Further
	// modifications to the builder will have no effect on rings previously
	// returned.
	Ring() Ring

	// Marshal will persist the data from this builder to the writer. You can
	// read it back with the UnmarshalBuilder function.
	Marshal(w io.Writer) error
}

type builder struct {
	rebalanced      time.Time
	builder         lowring.Builder
	nodes           []*builderNode
	tierIndexToName []string
	tierNameToIndex map[string]int
}

// NewBuilder returns a Builder that encapsulates the information needed to
// best balance ring assignments.
func NewBuilder() Builder {
	b := &builder{
		tierNameToIndex: map[string]int{},
	}
	b.Rebalance() // ensure all the defaults have been set
	return b
}

func (b *builder) Rebalanced() time.Time {
	return b.rebalanced
}

func (b *builder) Nodes() []BuilderNode {
	nodes := make([]BuilderNode, len(b.nodes))
	for i, n := range b.nodes {
		nodes[i] = n
	}
	return nodes
}

func (b *builder) AddNode() BuilderNode {
	if len(b.builder.Nodes) == int(lowring.NodeIndexNil) {
		return nil
	}
	n := &builderNode{builder: b}
	b.nodes = append(b.nodes, n)
	b.builder.Nodes = append(b.builder.Nodes, &n.node)
	return n
}

func (b *builder) RemoveNode(n BuilderNode) {
	if bn, ok := n.(*builderNode); ok {
		for i, bn2 := range b.nodes {
			if bn2 == bn {
				b.builder.RemoveNode(lowring.NodeIndexType(i))
				copy(b.nodes[i:], b.nodes[i+1:])
				b.nodes = b.nodes[:len(b.nodes)-1]
				return
			}
		}
	}
}

func (b *builder) MaxNodeCount() int {
	return int(lowring.NodeIndexNil - 1)
}

func (b *builder) ReplicaCount() int {
	return b.builder.ReplicaCount()
}

func (b *builder) SetReplicaCount(v int) {
	b.builder.SetReplicaCount(v)
	b.rebalanced = time.Now()
}

func (b *builder) PartitionCount() int {
	return b.builder.PartitionCount()
}

func (b *builder) LastMovedUnit() time.Duration {
	return b.builder.LastMovedUnit
}

func (b *builder) SetLastMovedUnit(v time.Duration) {
	b.builder.LastMovedUnit = v
}

func (b *builder) MoveWait() time.Duration {
	return b.builder.MoveWait
}

func (b *builder) SetMoveWait(v time.Duration) {
	b.builder.MoveWait = v
}

func (b *builder) MovesPerPartition() int {
	return b.builder.MovesPerPartition
}

func (b *builder) SetMovesPerPartition(v int) {
	b.builder.MovesPerPartition = v
}

func (b *builder) PointsAllowed() int {
	return b.builder.PointsAllowed
}

func (b *builder) SetPointsAllowed(v int) {
	b.builder.PointsAllowed = v
}

func (b *builder) MaxPartitionCount() int {
	return b.builder.MaxPartitionCount
}

func (b *builder) SetMaxPartitionCount(v int) {
	b.builder.MaxPartitionCount = v
}

func (b *builder) Rebalance() {
	b.builder.Rebalance()
	b.rebalanced = time.Now()
}

func (b *builder) Ring() Ring {
	nodes := make([]*node, len(b.nodes))
	for i, n := range b.nodes {
		nodes[i] = &node{
			disabled: n.node.Disabled,
			capacity: n.node.Capacity,
			tiers:    n.Tiers(),
			info:     n.info,
		}
	}
	return &rring{rebalanced: b.rebalanced, ring: b.builder.Ring.Copy(), nodes: nodes}
}

type builderJSON struct {
	MarshalVersion    int
	NodeIndexType     int
	LastMovedType     int
	Rebalanced        int64
	ReplicaCount      int
	PartitionCount    int
	LastMovedBase     int64
	LastMovedUnit     int64
	MoveWait          int64
	MovesPerPartition int
	PointsAllowed     int
	MaxPartitionCount int
	TierIndexToName   []string
	Nodes             []*builderNodeJSON
}

type builderNodeJSON struct {
	Disabled    bool
	Capacity    int
	TierIndexes []int
	Info        string
}

func (b *builder) Marshal(w io.Writer) error {
	var nit lowring.NodeIndexType
	var lmt lowring.LastMovedType
	j := &builderJSON{
		MarshalVersion:    0,
		NodeIndexType:     int(unsafe.Sizeof(nit)) * 8,
		LastMovedType:     int(unsafe.Sizeof(lmt)) * 8,
		Rebalanced:        b.rebalanced.UnixNano(),
		ReplicaCount:      b.ReplicaCount(),
		PartitionCount:    b.PartitionCount(),
		LastMovedBase:     b.builder.LastMovedBase.UnixNano(),
		LastMovedUnit:     int64(b.builder.LastMovedUnit),
		MoveWait:          int64(b.builder.MoveWait),
		MovesPerPartition: b.builder.MovesPerPartition,
		PointsAllowed:     b.builder.PointsAllowed,
		MaxPartitionCount: b.builder.MaxPartitionCount,
		TierIndexToName:   b.tierIndexToName,
		Nodes:             make([]*builderNodeJSON, len(b.nodes)),
	}
	for i, n := range b.nodes {
		j.Nodes[i] = &builderNodeJSON{
			Disabled:    n.node.Disabled,
			Capacity:    n.node.Capacity,
			TierIndexes: n.node.TierIndexes,
			Info:        n.info,
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
	for _, partitionToNodeIndex := range b.builder.Ring {
		if err := binary.Write(w, binary.LittleEndian, partitionToNodeIndex); err != nil {
			return err
		}
	}
	for _, partitionToLastMoved := range b.builder.LastMoved {
		if err := binary.Write(w, binary.LittleEndian, partitionToLastMoved); err != nil {
			return err
		}
	}
	return nil
}

// UnmarshalBuilder will return a builder created from data stored in the
// reader. You can persist a builder to a writer with the builder's Marshal
// method.
func UnmarshalBuilder(b io.Reader) (Builder, error) {
	var nit lowring.NodeIndexType
	var lmt lowring.LastMovedType
	j := &builderJSON{}
	jsonDecoder := json.NewDecoder(b)
	if err := jsonDecoder.Decode(j); err != nil {
		return nil, err
	}
	b = io.MultiReader(jsonDecoder.Buffered(), b)
	// These byte reads are to get past any trailing whitespace, newlines, etc.
	// the JSON encoder may or may not have written. When marshalling we
	// preface the raw ring data with a 0 byte.
	b0 := []byte{0}
	for {
		if n, err := b.Read(b0); err != nil {
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
		return nil, fmt.Errorf("builder data does not match compiled builder format. NodeIndexType is %d bits in the data and %d bits compiled.", j.NodeIndexType, int(unsafe.Sizeof(nit))*8)
	}
	if j.LastMovedType != int(unsafe.Sizeof(lmt))*8 {
		return nil, fmt.Errorf("builder data does not match compiled builder format. LastMovedType is %d bits in the data and %d bits compiled.", j.LastMovedType, int(unsafe.Sizeof(lmt))*8)
	}
	rv := &builder{
		rebalanced:      time.Unix(0, j.Rebalanced),
		nodes:           make([]*builderNode, len(j.Nodes)),
		tierIndexToName: j.TierIndexToName,
		tierNameToIndex: map[string]int{},
	}
	rv.builder.LastMovedBase = time.Unix(0, j.LastMovedBase)
	rv.builder.LastMovedUnit = time.Duration(j.LastMovedUnit)
	rv.builder.MoveWait = time.Duration(j.MoveWait)
	rv.builder.MovesPerPartition = j.MovesPerPartition
	rv.builder.PointsAllowed = j.PointsAllowed
	rv.builder.MaxPartitionCount = j.MaxPartitionCount
	for i, n := range rv.tierIndexToName {
		rv.tierNameToIndex[n] = i
	}
	for i, jn := range j.Nodes {
		rn := &builderNode{}
		rn.node.Disabled = jn.Disabled
		rn.node.Capacity = jn.Capacity
		rn.node.TierIndexes = jn.TierIndexes
		rn.info = jn.Info
		rv.nodes[i] = rn
	}
	rv.builder.Ring = make(lowring.Ring, j.ReplicaCount)
	for replica := 0; replica < j.ReplicaCount; replica++ {
		rv.builder.Ring[replica] = make([]lowring.NodeIndexType, j.PartitionCount)
		if err := binary.Read(b, binary.LittleEndian, rv.builder.Ring[replica]); err != nil {
			return nil, err
		}
	}
	rv.builder.LastMoved = make([][]lowring.LastMovedType, j.ReplicaCount)
	for replica := 0; replica < j.ReplicaCount; replica++ {
		rv.builder.LastMoved[replica] = make([]lowring.LastMovedType, j.PartitionCount)
		if err := binary.Read(b, binary.LittleEndian, rv.builder.LastMoved[replica]); err != nil {
			return nil, err
		}
	}
	return rv, nil
}
