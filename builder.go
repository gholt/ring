package ring

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/rand"
	"time"
	"unsafe"

	"github.com/gholt/ring/lowring"
)

// Builder will create and maintain rings.
type Builder struct {
	ring     *lowring.Ring
	nodes    []*BuilderNode
	groups   []*BuilderGroup
	randIntn func(int) int
}

// NewBuilder creates a new builder with the initial replica count given.
func NewBuilder(replicaCount int) *Builder {
	if replicaCount < 1 {
		replicaCount = 1
	}
	if replicaCount > 127 {
		replicaCount = 127
	}
	b := &Builder{ring: lowring.New(replicaCount), randIntn: rand.New(rand.NewSource(0)).Intn}
	b.groups = []*BuilderGroup{{builder: b}}
	return b
}

// Ring returns an immutable copy of the assignment information contained in
// the builder.
func (b *Builder) Ring() *Ring {
	ring := &Ring{
		nodes:                    make([]*Node, len(b.nodes)),
		groups:                   make([]*Group, len(b.groups)),
		nodeToGroup:              make([]int, len(b.nodes)),
		groupToGroup:             make([]int, len(b.groups)),
		replicaToPartitionToNode: make([][]lowring.Node, len(b.ring.ReplicaToPartitionToNode)),
		rebalanced:               b.ring.Rebalanced,
	}
	for i, n := range b.nodes {
		ring.nodes[i] = &Node{
			ring:     ring,
			index:    i,
			info:     n.info,
			capacity: b.ring.NodeToCapacity[i],
		}
	}
	for i, g := range b.groups {
		ring.groups[i] = &Group{
			ring:  ring,
			index: i,
			info:  g.info,
		}
	}
	copy(ring.nodeToGroup, b.ring.NodeToGroup)
	copy(ring.groupToGroup, b.ring.GroupToGroup)
	replicaCount := len(b.ring.ReplicaToPartitionToNode)
	partitionCount := len(b.ring.ReplicaToPartitionToNode[0])
	for replica := 0; replica < replicaCount; replica++ {
		ring.replicaToPartitionToNode[replica] = make([]lowring.Node, partitionCount)
		copy(ring.replicaToPartitionToNode[replica], b.ring.ReplicaToPartitionToNode[replica])
	}
	return ring
}

// Nodes returns a slice of all the nodes in the builder.
func (b *Builder) Nodes() []*BuilderNode {
	nodes := make([]*BuilderNode, len(b.nodes))
	copy(nodes, b.nodes)
	return nodes
}

// AddNode adds another node to the builder. Info is a user-defined string and
// is not used directly by the builder. Capacity specifies, relative to other
// nodes, how many assignments the node should have. The group indicates which
// group the node is in; the builder will do its best to keep similar
// assignments in dissimilar groups. The group may be nil.
func (b *Builder) AddNode(info string, capacity int, group *BuilderGroup) *BuilderNode {
	groupIndex := 0
	if group != nil {
		groupIndex = group.index
	}
	node := &BuilderNode{builder: b, index: int(b.ring.AddNode(capacity, groupIndex)), info: info}
	b.nodes = append(b.nodes, node)
	return node
}

// Groups returns a slice of all the groups in use by the builder.
func (b *Builder) Groups() []*BuilderGroup {
	groups := make([]*BuilderGroup, len(b.groups))
	copy(groups, b.groups)
	return groups
}

// AddGroup adds another group to the builder. Info is a user-defined string
// and is not used directly by the builder. The parent group offers a way to
// tier groups; the builder will do its best to keep similar assignments in
// dissimilar groups at each tier level. The parent may be nil. Cycles are not
// allowed, where the parent of a group ends up being a child of the group.
func (b *Builder) AddGroup(info string, parent *BuilderGroup) *BuilderGroup {
	parentIndex := 0
	if parent != nil {
		parentIndex = parent.index
	}
	index := len(b.ring.GroupToGroup)
	b.ring.GroupToGroup = append(b.ring.GroupToGroup, parentIndex)
	group := &BuilderGroup{builder: b, index: index, info: info}
	b.groups = append(b.groups, group)
	return group
}

// ReplicaCount returns the current replica count of the builder.
func (b *Builder) ReplicaCount() int {
	return len(b.ring.ReplicaToPartitionToNode)
}

// SetReplicaCount changes the replica count of the builder. Lowering the
// replica count will simply discard the higher replicas. Raising the replica
// count will create new higher replicas that will be completely unassigned and
// will require a call to Rebalance to become assigned.
func (b *Builder) SetReplicaCount(v int) {
	if v < 1 {
		v = 1
	}
	if v > 127 {
		v = 127
	}
	b.ring.SetReplicaCount(v)
}

// PartitionCount returns the current partition count for the builder.
func (b *Builder) PartitionCount() int {
	return len(b.ring.ReplicaToPartitionToNode[0])
}

// MaxPartitionCount returns the maximum partition count the builder will
// auto-grow to.
func (b *Builder) MaxPartitionCount() int {
	return b.ring.MaxPartitionCount
}

// SetMaxPartitionCount sets the maximum partition count the builder will
// auto-grow to.
func (b *Builder) SetMaxPartitionCount(v int) {
	if v < 1 {
		v = 1
	}
	b.ring.MaxPartitionCount = v
}

// AssignmentCount returns the number of assignments this builder will make; it
// is the replica count * the partition count.
func (b *Builder) AssignmentCount() int {
	return len(b.ring.ReplicaToPartitionToNode) * len(b.ring.ReplicaToPartitionToNode[0])
}

// Rebalanced returns the time the builder last had Rebalance called.
func (b *Builder) Rebalanced() time.Time {
	return b.ring.Rebalanced
}

// Rebalance analyzes all information and makes assignments to nodes as best as
// it can.
func (b *Builder) Rebalance() {
	b.ring.Rebalance(b.randIntn)
}

// PretendElapsed is mostly used for testing and will make the builder pretend
// the time duration has elapsed, usually freeing up time based reassignment
// restrictions.
func (b *Builder) PretendElapsed(d time.Duration) {
	minutesElapsed := int64(d / time.Minute)
	replicaCount := len(b.ring.ReplicaToPartitionToNode)
	partitionCount := len(b.ring.ReplicaToPartitionToNode[0])
	if minutesElapsed >= int64(b.ring.ReassignmentWait) || minutesElapsed >= int64(math.MaxUint16) {
		for replica := 0; replica < replicaCount; replica++ {
			partitionToWait := b.ring.ReplicaToPartitionToWait[replica]
			for partition := 0; partition < partitionCount; partition++ {
				partitionToWait[partition] = 0
			}
		}
	} else if minutesElapsed > 0 {
		for replica := 0; replica < replicaCount; replica++ {
			partitionToWait := b.ring.ReplicaToPartitionToWait[replica]
			for partition := 0; partition < partitionCount; partition++ {
				wait64 := int64(partitionToWait[0]) - minutesElapsed
				if wait64 < 0 {
					wait64 = 0
				}
				partitionToWait[partition] = uint16(wait64)
			}
		}
	}
}

// KeyNodes returns the nodes responsible for the key given. There will be on
// node for each replica; in other words:
// len(b.KeyNodes(k)) == b.ReplicaCount().
func (b *Builder) KeyNodes(key int) []*BuilderNode {
	nodes := make([]*BuilderNode, 0, len(b.ring.ReplicaToPartitionToNode))
	partition := key % len(b.ring.ReplicaToPartitionToNode[0])
	for _, partitionToNode := range b.ring.ReplicaToPartitionToNode {
		nodes = append(nodes, b.nodes[partitionToNode[partition]])
	}
	return nodes
}

// ReplicaPartitionNode returns the node responsible for a specific replica of
// a partition.
func (b *Builder) ReplicaPartitionNode(replica, partition int) *BuilderNode {
	return b.nodes[b.ring.ReplicaToPartitionToNode[replica][partition]]
}

// IsMoving returns true if the specific replica of the partition is currently
// in "reassignment wait", where a recent rebalance had reassigned the replica
// to a different node and so is giving time for the data to be moved.
func (b *Builder) IsMoving(replica, partition int) bool {
	return b.ring.ReplicaToPartitionToWait[replica][partition] > 0
}

// MovingAssignmentCount returns the number of assignments that are currently
// "in motion". If you were to call IsMoving for every replica of every
// partition and count the true responses, that would equal the
// MovingAssignmentCount.
func (b *Builder) MovingAssignmentCount() int {
	replicaCount := len(b.ring.ReplicaToPartitionToNode)
	partitionCount := len(b.ring.ReplicaToPartitionToNode[0])
	moving := 0
	for replica := 0; replica < replicaCount; replica++ {
		partitionToWait := b.ring.ReplicaToPartitionToWait[replica]
		for partition := 0; partition < partitionCount; partition++ {
			if partitionToWait[partition] > 0 {
				moving++
			}
		}
	}
	return moving
}

// ReassignmentWait is the time duration the builder will wait after making an
// assignment before considering reassigning that same data.
func (b *Builder) ReassignmentWait() time.Duration {
	return time.Duration(b.ring.ReassignmentWait) * time.Minute
}

// SetReassignmentWait sets the time duration the builder will wait after
// making an assignment before considering reassigning that same data.
func (b *Builder) SetReassignmentWait(v time.Duration) {
	i := int(v / time.Minute)
	if i < 1 {
		i = 1
	}
	if i > math.MaxUint16 {
		i = math.MaxUint16
	}
	b.ring.ReassignmentWait = uint16(i)
}

// MaxReplicaReassignableCount returns the maximum number of replicas of a
// partition the builder will set "in motion" during the reassignment wait
// period.
func (b *Builder) MaxReplicaReassignableCount() int {
	return int(b.ring.MaxReplicaReassignableCount)
}

// SetMaxReplicaReassignableCount sets the maximum number of replicas of a
// partition the builder will set "in motion" during the reassignment wait
// period.
//
// For a full replica use case, you probably want to set this to no more than
// one less of the majority of replicas so that a majority are always in place
// at any given time.
//
// For an erasure coding use case, you probably want to set this to no more
// than the number of parity shards so that there are always enough shards in
// place at any given time.
func (b *Builder) SetMaxReplicaReassignableCount(v int) {
	if v < 1 {
		v = 1
	}
	if v > 127 {
		v = 127
	}
	b.ring.MaxReplicaReassignableCount = int8(v)
}

// Assign will override the current builder's assignment and set a specific
// replica of a partition to a specific node. This is mostly just useful for
// testing, as future calls to Rebalance may move this assignment.
func (b *Builder) Assign(replica, partition int, node *BuilderNode) {
	b.ring.ReplicaToPartitionToNode[replica][partition] = lowring.Node(node.index)
	b.ring.ReplicaToPartitionToWait[replica][partition] = 0
}

type builderJSON struct {
	MarshalVersion              int
	NodeType                    int
	ReplicaCount                int
	PartitionCount              int
	Nodes                       []*builderNodeJSON
	Groups                      []*builderGroupJSON
	MaxPartitionCount           int
	Rebalanced                  int64
	ReassignmentWait            int
	MaxReplicaReassignableCount int
}

type builderNodeJSON struct {
	Info     string
	Capacity int
	Group    int
}

type builderGroupJSON struct {
	Info   string
	Parent int
}

// Marshal will write a JSON+binary encoded version of its contents to the
// io.Writer. You can use UnmarshalBuilder to read it back later.
func (b *Builder) Marshal(w io.Writer) error {
	var nodeType lowring.Node
	j := &builderJSON{
		MarshalVersion:              0,
		NodeType:                    int(unsafe.Sizeof(nodeType)) * 8,
		ReplicaCount:                len(b.ring.ReplicaToPartitionToNode),
		PartitionCount:              len(b.ring.ReplicaToPartitionToNode[0]),
		Nodes:                       make([]*builderNodeJSON, len(b.nodes)),
		Groups:                      make([]*builderGroupJSON, len(b.groups)),
		MaxPartitionCount:           b.ring.MaxPartitionCount,
		Rebalanced:                  b.ring.Rebalanced.UnixNano(),
		ReassignmentWait:            int(b.ring.ReassignmentWait),
		MaxReplicaReassignableCount: int(b.ring.MaxReplicaReassignableCount),
	}
	for i, n := range b.nodes {
		j.Nodes[i] = &builderNodeJSON{
			Info:     n.info,
			Capacity: b.ring.NodeToCapacity[n.index],
			Group:    b.ring.NodeToGroup[n.index],
		}
	}
	for i, g := range b.groups {
		j.Groups[i] = &builderGroupJSON{
			Info:   g.info,
			Parent: b.ring.GroupToGroup[g.index],
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
	for _, partitionToNode := range b.ring.ReplicaToPartitionToNode {
		if err := binary.Write(w, binary.LittleEndian, partitionToNode); err != nil {
			return err
		}
	}
	for _, partitionToWait := range b.ring.ReplicaToPartitionToWait {
		if err := binary.Write(w, binary.LittleEndian, partitionToWait); err != nil {
			return err
		}
	}
	return nil
}

// UnmarshalBuilder returns a builder based on the JSON+binary encoded
// information read from the io.Reader, presumably previously written by the
// builder's Marshal method.
func UnmarshalBuilder(b io.Reader) (*Builder, error) {
	var nodeType lowring.Node
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
	if j.NodeType != int(unsafe.Sizeof(nodeType))*8 {
		return nil, fmt.Errorf("builder data does not match compiled builder format. NodeType is %d bits in the data and %d bits compiled.", j.NodeType, int(unsafe.Sizeof(nodeType))*8)
	}
	rv := &Builder{
		ring:     &lowring.Ring{},
		nodes:    make([]*BuilderNode, len(j.Nodes)),
		groups:   make([]*BuilderGroup, len(j.Groups)),
		randIntn: rand.New(rand.NewSource(0)).Intn,
	}
	rv.ring.NodeToCapacity = make([]int, len(j.Nodes))
	rv.ring.NodeToGroup = make([]int, len(j.Nodes))
	rv.ring.GroupToGroup = make([]int, len(j.Groups))
	rv.ring.MaxPartitionCount = j.MaxPartitionCount
	rv.ring.Rebalanced = time.Unix(0, j.Rebalanced)
	rv.ring.ReassignmentWait = uint16(j.ReassignmentWait)
	rv.ring.MaxReplicaReassignableCount = int8(j.MaxReplicaReassignableCount)
	for i, jn := range j.Nodes {
		rv.nodes[i] = &BuilderNode{builder: rv, index: i, info: jn.Info}
		rv.ring.NodeToCapacity[i] = jn.Capacity
		rv.ring.NodeToGroup[i] = jn.Group
	}
	for i, jg := range j.Groups {
		rv.groups[i] = &BuilderGroup{builder: rv, index: i, info: jg.Info}
		rv.ring.GroupToGroup[i] = jg.Parent
	}
	rv.ring.ReplicaToPartitionToNode = make([][]lowring.Node, j.ReplicaCount)
	for replica := 0; replica < j.ReplicaCount; replica++ {
		rv.ring.ReplicaToPartitionToNode[replica] = make([]lowring.Node, j.PartitionCount)
		if err := binary.Read(b, binary.LittleEndian, rv.ring.ReplicaToPartitionToNode[replica]); err != nil {
			return nil, err
		}
	}
	rv.ring.ReplicaToPartitionToWait = make([][]uint16, j.ReplicaCount)
	for replica := 0; replica < j.ReplicaCount; replica++ {
		rv.ring.ReplicaToPartitionToWait[replica] = make([]uint16, j.PartitionCount)
		if err := binary.Read(b, binary.LittleEndian, rv.ring.ReplicaToPartitionToWait[replica]); err != nil {
			return nil, err
		}
	}
	return rv, nil
}
