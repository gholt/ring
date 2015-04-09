package ring

import (
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"
)

// Builder is used to construct Rings over time. Rings are the immutable state
// of a Builder's assignments at a given point in time.
type Builder struct {
	tierBase
	version                       int64
	nodes                         []*node
	partitionBitCount             uint16
	replicaToPartitionToNodeIndex [][]int32
	replicaToPartitionToLastMove  [][]uint16
	pointsAllowed                 byte
	maxPartitionBitCount          uint16
	moveWait                      uint16
	moveWaitBase                  int64
}

// NewBuilder creates an empty Builder with all default settings.
func NewBuilder() *Builder {
	b := &Builder{
		partitionBitCount:             1,
		replicaToPartitionToNodeIndex: make([][]int32, 1),
		replicaToPartitionToLastMove:  make([][]uint16, 1),
		pointsAllowed:                 1,
		// 1 << 23 is 8388608 which, with 3 replicas, would use about 100M of
		// memory.
		maxPartitionBitCount: 23,
		moveWait:             60, // 1 hour default
	}
	b.replicaToPartitionToNodeIndex[0] = []int32{-1, -1}
	b.replicaToPartitionToLastMove[0] = []uint16{math.MaxUint16, math.MaxUint16}
	return b
}

// LoadBuilder creates a new Builder instance based on the persisted data from
// the Reader (presumably previously saved with the Persist method).
func LoadBuilder(r io.Reader) (*Builder, error) {
	// CONSIDER: This code uses binary.Read which incurs fleeting allocations;
	// these could be reduced by creating a buffer upfront and using
	// binary.Put* calls instead.
	gr, err := gzip.NewReader(r)
	if err != nil {
		return nil, err
	}
	defer gr.Close() // does not close the underlying reader
	header := make([]byte, 16)
	_, err = io.ReadFull(gr, header)
	if err != nil {
		return nil, err
	}
	if string(header) != "RINGBUILDERv0001" {
		return nil, fmt.Errorf("unknown header %s", string(header))
	}
	b := &Builder{}
	err = binary.Read(gr, binary.BigEndian, &b.version)
	if err != nil {
		return nil, err
	}
	var vint32 int32
	err = binary.Read(gr, binary.BigEndian, &vint32)
	if err != nil {
		return nil, err
	}
	b.tiers = make([][]string, vint32)
	for i := int32(0); i < vint32; i++ {
		var vvint32 int32
		err = binary.Read(gr, binary.BigEndian, &vvint32)
		if err != nil {
			return nil, err
		}
		b.tiers[i] = make([]string, vvint32)
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
			b.tiers[i][j] = string(byts)
		}
	}
	err = binary.Read(gr, binary.BigEndian, &vint32)
	if err != nil {
		return nil, err
	}
	b.nodes = make([]*node, vint32)
	for i := int32(0); i < vint32; i++ {
		b.nodes[i] = &node{tierBase: &b.tierBase}
		err = binary.Read(gr, binary.BigEndian, &b.nodes[i].id)
		if err != nil {
			return nil, err
		}
		tf := byte(0)
		err = binary.Read(gr, binary.BigEndian, &tf)
		if err != nil {
			return nil, err
		}
		if tf == 1 {
			b.nodes[i].inactive = true
		}
		err = binary.Read(gr, binary.BigEndian, &b.nodes[i].capacity)
		if err != nil {
			return nil, err
		}
		var vvint32 int32
		err = binary.Read(gr, binary.BigEndian, &vvint32)
		if err != nil {
			return nil, err
		}
		b.nodes[i].tierIndexes = make([]int32, vvint32)
		for j := int32(0); j < vvint32; j++ {
			err = binary.Read(gr, binary.BigEndian, &b.nodes[i].tierIndexes[j])
			if err != nil {
				return nil, err
			}
		}
		err = binary.Read(gr, binary.BigEndian, &vvint32)
		if err != nil {
			return nil, err
		}
		b.nodes[i].addresses = make([]string, vvint32)
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
			b.nodes[i].addresses[j] = string(byts)
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
		b.nodes[i].meta = string(byts)
	}
	err = binary.Read(gr, binary.BigEndian, &b.partitionBitCount)
	if err != nil {
		return nil, err
	}
	err = binary.Read(gr, binary.BigEndian, &vint32)
	if err != nil {
		return nil, err
	}
	b.replicaToPartitionToNodeIndex = make([][]int32, vint32)
	for i := int32(0); i < vint32; i++ {
		var vvint32 int32
		err = binary.Read(gr, binary.BigEndian, &vvint32)
		if err != nil {
			return nil, err
		}
		b.replicaToPartitionToNodeIndex[i] = make([]int32, vvint32)
		err = binary.Read(gr, binary.BigEndian, b.replicaToPartitionToNodeIndex[i])
	}
	err = binary.Read(gr, binary.BigEndian, &vint32)
	if err != nil {
		return nil, err
	}
	b.replicaToPartitionToLastMove = make([][]uint16, vint32)
	for i := int32(0); i < vint32; i++ {
		var vvint32 int32
		err = binary.Read(gr, binary.BigEndian, &vvint32)
		if err != nil {
			return nil, err
		}
		b.replicaToPartitionToLastMove[i] = make([]uint16, vvint32)
		err = binary.Read(gr, binary.BigEndian, b.replicaToPartitionToLastMove[i])
	}
	err = binary.Read(gr, binary.BigEndian, &b.pointsAllowed)
	if err != nil {
		return nil, err
	}
	err = binary.Read(gr, binary.BigEndian, &b.maxPartitionBitCount)
	if err != nil {
		return nil, err
	}
	err = binary.Read(gr, binary.BigEndian, &b.moveWait)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// Persist saves the Builder state to the given Writer for later reloading via
// the LoadBuilder method.
func (b *Builder) Persist(w io.Writer) error {
	b.minimizeTiers()
	// CONSIDER: This code uses binary.Write which incurs fleeting allocations;
	// these could be reduced by creating a buffer upfront and using
	// binary.Put* calls instead.
	gw := gzip.NewWriter(w)
	defer gw.Close() // does not close the underlying writer
	_, err := gw.Write([]byte("RINGBUILDERv0001"))
	if err != nil {
		return err
	}
	err = binary.Write(gw, binary.BigEndian, b.version)
	if err != nil {
		return err
	}
	err = binary.Write(gw, binary.BigEndian, int32(len(b.tiers)))
	if err != nil {
		return err
	}
	for _, tier := range b.tiers {
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
	err = binary.Write(gw, binary.BigEndian, int32(len(b.nodes)))
	if err != nil {
		return err
	}
	for _, n := range b.nodes {
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
	err = binary.Write(gw, binary.BigEndian, b.partitionBitCount)
	if err != nil {
		return err
	}
	err = binary.Write(gw, binary.BigEndian, int32(len(b.replicaToPartitionToNodeIndex)))
	if err != nil {
		return err
	}
	for _, partitionToNodeIndex := range b.replicaToPartitionToNodeIndex {
		err = binary.Write(gw, binary.BigEndian, int32(len(partitionToNodeIndex)))
		if err != nil {
			return err
		}
		err = binary.Write(gw, binary.BigEndian, partitionToNodeIndex)
		if err != nil {
			return err
		}
	}
	err = binary.Write(gw, binary.BigEndian, int32(len(b.replicaToPartitionToLastMove)))
	if err != nil {
		return err
	}
	for _, partitionToLastMove := range b.replicaToPartitionToLastMove {
		err = binary.Write(gw, binary.BigEndian, int32(len(partitionToLastMove)))
		if err != nil {
			return err
		}
		err = binary.Write(gw, binary.BigEndian, partitionToLastMove)
		if err != nil {
			return err
		}
	}
	err = binary.Write(gw, binary.BigEndian, b.pointsAllowed)
	if err != nil {
		return err
	}
	err = binary.Write(gw, binary.BigEndian, b.maxPartitionBitCount)
	if err != nil {
		return err
	}
	err = binary.Write(gw, binary.BigEndian, b.moveWait)
	if err != nil {
		return err
	}
	return nil
}

func (b *Builder) minimizeTiers() {
	u := make([][]bool, len(b.tiers))
	for i, t := range b.tiers {
		u[i] = make([]bool, len(t))
	}
	for _, n := range b.nodes {
		for lv, i := range n.tierIndexes {
			u[lv][i] = true
		}
	}
	for lv, us := range u {
		for i := len(us) - 1; i > 0; i-- {
			if us[i] {
				continue
			}
			b.tiers[lv][i] = ""
			for _, n := range b.nodes {
				if n.tierIndexes[lv] > int32(i) {
					n.tierIndexes[lv]--
				}
			}
		}
	}
	for lv := 0; lv < len(b.tiers); lv++ {
		ts := make([]string, 1, len(b.tiers[lv]))
		for _, t := range b.tiers[lv][1:] {
			if t != "" {
				ts = append(ts, t)
			}
		}
		b.tiers[lv] = ts
	}
}

func (b *Builder) ReplicaCount() int {
	return len(b.replicaToPartitionToNodeIndex)
}

func (b *Builder) SetReplicaCount(count int) {
	if count < 1 {
		count = 1
	}
	if count < len(b.replicaToPartitionToNodeIndex) {
		b.replicaToPartitionToNodeIndex = b.replicaToPartitionToNodeIndex[:count]
		b.replicaToPartitionToLastMove = b.replicaToPartitionToLastMove[:count]
	} else if count > len(b.replicaToPartitionToNodeIndex) {
		partitionCount := len(b.replicaToPartitionToNodeIndex[0])
		for count > len(b.replicaToPartitionToNodeIndex) {
			newPartitionToNodeIndex := make([]int32, partitionCount)
			newPartitionToLastMove := make([]uint16, partitionCount)
			for i := 0; i < partitionCount; i++ {
				newPartitionToNodeIndex[i] = -1
				newPartitionToLastMove[i] = math.MaxUint16
			}
			b.replicaToPartitionToNodeIndex = append(b.replicaToPartitionToNodeIndex, newPartitionToNodeIndex)
			b.replicaToPartitionToLastMove = append(b.replicaToPartitionToLastMove, newPartitionToLastMove)
		}
	}
}

// PointsAllowed is the number of percentage points over or under that the ring
// will try to keep data assignments within. The default is 1 for one percent
// extra or less data.
func (b *Builder) PointsAllowed() byte {
	return b.pointsAllowed
}

func (b *Builder) SetPointsAllowed(points byte) {
	b.pointsAllowed = points
}

// MaxPartitionBitCount caps how large the ring can grow. The default is 23,
// which means 2**23 or 8,388,608 partitions, which is about 100M for a 3
// replica ring (each partition replica assignment is an int32).
func (b *Builder) MaxPartitionBitCount() uint16 {
	return b.maxPartitionBitCount
}

func (b *Builder) SetMaxPartitionBitCount(count uint16) {
	b.maxPartitionBitCount = count
}

// MoveWait is the number of minutes that should elapse before reassigning a
// replica of a partition again.
func (b *Builder) MoveWait() uint16 {
	return b.moveWait
}

func (b *Builder) SetMoveWait(minutes uint16) {
	b.moveWait = minutes
}

// PretendElapsed shifts the last movement records by the number of minutes
// given. This can be useful in testing, as the ring algorithms will not
// reassign replicas for a partition more often than once per MoveWait in order
// to let reassignments take effect before moving the same data yet again.
func (b *Builder) PretendElapsed(minutes uint16) {
	for _, partitionToLastMove := range b.replicaToPartitionToLastMove {
		for partition := len(partitionToLastMove) - 1; partition >= 0; partition-- {
			if math.MaxUint16-partitionToLastMove[partition] > minutes {
				partitionToLastMove[partition] = math.MaxUint16
			} else {
				partitionToLastMove[partition] += minutes
			}
		}
	}
}

// Nodes returns a NodeSlice of the nodes the Builder references, but each Node
// in the slice can be typecast into a BuilderNode if needed.
func (b *Builder) Nodes() NodeSlice {
	nodes := make(NodeSlice, len(b.nodes))
	for i := len(nodes) - 1; i >= 0; i-- {
		nodes[i] = b.nodes[i]
	}
	return nodes
}

// AddNode will add a new node to the builder for data assigment. Actual data
// assignment won't ocurr until the Ring method is called, so you can add
// multiple nodes or alter node values after creation if desired.
func (b *Builder) AddNode(active bool, capacity uint32, tiers []string, addresses []string, meta string) BuilderNode {
	addressesCopy := make([]string, len(addresses))
	copy(addressesCopy, addresses)
	n := newNode(&b.tierBase, b.nodes)
	n.inactive = !active
	n.capacity = capacity
	n.addresses = addressesCopy
	n.meta = meta
	for level, value := range tiers {
		n.SetTier(level, value)
	}
	b.nodes = append(b.nodes, n)
	return n
}

// RemoveNode will remove the node from the list of nodes for this
// builder/ring. Note that this can be relatively expensive as all nodes that
// had been added after the removed node had been originally added will have
// their internal indexes shifted down one and all the
// replica-to-partition-to-node indexing will have to be updated, as well as
// clearing any assignments that were to the removed node. Normally it is
// better to just leave a "dead" node in place and simply set it as inactive.
func (b *Builder) RemoveNode(nodeID uint64) {
	for i, n := range b.nodes {
		if n.id == nodeID {
			copy(b.nodes[i:], b.nodes[i+1:])
			b.nodes = b.nodes[:len(b.nodes)-1]
			for _, partitionToNodeIndex := range b.replicaToPartitionToNodeIndex {
				for j := len(partitionToNodeIndex) - 1; j >= 0; j-- {
					if partitionToNodeIndex[j] == int32(i) {
						partitionToNodeIndex[j] = -1
					} else if partitionToNodeIndex[j] > int32(i) {
						partitionToNodeIndex[j]--
					}
				}
			}
			break
		}
	}
}

// Node returns the node instance identified, if there is one.
func (b *Builder) Node(nodeID uint64) BuilderNode {
	for _, n := range b.nodes {
		if n.id == nodeID {
			return n
		}
	}
	return nil
}

// Tiers returns the tier values in use at each level. Note that an empty
// string is always an available value at any level, although it is not
// returned from this method.
func (b *Builder) Tiers() [][]string {
	rv := make([][]string, len(b.tiers))
	for i, t := range b.tiers {
		rv[i] = make([]string, len(t)-1)
		copy(rv[i], t[1:])
	}
	return rv
}

// Ring returns a Ring instance of the data defined by the builder. This will
// cause any pending rebalancing actions to be performed. The Ring returned
// will be immutable; to obtain updated ring data, Ring() must be called again.
func (b *Builder) Ring() Ring {
	validNodes := false
	for _, n := range b.nodes {
		if !n.inactive {
			validNodes = true
		}
	}
	if !validNodes {
		panic("no valid nodes yet")
	}
	newBase := time.Now().UnixNano()
	d := (time.Now().UnixNano() - b.moveWaitBase) / 6000000000 // minutes
	if d > 0 {
		var d16 uint16 = math.MaxUint16
		if d < math.MaxUint16 {
			d16 = uint16(d)
		}
		b.PretendElapsed(d16)
		b.moveWaitBase = newBase
	}
	if b.resizeIfNeeded() {
		b.version = newBase
	}
	if newRebalancer(b).rebalance() {
		b.version = newBase
	}
	tiers := make([][]string, len(b.tiers))
	for i, tier := range b.tiers {
		tiers[i] = make([]string, len(tier))
		copy(tiers[i], tier)
	}
	nodes := make([]*node, len(b.nodes))
	copy(nodes, b.nodes)
	replicaToPartitionToNodeIndex := make([][]int32, len(b.replicaToPartitionToNodeIndex))
	for i := 0; i < len(replicaToPartitionToNodeIndex); i++ {
		replicaToPartitionToNodeIndex[i] = make([]int32, len(b.replicaToPartitionToNodeIndex[i]))
		copy(replicaToPartitionToNodeIndex[i], b.replicaToPartitionToNodeIndex[i])
	}
	return &ring{
		tierBase:          tierBase{tiers: tiers},
		version:           b.version,
		localNodeIndex:    -1,
		partitionBitCount: b.partitionBitCount,
		nodes:             nodes,
		replicaToPartitionToNodeIndex: replicaToPartitionToNodeIndex,
	}
}

func (b *Builder) resizeIfNeeded() bool {
	if b.partitionBitCount >= b.maxPartitionBitCount {
		return false
	}
	replicaCount := len(b.replicaToPartitionToNodeIndex)
	// Calculate the partition count needed.
	// Each node is examined to see how much under or overweight it would be
	// and increasing the partition count until the difference is under the
	// points allowed.
	totalCapacity := uint64(0)
	for _, n := range b.nodes {
		if !n.inactive {
			totalCapacity += (uint64)(n.capacity)
		}
	}
	partitionCount := len(b.replicaToPartitionToNodeIndex[0])
	partitionBitCount := b.partitionBitCount
	pointsAllowed := float64(b.pointsAllowed) * 0.01
	for _, n := range b.nodes {
		if n.inactive {
			continue
		}
		desiredPartitionCount := float64(partitionCount) * float64(replicaCount) * (float64(n.capacity) / float64(totalCapacity))
		under := (desiredPartitionCount - float64(int(desiredPartitionCount))) / desiredPartitionCount
		over := float64(0)
		if desiredPartitionCount > float64(int(desiredPartitionCount)) {
			over = (float64(int(desiredPartitionCount)+1) - desiredPartitionCount) / desiredPartitionCount
		}
		if under > pointsAllowed || over > pointsAllowed {
			partitionCount <<= 1
			partitionBitCount++
			if partitionBitCount == b.maxPartitionBitCount {
				break
			}
		}
	}
	// Grow the partitionToNodeIndex slices if the partition count grew.
	if partitionCount > len(b.replicaToPartitionToNodeIndex[0]) {
		shift := partitionBitCount - b.partitionBitCount
		for replica := 0; replica < replicaCount; replica++ {
			partitionToNodeIndex := make([]int32, partitionCount)
			partitionToLastMove := make([]uint16, partitionCount)
			for partition := 0; partition < partitionCount; partition++ {
				partitionToNodeIndex[partition] = b.replicaToPartitionToNodeIndex[replica][partition>>shift]
				partitionToLastMove[partition] = b.replicaToPartitionToLastMove[replica][partition>>shift]
			}
			b.replicaToPartitionToNodeIndex[replica] = partitionToNodeIndex
			b.replicaToPartitionToLastMove[replica] = partitionToLastMove
		}
		b.partitionBitCount = partitionBitCount
		return true
	}
	// Consider: Shrinking the partitionToNodeIndex slices doesn't happen
	// because it would normally cause more data movements than it's worth.
	// Perhaps in the future we can add detection of cases when shrinking makes
	// sense.
	return false
}
