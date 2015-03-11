package ring

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"
)

type Builder struct {
	version                       int64
	nodes                         []*Node
	partitionBitCount             uint16
	replicaToPartitionToNodeIndex [][]int32
	replicaToPartitionToLastMove  [][]uint16
	pointsAllowed                 byte
	maxPartitionBitCount          uint16
	moveWait                      uint16
}

func NewBuilder(replicaCount int) *Builder {
	b := &Builder{
		nodes:                         make([]*Node, 0),
		partitionBitCount:             1,
		replicaToPartitionToNodeIndex: make([][]int32, replicaCount),
		replicaToPartitionToLastMove:  make([][]uint16, replicaCount),
		pointsAllowed:                 1,
		// 1 << 23 is 8388608 which, with 3 replicas, would use about 100M of
		// memory.
		maxPartitionBitCount: 23,
		moveWait:             60, // 1 hour default
	}
	for replica := 0; replica < replicaCount; replica++ {
		b.replicaToPartitionToNodeIndex[replica] = []int32{-1, -1}
		b.replicaToPartitionToLastMove[replica] = []uint16{math.MaxUint16, math.MaxUint16}
	}
	return b
}

func LoadBuilder(r io.Reader) (*Builder, error) {
	header := make([]byte, 16)
	_, err := io.ReadFull(r, header)
	if err != nil {
		return nil, err
	}
	if string(header) != "RINGBUILDERv0001" {
		return nil, fmt.Errorf("unknown header %s", string(header))
	}
	b := &Builder{}
	err = binary.Read(r, binary.BigEndian, &b.version)
	if err != nil {
		return nil, err
	}
	var vint32 int32
	err = binary.Read(r, binary.BigEndian, &vint32)
	if err != nil {
		return nil, err
	}
	b.nodes = make([]*Node, vint32)
	for i := int32(0); i < vint32; i++ {
		b.nodes[i] = &Node{}
		err = binary.Read(r, binary.BigEndian, &b.nodes[i].ID)
		if err != nil {
			return nil, err
		}
		tf := byte(0)
		err = binary.Read(r, binary.BigEndian, &tf)
		if err != nil {
			return nil, err
		}
		if tf == 1 {
			b.nodes[i].Inactive = true
		}
		err = binary.Read(r, binary.BigEndian, &b.nodes[i].Capacity)
		if err != nil {
			return nil, err
		}
		var vvint32 int32
		err = binary.Read(r, binary.BigEndian, &vvint32)
		if err != nil {
			return nil, err
		}
		b.nodes[i].TierValues = make([]int32, vvint32)
		for j := int32(0); j < vvint32; j++ {
			err = binary.Read(r, binary.BigEndian, &b.nodes[i].TierValues[j])
			if err != nil {
				return nil, err
			}
		}
		err = binary.Read(r, binary.BigEndian, &vvint32)
		if err != nil {
			return nil, err
		}
		byts := make([]byte, vvint32)
		_, err = io.ReadFull(r, byts)
		if err != nil {
			return nil, err
		}
		b.nodes[i].Address = string(byts)
		err = binary.Read(r, binary.BigEndian, &vvint32)
		if err != nil {
			return nil, err
		}
		byts = make([]byte, vvint32)
		_, err = io.ReadFull(r, byts)
		if err != nil {
			return nil, err
		}
		b.nodes[i].Meta = string(byts)
	}
	err = binary.Read(r, binary.BigEndian, &b.partitionBitCount)
	if err != nil {
		return nil, err
	}
	err = binary.Read(r, binary.BigEndian, &vint32)
	if err != nil {
		return nil, err
	}
	b.replicaToPartitionToNodeIndex = make([][]int32, vint32)
	for i := int32(0); i < vint32; i++ {
		var vvint32 int32
		err = binary.Read(r, binary.BigEndian, &vvint32)
		if err != nil {
			return nil, err
		}
		b.replicaToPartitionToNodeIndex[i] = make([]int32, vvint32)
		err = binary.Read(r, binary.BigEndian, b.replicaToPartitionToNodeIndex[i])
	}
	err = binary.Read(r, binary.BigEndian, &vint32)
	if err != nil {
		return nil, err
	}
	b.replicaToPartitionToLastMove = make([][]uint16, vint32)
	for i := int32(0); i < vint32; i++ {
		var vvint32 int32
		err = binary.Read(r, binary.BigEndian, &vvint32)
		if err != nil {
			return nil, err
		}
		b.replicaToPartitionToLastMove[i] = make([]uint16, vvint32)
		err = binary.Read(r, binary.BigEndian, b.replicaToPartitionToLastMove[i])
	}
	err = binary.Read(r, binary.BigEndian, &b.pointsAllowed)
	if err != nil {
		return nil, err
	}
	err = binary.Read(r, binary.BigEndian, &b.maxPartitionBitCount)
	if err != nil {
		return nil, err
	}
	err = binary.Read(r, binary.BigEndian, &b.moveWait)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (b *Builder) Persist(w io.Writer) error {
	_, err := w.Write([]byte("RINGBUILDERv0001"))
	if err != nil {
		return err
	}
	err = binary.Write(w, binary.BigEndian, b.version)
	if err != nil {
		return err
	}
	err = binary.Write(w, binary.BigEndian, int32(len(b.nodes)))
	if err != nil {
		return err
	}
	for _, node := range b.nodes {
		err = binary.Write(w, binary.BigEndian, node.ID)
		if err != nil {
			return err
		}
		tf := byte(0)
		if node.Inactive {
			tf = 1
		}
		err = binary.Write(w, binary.BigEndian, tf)
		if err != nil {
			return err
		}
		err = binary.Write(w, binary.BigEndian, node.Capacity)
		if err != nil {
			return err
		}
		err = binary.Write(w, binary.BigEndian, int32(len(node.TierValues)))
		if err != nil {
			return err
		}
		for v := range node.TierValues {
			err = binary.Write(w, binary.BigEndian, v)
			if err != nil {
				return err
			}
		}
		b := []byte(node.Address)
		err = binary.Write(w, binary.BigEndian, int32(len(b)))
		if err != nil {
			return err
		}
		_, err = w.Write(b)
		if err != nil {
			return err
		}
		b = []byte(node.Meta)
		err = binary.Write(w, binary.BigEndian, int32(len(b)))
		if err != nil {
			return err
		}
		_, err = w.Write(b)
		if err != nil {
			return err
		}
	}
	err = binary.Write(w, binary.BigEndian, b.partitionBitCount)
	if err != nil {
		return err
	}
	err = binary.Write(w, binary.BigEndian, int32(len(b.replicaToPartitionToNodeIndex)))
	if err != nil {
		return err
	}
	for _, partitionToNodeIndex := range b.replicaToPartitionToNodeIndex {
		err = binary.Write(w, binary.BigEndian, int32(len(partitionToNodeIndex)))
		if err != nil {
			return err
		}
		err = binary.Write(w, binary.BigEndian, partitionToNodeIndex)
		if err != nil {
			return err
		}
	}
	err = binary.Write(w, binary.BigEndian, int32(len(b.replicaToPartitionToLastMove)))
	if err != nil {
		return err
	}
	for _, partitionToLastMove := range b.replicaToPartitionToLastMove {
		err = binary.Write(w, binary.BigEndian, int32(len(partitionToLastMove)))
		if err != nil {
			return err
		}
		err = binary.Write(w, binary.BigEndian, partitionToLastMove)
		if err != nil {
			return err
		}
	}
	err = binary.Write(w, binary.BigEndian, b.pointsAllowed)
	if err != nil {
		return err
	}
	err = binary.Write(w, binary.BigEndian, b.maxPartitionBitCount)
	if err != nil {
		return err
	}
	err = binary.Write(w, binary.BigEndian, b.moveWait)
	if err != nil {
		return err
	}
	return nil
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

func (b *Builder) PretendMoveElapsed(minutes uint16) {
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

func (b *Builder) Add(n *Node) {
	b.nodes = append(b.nodes, n)
}

// Remove will remove the node from the list of nodes for this builder/ring.
// Note that this can be relatively expensive as all nodes that had been added
// after the removed node had been originally added will have their internal
// indexes shifted down one and all the replica-to-partition-to-node indexing
// will have to be updated, as well as clearing any assignments that were to
// the removed node. Normally it is better to just leave a "dead" node in place
// and simply set it as inactive.
func (b *Builder) Remove(nodeID uint64) {
	for i, node := range b.nodes {
		if node.ID == nodeID {
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

func (b *Builder) Node(nodeID uint64) *Node {
	for _, node := range b.nodes {
		if node.ID == nodeID {
			return node
		}
	}
	return nil
}

// Ring returns a Ring instance of the data defined by the builder. This will
// cause any pending rebalancing actions to be performed. The Ring returned
// will be immutable; to obtain updated ring data, Ring() must be called again.
// The localNodeID is so the Ring instance can provide local responsibility
// information; you can give 0 if you don't intend to use those features.
func (b *Builder) Ring(localNodeID uint64) *Ring {
	originalVersion := b.version
	if b.resizeIfNeeded() {
		b.version = time.Now().UnixNano()
	}
	if newRebalancer(b).rebalance() {
		b.version = time.Now().UnixNano()
	}
	if b.version != originalVersion {
		d := (b.version - originalVersion) / 6000000000 // minutes
		if d > 0 {
			d16 := uint16(0)
			if d < math.MaxUint16 {
				d16 = uint16(d)
			}
			b.PretendMoveElapsed(d16)
		}
	}
	localNodeIndex := int32(-1)
	nodes := make([]*Node, len(b.nodes))
	copy(nodes, b.nodes)
	for i, node := range nodes {
		if node.ID == localNodeID {
			localNodeIndex = int32(i)
		}
	}
	replicaToPartitionToNodeIndex := make([][]int32, len(b.replicaToPartitionToNodeIndex))
	for i := 0; i < len(replicaToPartitionToNodeIndex); i++ {
		replicaToPartitionToNodeIndex[i] = make([]int32, len(b.replicaToPartitionToNodeIndex[i]))
		copy(replicaToPartitionToNodeIndex[i], b.replicaToPartitionToNodeIndex[i])
	}
	return &Ring{
		version:           b.version,
		localNodeIndex:    localNodeIndex,
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
	for _, node := range b.nodes {
		if !node.Inactive {
			totalCapacity += (uint64)(node.Capacity)
		}
	}
	partitionCount := len(b.replicaToPartitionToNodeIndex[0])
	partitionBitCount := b.partitionBitCount
	pointsAllowed := float64(b.pointsAllowed) * 0.01
	for _, node := range b.nodes {
		if node.Inactive {
			continue
		}
		desiredPartitionCount := float64(partitionCount) * float64(replicaCount) * (float64(node.Capacity) / float64(totalCapacity))
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
	// TODO: Shrinking the partitionToNodeIndex slices doesn't happen because
	// it would normally cause more data movements than it's worth. Perhaps in
	// the future we can add detection of cases when shrinking makes sense.
	return false
}
