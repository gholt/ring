package ring

var PRETEND_BUILDER *Builder

type pretendNode struct {
	id         uint64
	active   bool
	capacity   uint32
	tierValues []int
	address    string
}

func newPretendNode(id uint64, address string) *pretendNode {
    return &pretendNode{id:id, active:true,capacity:1,address:address}
}

func (node *pretendNode) NodeID() uint64 {
	return node.id
}

func (node *pretendNode) Active() bool {
	return node.active
}

func (node *pretendNode) Capacity() uint32 {
	return node.capacity
}

func (node *pretendNode) TierValues() []int {
	return node.tierValues
}

func (node *pretendNode) Address() string {
	return node.address
}

func init() {
    PRETEND_BUILDER = NewBuilder(3)
    PRETEND_BUILDER.Add(newPretendNode(1, "1.2.3.4:21212"))
    PRETEND_BUILDER.Add(newPretendNode(2, "5.6.7.8:21212"))
    PRETEND_BUILDER.Add(newPretendNode(3, "9.10.11.12:21211"))
    // Example to get a ring with the local node bound to the first node:
    // ring := PRETEND_BUILDER.Ring(1)
}
