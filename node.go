package ring

import "github.com/gholt/ring/lowring"

// Node is a target of a ring assignment; it is the immutable version of the
// BuilderNode for use with the immutable Ring.
type Node interface {

	// Disabled is whether this node can currently be assigned replicas.
	// See Capacity below for a bit more information.
	Disabled() bool

	// Capacity indicates how many replicas this node should be assigned; it is
	// relative to other nodes' capacities.
	//
	// Values less than 1 are treated as 0 and indicate the node should not
	// have any assignments, but if it already has some and they cannot yet be
	// moved due to some restrictions, that is okay for now.
	//
	// 0 capacity differs from Disabled in that Disabled means that no
	// assignments can be made, overriding the restrictions.
	//
	// 0 capacity might be used to "drain" a node for maintenance once enough
	// rebalances clear it of any assignments.
	Capacity() int

	// Tiers specify which tiers a Node is within.
	//
	// Tiers indicate the layout of the node with respect to other nodes. For
	// example, the lowest tier, tier 0, might be the server (where each node
	// represents a drive on that server). The next tier, 1, might then be the
	// power zone the server is in. The number of tiers is flexible, so later
	// an additional tier for geographic region could be added.
	//
	// Different replicas of a given partition are attempted to be assigned to
	// distinct tiers at each level.
	//
	// If a tier does not exist for a node (but exists for some other node) it
	// should be assumed to be an empty string.
	Tiers() []string

	// Tier returns a single tier's string, identical to Tiers()[t] except that
	// no []string is allocated and bounds are checked.
	Tier(t int) string

	// Info returns the info string for a node. This string may be anything and
	// is user dependent.
	Info() string
}

type node struct {
	disabled bool
	capacity int
	tiers    []string
	info     string
}

func (n *node) Disabled() bool {
	return n.disabled
}

func (n *node) Capacity() int {
	return n.capacity
}

func (n *node) Tiers() []string {
	t := make([]string, len(n.tiers))
	copy(t, n.tiers)
	return t
}

func (n *node) Tier(t int) string {
	if t < 0 || t >= len(n.tiers) {
		return ""
	}
	return n.tiers[t]
}

func (n *node) Info() string {
	return n.info
}

// BuilderNode is a target of a ring assignment. For example, it might
// represent a disk drive in a large storage cluster.
type BuilderNode interface {
	Node

	// SetDisabled determines whether this node can currently be assigned
	// replicas. See Capacity below for a bit more information.
	SetDisabled(v bool)

	// SetCapacity indicates how many replicas this node should be assigned; it
	// is relative to other nodes' capacities.
	//
	// Values less than 1 are treated as 0 and indicate the node should not
	// have any assignments, but if it already has some and they cannot yet be
	// moved due to some restrictions, that is okay for now.
	//
	// 0 capacity differs from Disabled in that Disabled means that no
	// assignments can be made, overriding the restrictions.
	//
	// 0 capacity might be used to "drain" a node for maintenance once enough
	// rebalances clear it of any assignments.
	SetCapacity(v int)

	// SetTiers specify which tiers a Node is within.
	//
	// Tiers indicate the layout of the node with respect to other nodes. For
	// example, the lowest tier, tier 0, might be the server (where each node
	// represents a drive on that server). The next tier, 1, might then be the
	// power zone the server is in. The number of tiers is flexible, so later
	// an additional tier for geographic region could be added.
	//
	// Different replicas of a given partition are attempted to be assigned to
	// distinct tiers at each level.
	//
	// If a tier does not exist for a node (but exists for some other node) it
	// will be assumed to be an empty string.
	SetTiers(v []string)

	// SetTier defines a single tier's string.
	SetTier(t int, v string)

	// SetInfo sets the info string for a node. This string may be anything and
	// is user dependent.
	SetInfo(v string)
}

type builderNode struct {
	builder *builder
	node    lowring.Node
	info    string
}

func (n *builderNode) Builder() Builder {
	return n.builder
}

func (n *builderNode) Disabled() bool {
	return n.node.Disabled
}

func (n *builderNode) SetDisabled(v bool) {
	n.node.Disabled = v
}

func (n *builderNode) Capacity() int {
	return n.node.Capacity
}

func (n *builderNode) SetCapacity(v int) {
	n.node.Capacity = v
}

func (n *builderNode) Tiers() []string {
	tiers := make([]string, len(n.node.TierIndexes))
	for i, ti := range n.node.TierIndexes {
		tiers[i] = n.builder.tierIndexToName[ti]
	}
	return tiers
}

func (n *builderNode) SetTiers(v []string) {
	n.node.TierIndexes = make([]int, len(v))
	for i, tn := range v {
		ti, ok := n.builder.tierNameToIndex[tn]
		if !ok {
			ti = len(n.builder.tierIndexToName)
			n.builder.tierIndexToName = append(n.builder.tierIndexToName, tn)
			n.builder.tierNameToIndex[tn] = ti
		}
		n.node.TierIndexes[i] = ti
	}
}

func (n *builderNode) Tier(t int) string {
	if t < 0 || t >= len(n.builder.tierIndexToName) {
		return ""
	}
	return n.builder.tierIndexToName[t]
}

func (n *builderNode) SetTier(t int, v string) {
	if t < 0 {
		return
	}
	for t >= len(n.node.TierIndexes) {
		n.node.TierIndexes = append(n.node.TierIndexes, 0)
	}
	ti, ok := n.builder.tierNameToIndex[v]
	if !ok {
		ti = len(n.builder.tierIndexToName)
		n.builder.tierIndexToName = append(n.builder.tierIndexToName, v)
		n.builder.tierNameToIndex[v] = ti
	}
	n.node.TierIndexes[t] = ti
}

func (n *builderNode) Info() string {
	return n.info
}

func (n *builderNode) SetInfo(v string) {
	n.info = v
}
