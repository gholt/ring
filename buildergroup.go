package ring

// BuilderGroup is a group within a builder; a group is a collection of nodes,
// and perhaps other groups.
type BuilderGroup struct {
	builder *Builder
	index   int
	info    string
}

// Info returns the associated info string for the group; this is user-defined
// and not used directly by the builder.
func (g *BuilderGroup) Info() string {
	return g.info
}

// SetInfo sets the associated info string for the group; this is user-defined
// and not used directly by the builder.
func (g *BuilderGroup) SetInfo(v string) {
	g.info = v
}

// Parent returns the parent group, or nil if there is no parent, of the group.
func (g *BuilderGroup) Parent() *BuilderGroup {
	parent := g.builder.ring.GroupToGroup[g.index]
	if parent == 0 {
		return nil
	}
	return g.builder.groups[parent]
}

// SetParent sets the parent group of this group; it may be nil to indicate
// this group is a top-level group.
func (g *BuilderGroup) SetParent(v *BuilderGroup) {
	g.builder.ring.GroupToGroup[g.index] = v.index
}

// Nodes returns the slice of nodes within this group and all its child groups.
func (g *BuilderGroup) Nodes() []*BuilderNode {
	nodes := []*BuilderNode{}
	for node, group := range g.builder.ring.NodeToGroup {
		if group == g.index {
			nodes = append(nodes, g.builder.nodes[node])
		}
	}
	return nodes
}

// AddNode will create a new node in the associated builder with this group set
// as the node's parent. Info is a user-defined string and is not used directly
// by the builder. Capacity specifies, relative to other nodes, how many
// assignments the node should have.
func (g *BuilderGroup) AddNode(info string, capacity int) *BuilderNode {
	return g.builder.AddNode(info, capacity, g)
}

// Groups returns the slice of groups that are the direct children of this
// group.
func (g *BuilderGroup) Groups() []*BuilderGroup {
	groups := []*BuilderGroup{}
	for child, parent := range g.builder.ring.GroupToGroup {
		if parent == g.index {
			groups = append(groups, g.builder.groups[child])
		}
	}
	return groups
}

// AddGroup adds a new group to the associated builder with this group set as
// the new group's parent. Info is a user-defined string and is not used
// directly by the builder.
func (g *BuilderGroup) AddGroup(info string) *BuilderGroup {
	return g.builder.AddGroup(info, g)
}
