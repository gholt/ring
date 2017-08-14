package ring

type BuilderGroup struct {
	builder *Builder
	index   int
	info    string
}

func (g *BuilderGroup) Info() string {
	return g.info
}

func (g *BuilderGroup) SetInfo(v string) {
	g.info = v
}

func (g *BuilderGroup) Parent() *BuilderGroup {
	parent := g.builder.ring.GroupToGroup[g.index]
	if parent == 0 {
		return nil
	}
	return g.builder.groups[parent]
}

func (g *BuilderGroup) SetParent(v *BuilderGroup) {
	g.builder.ring.GroupToGroup[g.index] = v.index
}

func (g *BuilderGroup) Nodes() []*BuilderNode {
	nodes := []*BuilderNode{}
	for node, group := range g.builder.ring.NodeToGroup {
		if group == g.index {
			nodes = append(nodes, g.builder.nodes[node])
		}
	}
	return nodes
}

func (g *BuilderGroup) AddNode(info string, capacity int) *BuilderNode {
	return g.builder.AddNode(info, capacity, g)
}

func (g *BuilderGroup) Groups() []*BuilderGroup {
	groups := []*BuilderGroup{}
	for child, parent := range g.builder.ring.GroupToGroup {
		if parent == g.index {
			groups = append(groups, g.builder.groups[child])
		}
	}
	return groups
}

func (g *BuilderGroup) AddGroup(info string) *BuilderGroup {
	return g.builder.AddGroup(info, g)
}
