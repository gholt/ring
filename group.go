package ring

type Group struct {
	ring  *Ring
	index int
	info  string
}

func (g *Group) Info() string {
	return g.info
}

func (g *Group) Parent() *Group {
	parent := g.ring.groupToGroup[g.index]
	if parent == 0 {
		return nil
	}
	return g.ring.groups[parent]
}

func (g *Group) Nodes() []*Node {
	nodes := []*Node{}
	for node, group := range g.ring.nodeToGroup {
		if group == g.index {
			nodes = append(nodes, g.ring.nodes[node])
		}
	}
	return nodes
}

func (g *Group) Groups() []*Group {
	groups := []*Group{}
	for child, parent := range g.ring.groupToGroup {
		if parent == g.index {
			groups = append(groups, g.ring.groups[child])
		}
	}
	return groups
}
