package lowring

import (
	"math/rand"
	"sort"
	"testing"
)

func TestRebalancerBasic(t *testing.T) {
	// 128 node, 3 replica ring should end up with 128 partitions and each node
	// assigned 3 replicas.
	b := &Builder{rnd: rand.New(rand.NewSource(0))}
	b.SetReplicaCount(3)
	for i := 0; i < 128; i++ {
		b.Nodes = append(b.Nodes, &Node{Capacity: 1})
	}
	b.growIfNeeded()
	rb := newRebalancer(b)
	rb.rebalance()
	counts := make([]int, 128)
	for _, partitionToNodeIndex := range b.Ring {
		for _, nodeIndex := range partitionToNodeIndex {
			counts[nodeIndex]++
		}
	}
	for i, count := range counts {
		if count != 3 {
			t.Fatal(i, count)
		}
	}
}

func TestRebalancerTier0(t *testing.T) {
	// 128 nodes with 16 evenly distributed tiers, 4 replicas should end up
	// with 32 partitions, 8 assignments per tier, and 1 assignment per node.
	// Also, no partition should have replicas in the same tier.
	b := &Builder{rnd: rand.New(rand.NewSource(0))}
	b.SetReplicaCount(4)
	for i := 0; i < 128; i++ {
		b.Nodes = append(b.Nodes, &Node{Capacity: 1, TierIndexes: []int{i % 16}})
	}
	b.growIfNeeded()
	rb := newRebalancer(b)
	rb.rebalance()
	counts := make([]int, 128)
	tiers := make(map[int]int)
	for _, partitionToNodeIndex := range b.Ring {
		for _, nodeIndex := range partitionToNodeIndex {
			counts[nodeIndex]++
			tiers[b.Nodes[nodeIndex].TierIndexes[0]]++
		}
	}
	for tier, count := range tiers {
		if count != 8 {
			t.Fatal(tier, count)
		}
	}
	for i, count := range counts {
		if count != 1 {
			t.Fatal(i, count)
		}
	}
	for p := 0; p < len(b.Ring[0]); p++ {
		tiers2 := make(map[int]bool)
		for _, partitionToNodeIndex := range b.Ring {
			tier := b.Nodes[partitionToNodeIndex[p]].TierIndexes[0]
			if _, ok := tiers2[tier]; ok {
				t.Fatal(tier)
			}
			tiers2[tier] = true
		}
	}
}

func TestRebalancerTier0b(t *testing.T) {
	// Same as TestRebalancerTier0 but give tier0 one of tier15's nodes.
	b := &Builder{rnd: rand.New(rand.NewSource(0))}
	b.SetReplicaCount(4)
	for i := 0; i < 127; i++ {
		b.Nodes = append(b.Nodes, &Node{Capacity: 1, TierIndexes: []int{i % 16}})
	}
	b.Nodes = append(b.Nodes, &Node{Capacity: 1, TierIndexes: []int{0}})
	b.growIfNeeded()
	rb := newRebalancer(b)
	rb.rebalance()
	counts := make([]int, 128)
	tiers := make(map[int]int)
	for _, partitionToNodeIndex := range b.Ring {
		for _, nodeIndex := range partitionToNodeIndex {
			counts[nodeIndex]++
			tiers[b.Nodes[nodeIndex].TierIndexes[0]]++
		}
	}
	for tier, count := range tiers {
		if tier == 0 {
			if count != 9 {
				t.Fatal(tier, count)
			}
		} else if tier == 15 {
			if count != 7 {
				t.Fatal(tier, count)
			}
		} else if count != 8 {
			t.Fatal(tier, count)
		}
	}
	for i, count := range counts {
		if count != 1 {
			t.Fatal(i, count)
		}
	}
	for p := 0; p < len(b.Ring[0]); p++ {
		tiers2 := make(map[int]bool)
		for _, partitionToNodeIndex := range b.Ring {
			tier := b.Nodes[partitionToNodeIndex[p]].TierIndexes[0]
			if _, ok := tiers2[tier]; ok {
				t.Fatal(tier)
			}
			tiers2[tier] = true
		}
	}
}

func TestRebalancerTier0c(t *testing.T) {
	// A bit more extreme than TestRebalancerTier0b; giving tier0 a lot more
	// nodes than all the others. Here, the per node balance will suffer but
	// the tier distinctions should remain.
	b := &Builder{rnd: rand.New(rand.NewSource(0))}
	b.SetReplicaCount(4)
	for i := 0; i < 96; i++ {
		b.Nodes = append(b.Nodes, &Node{Capacity: 1, TierIndexes: []int{i % 16}})
	}
	for i := 0; i < 32; i++ {
		b.Nodes = append(b.Nodes, &Node{Capacity: 1, TierIndexes: []int{0}})
	}
	b.growIfNeeded()
	rb := newRebalancer(b)
	rb.rebalance()
	counts := make([]int, 128)
	tiers := make(map[int]int)
	for _, partitionToNodeIndex := range b.Ring {
		for _, nodeIndex := range partitionToNodeIndex {
			counts[nodeIndex]++
			tiers[b.Nodes[nodeIndex].TierIndexes[0]]++
		}
	}
	for tier, count := range tiers {
		if tier == 0 {
			if count != 32 {
				t.Fatal(tier, count)
			}
		} else if count < 6 || count > 7 {
			t.Fatal(tier, count, tiers)
		}
	}
	countCounts := make([]int, 3)
	for i, count := range counts {
		if count > 2 {
			t.Fatal(i, count)
		}
		countCounts[count]++
	}
	if countCounts[0] != 6 {
		t.Fatal(countCounts[0])
	}
	if countCounts[1] != 116 {
		t.Fatal(countCounts[1])
	}
	if countCounts[2] != 6 {
		t.Fatal(countCounts[2])
	}
	for p := 0; p < len(b.Ring[0]); p++ {
		tiers2 := make(map[int]bool)
		for _, partitionToNodeIndex := range b.Ring {
			tier := b.Nodes[partitionToNodeIndex[p]].TierIndexes[0]
			if _, ok := tiers2[tier]; ok {
				t.Fatal(tier)
			}
			tiers2[tier] = true
		}
	}
}

func TestRebalancerTier1(t *testing.T) {
	// 128 nodes with 32 evenly distributed tier0s and 16 tier1s, 4 replicas
	// should end up with 32 partitions, 4 assignments per tier0, 8 assignments
	// per tier1, and 1 assignment per node. Also, no partition should have
	// replicas in the same tier.
	b := &Builder{rnd: rand.New(rand.NewSource(0))}
	b.SetReplicaCount(4)
	for i := 0; i < 128; i++ {
		b.Nodes = append(b.Nodes, &Node{Capacity: 1, TierIndexes: []int{i % 32, i % 16}})
	}
	b.growIfNeeded()
	rb := newRebalancer(b)
	rb.rebalance()
	counts := make([]int, 128)
	tier0s := make(map[int]int)
	tier1s := make(map[int]int)
	for _, partitionToNodeIndex := range b.Ring {
		for _, nodeIndex := range partitionToNodeIndex {
			counts[nodeIndex]++
			tier0s[b.Nodes[nodeIndex].TierIndexes[0]]++
			tier1s[b.Nodes[nodeIndex].TierIndexes[1]]++
		}
	}
	for tier, count := range tier0s {
		if count != 4 {
			t.Fatal(tier, count)
		}
	}
	for tier, count := range tier1s {
		if count != 8 {
			t.Fatal(tier, count)
		}
	}
	for i, count := range counts {
		if count != 1 {
			t.Fatal(i, count)
		}
	}
	for p := 0; p < len(b.Ring[0]); p++ {
		tier0s2 := make(map[int]bool)
		tier1s2 := make(map[int]bool)
		for _, partitionToNodeIndex := range b.Ring {
			tier := b.Nodes[partitionToNodeIndex[p]].TierIndexes[0]
			if _, ok := tier0s2[tier]; ok {
				t.Fatal(tier)
			}
			tier0s2[tier] = true
			tier = b.Nodes[partitionToNodeIndex[p]].TierIndexes[1]
			if _, ok := tier1s2[tier]; ok {
				t.Fatal(tier)
			}
			tier1s2[tier] = true
		}
	}
}

func TestRebalancerTier1b(t *testing.T) {
	// Similar to TestRebalancerTier1 but give tier0-0,tier1-0 a lot more
	// nodes. Assert that balancing suffers but tier distinctions are kept.
	b := &Builder{rnd: rand.New(rand.NewSource(0))}
	b.SetReplicaCount(4)
	for i := 0; i < 96; i++ {
		b.Nodes = append(b.Nodes, &Node{Capacity: 1, TierIndexes: []int{i % 32, i % 16}})
	}
	for i := 0; i < 32; i++ {
		b.Nodes = append(b.Nodes, &Node{Capacity: 1, TierIndexes: []int{0, 0}})
	}
	b.growIfNeeded()
	rb := newRebalancer(b)
	rb.rebalance()
	counts := make([]int, 128)
	tier0s := make(map[int]int)
	tier1s := make(map[int]int)
	for _, partitionToNodeIndex := range b.Ring {
		for _, nodeIndex := range partitionToNodeIndex {
			counts[nodeIndex]++
			tier0s[b.Nodes[nodeIndex].TierIndexes[0]]++
			tier1s[b.Nodes[nodeIndex].TierIndexes[1]]++
		}
	}
	for tier, count := range tier0s {
		if tier == 0 {
			if count != 29 {
				t.Fatal(tier, count, tier0s)
			}
		} else if count < 3 || count > 4 {
			t.Fatal(tier, count, tier0s)
		}
	}
	for tier, count := range tier1s {
		if tier == 0 {
			if count != 32 {
				t.Fatal(tier, count)
			}
		} else if count > 12 {
			t.Fatal(tier, count)
		}
	}
	for i, count := range counts {
		if count > 2 {
			t.Fatal(i, count)
		}
	}
	for p := 0; p < len(b.Ring[0]); p++ {
		tier0s2 := make(map[int]bool)
		tier1s2 := make(map[int]bool)
		for _, partitionToNodeIndex := range b.Ring {
			tier := b.Nodes[partitionToNodeIndex[p]].TierIndexes[0]
			if _, ok := tier0s2[tier]; ok {
				t.Fatal(tier)
			}
			tier0s2[tier] = true
			tier = b.Nodes[partitionToNodeIndex[p]].TierIndexes[1]
			if _, ok := tier1s2[tier]; ok {
				t.Fatal(tier)
			}
			tier1s2[tier] = true
		}
	}
}

func TestNodeIndexByDesireSorter(t *testing.T) {
	nodeIndexes := []NodeIndexType{0, 1, 2, 3, 4}
	nodeIndexToDesire := []int32{10, 5, 8, 20, 3}
	sort.Sort(&nodeIndexByDesireSorter{
		nodeIndexes:       nodeIndexes,
		nodeIndexToDesire: nodeIndexToDesire,
	})
	if nodeIndexes[0] != 3 ||
		nodeIndexes[1] != 0 ||
		nodeIndexes[2] != 2 ||
		nodeIndexes[3] != 1 ||
		nodeIndexes[4] != 4 {
		t.Fatalf("nodeIndexByDesireSorter resulted in %v instead of [3 0 2 1 4]", nodeIndexes)
	}
}
