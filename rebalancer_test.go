package ring

import (
	"fmt"
	"testing"
)

func TestRebalancerBasic(t *testing.T) {
	// 128 node, 3 replica ring should end up with 128 partitions and each node
	// assigned 3 replicas.
	b := NewBuilder()
	b.SetReplicaCount(3)
	for i := 0; i < 128; i++ {
		b.AddNode(true, 1, nil, nil, "")
	}
	b.resizeIfNeeded()
	rb := newRebalancer(b)
	rb.rebalance()
	counts := make([]int, 128)
	for _, partitionToNodeIndex := range b.replicaToPartitionToNodeIndex {
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
	b := NewBuilder()
	b.SetReplicaCount(4)
	for i := 0; i < 128; i++ {
		b.AddNode(true, 1, []string{fmt.Sprintf("tier%d", i%16)}, nil, "")
	}
	b.resizeIfNeeded()
	rb := newRebalancer(b)
	rb.rebalance()
	counts := make([]int, 128)
	tiers := make(map[string]int)
	for _, partitionToNodeIndex := range b.replicaToPartitionToNodeIndex {
		for _, nodeIndex := range partitionToNodeIndex {
			counts[nodeIndex]++
			tiers[b.nodes[nodeIndex].Tier(0)]++
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
	for p := 0; p < len(b.replicaToPartitionToNodeIndex[0]); p++ {
		tiers := make(map[string]bool)
		for _, partitionToNodeIndex := range b.replicaToPartitionToNodeIndex {
			tier := b.nodes[partitionToNodeIndex[p]].Tier(0)
			if _, ok := tiers[tier]; ok {
				t.Fatal(tier)
			}
			tiers[tier] = true
		}
	}
}

func TestRebalancerTier0b(t *testing.T) {
	// Same as TestRebalancerTier0 but give tier0 one of tier15's nodes.
	b := NewBuilder()
	b.SetReplicaCount(4)
	for i := 0; i < 127; i++ {
		b.AddNode(true, 1, []string{fmt.Sprintf("tier%d", i%16)}, nil, "")
	}
	b.AddNode(true, 1, []string{"tier0"}, nil, "")
	b.resizeIfNeeded()
	rb := newRebalancer(b)
	rb.rebalance()
	counts := make([]int, 128)
	tiers := make(map[string]int)
	for _, partitionToNodeIndex := range b.replicaToPartitionToNodeIndex {
		for _, nodeIndex := range partitionToNodeIndex {
			counts[nodeIndex]++
			tiers[b.nodes[nodeIndex].Tier(0)]++
		}
	}
	for tier, count := range tiers {
		if tier == "tier0" {
			if count != 9 {
				t.Fatal(tier, count)
			}
		} else if tier == "tier15" {
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
	for p := 0; p < len(b.replicaToPartitionToNodeIndex[0]); p++ {
		tiers := make(map[string]bool)
		for _, partitionToNodeIndex := range b.replicaToPartitionToNodeIndex {
			tier := b.nodes[partitionToNodeIndex[p]].Tier(0)
			if _, ok := tiers[tier]; ok {
				t.Fatal(tier)
			}
			tiers[tier] = true
		}
	}
}

func TestRebalancerTier0c(t *testing.T) {
	// A bit more extreme than TestRebalancerTier0b; giving tier0 a lot more
	// nodes than all the others. Here, the per node balance will suffer but
	// the tier distinctions should remain.
	b := NewBuilder()
	b.SetReplicaCount(4)
	for i := 0; i < 96; i++ {
		b.AddNode(true, 1, []string{fmt.Sprintf("tier%d", i%16)}, nil, "")
	}
	for i := 0; i < 32; i++ {
		b.AddNode(true, 1, []string{"tier0"}, nil, "")
	}
	b.resizeIfNeeded()
	rb := newRebalancer(b)
	rb.rebalance()
	counts := make([]int, 128)
	tiers := make(map[string]int)
	for _, partitionToNodeIndex := range b.replicaToPartitionToNodeIndex {
		for _, nodeIndex := range partitionToNodeIndex {
			counts[nodeIndex]++
			tiers[b.nodes[nodeIndex].Tier(0)]++
		}
	}
	for tier, count := range tiers {
		if tier == "tier0" {
			if count != 32 {
				t.Fatal(tier, count)
			}
		} else if tier == "tier1" {
			if count != 12 {
				t.Fatal(tier, count)
			}
		} else if count != 6 {
			t.Fatal(tier, count)
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
	for p := 0; p < len(b.replicaToPartitionToNodeIndex[0]); p++ {
		tiers := make(map[string]bool)
		for _, partitionToNodeIndex := range b.replicaToPartitionToNodeIndex {
			tier := b.nodes[partitionToNodeIndex[p]].Tier(0)
			if _, ok := tiers[tier]; ok {
				t.Fatal(tier)
			}
			tiers[tier] = true
		}
	}
}

func TestRebalancerTier1(t *testing.T) {
	// 128 nodes with 32 evenly distributed tier0s and 16 tier1s, 4 replicas
	// should end up with 32 partitions, 4 assignments per tier0, 8 assignments
	// per tier1, and 1 assignment per node. Also, no partition should have
	// replicas in the same tier.
	b := NewBuilder()
	b.SetReplicaCount(4)
	for i := 0; i < 128; i++ {
		b.AddNode(true, 1, []string{fmt.Sprintf("tier0-%d", i%32), fmt.Sprintf("tier1-%d", i%16)}, nil, "")
	}
	b.resizeIfNeeded()
	rb := newRebalancer(b)
	rb.rebalance()
	counts := make([]int, 128)
	tier0s := make(map[string]int)
	tier1s := make(map[string]int)
	for _, partitionToNodeIndex := range b.replicaToPartitionToNodeIndex {
		for _, nodeIndex := range partitionToNodeIndex {
			counts[nodeIndex]++
			tier0s[b.nodes[nodeIndex].Tier(0)]++
			tier1s[b.nodes[nodeIndex].Tier(1)]++
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
	for p := 0; p < len(b.replicaToPartitionToNodeIndex[0]); p++ {
		tier0s := make(map[string]bool)
		tier1s := make(map[string]bool)
		for _, partitionToNodeIndex := range b.replicaToPartitionToNodeIndex {
			tier := b.nodes[partitionToNodeIndex[p]].Tier(0)
			if _, ok := tier0s[tier]; ok {
				t.Fatal(tier)
			}
			tier0s[tier] = true
			tier = b.nodes[partitionToNodeIndex[p]].Tier(1)
			if _, ok := tier1s[tier]; ok {
				t.Fatal(tier)
			}
			tier1s[tier] = true
		}
	}
}

func TestRebalancerTier1b(t *testing.T) {
	// Similar to TestRebalancerTier1 but give tier0-0,tier1-0 a lot more
	// nodes. Assert that balancing suffers but tier distinctions are kept.
	b := NewBuilder()
	b.SetReplicaCount(4)
	for i := 0; i < 96; i++ {
		b.AddNode(true, 1, []string{fmt.Sprintf("tier0-%d", i%32), fmt.Sprintf("tier1-%d", i%16)}, nil, "")
	}
	for i := 0; i < 32; i++ {
		b.AddNode(true, 1, []string{"tier0-0", "tier1-0"}, nil, "")
	}
	b.resizeIfNeeded()
	rb := newRebalancer(b)
	rb.rebalance()
	counts := make([]int, 128)
	tier0s := make(map[string]int)
	tier1s := make(map[string]int)
	for _, partitionToNodeIndex := range b.replicaToPartitionToNodeIndex {
		for _, nodeIndex := range partitionToNodeIndex {
			counts[nodeIndex]++
			tier0s[b.nodes[nodeIndex].Tier(0)]++
			tier1s[b.nodes[nodeIndex].Tier(1)]++
		}
	}
	for tier, count := range tier0s {
		if tier == "tier0-0" {
			if count != 32 {
				t.Fatal(tier, count)
			}
		} else if count > 6 {
			t.Fatal(tier, count)
		}
	}
	for tier, count := range tier1s {
		if tier == "tier1-0" {
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
	for p := 0; p < len(b.replicaToPartitionToNodeIndex[0]); p++ {
		tier0s := make(map[string]bool)
		tier1s := make(map[string]bool)
		for _, partitionToNodeIndex := range b.replicaToPartitionToNodeIndex {
			tier := b.nodes[partitionToNodeIndex[p]].Tier(0)
			if _, ok := tier0s[tier]; ok {
				t.Fatal(tier)
			}
			tier0s[tier] = true
			tier = b.nodes[partitionToNodeIndex[p]].Tier(1)
			if _, ok := tier1s[tier]; ok {
				t.Fatal(tier)
			}
			tier1s[tier] = true
		}
	}
}
