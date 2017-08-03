package ring

import "testing"

func TestBuilderReflections(t *testing.T) {
	b := &Builder{}
	zones := 5
	servers := 5
	devices := 10
	for zone := 0; zone < zones; zone++ {
		for server := 0; server < servers; server++ {
			for device := 0; device < devices; device++ {
				b.Nodes = append(b.Nodes, &Node{Capacity: 1, TierIndexes: []int{server, zone}})
			}
		}
	}
	noRndHighestCommonPartitionCount := -1
	noRndSmallestNodePartitions := 0
	yesRndHighestCommonPartitionCount := -1
	yesRndSmallestNodePartitions := 0
	for loop := 0; loop < 2; loop++ {
		if loop == 0 {
			b.rnd = noRnd
		} else {
			b.rnd = nil
		}
		b.Ring = nil
		b.LastMoved = nil
		b.ChangeReplicaCount(3)
		b.Rebalance()
		nodeHasPartition := make([]map[int]bool, len(b.Nodes))
		for nodeIndex := 0; nodeIndex < len(b.Nodes); nodeIndex++ {
			nodeHasPartition[nodeIndex] = make(map[int]bool)
		}
		for _, partitionToNodeIndex := range b.Ring {
			for partition, nodeIndex := range partitionToNodeIndex {
				nodeHasPartition[nodeIndex][partition] = true
			}
		}
		highestCommonPartitionCount := 0
		smallestNodePartitions := 0
		for nodeIndex := 0; nodeIndex < len(b.Nodes); nodeIndex++ {
			for otherNodeIndex := nodeIndex + 1; otherNodeIndex < len(b.Nodes); otherNodeIndex++ {
				commonPartitionCount := 0
				for partition := range nodeHasPartition[nodeIndex] {
					if nodeHasPartition[otherNodeIndex][partition] {
						commonPartitionCount++
					}
				}
				if commonPartitionCount > highestCommonPartitionCount {
					highestCommonPartitionCount = commonPartitionCount
					smallestNodePartitions = len(nodeHasPartition[nodeIndex])
					if len(nodeHasPartition[otherNodeIndex]) < smallestNodePartitions {
						smallestNodePartitions = len(nodeHasPartition[otherNodeIndex])
					}
				}
			}
		}
		if loop == 0 {
			noRndHighestCommonPartitionCount = highestCommonPartitionCount
			noRndSmallestNodePartitions = smallestNodePartitions
		} else {
			yesRndHighestCommonPartitionCount = highestCommonPartitionCount
			yesRndSmallestNodePartitions = smallestNodePartitions
		}
	}
	t.Logf("info: worst mirror without rnd was %d of %d compared to with rnd at %d of %d", noRndHighestCommonPartitionCount, noRndSmallestNodePartitions, yesRndHighestCommonPartitionCount, yesRndSmallestNodePartitions)
	if noRndHighestCommonPartitionCount <= yesRndHighestCommonPartitionCount {
		t.Fatal("no improvement with rnd")
	}
}

func TestBuilderRingStats(t *testing.T) {
	b := &Builder{}
	s := b.RingStats()
	if s.ReplicaCount != 0 ||
		s.EnabledNodeCount != 0 ||
		s.DisabledNodeCount != 0 ||
		s.PartitionCount != 0 ||
		s.UnassignedCount != 0 ||
		s.EnabledCapacity != 0 ||
		s.DisabledCapacity != 0 ||
		s.MaxUnderNodePercentage != 0 ||
		s.MaxUnderNodeIndex != 0 ||
		s.MaxOverNodePercentage != 0 ||
		s.MaxOverNodeIndex != 0 {
		t.Fatal(s)
	}
	b.ChangeReplicaCount(1)
	s = b.RingStats()
	if s.ReplicaCount != 1 ||
		s.EnabledNodeCount != 0 ||
		s.DisabledNodeCount != 0 ||
		s.PartitionCount != 1 ||
		s.UnassignedCount != 1 ||
		s.EnabledCapacity != 0 ||
		s.DisabledCapacity != 0 ||
		s.MaxUnderNodePercentage != 0 ||
		s.MaxUnderNodeIndex != 0 ||
		s.MaxOverNodePercentage != 0 ||
		s.MaxOverNodeIndex != 0 {
		t.Fatal(s)
	}
}
