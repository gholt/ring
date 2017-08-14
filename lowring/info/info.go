package info

import (
	"fmt"
	"math"
	"time"

	"github.com/gholt/ring/lowring"
)

type Info struct {
	Time                                  time.Time
	NodeCount                             int
	ActiveNodeCount                       int
	DisabledNodeCount                     int
	DrainingNodeCount                     int
	TotalCapacity                         int
	GroupCount                            int
	GroupToTier                           []int
	TierCount                             int
	TierToGroups                          [][]int
	ReplicaCount                          int
	PartitionCount                        int
	AssignmentCount                       int
	MaxPartitionCount                     int
	Rebalanced                            time.Time
	ReassignmentWait                      int
	MaxReplicaReassignableCount           int
	AssignmentInWaitCountAtRebalancedTime int
	AssignmentInWaitCountAtInfoTime       int
	MostUnderweight                       float64
	MostUnderweightNode                   int
	MostOverweight                        float64
	MostOverweightNode                    int
	ReplicaToMost                         []float64
	ReplicaToMostCount                    []int
	ReplicaToMostNode                     []int
	NodeToAssignmentCount                 []int
	NodeLevelRiskyPartitions              []*InfoRiskyPartition
	TierToRiskyPartitions                 [][]*InfoRiskyPartition
	WorstMirrorPercentage                 float64
	WorstMirrorCount                      int
	WorstMirrorNodeA                      int
	WorstMirrorNodeB                      int
	Warnings                              []string
}

type InfoRiskyPartition struct {
	Partition int
	Nodes     []int
}

func New(ring *lowring.Ring, quick bool, mirroring bool) (*Info, error) {
	info := &Info{Time: time.Now()}
	if quick {
		info.ReplicaCount = len(ring.ReplicaToPartitionToNode)
		info.PartitionCount = len(ring.ReplicaToPartitionToNode[0])
		info.NodeCount = len(ring.NodeToCapacity)
		info.GroupCount = len(ring.GroupToGroup)
		info.AssignmentCount = info.ReplicaCount * info.PartitionCount
		info.MaxPartitionCount = ring.MaxPartitionCount
		info.Rebalanced = ring.Rebalanced
		info.ReassignmentWait = int(ring.ReassignmentWait)
		info.MaxReplicaReassignableCount = int(ring.MaxReplicaReassignableCount)
		for partition := 0; partition < info.PartitionCount; partition++ {
			for replica := 0; replica < info.ReplicaCount; replica++ {
				if ring.ReplicaToPartitionToWait[replica][partition] > 0 {
					info.AssignmentInWaitCountAtRebalancedTime++
				}
			}
		}
	} else {
		if len(ring.NodeToCapacity) != len(ring.NodeToGroup) {
			return nil, fmt.Errorf("len(ring.NodeToCapacity) does not match len(ring.NodeToGroup); %d != %d", len(ring.NodeToCapacity), len(ring.NodeToGroup))
		}
		if len(ring.GroupToGroup) == 0 {
			return nil, fmt.Errorf("ring.GroupToGroup should have at least one entry and does not")
		}
		if ring.GroupToGroup[0] != 0 {
			return nil, fmt.Errorf("ring.GroupToGroup[0] should be 0 and is %d", ring.GroupToGroup[0])
		}
		info.ReplicaCount = len(ring.ReplicaToPartitionToNode)
		if info.ReplicaCount < 1 || info.ReplicaCount > 127 {
			return nil, fmt.Errorf("Replica count is not the range 1-127; it is %d", info.ReplicaCount)
		}
		info.PartitionCount = len(ring.ReplicaToPartitionToNode[0])
		if info.PartitionCount < 1 {
			return nil, fmt.Errorf("ring partition count is less than 1; no ring info to report on")
		}
		if ring.MaxPartitionCount < 1 {
			info.Warnings = append(info.Warnings, fmt.Sprintf("ring.MaxPartitionCount is less than 1 which is weird, but not exactly an error; it was %d", ring.MaxPartitionCount))
		}
		if ring.ReassignmentWait < 1 {
			info.Warnings = append(info.Warnings, fmt.Sprintf("ring.ReassignmentWait is less than 1 which is weird, but not exactly an error; it was %d", ring.ReassignmentWait))
		}
		if ring.MaxReplicaReassignableCount < 1 {
			return nil, fmt.Errorf("ring.MaxReplicaReassignableCount is %d which is less than 1 and will cause problems", ring.MaxReplicaReassignableCount)
		}
		info.NodeCount = len(ring.NodeToCapacity)
		for node := 0; node < info.NodeCount; node++ {
			capacity := ring.NodeToCapacity[node]
			if capacity > 0 {
				info.ActiveNodeCount++
				info.TotalCapacity += capacity
			} else if capacity == 0 {
				info.ActiveNodeCount++
				info.DrainingNodeCount++
			} else {
				info.DisabledNodeCount++
			}
			group := ring.NodeToGroup[node]
			for {
				if group >= len(ring.GroupToGroup) {
					return nil, fmt.Errorf("group %d has no entry in ring.GroupToGroup; len(ring.GroupToGroup) == %d", group, len(ring.GroupToGroup))
				}
				group = ring.GroupToGroup[group]
				if group == 0 {
					break
				}
			}
		}
		info.GroupCount = len(ring.GroupToGroup)
		info.GroupToTier = make([]int, info.GroupCount)
		info.TierToGroups = [][]int{}
		tierParents := []int{0}
		for tier := 0; ; tier++ {
			var nextTierParents []int
			for child, parent := range ring.GroupToGroup {
				if child == 0 {
					continue
				}
				for _, tierParent := range tierParents {
					if parent == tierParent {
						info.GroupToTier[child] = tier
						if tier == len(info.TierToGroups) {
							info.TierToGroups = append(info.TierToGroups, []int{})
						}
						info.TierToGroups[tier] = append(info.TierToGroups[tier], child)
						nextTierParents = append(nextTierParents, child)
						break
					}
				}
			}
			if tier == len(info.TierToGroups) {
				break
			}
			tierParents = nextTierParents
		}
		info.TierCount = len(info.TierToGroups)
		info.AssignmentCount = info.ReplicaCount * info.PartitionCount
		info.MaxPartitionCount = ring.MaxPartitionCount
		info.Rebalanced = ring.Rebalanced
		info.ReassignmentWait = int(ring.ReassignmentWait)
		info.MaxReplicaReassignableCount = int(ring.MaxReplicaReassignableCount)
		infoReplicaToPartitionToWait := make([][]uint16, len(ring.ReplicaToPartitionToWait))
		for replica := 0; replica < info.ReplicaCount; replica++ {
			infoReplicaToPartitionToWait[replica] = make([]uint16, len(ring.ReplicaToPartitionToWait[replica]))
			copy(infoReplicaToPartitionToWait[replica], ring.ReplicaToPartitionToWait[replica])
		}
		minutesElapsed := int64(info.Time.Sub(ring.Rebalanced) / time.Minute)
		if minutesElapsed > int64(ring.ReassignmentWait) || minutesElapsed >= int64(math.MaxUint16) {
			for replica := 0; replica < info.ReplicaCount; replica++ {
				for partition := 0; partition < info.PartitionCount; partition++ {
					infoReplicaToPartitionToWait[replica][partition] = 0
				}
			}
		} else if minutesElapsed > 0 {
			for replica := 0; replica < info.ReplicaCount; replica++ {
				for partition := 0; partition < info.PartitionCount; partition++ {
					if int64(infoReplicaToPartitionToWait[replica][partition])-minutesElapsed <= 0 {
						infoReplicaToPartitionToWait[replica][partition] = 0
					} else {
						infoReplicaToPartitionToWait[replica][partition] -= uint16(minutesElapsed)
					}
				}
			}
		}
		info.NodeToAssignmentCount = make([]int, info.NodeCount)
		replicaToNodeToCount := make([][]int, info.ReplicaCount)
		for replica := 0; replica < info.ReplicaCount; replica++ {
			replicaToNodeToCount[replica] = make([]int, info.NodeCount)
		}
		info.TierToRiskyPartitions = make([][]*InfoRiskyPartition, info.TierCount)
		var nodeLevelRiskyPartition *InfoRiskyPartition
		tierToRiskyPartition := make([]*InfoRiskyPartition, info.TierCount)
		nodeUsed := make([]bool, info.NodeCount)
		nodeUsed_Clearer := make([]bool, info.NodeCount)
		groupUsed := make([]bool, info.GroupCount)
		groupUsed_Clearer := make([]bool, info.GroupCount)
		for partition := 0; partition < info.PartitionCount; partition++ {
			for replica := 0; replica < info.ReplicaCount; replica++ {
				if replica == 0 {
					copy(nodeUsed, nodeUsed_Clearer)
					copy(groupUsed, groupUsed_Clearer)
					nodeLevelRiskyPartition = nil
					for tier := 0; tier < info.TierCount; tier++ {
						tierToRiskyPartition[tier] = nil
					}
				}
				node := ring.ReplicaToPartitionToNode[replica][partition]
				if nodeUsed[node] {
					if nodeLevelRiskyPartition == nil {
						nodeLevelRiskyPartition = &InfoRiskyPartition{Partition: partition, Nodes: []int{int(node)}}
						info.NodeLevelRiskyPartitions = append(info.NodeLevelRiskyPartitions, nodeLevelRiskyPartition)
					} else {
						nodeLevelRiskyPartition.Nodes = append(nodeLevelRiskyPartition.Nodes, int(node))
					}
				}
				nodeUsed[node] = true
				group := ring.NodeToGroup[node]
				for {
					if groupUsed[group] {
						tier := info.GroupToTier[group]
						riskyPartition := tierToRiskyPartition[tier]
						if riskyPartition == nil {
							riskyPartition = &InfoRiskyPartition{Partition: partition, Nodes: []int{int(node)}}
							tierToRiskyPartition[tier] = riskyPartition
							info.TierToRiskyPartitions[tier] = append(info.TierToRiskyPartitions[tier], riskyPartition)
						} else {
							riskyPartition.Nodes = append(riskyPartition.Nodes, int(node))
						}
					}
					groupUsed[group] = true
					group = ring.GroupToGroup[group]
					if group == 0 {
						break
					}
				}
				info.NodeToAssignmentCount[node]++
				replicaToNodeToCount[replica][node]++
				if ring.ReplicaToPartitionToWait[replica][partition] > 0 {
					info.AssignmentInWaitCountAtRebalancedTime++
				}
				if infoReplicaToPartitionToWait[replica][partition] > 0 {
					info.AssignmentInWaitCountAtInfoTime++
				}
			}
		}
		info.MostOverweight = -math.MaxFloat64
		info.MostUnderweight = math.MaxFloat64
		info.ReplicaToMost = make([]float64, info.ReplicaCount)
		info.ReplicaToMostCount = make([]int, info.ReplicaCount)
		info.ReplicaToMostNode = make([]int, info.ReplicaCount)
		info.WorstMirrorPercentage = 0
		info.WorstMirrorCount = -1
		info.WorstMirrorNodeA = -1
		info.WorstMirrorNodeB = -1
		for node := 0; node < info.NodeCount; node++ {
			capacity := ring.NodeToCapacity[node]
			nodeAssignmentCount := info.NodeToAssignmentCount[node]
			if capacity < 0 {
				if nodeAssignmentCount != 0 {
					return nil, fmt.Errorf("ring has %d assignments to node %d which is disabled with a capacity of %d", nodeAssignmentCount, node, capacity)
				}
				continue
			}
			if capacity == 0 {
				if nodeAssignmentCount != 0 {
					info.Warnings = append(info.Warnings, fmt.Sprintf("ring has %d assignments to node %d which is draining with a capacity of %d", nodeAssignmentCount, node, capacity))
				}
				continue
			}
			desire := float64(capacity) / float64(info.TotalCapacity) * float64(info.AssignmentCount)
			weight := (float64(nodeAssignmentCount) - desire) / desire
			if weight > info.MostOverweight {
				info.MostOverweight = weight
				info.MostOverweightNode = node
			}
			if weight < info.MostUnderweight {
				info.MostUnderweight = weight
				info.MostUnderweightNode = node
			}
			for replica := 0; replica < info.ReplicaCount; replica++ {
				replicaCount := replicaToNodeToCount[replica][node]
				replicaMost := float64(replicaCount) / float64(nodeAssignmentCount)
				if replicaMost > info.ReplicaToMost[replica] {
					info.ReplicaToMost[replica] = replicaMost
					info.ReplicaToMostCount[replica] = replicaCount
					info.ReplicaToMostNode[replica] = node
				}
			}
			if mirroring {
				ring.FillReplicaToNodeToPartitions()
				usedPartition := make([]bool, info.AssignmentCount)
				for replica := 0; replica < info.ReplicaCount; replica++ {
					for _, partition := range ring.ReplicaToNodeToPartitions[replica][node] {
						usedPartition[partition] = true
					}
				}
				for nodeB := node + 1; nodeB < info.NodeCount; nodeB++ {
					mirrorCount := 0
					for replica := 0; replica < info.ReplicaCount; replica++ {
						for _, partition := range ring.ReplicaToNodeToPartitions[replica][nodeB] {
							if usedPartition[partition] {
								mirrorCount++
							}
						}
					}
					if float64(mirrorCount)/float64(info.NodeToAssignmentCount[node]) > info.WorstMirrorPercentage {
						info.WorstMirrorPercentage = float64(mirrorCount) / float64(info.NodeToAssignmentCount[node])
						info.WorstMirrorCount = mirrorCount
						info.WorstMirrorNodeA = node
						info.WorstMirrorNodeB = nodeB
					}
					if float64(mirrorCount)/float64(info.NodeToAssignmentCount[nodeB]) > info.WorstMirrorPercentage {
						info.WorstMirrorPercentage = float64(mirrorCount) / float64(info.NodeToAssignmentCount[nodeB])
						info.WorstMirrorCount = mirrorCount
						info.WorstMirrorNodeA = node
						info.WorstMirrorNodeB = nodeB
					}
				}
			}
		}
	}
	return info, nil
}
