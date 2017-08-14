package lowring

import (
	"math"
	"time"

	"github.com/gholt/holdme"
)

type Node uint16

const NilNode Node = math.MaxUint16

type Ring struct {
	NodeToCapacity              []int
	NodeToGroup                 []int
	GroupToGroup                []int
	ReplicaToPartitionToNode    [][]Node
	MaxPartitionCount           int
	Rebalanced                  time.Time
	ReplicaToPartitionToWait    [][]uint16
	ReassignmentWait            uint16
	MaxReplicaReassignableCount int8
	ReplicaToNodeToPartitions   [][][]uint32
}

func New(replicaCount int) *Ring {
	ring := &Ring{
		GroupToGroup:             []int{0},
		ReplicaToPartitionToNode: make([][]Node, replicaCount),
		MaxPartitionCount:        8388608,
		ReplicaToPartitionToWait: make([][]uint16, replicaCount),
		ReassignmentWait:         60,
	}
	m := replicaCount - int(float64(replicaCount)/2.0+0.5)
	if m < 1 {
		m = 1
	}
	ring.MaxReplicaReassignableCount = int8(m)
	for replica := 0; replica < replicaCount; replica++ {
		ring.ReplicaToPartitionToNode[replica] = []Node{NilNode}
		ring.ReplicaToPartitionToWait[replica] = []uint16{0}
	}
	return ring
}

func (ring *Ring) FillReplicaToNodeToPartitions() {
	if ring.ReplicaToNodeToPartitions == nil {
		replicaCount := len(ring.ReplicaToPartitionToNode)
		partitionCount := len(ring.ReplicaToPartitionToNode[0])
		nodeCount := len(ring.NodeToCapacity)
		for replica := 0; replica < replicaCount; replica++ {
			nodeToPartitions := make([][]uint32, nodeCount)
			ring.ReplicaToNodeToPartitions = append(ring.ReplicaToNodeToPartitions, nodeToPartitions)
			partitionToNode := ring.ReplicaToPartitionToNode[replica]
			for partition := 0; partition < partitionCount; partition++ {
				node := partitionToNode[partition]
				if node != NilNode {
					nodeToPartitions[node] = append(nodeToPartitions[node], uint32(partition))
				}
			}
		}
	}
}

func (ring *Ring) SetReplicaCount(v int) {
	if v < len(ring.ReplicaToPartitionToNode) {
		ring.ReplicaToPartitionToNode = ring.ReplicaToPartitionToNode[:v]
		ring.ReplicaToPartitionToWait = ring.ReplicaToPartitionToWait[:v]
		if ring.ReplicaToNodeToPartitions != nil {
			ring.ReplicaToNodeToPartitions = ring.ReplicaToNodeToPartitions[:v]
		}
	} else {
		for v > len(ring.ReplicaToPartitionToNode) {
			partitionToNode := make([]Node, len(ring.ReplicaToPartitionToNode[0]))
			for partition := len(partitionToNode) - 1; partition >= 0; partition-- {
				partitionToNode[partition] = NilNode
			}
			partitionToWait := make([]uint16, len(ring.ReplicaToPartitionToWait[0]))
			ring.ReplicaToPartitionToNode = append(ring.ReplicaToPartitionToNode, partitionToNode)
			ring.ReplicaToPartitionToWait = append(ring.ReplicaToPartitionToWait, partitionToWait)
			if ring.ReplicaToNodeToPartitions != nil {
				ring.ReplicaToNodeToPartitions = append(ring.ReplicaToNodeToPartitions, nil)
			}
		}
	}
}

func (ring *Ring) AddNode(capacity int, group int) Node {
	node := Node(len(ring.NodeToCapacity))
	ring.NodeToCapacity = append(ring.NodeToCapacity, capacity)
	ring.NodeToGroup = append(ring.NodeToGroup, group)
	if ring.ReplicaToNodeToPartitions != nil {
		for replica := len(ring.ReplicaToNodeToPartitions) - 1; replica >= 0; replica-- {
			ring.ReplicaToNodeToPartitions[replica] = append(ring.ReplicaToNodeToPartitions[replica], nil)
		}
	}
	return node
}

func (ring *Ring) doublePartitions() int {
	oldPartitionCount := len(ring.ReplicaToPartitionToNode[0])
	for replica := len(ring.ReplicaToPartitionToNode) - 1; replica >= 0; replica-- {
		partitionToNode := make([]Node, len(ring.ReplicaToPartitionToNode[replica])*2)
		copy(partitionToNode, ring.ReplicaToPartitionToNode[replica])
		copy(partitionToNode[len(ring.ReplicaToPartitionToNode[replica]):], ring.ReplicaToPartitionToNode[replica])
		ring.ReplicaToPartitionToNode[replica] = partitionToNode
		partitionToWait := make([]uint16, len(ring.ReplicaToPartitionToWait[replica])*2)
		copy(partitionToWait, ring.ReplicaToPartitionToWait[replica])
		copy(partitionToWait[len(ring.ReplicaToPartitionToWait[replica]):], ring.ReplicaToPartitionToWait[replica])
		ring.ReplicaToPartitionToWait[replica] = partitionToWait
		if ring.ReplicaToNodeToPartitions != nil {
			for node := len(ring.ReplicaToNodeToPartitions[replica]) - 1; node >= 0; node-- {
				partitions := make(holdme.OrderedUint32sNoDups, len(ring.ReplicaToNodeToPartitions[replica][node]), len(ring.ReplicaToNodeToPartitions[replica][node])*2)
				copy(partitions, ring.ReplicaToNodeToPartitions[replica][node])
				ln := len(ring.ReplicaToNodeToPartitions[replica][node])
				for i := 0; i < ln; i++ {
					partitions = append(partitions, uint32(oldPartitionCount)+ring.ReplicaToNodeToPartitions[replica][node][i])
				}
				ring.ReplicaToNodeToPartitions[replica][node] = partitions
			}
		}
	}
	return len(ring.ReplicaToPartitionToNode[0])
}

type rebalancer struct {
	ring                                *Ring
	nodeCount                           int
	groupCount                          int
	replicaCount                        int
	partitionCount                      int
	oldRebalanced                       time.Time
	totalCapacity                       float64
	partitionReassignmentsLeft          []int8
	replicaToPartitionToChangedThisPass [][]bool
	replicaToNodeToOptimal              [][]int32
	replicaToNodeToOptimal1             [][]int32
	replicaToDesiredNodes               []*desiredNodes
	replicaToGroupToOptimal             [][]int32
	replicaToGroupToDesiredNodes        [][]*desiredNodes
	tierCount                           int
	groupToTier                         []int
	replicaToTierToDesiredGroups        [][]*desiredGroups
	randIntn                            func(int) int
	nodeUsed                            []bool
	nodeUsedClearer                     []bool
	groupUsed                           []bool
	groupUsedClearer                    []bool
}

func (ring *Ring) Rebalance(randIntn func(int) int) {
	r := newRebalancer(ring, randIntn)
	r.rebalance()
}

func newRebalancer(ring *Ring, randIntn func(int) int) *rebalancer {
	r := &rebalancer{
		ring:           ring,
		nodeCount:      len(ring.NodeToCapacity),
		groupCount:     len(ring.GroupToGroup),
		replicaCount:   len(ring.ReplicaToPartitionToNode),
		partitionCount: len(ring.ReplicaToPartitionToNode[0]),
		oldRebalanced:  ring.Rebalanced,
		randIntn:       randIntn,
	}
	r.setRebalanced(time.Now())
	r.growIfNeeded()
	r.initWhatsLeftForThisPass()
	r.initNodeAndGroup()
	r.initTiers()
	return r
}

func (r *rebalancer) rebalance() {
	r.unassignDisabled()
	r.unassignDupNodes()
	r.assignUnassigned()
	r.reassignDupTiers()
	r.reassignBasedOnDesire()
}

func (r *rebalancer) setRebalanced(now time.Time) {
	minutesElapsed := int64(now.Sub(r.ring.Rebalanced) / time.Minute)
	r.ring.Rebalanced = now
	if minutesElapsed >= int64(r.ring.ReassignmentWait) || minutesElapsed >= int64(math.MaxUint16) {
		for replica := 0; replica < r.replicaCount; replica++ {
			partitionToWait := r.ring.ReplicaToPartitionToWait[replica]
			for partition := 0; partition < r.partitionCount; partition++ {
				partitionToWait[partition] = 0
			}
		}
	} else if minutesElapsed > 0 {
		for replica := 0; replica < r.replicaCount; replica++ {
			partitionToWait := r.ring.ReplicaToPartitionToWait[replica]
			for partition := 0; partition < r.partitionCount; partition++ {
				wait64 := int64(partitionToWait[0]) - minutesElapsed
				if wait64 < 0 {
					wait64 = 0
				}
				partitionToWait[partition] = uint16(wait64)
			}
		}
	}
}

func (r *rebalancer) growIfNeeded() {
	r.totalCapacity = 0
	for node := 0; node < r.nodeCount; node++ {
		nodeCapacity := r.ring.NodeToCapacity[node]
		if nodeCapacity > 0 {
			r.totalCapacity += float64(nodeCapacity)
		}
	}
	newPartitionCount := r.partitionCount
	doublings := uint(0)
	keepTrying := true
	for keepTrying && newPartitionCount < r.ring.MaxPartitionCount {
		keepTrying = false
		for node := 0; node < r.nodeCount; node++ {
			nodeCapacity := r.ring.NodeToCapacity[node]
			if nodeCapacity <= 0 {
				continue
			}
			targetAssignmentCount := float64(newPartitionCount) * (float64(nodeCapacity) / r.totalCapacity)
			under := (targetAssignmentCount - float64(int(targetAssignmentCount))) / targetAssignmentCount
			over := float64(0)
			if targetAssignmentCount > float64(int(targetAssignmentCount)) {
				over = (float64(int(targetAssignmentCount)+1) - targetAssignmentCount) / targetAssignmentCount
			}
			if under > 0.01 || over > 0.01 {
				newPartitionCount *= 2
				doublings++
				if newPartitionCount == r.ring.MaxPartitionCount {
					break
				} else if newPartitionCount > r.ring.MaxPartitionCount {
					newPartitionCount /= 2
					doublings--
					break
				}
				keepTrying = true
			}
		}
	}
	for newPartitionCount > r.partitionCount {
		r.partitionCount = r.ring.doublePartitions()
	}
}

func (r *rebalancer) initWhatsLeftForThisPass() {
	r.partitionReassignmentsLeft = make([]int8, r.partitionCount)
	r.replicaToPartitionToChangedThisPass = make([][]bool, r.replicaCount)
	if r.oldRebalanced.IsZero() {
		for partition := 0; partition < r.partitionCount; partition++ {
			r.partitionReassignmentsLeft[partition] = int8(r.replicaCount)
		}
		for replica := 0; replica < r.replicaCount; replica++ {
			partitionToChangedThisPass := make([]bool, r.partitionCount)
			for partition := 0; partition < r.partitionCount; partition++ {
				partitionToChangedThisPass[partition] = true
			}
			r.replicaToPartitionToChangedThisPass[replica] = partitionToChangedThisPass
		}
	} else {
		for partition := 0; partition < r.partitionCount; partition++ {
			r.partitionReassignmentsLeft[partition] = r.ring.MaxReplicaReassignableCount
		}
		for replica := 0; replica < r.replicaCount; replica++ {
			r.replicaToPartitionToChangedThisPass[replica] = make([]bool, r.partitionCount)
			partitionToWait := r.ring.ReplicaToPartitionToWait[replica]
			for partition := 0; partition < r.partitionCount; partition++ {
				if partitionToWait[partition] != 0 {
					r.partitionReassignmentsLeft[partition]--
				}
			}
		}
	}
}

func (r *rebalancer) initNodeAndGroup() {
	r.ring.FillReplicaToNodeToPartitions()
	r.replicaToNodeToOptimal = make([][]int32, r.replicaCount)
	r.replicaToNodeToOptimal1 = make([][]int32, r.replicaCount)
	r.replicaToDesiredNodes = make([]*desiredNodes, r.replicaCount)
	r.replicaToGroupToOptimal = make([][]int32, r.replicaCount)
	r.replicaToGroupToDesiredNodes = make([][]*desiredNodes, r.replicaCount)
	for replica := 0; replica < r.replicaCount; replica++ {
		r.replicaToNodeToOptimal[replica] = make([]int32, r.nodeCount)
		r.replicaToNodeToOptimal1[replica] = make([]int32, r.nodeCount)
		r.replicaToDesiredNodes[replica] = newDesiredNodes(r.nodeCount, r.randIntn)
		r.replicaToGroupToOptimal[replica] = make([]int32, r.groupCount)
		r.replicaToGroupToDesiredNodes[replica] = make([]*desiredNodes, r.groupCount)
		for group := 0; group < r.groupCount; group++ {
			r.replicaToGroupToDesiredNodes[replica][group] = newDesiredNodes(0, r.randIntn)
		}
	}
	for node := 0; node < r.nodeCount; node++ {
		capacity := r.ring.NodeToCapacity[node]
		if capacity > 0 {
			optimal := int32(float64(capacity)/r.totalCapacity*float64(r.partitionCount)*100 + 0.5)
			optimal1 := int32(10000/(float64(capacity)/r.totalCapacity*float64(r.partitionCount)) + 0.5)
			for replica := 0; replica < r.replicaCount; replica++ {
				r.replicaToNodeToOptimal[replica][node] = optimal
				r.replicaToNodeToOptimal1[replica][node] = optimal1
				desire := int32((int(optimal) - len(r.ring.ReplicaToNodeToPartitions[replica][node])*100) * 10000 / int(optimal))
				r.replicaToDesiredNodes[replica].Add(Node(node), desire)
				for group := r.ring.NodeToGroup[node]; group != 0; group = r.ring.GroupToGroup[group] {
					r.replicaToGroupToOptimal[replica][group] += optimal
					r.replicaToGroupToDesiredNodes[replica][group].Add(Node(node), desire)
				}
			}
		}
	}
	r.nodeUsed = make([]bool, r.nodeCount)
	r.nodeUsedClearer = make([]bool, r.nodeCount)
	r.groupUsed = make([]bool, r.groupCount)
	r.groupUsedClearer = make([]bool, r.groupCount)
}

func (r *rebalancer) initTiers() {
	r.groupToTier = make([]int, r.groupCount)
	r.replicaToTierToDesiredGroups = make([][]*desiredGroups, r.replicaCount)
	for replica := 0; replica < r.replicaCount; replica++ {
		r.replicaToTierToDesiredGroups[replica] = []*desiredGroups{}
	}
	tierParents := []int{0}
	for tier := 0; ; tier++ {
		var nextTierParents []int
		tierAdded := false
		for child := 1; child < r.groupCount; child++ {
			parent := r.ring.GroupToGroup[child]
			for i := len(tierParents) - 1; i >= 0; i-- {
				if parent == tierParents[i] {
					r.groupToTier[child] = tier
					for replica := 0; replica < r.replicaCount; replica++ {
						if tier == len(r.replicaToTierToDesiredGroups[replica]) {
							r.replicaToTierToDesiredGroups[replica] = append(r.replicaToTierToDesiredGroups[replica], newDesiredGroups(0, r.randIntn))
							tierAdded = true
						}
						r.replicaToTierToDesiredGroups[replica][tier].Add(child, 0)
						nextTierParents = append(nextTierParents, child)
					}
					break
				}
			}
		}
		if !tierAdded {
			break
		}
		tierParents = nextTierParents
	}
	r.tierCount = len(r.replicaToTierToDesiredGroups[0])
	for replica := 0; replica < r.replicaCount; replica++ {
		for tier := 0; tier < r.tierCount; tier++ {
			for _, group := range r.replicaToTierToDesiredGroups[replica][tier].byDesire {
				groupNodeCount := len(r.replicaToGroupToDesiredNodes[replica][group].byDesire)
				if groupNodeCount > 0 {
					sum := 0
					for i := 0; i < groupNodeCount; i++ {
						sum += int(r.replicaToDesiredNodes[replica].toDesire[r.replicaToGroupToDesiredNodes[replica][group].byDesire[i]])
					}
					r.replicaToTierToDesiredGroups[replica][tier].Move(group, int32(sum/groupNodeCount))
				}
			}
		}
	}
}

func (r *rebalancer) unassignDisabled() {
	for checkForDisabledNode := 0; checkForDisabledNode < r.nodeCount; checkForDisabledNode++ {
		if r.ring.NodeToCapacity[checkForDisabledNode] < 0 {
			disabledNode := Node(checkForDisabledNode)
			for replica := 0; replica < r.replicaCount; replica++ {
				for partition := 0; partition < r.partitionCount; partition++ {
					if r.ring.ReplicaToPartitionToNode[replica][partition] == disabledNode {
						r.ring.ReplicaToPartitionToNode[replica][partition] = NilNode
						if !r.replicaToPartitionToChangedThisPass[replica][partition] {
							r.ring.ReplicaToPartitionToWait[replica][partition] = r.ring.ReassignmentWait
							r.replicaToPartitionToChangedThisPass[replica][partition] = true
							r.partitionReassignmentsLeft[partition]--
						}
					}
				}
				r.ring.ReplicaToNodeToPartitions[replica][disabledNode] = nil
			}
		}
	}
}

func (r *rebalancer) unassignDupNodes() {
	replicaMax := r.replicaCount - 1
	for partition := 0; partition < r.partitionCount; partition++ {
		copy(r.nodeUsed, r.nodeUsedClearer)
		if r.ring.ReplicaToPartitionToNode[0][partition] != NilNode {
			r.nodeUsed[r.ring.ReplicaToPartitionToNode[0][partition]] = true
		}
		for replica := 1; replica < r.replicaCount; replica++ {
			if r.ring.ReplicaToPartitionToNode[replica][partition] != NilNode {
				if r.nodeUsed[r.ring.ReplicaToPartitionToNode[replica][partition]] {
					r.ring.ReplicaToPartitionToNode[replica][partition] = NilNode
					if !r.replicaToPartitionToChangedThisPass[replica][partition] {
						r.ring.ReplicaToPartitionToWait[replica][partition] = r.ring.ReassignmentWait
						r.replicaToPartitionToChangedThisPass[replica][partition] = true
						r.partitionReassignmentsLeft[partition]--
					}
				} else if replica < replicaMax {
					r.nodeUsed[r.ring.ReplicaToPartitionToNode[replica][partition]] = true
				}
			}
		}
	}
}

func (r *rebalancer) assignUnassigned() {
	replicaMax := r.replicaCount - 1
	for partition := 0; partition < r.partitionCount; partition++ {
		initted := false
		for replica := 0; replica < r.replicaCount; replica++ {
			if r.ring.ReplicaToPartitionToNode[replica][partition] == NilNode {
				if !initted {
					r.bestNodeInit(partition)
					initted = true
				}
				node := r.bestTieredNode(replica)
				if node == NilNode {
					node = r.bestNode(replica)
					if node == NilNode {
						node = r.replicaToDesiredNodes[replica].byDesire[0]
					}
				}
				r.assign(replica, partition, node)
				if replica < replicaMax {
					r.bestNodeUpdate(node)
				}
			}
		}
	}
}

func (r *rebalancer) reassignDupTiers() {
	replicaMax := r.replicaCount - 1
	for tier := 0; tier <= r.tierCount; tier++ {
		for partition := 0; partition < r.partitionCount; partition++ {
			copy(r.groupUsed, r.groupUsedClearer)
			for group := r.ring.NodeToGroup[r.ring.ReplicaToPartitionToNode[0][partition]]; group != 0; group = r.ring.GroupToGroup[group] {
				if r.groupToTier[group] == tier {
					r.groupUsed[group] = true
				}
			}
			for replica := 1; replica < r.replicaCount; replica++ {
				for group := r.ring.NodeToGroup[r.ring.ReplicaToPartitionToNode[replica][partition]]; group != 0; group = r.ring.GroupToGroup[group] {
					if r.groupToTier[group] == tier {
						if r.groupUsed[group] {
							hadChangedThisPass := r.replicaToPartitionToChangedThisPass[replica][partition]
							if !hadChangedThisPass {
								if r.ring.ReplicaToPartitionToWait[replica][partition] > 0 || r.partitionReassignmentsLeft[partition] < 1 {
									continue
								}
							}
							originalNode := r.ring.ReplicaToPartitionToNode[replica][partition]
							r.assign(replica, int(partition), NilNode)
							r.bestNodeInit(int(partition))
							node := r.bestTieredNodeIfNotNode(replica, originalNode)
							if node == NilNode {
								node = originalNode
							}
							r.assign(replica, int(partition), node)
							if node == originalNode {
								if !hadChangedThisPass {
									r.replicaToPartitionToChangedThisPass[replica][partition] = false
									r.ring.ReplicaToPartitionToWait[replica][partition] = 0
									r.partitionReassignmentsLeft[partition]++
								}
							} else {
								replica = 0
							}
						} else if replica < replicaMax {
							r.groupUsed[group] = true
						}
						break
					}
				}
			}
		}
	}
}

func (r *rebalancer) reassignBasedOnDesire() {
	for replica := 0; replica < r.replicaCount; replica++ {
		desiredNodes := r.replicaToDesiredNodes[replica]
		desiredNodesLen := len(desiredNodes.byDesire)
		nodeToOptimal1 := r.replicaToNodeToOptimal1[replica]
		nodeToPartitions := r.ring.ReplicaToNodeToPartitions[replica]
		partitionToChangedThisPass := r.replicaToPartitionToChangedThisPass[replica]
		partitionToWait := r.ring.ReplicaToPartitionToWait[replica]
	nextUnderweight:
		for i := 0; i < desiredNodesLen; {
			underweightNode := desiredNodes.byDesire[i]
			if desiredNodes.toDesire[underweightNode] <= 0 {
				break
			}
			if desiredNodes.toDesire[underweightNode]-nodeToOptimal1[underweightNode] < 0 {
				i++
				continue
			}
			underweightTopGroup := r.ring.NodeToGroup[underweightNode]
			for r.ring.GroupToGroup[underweightTopGroup] != 0 {
				underweightTopGroup = r.ring.GroupToGroup[underweightTopGroup]
			}
			for j := desiredNodesLen - 1; j >= 0; j-- {
				overweightNode := desiredNodes.byDesire[j]
				if desiredNodes.toDesire[overweightNode] >= 0 {
					break
				}
				if r.ring.NodeToCapacity[overweightNode] < 0 {
					continue
				}
				overweightTopGroup := r.ring.NodeToGroup[overweightNode]
				for r.ring.GroupToGroup[overweightTopGroup] != 0 {
					overweightTopGroup = r.ring.GroupToGroup[overweightTopGroup]
				}
				if overweightTopGroup == underweightTopGroup && underweightTopGroup != 0 {
					continue
				}
				for k := len(nodeToPartitions[overweightNode]) - 1; k >= 0; k-- {
					partition := nodeToPartitions[overweightNode][k]
					if !partitionToChangedThisPass[partition] && (partitionToWait[partition] > 0 || r.partitionReassignmentsLeft[partition] < 1) {
						continue
					}
					r.bestNodeInit(int(partition))
					if r.nodeUsed[underweightNode] || r.groupUsed[underweightTopGroup] {
						continue
					}
					r.assign(replica, int(partition), underweightNode)
					continue nextUnderweight
				}
			}
			i++
		}
	}
}

func (r *rebalancer) bestNodeInit(partition int) {
	copy(r.nodeUsed, r.nodeUsedClearer)
	copy(r.groupUsed, r.groupUsedClearer)
	for replica := 0; replica < r.replicaCount; replica++ {
		node := r.ring.ReplicaToPartitionToNode[replica][partition]
		if node != NilNode {
			r.nodeUsed[node] = true
			for group := r.ring.NodeToGroup[node]; group != 0; group = r.ring.GroupToGroup[group] {
				r.groupUsed[group] = true
			}
		}
	}
}

func (r *rebalancer) bestNodeUpdate(node Node) {
	r.nodeUsed[node] = true
	for group := r.ring.NodeToGroup[node]; group != 0; group = r.ring.GroupToGroup[group] {
		r.groupUsed[group] = true
	}
}

func (r *rebalancer) bestTieredNode(replica int) Node {
	bestNode := NilNode
	for tier := 0; tier < r.tierCount && bestNode == NilNode; tier++ {
		bestDesire := int32(math.MinInt32)
		for _, group := range r.replicaToTierToDesiredGroups[replica][tier].byDesire {
			if !r.groupUsed[group] && r.replicaToTierToDesiredGroups[replica][tier].toDesire[group] > bestDesire {
				byDesire := r.replicaToGroupToDesiredNodes[replica][group].byDesire
				toDesire := r.replicaToGroupToDesiredNodes[replica][group].toDesire
				bestNode = byDesire[0]
				bestNodeDesire := toDesire[bestNode]
				bestNodeOverallDesire := math.NaN()
				ln := len(byDesire)
				for i := 1; i < ln; i++ {
					node := byDesire[i]
					if toDesire[node] != bestNodeDesire {
						break
					}
					if math.IsNaN(bestNodeOverallDesire) {
						bestNodeOverallDesire = 0
						for replicaB := 0; replicaB < r.replicaCount; replicaB++ {
							bestNodeOverallDesire += float64(r.replicaToGroupToDesiredNodes[replicaB][group].toDesire[bestNode])
						}
					}
					var overallDesire float64
					for replicaB := 0; replicaB < r.replicaCount; replicaB++ {
						overallDesire += float64(r.replicaToGroupToDesiredNodes[replicaB][group].toDesire[node])
					}
					if overallDesire > bestNodeOverallDesire {
						bestNode = node
						bestNodeOverallDesire = overallDesire
					}
				}
				bestDesire = r.replicaToTierToDesiredGroups[replica][tier].toDesire[group]
			}
		}
	}
	return bestNode
}

func (r *rebalancer) bestTieredNodeIfNotNode(replica int, notNode Node) Node {
	bestNode := NilNode
	for tier := 0; tier < r.tierCount && bestNode == NilNode; tier++ {
		bestDesire := int32(math.MinInt32)
		for _, group := range r.replicaToTierToDesiredGroups[replica][tier].byDesire {
			if !r.groupUsed[group] && r.replicaToTierToDesiredGroups[replica][tier].toDesire[group] > bestDesire {
				byDesire := r.replicaToGroupToDesiredNodes[replica][group].byDesire
				toDesire := r.replicaToGroupToDesiredNodes[replica][group].toDesire
				bestNode = NilNode
				var i int
				var node Node
				for i, node = range r.replicaToGroupToDesiredNodes[replica][group].byDesire {
					if node != notNode {
						bestNode = node
						break
					}
				}
				if bestNode == NilNode {
					continue
				}
				bestNodeDesire := toDesire[bestNode]
				bestNodeOverallDesire := math.NaN()
				ln := len(byDesire)
				for ; i < ln; i++ {
					node = byDesire[i]
					if toDesire[node] != bestNodeDesire {
						break
					}
					if math.IsNaN(bestNodeOverallDesire) {
						bestNodeOverallDesire = 0
						for replicaB := 0; replicaB < r.replicaCount; replicaB++ {
							bestNodeOverallDesire += float64(r.replicaToGroupToDesiredNodes[replicaB][group].toDesire[bestNode])
						}
					}
					var overallDesire float64
					for replicaB := 0; replicaB < r.replicaCount; replicaB++ {
						overallDesire += float64(r.replicaToGroupToDesiredNodes[replicaB][group].toDesire[node])
					}
					if overallDesire > bestNodeOverallDesire {
						bestNode = node
						bestNodeOverallDesire = overallDesire
					}
				}
				bestDesire = r.replicaToTierToDesiredGroups[replica][tier].toDesire[group]
			}
		}
	}
	return bestNode
}

func (r *rebalancer) bestNode(replica int) Node {
	bestNode := NilNode
	bestDesire := int32(math.MinInt32)
	for _, node := range r.replicaToDesiredNodes[replica].byDesire {
		if !r.nodeUsed[node] && r.replicaToDesiredNodes[replica].toDesire[node] > bestDesire {
			bestNode = node
			bestDesire = r.replicaToDesiredNodes[replica].toDesire[node]
		}
	}
	return bestNode
}

func (r *rebalancer) assign(replica, partition int, node Node) {
	oldNode := r.ring.ReplicaToPartitionToNode[replica][partition]
	if oldNode != NilNode {
		r.ring.ReplicaToNodeToPartitions[replica][oldNode] = holdme.OrderedUint32sNoDups(r.ring.ReplicaToNodeToPartitions[replica][oldNode]).Remove(uint32(partition))
		desire := int32((int(r.replicaToNodeToOptimal[replica][oldNode]) - len(r.ring.ReplicaToNodeToPartitions[replica][oldNode])*100) * 10000 / int(r.replicaToNodeToOptimal[replica][oldNode]))
		r.replicaToDesiredNodes[replica].Move(oldNode, desire)
		for group := r.ring.NodeToGroup[oldNode]; group != 0; group = r.ring.GroupToGroup[group] {
			r.replicaToGroupToDesiredNodes[replica][group].Move(oldNode, desire)
			groupNodeCount := len(r.replicaToGroupToDesiredNodes[replica][group].byDesire)
			sum := 0
			for i := 0; i < groupNodeCount; i++ {
				sum += int(r.replicaToDesiredNodes[replica].toDesire[r.replicaToGroupToDesiredNodes[replica][group].byDesire[i]])
			}
			r.replicaToTierToDesiredGroups[replica][r.groupToTier[group]].Move(group, int32(sum/groupNodeCount))
		}
	}
	r.ring.ReplicaToPartitionToNode[replica][partition] = node
	if !r.replicaToPartitionToChangedThisPass[replica][partition] {
		r.ring.ReplicaToPartitionToWait[replica][partition] = r.ring.ReassignmentWait
		r.replicaToPartitionToChangedThisPass[replica][partition] = true
		r.partitionReassignmentsLeft[partition]--
	}
	if node != NilNode {
		r.ring.ReplicaToNodeToPartitions[replica][node] = holdme.OrderedUint32sNoDups(r.ring.ReplicaToNodeToPartitions[replica][node]).Add(uint32(partition))
		desire := int32((int(r.replicaToNodeToOptimal[replica][node]) - len(r.ring.ReplicaToNodeToPartitions[replica][node])*100) * 10000 / int(r.replicaToNodeToOptimal[replica][node]))
		r.replicaToDesiredNodes[replica].Move(node, desire)
		for group := r.ring.NodeToGroup[node]; group != 0; group = r.ring.GroupToGroup[group] {
			r.replicaToGroupToDesiredNodes[replica][group].Move(node, desire)
			groupNodeCount := len(r.replicaToGroupToDesiredNodes[replica][group].byDesire)
			sum := 0
			for i := 0; i < groupNodeCount; i++ {
				sum += int(r.replicaToDesiredNodes[replica].toDesire[r.replicaToGroupToDesiredNodes[replica][group].byDesire[i]])
			}
			r.replicaToTierToDesiredGroups[replica][r.groupToTier[group]].Move(group, int32(sum/groupNodeCount))
		}
	}
}
