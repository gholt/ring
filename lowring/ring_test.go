package lowring

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	replicaCount := 3
	partitionCount := 1
	ring := New(replicaCount)
	if ring.NodeToCapacity != nil {
		t.Fatal()
	}
	if ring.NodeToGroup != nil {
		t.Fatal()
	}
	if len(ring.GroupToGroup) != 1 {
		t.Fatal()
	}
	if ring.GroupToGroup[0] != 0 {
		t.Fatal()
	}
	if len(ring.ReplicaToPartitionToNode) != replicaCount {
		t.Fatal()
	}
	for replica := 0; replica < replicaCount; replica++ {
		if len(ring.ReplicaToPartitionToNode[replica]) != partitionCount {
			t.Fatal()
		}
		for partition := 0; partition < partitionCount; partition++ {
			if ring.ReplicaToPartitionToNode[replica][partition] != NilNode {
				t.Fatal()
			}
		}
	}
	if ring.MaxPartitionCount != 8388608 {
		t.Fatal()
	}
	if !ring.Rebalanced.IsZero() {
		t.Fatal()
	}
	if len(ring.ReplicaToPartitionToWait) != replicaCount {
		t.Fatal()
	}
	for replica := 0; replica < replicaCount; replica++ {
		if len(ring.ReplicaToPartitionToWait[replica]) != partitionCount {
			t.Fatal()
		}
		for partition := 0; partition < partitionCount; partition++ {
			if ring.ReplicaToPartitionToWait[replica][partition] != 0 {
				t.Fatal()
			}
		}
	}
	if ring.ReassignmentWait != 60 {
		t.Fatal()
	}
	if ring.MaxReplicaReassignableCount != 1 {
		t.Fatal(ring.MaxReplicaReassignableCount)
	}
	if ring.ReplicaToNodeToPartitions != nil {
		t.Fatal()
	}
	ring.FillReplicaToNodeToPartitions()
	if len(ring.ReplicaToNodeToPartitions) != replicaCount {
		t.Fatal()
	}
	for replica := 0; replica < replicaCount; replica++ {
		if len(ring.ReplicaToNodeToPartitions[replica]) != 0 {
			t.Fatal()
		}
	}
}

func TestSetReplicaCount(t *testing.T) {
	replicaCount := 1
	partitionCount := 1
	ring := New(replicaCount)
	if len(ring.ReplicaToPartitionToNode) != replicaCount {
		t.Fatal()
	}
	for replica := 0; replica < replicaCount; replica++ {
		if len(ring.ReplicaToPartitionToNode[replica]) != partitionCount {
			t.Fatal()
		}
		for partition := 0; partition < partitionCount; partition++ {
			if ring.ReplicaToPartitionToNode[replica][partition] != NilNode {
				t.Fatal()
			}
		}
	}
	if len(ring.ReplicaToPartitionToWait) != replicaCount {
		t.Fatal()
	}
	for replica := 0; replica < replicaCount; replica++ {
		if len(ring.ReplicaToPartitionToWait[replica]) != partitionCount {
			t.Fatal()
		}
		for partition := 0; partition < partitionCount; partition++ {
			if ring.ReplicaToPartitionToWait[replica][partition] != 0 {
				t.Fatal()
			}
		}
	}
	if ring.ReplicaToNodeToPartitions != nil {
		t.Fatal()
	}
	ring.FillReplicaToNodeToPartitions()
	if len(ring.ReplicaToNodeToPartitions) != replicaCount {
		t.Fatal()
	}
	for replica := 0; replica < replicaCount; replica++ {
		if len(ring.ReplicaToNodeToPartitions[replica]) != 0 {
			t.Fatal()
		}
	}
	replicaCount = 2
	ring.SetReplicaCount(replicaCount)
	if len(ring.ReplicaToPartitionToNode) != replicaCount {
		t.Fatal()
	}
	for replica := 0; replica < replicaCount; replica++ {
		if len(ring.ReplicaToPartitionToNode[replica]) != partitionCount {
			t.Fatal()
		}
		for partition := 0; partition < partitionCount; partition++ {
			if ring.ReplicaToPartitionToNode[replica][partition] != NilNode {
				t.Fatal()
			}
		}
	}
	if len(ring.ReplicaToPartitionToWait) != replicaCount {
		t.Fatal()
	}
	for replica := 0; replica < replicaCount; replica++ {
		if len(ring.ReplicaToPartitionToWait[replica]) != partitionCount {
			t.Fatal()
		}
		for partition := 0; partition < partitionCount; partition++ {
			if ring.ReplicaToPartitionToWait[replica][partition] != 0 {
				t.Fatal()
			}
		}
	}
	if len(ring.ReplicaToNodeToPartitions) != replicaCount {
		t.Fatal()
	}
	for replica := 0; replica < replicaCount; replica++ {
		if len(ring.ReplicaToNodeToPartitions[replica]) != 0 {
			t.Fatal()
		}
	}
	for replica := 0; replica < replicaCount; replica++ {
		for partition := 0; partition < partitionCount; partition++ {
			ring.ReplicaToPartitionToNode[replica][partition] = Node(partition*replicaCount + replica)
			ring.ReplicaToPartitionToWait[replica][partition] = 123
			ring.ReplicaToNodeToPartitions[replica] = [][]uint32{{1, 2, 3}}
		}
	}
	additionalReplicaCount := 2
	replicaCount += additionalReplicaCount
	ring.SetReplicaCount(replicaCount)
	if len(ring.ReplicaToPartitionToNode) != replicaCount {
		t.Fatal()
	}
	for replica := 0; replica < replicaCount-additionalReplicaCount; replica++ {
		if len(ring.ReplicaToPartitionToNode[replica]) != partitionCount {
			t.Fatal()
		}
		for partition := 0; partition < partitionCount; partition++ {
			if ring.ReplicaToPartitionToNode[replica][partition] != Node(partition*replicaCount+replica) {
				t.Fatal()
			}
		}
	}
	for replica := replicaCount - additionalReplicaCount; replica < replicaCount; replica++ {
		if len(ring.ReplicaToPartitionToNode[replica]) != partitionCount {
			t.Fatal()
		}
		for partition := 0; partition < partitionCount; partition++ {
			if ring.ReplicaToPartitionToNode[replica][partition] != NilNode {
				t.Fatal()
			}
		}
	}
	if len(ring.ReplicaToPartitionToWait) != replicaCount {
		t.Fatal()
	}
	for replica := 0; replica < replicaCount-additionalReplicaCount; replica++ {
		if len(ring.ReplicaToPartitionToWait[replica]) != partitionCount {
			t.Fatal()
		}
		for partition := 0; partition < partitionCount; partition++ {
			if ring.ReplicaToPartitionToWait[replica][partition] != 123 {
				t.Fatal()
			}
		}
	}
	for replica := replicaCount - additionalReplicaCount; replica < replicaCount; replica++ {
		if len(ring.ReplicaToPartitionToWait[replica]) != partitionCount {
			t.Fatal()
		}
		for partition := 0; partition < partitionCount; partition++ {
			if ring.ReplicaToPartitionToWait[replica][partition] != 0 {
				t.Fatal()
			}
		}
	}
	if len(ring.ReplicaToNodeToPartitions) != replicaCount {
		t.Fatal()
	}
	for replica := 0; replica < replicaCount-additionalReplicaCount; replica++ {
		if len(ring.ReplicaToNodeToPartitions[replica]) != 1 {
			t.Fatal()
		}
		if len(ring.ReplicaToNodeToPartitions[replica][0]) != 3 {
			t.Fatal()
		}
		for nodePartition := 0; nodePartition < 3; nodePartition++ {
			if ring.ReplicaToNodeToPartitions[replica][0][nodePartition] != uint32(nodePartition+1) {
				t.Fatal()
			}
		}
	}
	for replica := replicaCount - additionalReplicaCount; replica < replicaCount; replica++ {
		if ring.ReplicaToNodeToPartitions[replica] != nil {
			t.Fatal()
		}
	}
	replicaCount = 1
	ring.SetReplicaCount(replicaCount)
	if len(ring.ReplicaToPartitionToNode) != replicaCount {
		t.Fatal()
	}
	for replica := 0; replica < replicaCount; replica++ {
		if len(ring.ReplicaToPartitionToNode[replica]) != partitionCount {
			t.Fatal()
		}
		for partition := 0; partition < partitionCount; partition++ {
			if ring.ReplicaToPartitionToNode[replica][partition] != Node(partition*replicaCount+replica) {
				t.Fatal()
			}
		}
	}
	if len(ring.ReplicaToPartitionToWait) != replicaCount {
		t.Fatal()
	}
	for replica := 0; replica < replicaCount; replica++ {
		if len(ring.ReplicaToPartitionToWait[replica]) != partitionCount {
			t.Fatal()
		}
		for partition := 0; partition < partitionCount; partition++ {
			if ring.ReplicaToPartitionToWait[replica][partition] != 123 {
				t.Fatal()
			}
		}
	}
	if len(ring.ReplicaToNodeToPartitions) != replicaCount {
		t.Fatal()
	}
	for replica := 0; replica < replicaCount; replica++ {
		if len(ring.ReplicaToNodeToPartitions[replica]) != 1 {
			t.Fatal()
		}
		if len(ring.ReplicaToNodeToPartitions[replica][0]) != 3 {
			t.Fatal()
		}
		for nodePartition := 0; nodePartition < 3; nodePartition++ {
			if ring.ReplicaToNodeToPartitions[replica][0][nodePartition] != uint32(nodePartition+1) {
				t.Fatal()
			}
		}
	}
}

func Test_doublePartitions(t *testing.T) {
	replicaCount := 3
	ring := New(replicaCount)
	ring.ReplicaToPartitionToNode[0][0] = 0
	ring.ReplicaToPartitionToWait[0][0] = 1
	ring.ReplicaToNodeToPartitions = make([][][]uint32, replicaCount)
	ring.ReplicaToNodeToPartitions[0] = [][]uint32{{0}, nil, nil}
	ring.ReplicaToPartitionToNode[1][0] = 1
	ring.ReplicaToPartitionToWait[1][0] = 2
	ring.ReplicaToNodeToPartitions[1] = [][]uint32{nil, {0}, nil}
	ring.ReplicaToPartitionToNode[2][0] = 2
	ring.ReplicaToPartitionToWait[2][0] = 3
	ring.ReplicaToNodeToPartitions[2] = [][]uint32{nil, nil, {0}}
	ring.doublePartitions()
	if ring.ReplicaToPartitionToNode[0][0] != 0 {
		t.Fatal()
	}
	if ring.ReplicaToPartitionToWait[0][0] != 1 {
		t.Fatal()
	}
	if ring.ReplicaToPartitionToNode[0][1] != 0 {
		t.Fatal()
	}
	if ring.ReplicaToPartitionToWait[0][1] != 1 {
		t.Fatal()
	}
	if ring.ReplicaToNodeToPartitions[0][0][0] != 0 {
		t.Fatal()
	}
	if ring.ReplicaToNodeToPartitions[0][0][1] != 1 {
		t.Fatal()
	}
	if ring.ReplicaToPartitionToNode[1][0] != 1 {
		t.Fatal()
	}
	if ring.ReplicaToPartitionToWait[1][0] != 2 {
		t.Fatal()
	}
	if ring.ReplicaToPartitionToNode[1][1] != 1 {
		t.Fatal()
	}
	if ring.ReplicaToPartitionToWait[1][1] != 2 {
		t.Fatal()
	}
	if ring.ReplicaToNodeToPartitions[1][1][0] != 0 {
		t.Fatal()
	}
	if ring.ReplicaToNodeToPartitions[1][1][1] != 1 {
		t.Fatal()
	}
	if ring.ReplicaToPartitionToNode[2][0] != 2 {
		t.Fatal()
	}
	if ring.ReplicaToPartitionToWait[2][0] != 3 {
		t.Fatal()
	}
	if ring.ReplicaToPartitionToNode[2][1] != 2 {
		t.Fatal()
	}
	if ring.ReplicaToPartitionToWait[2][1] != 3 {
		t.Fatal()
	}
	if ring.ReplicaToNodeToPartitions[2][2][0] != 0 {
		t.Fatal()
	}
	if ring.ReplicaToNodeToPartitions[2][2][1] != 1 {
		t.Fatal()
	}
}

func Test_setRebalanced(t *testing.T) {
	ring := New(1)
	r := &rebalancer{ring: ring, replicaCount: 1, partitionCount: 1}
	now := time.Now()
	r.setRebalanced(now)
	ring.ReplicaToPartitionToWait[0][0] = 123
	r.setRebalanced(now)
	if ring.ReplicaToPartitionToWait[0][0] != 123 {
		t.Fatal()
	}
	now = now.Add(time.Minute)
	r.setRebalanced(now)
	if ring.ReplicaToPartitionToWait[0][0] != 122 {
		t.Fatal()
	}
	now = now.Add(59 * time.Minute)
	r.setRebalanced(now)
	if ring.ReplicaToPartitionToWait[0][0] != 63 {
		t.Fatal()
	}
	now = now.Add(60 * time.Minute)
	r.setRebalanced(now)
	if ring.ReplicaToPartitionToWait[0][0] != 0 {
		t.Fatal()
	}
	ring.ReplicaToPartitionToWait[0][0] = 123
	now = now.Add(-60 * time.Minute)
	r.setRebalanced(now)
	if ring.ReplicaToPartitionToWait[0][0] != 123 {
		t.Fatal()
	}
	now = now.Add(10000 * time.Hour)
	r.setRebalanced(now)
	if ring.ReplicaToPartitionToWait[0][0] != 0 {
		t.Fatal()
	}
	ring.ReplicaToPartitionToWait[0][0] = 40
	now = now.Add(41 * time.Minute)
	r.setRebalanced(now)
	if ring.ReplicaToPartitionToWait[0][0] != 0 {
		t.Fatal()
	}
}

func Test_growIfNeeded(t *testing.T) {
	ring := New(3)
	ring.NodeToCapacity = []int{1, 3, 4}
	r := &rebalancer{ring: ring, nodeCount: 3, replicaCount: 3, partitionCount: 1}
	r.growIfNeeded()
	if r.partitionCount != 8 {
		t.Fatal(r.partitionCount)
	}
	if r.totalCapacity != 8 {
		t.Fatal(r.totalCapacity)
	}
	ring.NodeToCapacity = []int{1, 1, 1, 1}
	r.nodeCount = len(ring.NodeToCapacity)
	r.growIfNeeded()
	if r.partitionCount != 8 {
		t.Fatal(r.partitionCount)
	}
	if len(ring.ReplicaToPartitionToNode[0]) != 8 {
		t.Fatal()
	}
	if r.totalCapacity != 4 {
		t.Fatal()
	}
	ring.NodeToCapacity = []int{1, 1, 1, 1, 0, -1}
	r.nodeCount = len(ring.NodeToCapacity)
	r.growIfNeeded()
	if r.partitionCount != 8 {
		t.Fatal(r.partitionCount)
	}
	if r.totalCapacity != 4 {
		t.Fatal()
	}
	ring.NodeToCapacity = []int{1, 1, 1, 1, 0, -1, 100}
	r.nodeCount = len(ring.NodeToCapacity)
	ring.MaxPartitionCount = 32
	r.growIfNeeded()
	if r.partitionCount != 32 {
		t.Fatal()
	}
	if r.totalCapacity != 104 {
		t.Fatal()
	}
	ring.MaxPartitionCount = 4000
	r.growIfNeeded()
	if r.partitionCount != 2048 {
		t.Fatal()
	}
	if r.totalCapacity != 104 {
		t.Fatal()
	}
	ring.MaxPartitionCount = 65536
	r.growIfNeeded()
	if r.partitionCount != 8192 {
		t.Fatal(r.partitionCount)
	}
	if r.totalCapacity != 104 {
		t.Fatal()
	}
	r.growIfNeeded()
	if r.partitionCount != 8192 {
		t.Fatal(r.partitionCount)
	}
	if r.totalCapacity != 104 {
		t.Fatal()
	}
}

func Test_initWhatsLeftForThisPass(t *testing.T) {
	for replicaCount := 1; replicaCount < 5; replicaCount++ {
		ring := New(replicaCount)
		r := &rebalancer{ring: ring, replicaCount: replicaCount, partitionCount: 1}
		r.initWhatsLeftForThisPass()
		if len(r.partitionReassignmentsLeft) != 1 {
			t.Fatal()
		}
		if int(r.partitionReassignmentsLeft[0]) != replicaCount {
			t.Fatal()
		}
		if len(r.replicaToPartitionToChangedThisPass) != replicaCount {
			t.Fatal()
		}
		for replica := 0; replica < replicaCount; replica++ {
			if len(r.replicaToPartitionToChangedThisPass[replica]) != 1 {
				t.Fatal()
			}
			if !r.replicaToPartitionToChangedThisPass[replica][0] {
				t.Fatal()
			}
		}
	}
	for replicaCount := 1; replicaCount < 5; replicaCount++ {
		ring := New(replicaCount)
		r := &rebalancer{ring: ring, replicaCount: replicaCount, partitionCount: 1, oldRebalanced: time.Now()}
		r.initWhatsLeftForThisPass()
		if len(r.partitionReassignmentsLeft) != 1 {
			t.Fatal()
		}
		if r.partitionReassignmentsLeft[0] != ring.MaxReplicaReassignableCount {
			t.Fatal()
		}
		if len(r.replicaToPartitionToChangedThisPass) != replicaCount {
			t.Fatal()
		}
		for replica := 0; replica < replicaCount; replica++ {
			if len(r.replicaToPartitionToChangedThisPass[replica]) != 1 {
				t.Fatal()
			}
			if r.replicaToPartitionToChangedThisPass[replica][0] {
				t.Fatal()
			}
		}
		ring.ReplicaToPartitionToWait[0][0] = 1
		r.initWhatsLeftForThisPass()
		if len(r.partitionReassignmentsLeft) != 1 {
			t.Fatal()
		}
		if r.partitionReassignmentsLeft[0] != ring.MaxReplicaReassignableCount-1 {
			t.Fatal()
		}
		if len(r.replicaToPartitionToChangedThisPass) != replicaCount {
			t.Fatal()
		}
		for replica := 0; replica < replicaCount; replica++ {
			if len(r.replicaToPartitionToChangedThisPass[replica]) != 1 {
				t.Fatal()
			}
			if r.replicaToPartitionToChangedThisPass[replica][0] {
				t.Fatal()
			}
		}
	}
}

func Test_initNodeAndGroup(t *testing.T) {
	for _, randIntn := range []func(int) int{nil, rand.New(rand.NewSource(0)).Intn} {
		replicaCount := 2
		ring := New(replicaCount)
		ring.NodeToCapacity = []int{1, 2, 3}
		ring.NodeToGroup = []int{1, 1, 2}
		ring.GroupToGroup = []int{0, 3, 4, 0, 0}
		ring.ReplicaToNodeToPartitions = make([][][]uint32, replicaCount)
		r := &rebalancer{ring: ring, nodeCount: len(ring.NodeToCapacity), groupCount: len(ring.GroupToGroup), replicaCount: replicaCount, partitionCount: 1, randIntn: randIntn}
		for replica := 0; replica < replicaCount; replica++ {
			ring.ReplicaToNodeToPartitions[replica] = make([][]uint32, r.nodeCount)
		}
		ring.ReplicaToNodeToPartitions[0][0] = []uint32{0}
		ring.ReplicaToNodeToPartitions[1][2] = []uint32{0}
		r.growIfNeeded()
		r.initNodeAndGroup()
		if r.partitionCount != 512 {
			t.Fatal(r.partitionCount)
		}
		for replica := 0; replica < replicaCount; replica++ {
			nodeToOptimal := r.replicaToNodeToOptimal[replica]
			if nodeToOptimal[0] != 8533 || nodeToOptimal[1] != 17067 || nodeToOptimal[2] != 25600 {
				t.Fatal(nodeToOptimal)
			}
			groupToOptimal := r.replicaToGroupToOptimal[replica]
			for group := 1; group < r.groupCount; group++ {
				if groupToOptimal[group] != 25600 {
					t.Fatal(groupToOptimal[group])
				}
			}
		}
	}
}

func Test_initTiers(t *testing.T) {
	//         7
	//       3
	//      /  8
	//     1
	//    / \  9
	//   /   4
	//  /      10
	// 0
	//  \      11
	//   \   5
	//    \ /  12
	//     2
	//      \  13
	//       6
	//         14
	r := &rebalancer{ring: New(2), replicaCount: 2, groupCount: 15}
	r.ring.GroupToGroup = []int{0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6}
	r.initNodeAndGroup()
	r.initTiers()
	if r.tierCount != 3 {
		t.Fatal(r.tierCount)
	}
	if fmt.Sprintf("%v", r.groupToTier) != "[0 0 0 1 1 1 1 2 2 2 2 2 2 2 2]" {
		t.Fatalf("%v", r.groupToTier)
	}
}

func Test_assignUnassigned(t *testing.T) {
	replicaCount := 1
	ring := New(replicaCount)
	ring.NodeToCapacity = []int{1, 3, 4}
	totalCapacity := 8
	ring.NodeToGroup = []int{0, 0, 0}
	ring.ReplicaToNodeToPartitions = make([][][]uint32, replicaCount)
	for replica := 0; replica < replicaCount; replica++ {
		ring.ReplicaToNodeToPartitions[replica] = make([][]uint32, len(ring.NodeToCapacity))
	}
	r := newRebalancer(ring, nil)
	r.rebalance()
	if len(ring.ReplicaToPartitionToNode[0]) != 8 {
		t.Fatal(len(ring.ReplicaToPartitionToNode[0]))
	}
	v := make([]float64, len(ring.NodeToCapacity))
	for node, capacity := range ring.NodeToCapacity {
		v[node] = float64(capacity) / float64(totalCapacity) * float64(len(ring.ReplicaToPartitionToNode[0]))
	}
	for replica := 0; replica < replicaCount; replica++ {
		if len(ring.ReplicaToNodeToPartitions[replica][0]) != int(v[0]) {
			t.Fatal(len(ring.ReplicaToNodeToPartitions[replica][0]), int(v[0]))
		}
		if len(ring.ReplicaToNodeToPartitions[replica][1]) != int(v[1]) {
			t.Fatal(len(ring.ReplicaToNodeToPartitions[replica][1]), int(v[1]))
		}
		if len(ring.ReplicaToNodeToPartitions[replica][2]) != int(v[2]) {
			t.Fatal(len(ring.ReplicaToNodeToPartitions[replica][2]), int(v[2]))
		}
	}
}
