package ring

import (
	"fmt"
	"os"
	"runtime/pprof"
	"testing"
	"time"
)

type TestNode struct {
	nodeID     uint64
	active     bool
	capacity   uint64
	tierValues []int
}

func (node *TestNode) NodeID() uint64 {
	return node.nodeID
}

func (node *TestNode) Active() bool {
	return node.active
}

func (node *TestNode) Capacity() uint64 {
	return node.capacity
}

func (node *TestNode) TierValues() []int {
	return node.tierValues
}

func TestNewRingBuilder(t *testing.T) {
	f, err := os.Create("ring_test.prof")
	if err != nil {
		t.Fatal(err)
	}
	pprof.StartCPUProfile(f)
	fmt.Println(" nodes inactive partitions bits capacity maxunder maxover seconds")
	for zones := 10; zones <= 200; {
		helperTestNewRingBuilder(t, zones)
		if zones < 100 {
			zones += 10
		} else {
			zones += 100
		}
	}
	pprof.StopCPUProfile()
}

func helperTestNewRingBuilder(t *testing.T, zones int) {
	ring := NewRingBuilder(3).(*ringBuilderImpl)
	nodeID := uint64(0)
	//capacity := uint64(1)
	capacity := uint64(100)
	for zone := 0; zone < zones; zone++ {
		for server := 0; server < 50; server++ {
			for device := 0; device < 2; device++ {
				nodeID++
				ring.Add(&TestNode{nodeID: nodeID, active: true, capacity: capacity, tierValues: []int{device, server, zone}})
				//capacity++
				//if capacity > 100 {
				//	capacity = 1
				//}
			}
		}
	}
	start := time.Now()
	ring.Ring(0)
	stats := ring.Stats()
	fmt.Printf("%6d %8d %10d %4d %8d %7.02f%% %6.02f%% %7d\n", stats.NodeCount, stats.InactiveNodeCount, stats.PartitionCount, stats.PartitionBits, stats.TotalCapacity, stats.MaxUnderNodePercentage, stats.MaxOverNodePercentage, int(time.Now().Sub(start)/time.Second))
	ring.Node(25).(*TestNode).active = false
	start = time.Now()
	ring.Ring(0)
	stats = ring.Stats()
	fmt.Printf("%6d %8d %10d %4d %8d %7.02f%% %6.02f%% %7d\n", stats.NodeCount, stats.InactiveNodeCount, stats.PartitionCount, stats.PartitionBits, stats.TotalCapacity, stats.MaxUnderNodePercentage, stats.MaxOverNodePercentage, int(time.Now().Sub(start)/time.Second))
}
