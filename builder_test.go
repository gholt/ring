package ring

import (
	"fmt"
	"os"
	"runtime/pprof"
	"testing"
	"time"
)

const RUN_LONG = false

type TestNode struct {
	nodeID     uint64
	active     bool
	capacity   uint32
	tierValues []int
	address    string
}

func (node *TestNode) NodeID() uint64 {
	return node.nodeID
}

func (node *TestNode) Active() bool {
	return node.active
}

func (node *TestNode) Capacity() uint32 {
	return node.capacity
}

func (node *TestNode) TierValues() []int {
	return node.tierValues
}

func (node *TestNode) Address() string {
	return node.address
}

func TestNewRingBuilder(t *testing.T) {
	if !RUN_LONG {
		return
	}
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
	builder := NewBuilder(3)
	nodeID := uint64(0)
	//capacity := uint32(1)
	capacity := uint32(100)
	for zone := 0; zone < zones; zone++ {
		for server := 0; server < 50; server++ {
			for device := 0; device < 2; device++ {
				nodeID++
				builder.Add(&TestNode{nodeID: nodeID, active: true, capacity: capacity, tierValues: []int{server, zone}})
				//capacity++
				//if capacity > 100 {
				//	capacity = 1
				//}
			}
		}
	}
	start := time.Now()
	builder.Ring(0)
	stats := builder.Stats()
	fmt.Printf("%6d %8d %10d %4d %8d %7.02f%% %6.02f%% %7d\n", stats.NodeCount, stats.InactiveNodeCount, stats.PartitionCount, stats.PartitionBitCount, stats.TotalCapacity, stats.MaxUnderNodePercentage, stats.MaxOverNodePercentage, int(time.Now().Sub(start)/time.Second))
	builder.Node(25).(*TestNode).active = false
	start = time.Now()
	builder.Ring(0)
	stats = builder.Stats()
	fmt.Printf("%6d %8d %10d %4d %8d %7.02f%% %6.02f%% %7d\n", stats.NodeCount, stats.InactiveNodeCount, stats.PartitionCount, stats.PartitionBitCount, stats.TotalCapacity, stats.MaxUnderNodePercentage, stats.MaxOverNodePercentage, int(time.Now().Sub(start)/time.Second))
	builder.Node(20).(*TestNode).capacity = 75
	start = time.Now()
	builder.Ring(0)
	stats = builder.Stats()
	fmt.Printf("%6d %8d %10d %4d %8d %7.02f%% %6.02f%% %7d\n", stats.NodeCount, stats.InactiveNodeCount, stats.PartitionCount, stats.PartitionBitCount, stats.TotalCapacity, stats.MaxUnderNodePercentage, stats.MaxOverNodePercentage, int(time.Now().Sub(start)/time.Second))
}
