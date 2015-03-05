package ring

import (
	"fmt"
	"os"
	"runtime/pprof"
	"testing"
	"time"
)

const RUN_LONG = false

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
				builder.Add(&testNode{id: nodeID, capacity: capacity, tierValues: []int{server, zone}})
				//capacity++
				//if capacity > 100 {
				//	capacity = 1
				//}
			}
		}
	}
	start := time.Now()
	stats := builder.Ring(0).Stats()
	fmt.Printf("%6d %8d %10d %4d %8d %7.02f%% %6.02f%% %7d\n", stats.NodeCount, stats.InactiveNodeCount, stats.PartitionCount, stats.PartitionBitCount, stats.TotalCapacity, stats.MaxUnderNodePercentage, stats.MaxOverNodePercentage, int(time.Now().Sub(start)/time.Second))
	builder.Node(25).(*testNode).inactive = true
	start = time.Now()
	stats = builder.Ring(0).Stats()
	fmt.Printf("%6d %8d %10d %4d %8d %7.02f%% %6.02f%% %7d\n", stats.NodeCount, stats.InactiveNodeCount, stats.PartitionCount, stats.PartitionBitCount, stats.TotalCapacity, stats.MaxUnderNodePercentage, stats.MaxOverNodePercentage, int(time.Now().Sub(start)/time.Second))
	builder.Node(20).(*testNode).capacity = 75
	start = time.Now()
	stats = builder.Ring(0).Stats()
	fmt.Printf("%6d %8d %10d %4d %8d %7.02f%% %6.02f%% %7d\n", stats.NodeCount, stats.InactiveNodeCount, stats.PartitionCount, stats.PartitionBitCount, stats.TotalCapacity, stats.MaxUnderNodePercentage, stats.MaxOverNodePercentage, int(time.Now().Sub(start)/time.Second))
}
