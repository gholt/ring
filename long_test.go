// Will be run if environment long_test=true
// Takes about 13 minutes or so on my MacBook Pro Retina 15".
// Probably best to run as:
// $ long_test=true go test -timeout 30m

package ring

import (
	"fmt"
	"math"
	"os"
	//"runtime/pprof"
	"testing"
	"time"
)

var RUN_LONG = false

func init() {
	if os.Getenv("long_test") == "true" {
		RUN_LONG = true
	}
}

func TestLongBuilder(t *testing.T) {
	if !RUN_LONG {
		t.Skip("skipping unless env long_test=true")
	}
	//f, err := os.Create("long_test.pprof")
	//if err != nil {
	//	t.Fatal(err)
	//}
	//pprof.StartCPUProfile(f)
	fmt.Println(" nodes disabled zones partitions capacity maxunder maxover seconds")
	for _, varyingCapacities := range []bool{false, true} {
		fmt.Println()
		for zones := uint32(10); zones <= 200; {
			longBuilderTester(t, zones, varyingCapacities)
			if zones < 100 {
				zones += 10
			} else {
				zones += 100
			}
		}
	}
	//pprof.StopCPUProfile()
}

func longBuilderTester(t *testing.T, zones uint32, varyingCapacities bool) {
	b := &Builder{}
	b.ChangeReplicaCount(3)
	capacity := uint32(100)
	if varyingCapacities {
		capacity = uint32(1)
	}
	for zone := uint32(0); zone < zones; zone++ {
		for server := uint32(0); server < 50; server++ {
			for device := uint32(0); device < 2; device++ {
				b.Nodes = append(b.Nodes, &Node{Capacity: capacity, TierIndexes: []uint32{server, zone}})
				if varyingCapacities {
					capacity++
					if capacity > 100 {
						capacity = 1
					}
				}
			}
		}
	}
	start := time.Now()
	b.AddLastMoved(math.MaxUint16)
	b.Rebalance()
	stats := b.RingStats()
	fmt.Printf("%6d %8d %5d %10d %8d %7.02f%% %6.02f%% %7d\n", stats.EnabledNodeCount, stats.DisabledNodeCount, zones, stats.PartitionCount, stats.EnabledCapacity, stats.MaxUnderNodePercentage, stats.MaxOverNodePercentage, int(time.Now().Sub(start)/time.Second))
	b.Nodes[25].Disabled = true
	start = time.Now()
	b.AddLastMoved(math.MaxUint16)
	b.Rebalance()
	stats = b.RingStats()
	fmt.Printf("%6d %8d %5d %10d %8d %7.02f%% %6.02f%% %7d\n", stats.EnabledNodeCount, stats.DisabledNodeCount, zones, stats.PartitionCount, stats.EnabledCapacity, stats.MaxUnderNodePercentage, stats.MaxOverNodePercentage, int(time.Now().Sub(start)/time.Second))
	b.Nodes[20].Capacity = 75
	start = time.Now()
	b.AddLastMoved(math.MaxUint16)
	b.Rebalance()
	stats = b.RingStats()
	fmt.Printf("%6d %8d %5d %10d %8d %7.02f%% %6.02f%% %7d\n", stats.EnabledNodeCount, stats.DisabledNodeCount, zones, stats.PartitionCount, stats.EnabledCapacity, stats.MaxUnderNodePercentage, stats.MaxOverNodePercentage, int(time.Now().Sub(start)/time.Second))
	start = time.Now()
	b.AddLastMoved(math.MaxUint16)
	b.Rebalance()
	stats = b.RingStats()
	fmt.Printf("%6d %8d %5d %10d %8d %7.02f%% %6.02f%% %7d\n", stats.EnabledNodeCount, stats.DisabledNodeCount, zones, stats.PartitionCount, stats.EnabledCapacity, stats.MaxUnderNodePercentage, stats.MaxOverNodePercentage, int(time.Now().Sub(start)/time.Second))
}
