// Will be run if environment long_test=true

package ring

import (
	"fmt"
	"math"
	"os"
	"runtime/pprof"
	"testing"
	"time"
)

var RUN_LONG bool = false

func init() {
	if os.Getenv("long_test") == "true" {
		RUN_LONG = true
	}
}

func TestNewRingBuilder(t *testing.T) {
	if !RUN_LONG {
		return
	}
	f, err := os.Create("long_test.prof")
	if err != nil {
		t.Fatal(err)
	}
	pprof.StartCPUProfile(f)
	fmt.Println(" nodes inactive partitions bits capacity maxunder maxover seconds")
	for zones := int32(10); zones <= 200; {
		helperTestNewRingBuilder(t, zones)
		if zones < 100 {
			zones += 10
		} else {
			zones += 100
		}
	}
	pprof.StopCPUProfile()
}

func helperTestNewRingBuilder(t *testing.T, zones int32) {
	b := NewBuilder()
	b.SetReplicaCount(3)
	nodeID := uint64(0)
	//capacity := uint32(1)
	capacity := uint32(100)
	for zone := int32(0); zone < zones; zone++ {
		for server := int32(0); server < 50; server++ {
			for device := int32(0); device < 2; device++ {
				nodeID++
				b.AddNode(true, capacity, []string{fmt.Sprintf("server%d", server), fmt.Sprintf("zone%d", zone)}, nil, "")
				//capacity++
				//if capacity > 100 {
				//	capacity = 1
				//}
			}
		}
	}
	start := time.Now()
	b.PretendElapsed(math.MaxUint16)
	stats := b.Ring().Stats()
	fmt.Printf("%6d %8d %10d %4d %8d %7.02f%% %6.02f%% %7d\n", stats.NodeCount, stats.InactiveNodeCount, stats.PartitionCount, stats.PartitionBitCount, stats.TotalCapacity, stats.MaxUnderNodePercentage, stats.MaxOverNodePercentage, int(time.Now().Sub(start)/time.Second))
	b.nodes[25].SetActive(false)
	start = time.Now()
	b.PretendElapsed(math.MaxUint16)
	stats = b.Ring().Stats()
	fmt.Printf("%6d %8d %10d %4d %8d %7.02f%% %6.02f%% %7d\n", stats.NodeCount, stats.InactiveNodeCount, stats.PartitionCount, stats.PartitionBitCount, stats.TotalCapacity, stats.MaxUnderNodePercentage, stats.MaxOverNodePercentage, int(time.Now().Sub(start)/time.Second))
	b.nodes[20].SetCapacity(75)
	start = time.Now()
	b.PretendElapsed(math.MaxUint16)
	stats = b.Ring().Stats()
	fmt.Printf("%6d %8d %10d %4d %8d %7.02f%% %6.02f%% %7d\n", stats.NodeCount, stats.InactiveNodeCount, stats.PartitionCount, stats.PartitionBitCount, stats.TotalCapacity, stats.MaxUnderNodePercentage, stats.MaxOverNodePercentage, int(time.Now().Sub(start)/time.Second))
	start = time.Now()
	f, err := os.Create("long_test.builder")
	if err != nil {
		t.Fatal(err)
	}
	err = b.Persist(f)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	f, err = os.Open("long_test.builder")
	if err != nil {
		t.Fatal(err)
	}
	b, err = LoadBuilder(f)
	if err != nil {
		t.Fatal(err)
	}
	b.PretendElapsed(math.MaxUint16)
	r := b.Ring()
	stats = r.Stats()
	fmt.Printf("%6d %8d %10d %4d %8d %7.02f%% %6.02f%% %7d\n", stats.NodeCount, stats.InactiveNodeCount, stats.PartitionCount, stats.PartitionBitCount, stats.TotalCapacity, stats.MaxUnderNodePercentage, stats.MaxOverNodePercentage, int(time.Now().Sub(start)/time.Second))
	start = time.Now()
	f, err = os.Create("long_test.ring")
	if err != nil {
		t.Fatal(err)
	}
	err = r.Persist(f)
	if err != nil {
		t.Fatal(err)
	}
	f.Close()
	f, err = os.Open("long_test.ring")
	if err != nil {
		t.Fatal(err)
	}
	r, err = LoadRing(f)
	if err != nil {
		t.Fatal(err)
	}
	stats = r.Stats()
	fmt.Printf("%6d %8d %10d %4d %8d %7.02f%% %6.02f%% %7d\n", stats.NodeCount, stats.InactiveNodeCount, stats.PartitionCount, stats.PartitionBitCount, stats.TotalCapacity, stats.MaxUnderNodePercentage, stats.MaxOverNodePercentage, int(time.Now().Sub(start)/time.Second))
}
