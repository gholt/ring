package ring

import (
	"fmt"
	"os"
	"runtime/pprof"
	"testing"
	"time"
)

type TestItem struct {
	active     bool
	capacity   uint64
	tierValues []int
}

func (item *TestItem) Active() bool {
	return item.active
}

func (item *TestItem) Capacity() uint64 {
	return item.capacity
}

func (item *TestItem) TierValues() []int {
	return item.tierValues
}

func TestNewRing(t *testing.T) {
	f, err := os.Create("ring_test.prof")
	if err != nil {
		t.Fatal(err)
	}
	pprof.StartCPUProfile(f)
	fmt.Println(" items inactive partitions capacity maxunder maxover seconds")
	for zones := 10; zones <= 200; {
		helperTestNewRing(t, zones)
		if zones < 100 {
			zones += 10
		} else {
			zones += 100
		}
	}
	pprof.StopCPUProfile()
}

func helperTestNewRing(t *testing.T, zones int) {
	ring := NewRing(3).(*ringImpl)
	//capacity := uint64(1)
	capacity := uint64(100)
	for zone := 0; zone < zones; zone++ {
		for server := 0; server < 50; server++ {
			for device := 0; device < 2; device++ {
				ring.Add(&TestItem{active: true, capacity: capacity, tierValues: []int{device, server, zone}})
				//capacity++
				//if capacity > 100 {
				//	capacity = 1
				//}
			}
		}
	}
	start := time.Now()
	ring.Rebalance()
	stats := ring.Stats()
	fmt.Printf("%6d %8d %10d %8d %7.02f%% %6.02f%% %7d\n", stats.ItemCount, stats.InactiveItemCount, stats.PartitionCount, stats.TotalCapacity, stats.MaxUnderItemPercentage, stats.MaxOverItemPercentage, int(time.Now().Sub(start)/time.Second))
	ring.Item(25).(*TestItem).active = false
	start = time.Now()
	ring.Rebalance()
	stats = ring.Stats()
	fmt.Printf("%6d %8d %10d %8d %7.02f%% %6.02f%% %7d\n", stats.ItemCount, stats.InactiveItemCount, stats.PartitionCount, stats.TotalCapacity, stats.MaxUnderItemPercentage, stats.MaxOverItemPercentage, int(time.Now().Sub(start)/time.Second))
}
