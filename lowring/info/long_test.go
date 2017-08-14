// +build long
//
// Note that mirror checking can take quite a while, especially as the node
// counts get high, but you can enable it with the -mirrorchecking flag.
//
// If you're going to run this with profiling in mind, you probably want to run
// with -quick which will disable most ring analysis allowing you to focus on
// the ring code.
//
// go test -cpuprofile info.test.cpuprofile -timeout 24h -run TestLong -tags=long -quick

package info

import (
	"flag"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/gholt/ring/lowring"
)

var quick = flag.Bool("quick", false, "turn off nearly all analysis; useful when profiling")
var mirrorchecking = flag.Bool("mirrorchecking", false, "enable node mirror checking")

func TestLong(t *testing.T) {
	for _, varyingCapacities := range []bool{false, true} {
		fmt.Println()
		headerEmitted = false
		for _, zones := range []int{10, 50, 100, 200} {
			longTester(t, zones, varyingCapacities)
		}
	}
}

func longTester(t *testing.T, zones int, varyingCapacities bool) {
	previousInfo = nil
	replicaCount := 3
	ring := lowring.New(replicaCount)
	capacity := 100
	if varyingCapacities {
		capacity = 1
	}
	servers := 50
	devices := 2
	for zone := 0; zone < zones; zone++ {
		zoneGroup := len(ring.GroupToGroup)
		ring.GroupToGroup = append(ring.GroupToGroup, 0)
		for server := 0; server < servers; server++ {
			serverGroup := len(ring.GroupToGroup)
			ring.GroupToGroup = append(ring.GroupToGroup, zoneGroup)
			for device := 0; device < devices; device++ {
				ring.AddNode(capacity, serverGroup)
				if varyingCapacities {
					capacity++
					if capacity > 100 {
						capacity = 1
					}
				}
			}
		}
	}
	longRebalancer(t, "i", ring)

	ring.NodeToCapacity[len(ring.NodeToCapacity)/6] = -1
	ring.NodeToCapacity[len(ring.NodeToCapacity)/6*2] = -1
	ring.NodeToCapacity[len(ring.NodeToCapacity)/6*3] = -1
	ring.NodeToCapacity[len(ring.NodeToCapacity)/6*4] = -1
	ring.NodeToCapacity[len(ring.NodeToCapacity)/6*5] = -1
	longRebalancer(t, "d", ring)

	capacity = 234
	if varyingCapacities {
		capacity = 123
	}
	zoneGroup := len(ring.GroupToGroup)
	ring.GroupToGroup = append(ring.GroupToGroup, 0)
	for server := 0; server < servers; server++ {
		serverGroup := len(ring.GroupToGroup)
		ring.GroupToGroup = append(ring.GroupToGroup, zoneGroup)
		for device := 0; device < devices; device++ {
			ring.AddNode(capacity, serverGroup)
			if varyingCapacities {
				capacity++
				if capacity > 345 {
					capacity = 123
				}
			}
		}
	}
	longRebalancer(t, "a", ring)
}

var previousInfo *Info

func longRebalancer(t *testing.T, key string, ring *lowring.Ring) {
	start := time.Now()
	if !ring.Rebalanced.IsZero() {
		ring.Rebalanced = ring.Rebalanced.Add(-(time.Duration(ring.ReassignmentWait) * time.Minute))
	}
	ring.Rebalance(rand.New(rand.NewSource(0)).Intn)
	elapsed := time.Now().Sub(start)
	info, err := New(ring, *quick, *mirrorchecking)
	if err != nil {
		t.Fatal(err)
	}
	printInfo(t, key, ring, int(elapsed/time.Second), info, previousInfo)
	if info.AssignmentInWaitCountAtRebalancedTime == 0 {
		fmt.Println("      1")
		return
	}
	previousInfo = info
	iteration := 0
	for {
		iteration++
		start = time.Now()
		ring.Rebalanced = ring.Rebalanced.Add(-(time.Duration(ring.ReassignmentWait) * time.Minute))
		ring.Rebalance(rand.New(rand.NewSource(0)).Intn)
		elapsed := time.Now().Sub(start)
		info, err = New(ring, *quick, *mirrorchecking)
		if err != nil {
			t.Fatal(err)
		}
		if info.AssignmentInWaitCountAtRebalancedTime == 0 {
			fmt.Printf(" %6d\n", iteration)
			break
		}
		fmt.Println()
		printInfo(t, " ", ring, int(elapsed/time.Second), info, previousInfo)
		previousInfo = info
	}
}

var headerEmitted = false

func printInfo(t *testing.T, key string, ring *lowring.Ring, seconds int, info *Info, previousInfo *Info) {
	if !headerEmitted {
		worstReplicaBalance := "replica imbalance"
		for len(worstReplicaBalance) < info.ReplicaCount*7+1 {
			if len(worstReplicaBalance)%2 == 0 {
				worstReplicaBalance += " "
			} else {
				worstReplicaBalance = " " + worstReplicaBalance
			}
		}
		risky := "risky partitions"
		for len(risky) < info.TierCount*10+9 {
			if len(risky)%2 == 0 {
				risky += " "
			} else {
				risky = " " + risky
			}
		}
		fmt.Printf("  nodes dr di capacity   part       moving       under   over %s %s   mirror  secs stable\n", worstReplicaBalance, risky)
		//          - 12345 12 12 12345678 1234567 [1234567 12.45%] 12.45% 12.45%       [123 12%] 1234
		headerEmitted = true
	}
	replicaToMost := make([]string, info.ReplicaCount)
	for replica := 0; replica < info.ReplicaCount; replica++ {
		if *quick {
			replicaToMost[replica] = "      "
		} else {
			replicaToMost[replica] = fmt.Sprintf("%5.02f%%", info.ReplicaToMost[replica]*100-100/float64(info.ReplicaCount))
		}
	}
	tierToRisky := make([]int, info.TierCount)
	if !*quick {
		for tier, r := range info.TierToRiskyPartitions {
			tierToRisky[tier] += len(r)
		}
	}
	risky := make([]string, info.TierCount+1)
	if *quick {
		risky = []string{"    ", "    ", "    "}
	} else {
		risky[0] = fmt.Sprintf("%d %4.02f%%", len(info.NodeLevelRiskyPartitions), float64(len(info.NodeLevelRiskyPartitions))/float64(info.PartitionCount)*100)
		for tier, count := range tierToRisky {
			risky[info.TierCount-tier] = fmt.Sprintf("%3d %4.02f%%", count, float64(count)/float64(info.PartitionCount)*100)
		}
		for _, riskyPartition := range info.NodeLevelRiskyPartitions {
			fmt.Printf("          %#v", riskyPartition)
			for replica := 0; replica < info.ReplicaCount; replica++ {
				fmt.Print(" ", ring.ReplicaToPartitionToNode[replica][riskyPartition.Partition])
			}
			fmt.Println()
		}
	}
	for _, warning := range info.Warnings {
		fmt.Println("          " + warning)
	}
	// if info.MostUnderweight <= -0.1 {
	//  fmt.Printf("          %d is most underweight with %d assignments\n", info.MostUnderweightNode, info.NodeToAssignmentCount[info.MostUnderweightNode])
	// }
	if info.MostOverweight >= 0.1 {
		if previousInfo == nil {
			fmt.Printf("          %d is most overweight with %d assignments\n", info.MostOverweightNode, info.NodeToAssignmentCount[info.MostOverweightNode])
		} else {
			fmt.Printf("          %d is most overweight with %d assignments; it had %d previously\n", info.MostOverweightNode, info.NodeToAssignmentCount[info.MostOverweightNode], previousInfo.NodeToAssignmentCount[info.MostOverweightNode])
		}
	}
	mirror := "[       ]"
	if *mirrorchecking {
		mirror = fmt.Sprintf("[%3d %2d%%]", info.WorstMirrorCount, int(info.WorstMirrorPercentage*100+.5))
	}
	if key == " " {
		fmt.Printf("                               [%7d %5.02f%%] %5.02f%% %5.02f%% %v %v %s %4d", info.AssignmentInWaitCountAtRebalancedTime, float64(info.AssignmentInWaitCountAtRebalancedTime)/float64(info.AssignmentCount)*100, -info.MostUnderweight*100, info.MostOverweight*100, replicaToMost, risky, mirror, seconds)
	} else {
		fmt.Printf("%s %5d %2d %2d %8d %7d [%7d %5.02f%%] %5.02f%% %5.02f%% %v %v %s %4d", key, info.NodeCount, info.DrainingNodeCount, info.DisabledNodeCount, info.TotalCapacity, info.PartitionCount, info.AssignmentInWaitCountAtRebalancedTime, float64(info.AssignmentInWaitCountAtRebalancedTime)/float64(info.AssignmentCount)*100, -info.MostUnderweight*100, info.MostOverweight*100, replicaToMost, risky, mirror, seconds)
	}
}
