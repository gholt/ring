// +build long
//
// Note that mirror checking can take quite a while, especially as the node
// counts get high, but you can enable it with the -mirrorchecking flag.
//
// If you're going to run this with profiling in mind, you probably want to run
// with -quick which will disable most ring analysis allowing you to focus on
// the ring code.
// go test -cpuprofile info.test.cpuprofile -timeout 24h -run TestLong -tags=long -quick
//
// Below is the output of a full run with mirrorchecking:
//
//   nodes dr di capacity   part       moving       under   over    replica imbalance         risky partitions          mirror  secs stable
// i  1000  0  0   100000   65536 [      0  0.00%]  0.31%  0.20% [ 0.34%  0.34%  0.34%] [0 0.00%   0 0.00%   0 0.00%] [ 63 32%]    0      1
// d  1000  0  5    99500  131072 [   1968  0.50%]  0.05%  0.20% [ 0.08%  0.08%  0.08%] [0 0.00%   0 0.00%   0 0.00%] [126 32%]    0      1
// a  1100  0  5   122900  131072 [  74700 19.00%]  0.61%  0.33% [ 0.21%  0.21%  0.21%] [0 0.00%   0 0.00%   0 0.00%] [ 77 24%]    0      1
// i  5000  0  0   500000  524288 [      0  0.00%]  0.18%  0.14% [ 0.11%  0.11%  0.11%] [0 0.00%   0 0.00%   0 0.00%] [  5  2%]    7      1
// d  5000  0  5   499500  524288 [   1573  0.10%]  0.28%  0.04% [ 0.11%  0.11%  0.11%] [0 0.00%   0 0.00%   0 0.00%] [  5  2%]    0      1
// a  5100  0  5   522900  524288 [  70200  4.46%]  0.26%  0.73% [ 0.22%  0.22%  0.22%] [0 0.00%   0 0.00%   0 0.00%] [  5  2%]    0      1
// i 10000  0  0  1000000 1048576 [      0  0.00%]  0.18%  0.14% [ 0.11%  0.11%  0.11%] [0 0.00%   0 0.00%   0 0.00%] [  4  1%]   23      1
// d 10000  0  5   999500 1048576 [   1574  0.05%]  0.23%  0.09% [ 0.11%  0.11%  0.11%] [0 0.00%   0 0.00%   0 0.00%] [  4  1%]    0      1
// a 10100  0  5  1022900 1048576 [  71700  2.28%]  0.50%  0.48% [ 0.22%  0.22%  0.22%] [0 0.00%   0 0.00%   0 0.00%] [  4  1%]    1      1
// i 20000  0  0  2000000 2097152 [      0  0.00%]  0.18%  0.14% [ 0.11%  0.11%  0.11%] [0 0.00%   0 0.00%   0 0.00%] [  4  1%]   83      1
// d 20000  0  5  1999500 2097152 [   1574  0.03%]  0.21%  0.11% [ 0.11%  0.11%  0.11%] [0 0.00%   0 0.00%   0 0.00%] [  4  1%]    4      1
// a 20100  0  5  2022900 2097152 [  72600  1.15%]  0.65%  0.32% [ 0.22%  0.22%  0.22%] [0 0.00%   0 0.00%   0 0.00%] [  4  1%]    5      1
//
//   nodes dr di capacity   part       moving       under   over    replica imbalance         risky partitions          mirror  secs stable
// i  1000  0  0    50500 8388608 [      0  0.00%]  0.01%  0.54% [ 0.02%  0.02%  0.02%] [0 0.00%   0 0.00%   0 0.00%] [ 16  3%]   58      1
// d  1000  0  5    50205 8388608 [ 147006  0.58%]  0.01%  0.55% [ 0.01%  0.01%  0.01%] [0 0.00%   0 0.00%   0 0.00%] [ 16  3%]    2      1
// a  1100  0  5    67455 8388608 [6429526 25.55%]  0.29%  0.05% [ 0.06%  0.06%  0.06%] [0 0.00%   0 0.00%   0 0.00%] [ 11  3%]  388
//                                [   3857  0.02%]  0.29%  0.02% [ 0.02%  0.04%  0.04%] [0 0.00%   0 0.00%   0 0.00%] [ 11  3%]    8
//                                [    996  0.00%]  0.29%  0.02% [ 0.02%  0.02%  0.02%] [0 0.00%   0 0.00%   0 0.00%] [ 11  3%]    3
//                                [    369  0.00%]  0.29%  0.02% [ 0.02%  0.01%  0.01%] [0 0.00%   0 0.00%   0 0.00%] [ 11  3%]    2
//                                [    169  0.00%]  0.29%  0.02% [ 0.02%  0.02%  0.01%] [0 0.00%   0 0.00%   0 0.00%] [ 11  3%]    1
//                                [     65  0.00%]  0.29%  0.01% [ 0.02%  0.02%  0.01%] [0 0.00%   0 0.00%   0 0.00%] [ 11  3%]    1
//                                [     45  0.00%]  0.29%  0.01% [ 0.02%  0.01%  0.01%] [0 0.00%   0 0.00%   0 0.00%] [ 11  3%]    1
//                                [     25  0.00%]  0.29%  0.01% [ 0.01%  0.01%  0.01%] [0 0.00%   0 0.00%   0 0.00%] [ 11  3%]    1
//                                [      7  0.00%]  0.29%  0.01% [ 0.01%  0.01%  0.01%] [0 0.00%   0 0.00%   0 0.00%] [ 11  3%]    1
//                                [      3  0.00%]  0.29%  0.01% [ 0.01%  0.01%  0.01%] [0 0.00%   0 0.00%   0 0.00%] [ 11  3%]    1
//                                [      1  0.00%]  0.29%  0.01% [ 0.01%  0.01%  0.01%] [0 0.00%   0 0.00%   0 0.00%] [ 11  3%]    1     11
// i  5000  0  0   252500 8388608 [      0  0.00%]  0.03%  2.34% [ 0.33%  0.33%  0.33%] [0 0.00%   0 0.00%   0 0.00%] [  7  7%]   86      1
// d  5000  0  5   252200 8388608 [  29913  0.12%]  0.03%  2.22% [ 0.33%  0.33%  0.33%] [0 0.00%   0 0.00%   0 0.00%] [  7  7%]    3      1
// a  5100  0  5   269450 8388608 [1610934  6.40%]  0.43%  0.04% [ 0.10%  0.10%  0.10%] [0 0.00%   0 0.00%   0 0.00%] [  6  6%]   16      1
// i 10000  0  0   505000 8388608 [      0  0.00%]  1.67%  2.34% [ 0.67%  1.36%  0.67%] [0 0.00%   0 0.00%   0 0.00%] [  4  8%]  127      1
// d 10000  0  5   504705 8388608 [  14723  0.06%]  1.73%  2.28% [ 0.67%  1.36%  0.67%] [0 0.00%   0 0.00%   0 0.00%] [  4  8%]    4      1
// a 10100  0  5   521955 8388608 [ 831561  3.30%]  0.44%  0.06% [ 0.03%  0.03%  0.03%] [0 0.00%   0 0.00%   0 0.00%] [  4  8%]   10      1
// i 20000  0  0  1010000 8388608 [      0  0.00%]  0.12%  8.36% [ 0.10%  0.10%  0.10%] [0 0.00%   0 0.00%   0 0.00%] [  3 11%]  218      1
// d 20000  0  5  1009700 8388608 [   7473  0.03%]  0.12%  8.33% [ 0.05%  0.06%  0.06%] [0 0.00%   0 0.00%   0 0.00%] [  3 11%]   15      1
// a 20100  0  5  1026950 8388608 [ 422568  1.68%]  2.06%  0.13% [ 0.10%  0.10%  0.10%] [0 0.00%   0 0.00%   0 0.00%] [  3 13%]   18      1

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
