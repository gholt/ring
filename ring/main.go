package main

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"

	"github.com/gholt/brimtext-v1"
	"github.com/gholt/ring"
)

func main() {
	if err := main2(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func main2(args []string) error {
	var err error
	if len(args) < 2 {
		fmt.Printf(`Ring Command Line Development Version <github.com/gholt/ring>

%[1]s <file>
    Shows general information about the data within the <file>.

%[1]s <file> node
    Lists the nodes contained within the <file>.

%[1]s <file> node id=<value>
    Lists detailed information about the node identified.

%[1]s <file> node address=<value>
    Lists detailed information about the node(s) identified by the address.

%[1]s <file> node tiers=[names]
    Lists detailed information about the node(s) identified by the comma
    separated list of tier names. Use * to match all names for a given tier.

%[1]s <file> node meta=<value>
    Lists detailed information about the node(s) identified by the meta value.

%[1]s <file> partition <value>
    Lists information about the given partition's node assignments.

%[1]s <builder-file> create [<name>=<value>] [<name>=<value>] ...
    Creates a new <builder-file> with any optional attributes. Attributes:
        replicas=<value>
            The <value> is a positive number that defaults to 3.
        points-allowed=<value>
            The <value> is a positive number that defaults to 1 and indicates
            the number of percentage points the builder should try to keep each
            node balanced within. In other words, by default, plus or minus one
            percent assignments per node based on the relative capacity.
        max-partition-bit-count=<value>
            The <value> is a positive number that defaults to 23 and indicates
            the maximum bit count for partition numbers, which limits the
            memory size of the ring but also limits the balancing ability. 23
            allows for 8388608 partitions which, with a 3 replica ring, would
            use about 100M of memory.
        move-wait=<value>
            The <value> is a positive number that defaults to 60 and indicates
            the number of minutes to wait before reassigning a given replica of
            a partition. This is to give time for actual data to rebalance in
            the system before changing where it is assigned again.

%[1]s <builder-file> node <name>=<value> [<name>=<value>] ...
    Sets node attributes, creating a new node if needed. Attributes:
        id=<value>
            The <value> is a hex number from 0 to ffffffffffffffff. If not
            specified, a new unique id will be generated, creating a new node.
        address=<value>
            The <value> is a network address that can be accepted by Go's
            network library <golang.org/pkg/net/#Dial>; some examples:
            12.34.56.78:80 host.com:http [2001:db8::1]:http [fe80::1%%lo0]:80
        capacity=<value>
            The <value> is a decimal number from 0 to 4294967295 and indicates
            how much of the ring to assign to the node relative to other nodes.
        inactive=<true|false>
            Nodes are active by default; this attribute can toggle that status.
        tiers=[names]
            If [names] is empty, all tier information for the node will be
            cleared. Otherwise, names should be a comma separated list of tier
            names, sorted from lowest tier to highest.
        meta=[value]
            The [value] can be any string and is not used directly by the
            builder or ring. It can be used for notes about the node if
            desired, such as the model or serial number.

%[1]s <builder-file> ring
    Writes a new ring file based on the information contained in the builder.
    This may take a while if rebalancing ring assignments are needed. The ring
    file's name will be the base name of the builder file plus a .ring
    extension.
`, filepath.Base(args[0]))
		return nil
	} else if len(args) > 2 && args[2] == "create" {
		replicas := 3
		if len(args) > 3 {
			if replicas, err = strconv.Atoi(args[3]); err != nil {
				return err
			} else if replicas < 1 {
				return fmt.Errorf("replicas cannot be less than 1")
			}
		}
		if _, err = os.Stat(args[1]); err == nil {
			return fmt.Errorf("file already exists")
		} else if !os.IsNotExist(err) {
			return err
		} else if f, err := os.Create(args[1]); err != nil {
			return err
		} else {
			b := ring.NewBuilder(replicas)
			if err = b.Persist(f); err != nil {
				return err
			} else if err = f.Close(); err != nil {
				return err
			} else {
				return nil
			}
		}
	} else if tf, rb, err := ringOrBuilder(args[1]); err != nil {
		return err
	} else if tf {
		return mainRing(args, rb.(*ring.Ring))
	} else {
		return mainBuilder(args, rb.(*ring.Builder))
	}
}

func ringOrBuilder(fileName string) (bool, interface{}, error) {
	if f, err := os.Open(fileName); err != nil {
		return false, nil, err
	} else if gf, err := gzip.NewReader(f); err != nil {
		return false, nil, err
	} else {
		header := make([]byte, 16)
		if _, err = io.ReadFull(gf, header); err != nil {
			return false, nil, err
		} else if string(header[:5]) == "RINGv" {
			gf.Close()
			if _, err = f.Seek(0, 0); err != nil {
				return false, nil, err
			} else if r, err := ring.LoadRing(f); err != nil {
				return false, nil, err
			} else {
				return true, r, nil
			}
		} else if string(header[:12]) == "RINGBUILDERv" {
			gf.Close()
			if _, err = f.Seek(0, 0); err != nil {
				return false, nil, err
			} else if b, err := ring.LoadBuilder(f); err != nil {
				return false, nil, err
			} else {
				return false, b, nil
			}
		}
	}
	return false, nil, fmt.Errorf("Should be impossible to get here")
}

func mainRing(args []string, r *ring.Ring) error {
	s := r.Stats()
	report := [][]string{
		[]string{brimtext.ThousandsSep(int64(s.PartitionCount), ","), "Partitions"},
		[]string{brimtext.ThousandsSep(int64(s.PartitionBitCount), ","), "Partition Bits"},
		[]string{brimtext.ThousandsSep(int64(s.NodeCount), ","), "Nodes"},
		[]string{brimtext.ThousandsSep(int64(s.InactiveNodeCount), ","), "Inactive Nodes"},
		[]string{brimtext.ThousandsSepU(s.TotalCapacity, ","), "Total Node Capacity"},
		[]string{fmt.Sprintf("%.02f%%", s.MaxUnderNodePercentage), fmt.Sprintf("Worst Underweight Node (ID %x)", r.Nodes()[s.MaxUnderNodeIndex].ID)},
		[]string{fmt.Sprintf("%.02f%%", s.MaxOverNodePercentage), fmt.Sprintf("Worst Overweight Node (ID %x)", r.Nodes()[s.MaxOverNodeIndex].ID)},
	}
	reportOpts := brimtext.NewDefaultAlignOptions()
	reportOpts.Alignments = []brimtext.Alignment{brimtext.Right, brimtext.Left}
	fmt.Print(brimtext.Align(report, reportOpts))
	return nil
}

func mainBuilder(args []string, b *ring.Builder) error {
	fmt.Printf("Builder %p loaded fine.\n", b)
	return nil
}
