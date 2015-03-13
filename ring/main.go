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
		fmt.Printf("Syntax: %s <file> [command]\n", filepath.Base(args[0]))
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
