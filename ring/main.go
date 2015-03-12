package main

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"

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
	if len(args) < 2 {
		fmt.Printf("Syntax: %s <file> [command]\n", filepath.Base(args[0]))
		return nil
	}
	f, err := os.Open(args[1])
	if err != nil {
		return err
	}
	gf, err := gzip.NewReader(f)
	if err != nil {
		return err
	}
	header := make([]byte, 16)
	_, err = io.ReadFull(gf, header)
	if err != nil {
		return err
	}
	if string(header[:5]) == "RINGv" {
		gf.Close()
		_, err = f.Seek(0, 0)
		if err != nil {
			return err
		}
		return mainRing(args, f)
	} else if string(header[:12]) == "RINGBUILDERv" {
		gf.Close()
		_, err = f.Seek(0, 0)
		if err != nil {
			return err
		}
		return mainBuilder(args, f)
	}
	return fmt.Errorf("unknown file type")
}

func mainRing(args []string, f *os.File) error {
	r, err := ring.LoadRing(f)
	if err != nil {
		return err
	}
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
	fmt.Println(brimtext.Align(report, reportOpts))
	return nil
}

func mainBuilder(args []string, f *os.File) error {
	b, err := ring.LoadBuilder(f)
	if err != nil {
		return err
	}
	fmt.Printf("Builder %p loaded fine.\n", b)
	return nil
}
