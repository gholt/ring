package main

import (
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"

	"github.com/gholt/ring"
	"gopkg.in/gholt/brimtext.v1"
)

func main() {
	if err := mainEntry(os.Args); err != nil {
		fmt.Fprintln(os.Stderr, brimtext.Sentence(err.Error()))
		os.Exit(1)
	}
}

func mainEntry(args []string) error {
	var r ring.Ring
	var b *ring.Builder
	var err error
	if len(args) < 2 {
		return helpCmd(args)
	}
	if len(args) > 2 && args[2] == "create" {
		return createCmd(args[1], args[3:])
	}
	if r, b, err = ringOrBuilder(args[1]); err != nil {
		return err
	}
	if len(args) < 3 {
		return mainCmd(r, b)
	}
	switch args[2] {
	case "node", "nodes":
		changed := false
		if changed, err = nodeCmd(r, b, args[3:], false); err != nil {
			return err
		}
		if changed {
			return persist(r, b, args[1])
		}
		return nil
	case "fullnode", "fullnodes":
		changed := false
		if changed, err = nodeCmd(r, b, args[3:], true); err != nil {
			return err
		}
		if changed {
			return persist(r, b, args[1])
		}
		return nil
	case "tier", "tiers":
		return tierCmd(r, b, args[3:])
	case "part", "partition":
		return partitionCmd(r, b, args[3:])
	case "add":
		if err = addOrSetCmd(r, b, args[3:], nil); err != nil {
			return err
		}
		return persist(r, b, args[1])
	case "remove":
		if err = removeCmd(r, b, args[3:]); err != nil {
			return err
		}
		return persist(r, b, args[1])
	case "ring":
		return ringCmd(r, b, args[1])
	case "pretend-elapsed":
		if err = pretendElapsedCmd(r, b, args[3:]); err != nil {
			return err
		}
		return persist(r, b, args[1])
	}
	return fmt.Errorf("unknown command: %#v", args[2])
}

func helpCmd(args []string) error {
	fmt.Printf(`Ring Command Line Development Version <github.com/gholt/ring>

%[1]s <file>
    Shows general information about the data within the <file>.

%[1]s <file> node [filter] ...
    Lists the nodes contained within the <file>, with optional filtering.
    Filters are attribute=value for exact string matches or attribute~=value
    for regular expression matches (per the http://golang.org/pkg/regexp/
    implementation). Of note, matches are case sensitive by default but you can
    prefix them with "(?i)" to switch to case insensitive, such as:
    
        meta~="(?i)model 50x"

    Available Filter Attributes:

    id          A node's id.
    active      Whether a node is active or not (use "true" or "false").
    capacity    A node's capacity.
    tier        Any tier of a node.
    tierX       A node's specific tier level specified by X.
    address     Any address of a node.
    addressX    A node's specific address index specified by X.
    meta        A node's meta attribute.

%[1]s <file> fullnode [filter] ...
    Same as "node" above, but always lists the full information for each
    matching node.

%[1]s <file> tier
    Lists the tiers in the ring or builder file.

%[1]s <ring-file> partition <value>
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
        max-partition-bits=<value>
            The <value> is a positive number that defaults to 23 and indicates
            the maximum bits for partition numbers, which limits the memory
            size of the ring but also limits the balancing ability. 23 allows
            for 8388608 partitions which, with a 3 replica ring, would use
            about 100M of memory.
        move-wait=<value>
            The <value> is a positive number that defaults to 60 and indicates
            the number of minutes to wait before reassigning a given replica of
            a partition. This is to give time for actual data to rebalance in
            the system before changing where it is assigned again.

%[1]s <builder-file> add [<name>=<value>] ...
    Adds a new node to the builder. Available attributes:
        active=<true|false>
            Nodes are active by default; this attribute can change that status.
        capacity=<value>
            The <value> is a decimal number from 0 to 4294967295 and indicates
            how much of the ring to assign to the node relative to other nodes.
        tierX=<value>
            Sets the value for the tier level specified by X.
            For example: "tier0=server233 tier1=zone74"
        addressX=<value>
            Sets the value for the address index specified by X.
            For example: "address0=10.1.2.3:12345 address1="192.1.2.3:54321"
            The <value> is a network address that can be accepted by Go's
            network library <golang.org/pkg/net/#Dial>; some examples:
            12.34.56.78:80 host.com:http [2001:db8::1]:http [fe80::1%%lo0]:80
        meta=[value]
            The [value] can be any string and is not used directly by the
            builder or ring. It can be used for notes about the node if
            desired, such as the model or serial number.

%[1]s <builder-file> remove id=<value>
    Removes the specified node from the builder. Often it's quicker to just set
    a node as inactive with "node id=<value> set active=false" and that will
    retain a record of the node having existed, but permanent removal from the
    record may be desired.

%[1]s <builder-file> node [filter] ... set [<name>=<value>] ...
    Updates existing node attributes. The filters are the same as for the
    generic "node" command above. The available attributes to set are the same
    as for the "add" command above. Examples:

    %[1]s my.builder node id=a839da27 set active=false
    %[1]s my.builder node tier0=server50 set capacity=3 meta="3T WD3003FZEX"

%[1]s <builder-file> ring
    Writes a new ring file based on the information contained in the builder.
    This may take a while if rebalancing ring assignments are needed. The ring
    file name will be the base name of the builder file plus a .ring extension.

%[1]s <builder-file> pretend-elapsed <minutes>
    Pretends the number of <minutes> have elapsed. Useful for testing and you
    want to work around the protections that restrict reassigning data quicker
    than the move-wait limit.
`, path.Base(args[0]))
	return nil
}

func mainCmd(r ring.Ring, b *ring.Builder) error {
	if r != nil {
		// TODO:
		//  Version Info (the value as well as the time translation)
		//  Number of tier levels
		//  Replica count
		//  Indication of how risky the assignments are:
		//      Replicas not in distinct tiers, nodes
		s := r.Stats()
		report := [][]string{
			[]string{brimtext.ThousandsSep(int64(s.PartitionCount), ","), "Partitions"},
			[]string{brimtext.ThousandsSep(int64(s.PartitionBitCount), ","), "Partition Bits"},
			[]string{brimtext.ThousandsSep(int64(s.NodeCount), ","), "Nodes"},
			[]string{brimtext.ThousandsSep(int64(s.InactiveNodeCount), ","), "Inactive Nodes"},
			[]string{brimtext.ThousandsSepU(s.TotalCapacity, ","), "Total Node Capacity"},
			[]string{fmt.Sprintf("%.02f%%", s.MaxUnderNodePercentage), fmt.Sprintf("Worst Underweight Node (ID %016x)", s.MaxUnderNodeID)},
			[]string{fmt.Sprintf("%.02f%%", s.MaxOverNodePercentage), fmt.Sprintf("Worst Overweight Node (ID %016x)", s.MaxOverNodeID)},
		}
		reportOpts := brimtext.NewDefaultAlignOptions()
		reportOpts.Alignments = []brimtext.Alignment{brimtext.Right, brimtext.Left}
		fmt.Print(brimtext.Align(report, reportOpts))
	}
	if b != nil {
		// TODO:
		//  Inactive node count
		//  Total capacity
		report := [][]string{
			[]string{brimtext.ThousandsSep(int64(len(b.Nodes())), ","), "Nodes"},
			[]string{brimtext.ThousandsSep(int64(b.ReplicaCount()), ","), "Replicas"},
			[]string{brimtext.ThousandsSep(int64(b.PointsAllowed()), ","), "Points Allowed"},
			[]string{brimtext.ThousandsSep(int64(b.MaxPartitionBitCount()), ","), "Max Partition Bits"},
			[]string{brimtext.ThousandsSep(int64(b.MoveWait()), ","), "Move Wait"},
		}
		reportOpts := brimtext.NewDefaultAlignOptions()
		reportOpts.Alignments = []brimtext.Alignment{brimtext.Right, brimtext.Left}
		fmt.Print(brimtext.Align(report, reportOpts))
		return nil
	}
	return nil
}

func nodeCmd(r ring.Ring, b *ring.Builder, args []string, full bool) (changed bool, err error) {
	var nodes ring.NodeSlice
	if r != nil {
		nodes = r.Nodes()
	} else {
		bnodes := b.Nodes()
		nodes = make(ring.NodeSlice, len(bnodes))
		for i := len(nodes) - 1; i >= 0; i-- {
			nodes[i] = bnodes[i]
		}
	}
	filterArgs := make([]string, 0)
	setArgs := make([]string, 0)
	setFound := false
	for _, arg := range args {
		if arg == "set" {
			setFound = true
			continue
		}
		if setFound {
			setArgs = append(setArgs, arg)
		} else {
			filterArgs = append(filterArgs, arg)
		}
	}
	if len(setArgs) > 0 && b == nil {
		err = fmt.Errorf("set is only valid for builder files")
		return
	}
	if nodes, err = nodes.Filter(filterArgs); err != nil {
		return
	}
	if len(nodes) > 0 && len(setArgs) > 0 {
		for _, n := range nodes {
			if err = addOrSetCmd(r, b, setArgs, n.(ring.BuilderNode)); err != nil {
				return
			}
			changed = true
		}
	}
	if full || len(nodes) == 1 {
		first := true
		for _, n := range nodes {
			if first {
				first = false
			} else {
				fmt.Println()
			}
			report := [][]string{
				[]string{"ID:", fmt.Sprintf("%016x", n.ID())},
				[]string{"Active:", fmt.Sprintf("%v", n.Active())},
				[]string{"Capacity:", fmt.Sprintf("%d", n.Capacity())},
				[]string{"Tiers:", strings.Join(n.Tiers(), "\n")},
				[]string{"Addresses:", strings.Join(n.Addresses(), "\n")},
				[]string{"Meta:", n.Meta()},
			}
			fmt.Print(brimtext.Align(report, nil))
		}
		return
	}
	header := []string{
		"ID",
		"Active",
		"Capacity",
		"Address",
		"Meta",
	}
	report := [][]string{header}
	reportAlign := brimtext.NewDefaultAlignOptions()
	reportAlign.Alignments = []brimtext.Alignment{
		brimtext.Left,
		brimtext.Right,
		brimtext.Right,
		brimtext.Right,
		brimtext.Left,
	}
	reportLine := func(n ring.Node) []string {
		return []string{
			fmt.Sprintf("%016x", n.ID()),
			fmt.Sprintf("%v", n.Active()),
			fmt.Sprintf("%d", n.Capacity()),
			n.Address(0),
			n.Meta(),
		}
	}
	for _, n := range nodes {
		if n.Active() {
			report = append(report, reportLine(n))
		}
	}
	for _, n := range nodes {
		if !n.Active() {
			report = append(report, reportLine(n))
		}
	}
	fmt.Print(brimtext.Align(report, reportAlign))
	return
}

func tierCmd(r ring.Ring, b *ring.Builder, args []string) error {
	var tiers [][]string
	if r == nil {
		tiers = b.Tiers()
	} else {
		tiers = r.Tiers()
	}
	report := [][]string{
		[]string{"Tier", "Existing"},
		[]string{"Level", "Values"},
	}
	reportOpts := brimtext.NewDefaultAlignOptions()
	reportOpts.Alignments = []brimtext.Alignment{brimtext.Right, brimtext.Left}
	fmted := false
OUT:
	for _, values := range tiers {
		for _, value := range values {
			if strings.Contains(value, " ") {
				fmted = true
				break OUT
			}
		}
	}
	for level, values := range tiers {
		sort.Strings(values)
		var pvalue string
		if fmted {
			for _, value := range values {
				pvalue += fmt.Sprintf(" %#v", value)
			}
			pvalue = strings.Trim(pvalue, " ")
		} else {
			pvalue = strings.Join(values, " ")
		}
		report = append(report, []string{
			strconv.Itoa(level),
			strings.Trim(pvalue, " "),
		})
	}
	fmt.Print(brimtext.Align(report, reportOpts))
	return nil
}

func partitionCmd(r ring.Ring, b *ring.Builder, args []string) error {
	if b != nil {
		return fmt.Errorf("cannot use partition command with a builder; generate a ring and use it on that")
	}
	if len(args) != 1 {
		return fmt.Errorf("use the syntax: partition <value>")
	}
	p, err := strconv.Atoi(args[0])
	if err != nil {
		return err
	}
	first := true
	for _, n := range r.ResponsibleNodes(uint32(p)) {
		if first {
			first = false
		} else {
			fmt.Println()
		}
		report := [][]string{
			[]string{"ID:", fmt.Sprintf("%016x", n.ID())},
			[]string{"Capacity:", fmt.Sprintf("%d", n.Capacity())},
			[]string{"Tiers:", strings.Join(n.Tiers(), "\n")},
			[]string{"Addresses:", strings.Join(n.Addresses(), "\n")},
			[]string{"Meta:", n.Meta()},
		}
		fmt.Print(brimtext.Align(report, nil))
	}
	return nil
}

func createCmd(filename string, args []string) error {
	replicaCount := 3
	pointsAllowed := 1
	maxPartitionBitCount := 23
	moveWait := 60
	var err error
	for _, arg := range args {
		switch arg {
		case "replicas":
			if replicaCount, err = strconv.Atoi(arg); err != nil {
				return err
			}
			if replicaCount < 1 {
				replicaCount = 1
			}
		case "points-allowed":
			if pointsAllowed, err = strconv.Atoi(arg); err != nil {
				return err
			}
			if pointsAllowed < 0 {
				pointsAllowed = 0
			} else if pointsAllowed > 255 {
				pointsAllowed = 255
			}
		case "max-partition-bits":
			if maxPartitionBitCount, err = strconv.Atoi(arg); err != nil {
				return err
			}
			if maxPartitionBitCount < 1 {
				maxPartitionBitCount = 1
			} else if maxPartitionBitCount > 64 {
				maxPartitionBitCount = 64
			}
		case "move-wait":
			if moveWait, err = strconv.Atoi(arg); err != nil {
				return err
			}
			if moveWait < 0 {
				moveWait = 0
			} else if moveWait > math.MaxUint16 {
				moveWait = math.MaxUint16
			}
		}
	}
	if _, err = os.Stat(filename); err == nil {
		return fmt.Errorf("file already exists")
	}
	if !os.IsNotExist(err) {
		return err
	}
	var f *os.File
	if f, err = os.Create(filename); err != nil {
		return err
	}
	b := ring.NewBuilder()
	b.SetReplicaCount(replicaCount)
	b.SetPointsAllowed(byte(pointsAllowed))
	b.SetMaxPartitionBitCount(uint16(maxPartitionBitCount))
	b.SetMoveWait(uint16(moveWait))
	if err = b.Persist(f); err != nil {
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return nil
}

func addOrSetCmd(r ring.Ring, b *ring.Builder, args []string, n ring.BuilderNode) error {
	if r != nil {
		return fmt.Errorf("cannot add a node to ring; use with a builder instead")
	}
	active := true
	capacity := uint32(1)
	var tiers []string
	var addresses []string
	meta := ""
	for _, arg := range args {
		sarg := strings.SplitN(arg, "=", 2)
		if len(sarg) != 2 {
			return fmt.Errorf(`invalid expression %#v; needs "="`, arg)
		}
		if sarg[0] == "" {
			return fmt.Errorf(`invalid expression %#v; nothing was left of "="`, arg)
		}
		if sarg[1] == "" {
			return fmt.Errorf(`invalid expression %#v; nothing was right of "="`, arg)
		}
		switch sarg[0] {
		case "active":
			switch sarg[1] {
			case "true":
				active = true
			case "false":
				active = false
			default:
				return fmt.Errorf(`invalid expression %#v; use "true" or "false" for the value of active`, arg)
			}
			if n != nil {
				n.SetActive(active)
			}
		case "capacity":
			c, err := strconv.Atoi(sarg[1])
			if err != nil {
				return fmt.Errorf("invalid expression %#v; %s", arg, err)
			}
			if c < 0 {
				return fmt.Errorf("invalid expression %#v; min is 0", arg)
			}
			if c > math.MaxUint32 {
				return fmt.Errorf("invalid expression %#v; max is %d", arg, math.MaxUint32)
			}
			capacity = uint32(c)
			if n != nil {
				n.SetCapacity(capacity)
			}
		case "meta":
			meta = sarg[1]
			if n != nil {
				n.SetMeta(meta)
			}
		default:
			if strings.HasPrefix(sarg[0], "tier") {
				level, err := strconv.Atoi(sarg[0][4:])
				if err != nil {
					return fmt.Errorf("invalid expression %#v; %#v doesn't specify a number", arg, sarg[0][4:])
				}
				if level < 0 {
					return fmt.Errorf("invalid expression %#v; minimum level is 0", arg)
				}
				if len(tiers) <= level {
					t := make([]string, level+1)
					copy(t, tiers)
					tiers = t
				}
				tiers[level] = sarg[1]
				if n != nil {
					n.SetTier(level, tiers[level])
				}
			} else if strings.HasPrefix(sarg[0], "address") {
				index, err := strconv.Atoi(sarg[0][7:])
				if err != nil {
					return fmt.Errorf("invalid expression %#v; %#v doesn't specify a number", arg, sarg[0][4:])
				}
				if index < 0 {
					return fmt.Errorf("invalid expression %#v; minimum index is 0", arg)
				}
				if len(addresses) <= index {
					a := make([]string, index+1)
					copy(a, addresses)
					addresses = a
				}
				addresses[index] = sarg[1]
				if n != nil {
					n.SetAddress(index, addresses[index])
				}
			}
		}
	}
	if n == nil {
		n = b.AddNode(active, capacity, tiers, addresses, meta)
		report := [][]string{
			[]string{"ID:", fmt.Sprintf("%016x", n.ID())},
			[]string{"Active:", fmt.Sprintf("%v", n.Active())},
			[]string{"Capacity:", fmt.Sprintf("%d", n.Capacity())},
			[]string{"Tiers:", strings.Join(n.Tiers(), "\n")},
			[]string{"Addresses:", strings.Join(n.Addresses(), "\n")},
			[]string{"Meta:", n.Meta()},
		}
		fmt.Print(brimtext.Align(report, nil))
	}
	return nil
}

func removeCmd(r ring.Ring, b *ring.Builder, args []string) error {
	if r != nil {
		return fmt.Errorf("cannot remove a node from a ring; use with a builder instead")
	}
	if len(args) != 1 || !strings.HasPrefix(args[0], "id=") {
		return fmt.Errorf("must specify node to remove with id=<value>")
	}
	id, err := strconv.ParseUint(args[0][3:], 16, 64)
	if err != nil {
		return fmt.Errorf("invalid id %#v", args[0][3:])
	}
	b.RemoveNode(id)
	return nil
}

func ringCmd(r ring.Ring, b *ring.Builder, filename string) error {
	if b == nil {
		return fmt.Errorf("only valid for builder files")
	}
	r = b.Ring()
	if err := persist(nil, b, filename); err != nil {
		return err
	}
	return persist(r, nil, strings.TrimSuffix(filename, ".builder")+".ring")
}

func pretendElapsedCmd(r ring.Ring, b *ring.Builder, args []string) error {
	if b == nil {
		return fmt.Errorf("only valid for builder files")
	}
	if len(args) != 1 {
		return fmt.Errorf("syntax: <minutes>")
	}
	m, err := strconv.Atoi(args[0])
	if err != nil {
		return fmt.Errorf("could not parse %#v: %s", args[0], err.Error())
	}
	if m < 0 {
		return fmt.Errorf("cannot pretend to go backwards in time")
	}
	if m > math.MaxUint16 {
		return fmt.Errorf("cannot pretend to elapse more than %d minutes", math.MaxUint16)
	}
	b.PretendElapsed(uint16(m))
	return nil
}

func ringOrBuilder(fileName string) (r ring.Ring, b *ring.Builder, err error) {
	var f *os.File
	if f, err = os.Open(fileName); err != nil {
		return
	}
	var gf *gzip.Reader
	if gf, err = gzip.NewReader(f); err != nil {
		return
	}
	header := make([]byte, 16)
	if _, err = io.ReadFull(gf, header); err != nil {
		return
	}
	if string(header[:5]) == "RINGv" {
		gf.Close()
		if _, err = f.Seek(0, 0); err != nil {
			return
		}
		r, err = ring.LoadRing(f)
	} else if string(header[:12]) == "RINGBUILDERv" {
		gf.Close()
		if _, err = f.Seek(0, 0); err != nil {
			return
		}
		b, err = ring.LoadBuilder(f)
	}
	return
}

func persist(r ring.Ring, b *ring.Builder, filename string) error {
	dir, name := path.Split(filename)
	if dir == "" {
		dir = "."
	}
	f, err := ioutil.TempFile(dir, name+".")
	if err != nil {
		return err
	}
	tmp := f.Name()
	if r != nil {
		err = r.Persist(f)
	} else {
		err = b.Persist(f)
	}
	if err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return os.Rename(path.Join(dir, tmp), filename)
}
