package ring

import (
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gholt/brimtext"
)

// CLI is the "ring" command line interface; it's included in the ring package
// itself so you can easily stub it to whatever executable name you might want,
// with code like:
//
//      package main
//
//      import (
//      	"fmt"
//      	"os"
//
//      	"github.com/gholt/ring"
//      	"github.com/gholt/brimtext"
//      )
//
//      func main() {
//      	if err := ring.CLI(os.Args, os.Stdout); err != nil {
//      		fmt.Fprintln(os.Stderr, brimtext.Sentence(err.Error()))
//      		os.Exit(1)
//      	}
//      }
//
// The individual subcommands are also included in the ring package, prefixed
// with CLI, so you can build your command line interface with just the
// commands you want, or leverage them to build a web interface, etc.
func CLI(args []string, output io.Writer) error {
	var r Ring
	var b *Builder
	var err error
	if len(args) < 2 || (len(args) > 1 && args[1] == "help") {
		return CLIHelp(args, output)
	}
	if len(args) > 2 && args[2] == "create" {
		return CLICreate(args[1], args[3:], output)
	}
	if r, b, err = RingOrBuilder(args[1]); err != nil {
		return err
	}
	if len(args) < 3 {
		return CLIInfo(r, b, output)
	}
	switch args[2] {
	case "node", "nodes":
		changed := false
		if changed, err = CLINode(r, b, args[3:], false, output); err != nil {
			return err
		}
		if changed {
			return PersistRingOrBuilder(r, b, args[1])
		}
		return nil
	case "fullnode", "fullnodes":
		changed := false
		if changed, err = CLINode(r, b, args[3:], true, output); err != nil {
			return err
		}
		if changed {
			return PersistRingOrBuilder(r, b, args[1])
		}
		return nil
	case "tier", "tiers":
		return CLITier(r, b, args[3:], output)
	case "part", "partition":
		return CLIPartition(r, b, args[3:], output)
	case "add":
		if r != nil {
			return fmt.Errorf("cannot add a node to ring; use with a builder instead")
		}
		if err = CLIAddOrSet(b, args[3:], nil, output); err != nil {
			return err
		}
		return PersistRingOrBuilder(r, b, args[1])
	case "remove":
		if r != nil {
			return fmt.Errorf("cannot remove a node from a ring; use with a builder instead")
		}
		if err = CLIRemove(b, args[3:], output); err != nil {
			return err
		}
		return PersistRingOrBuilder(r, b, args[1])
	case "ring":
		if b == nil {
			return fmt.Errorf("only valid for builder files")
		}
		return CLIRing(b, args[1], output)
	case "pretend-elapsed":
		if b == nil {
			return fmt.Errorf("only valid for builder files")
		}
		if err = CLIPretendElapsed(b, args[3:], output); err != nil {
			return err
		}
		return PersistRingOrBuilder(r, b, args[1])
	case "config":
		if changed, err := CLIConfig(r, b, args[3:], output); err != nil {
			return err
		} else if changed {
			return PersistRingOrBuilder(r, b, args[1])
		}
		return nil
	case "config-file":
		if changed, err := CLIConfigFile(r, b, args[3:], output); err != nil {
			return err
		} else if changed {
			return PersistRingOrBuilder(r, b, args[1])
		}
		return nil
	}
	return fmt.Errorf("unknown command: %#v", args[2])
}

// CLIHelp outputs the help text for the default CLI.
func CLIHelp(args []string, output io.Writer) error {
	fmt.Fprintf(output, `Ring Command Line Development Version <github.com/gholt/ring>

%[1]s <file>
    Shows general information about the data within the <file>.

%[1]s <file> node [filter] ...
    Lists the nodes contained within the <file>, with optional filtering.
    Filters are attribute=value for exact string matches or attribute~=value
    for regular expression matches (per the http://golang.org/pkg/regexp/
    implementation). Multiple filters will be ANDed together; meaning the
    resulting list will match all the filters given. Of note, matches are case
    sensitive by default but you can prefix them with "(?i)" to switch to case
    insensitive, such as:
    
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
        config=<value>
            The <value> is the string to be stored as the global config value.
        config-file=<value>
            The <value> is the path to a file that will be byte encoded and
            stored as the global config value.
        id-bits=<value>
            The <value> is a number from 1 to 64 that defaults to 64 and
            indicates the number of bits to use for node IDs in the ring.
            Perhaps a strange setting, but limiting the bits may required or
            useful in applications that store or transmit node IDs.

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
            builder or  It can be used for notes about the node if
            desired, such as the model or serial number.
        config=<value>
            The <value> is the string to be stored in the node's config field.
        config-file=<value>
            The <value> is the path to a file that will be byte encoded and
            stored in the node's config field.

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

%[1]s <file> config [value]
    Displays or sets the global config in the provided ring or builder file.

%[1]s <file> config-file [path]
    Displays or sets the global config in the provided ring or builder file.
    This will output the raw bytes if no [path] is given, or it will set the
    config based on the contents of the file at the [path] given.
`, path.Base(args[0]))
	return nil
}

// CLIInfo outputs information about the ring or builder such as node count,
// partition count, etc.
//
// Provide either the ring or the builder, but not both; set the other to nil.
// Normally the results from RingOrBuilder.
func CLIInfo(r Ring, b *Builder, output io.Writer) error {
	if r != nil {
		// TODO:
		//  Indication of how risky the assignments are:
		//      Replicas not in distinct tiers, nodes
		s := r.Stats()
		report := [][]string{
			[]string{brimtext.ThousandsSep(int64(s.PartitionCount), ","), "Partitions"},
			[]string{brimtext.ThousandsSep(int64(s.PartitionBitCount), ","), "Partition Bits"},
			[]string{brimtext.ThousandsSep(int64(r.ReplicaCount()), ","), "Replicas"},
			[]string{brimtext.ThousandsSep(int64(s.NodeCount), ","), "Nodes"},
			[]string{brimtext.ThousandsSep(int64(s.InactiveNodeCount), ","), "Inactive Nodes"},
			[]string{brimtext.ThousandsSep(int64(len(r.Tiers())), ","), "Tier Levels"},
			[]string{brimtext.ThousandsSepU(s.TotalCapacity, ","), "Total Node Capacity"},
			[]string{fmt.Sprintf("%.02f%%", s.MaxUnderNodePercentage), fmt.Sprintf("Worst Underweight Node (ID %d)", s.MaxUnderNodeID)},
			[]string{fmt.Sprintf("%.02f%%", s.MaxOverNodePercentage), fmt.Sprintf("Worst Overweight Node (ID %d)", s.MaxOverNodeID)},
			[]string{"Version", fmt.Sprintf("%d   %s", r.Version(), time.Unix(0, r.Version()).Format("2006-01-02 15:04:05.000"))},
		}
		reportOpts := brimtext.NewDefaultAlignOptions()
		reportOpts.Alignments = []brimtext.Alignment{brimtext.Right, brimtext.Left}
		fmt.Fprint(output, brimtext.Align(report, reportOpts))
	}
	if b != nil {
		// TODO:
		//  Inactive node count
		//  Total capacity
		report := [][]string{
			[]string{brimtext.ThousandsSep(int64(len(b.Nodes())), ","), "Nodes"},
			[]string{brimtext.ThousandsSep(int64(b.ReplicaCount()), ","), "Replicas"},
			[]string{brimtext.ThousandsSep(int64(len(b.Tiers())), ","), "Tier Levels"},
			[]string{brimtext.ThousandsSep(int64(b.PointsAllowed()), ","), "Points Allowed"},
			[]string{brimtext.ThousandsSep(int64(b.MaxPartitionBitCount()), ","), "Max Partition Bits"},
			[]string{brimtext.ThousandsSep(int64(b.MoveWait()), ","), "Move Wait"},
			[]string{brimtext.ThousandsSep(int64(b.IDBits()), ","), "ID Bits"},
		}
		reportOpts := brimtext.NewDefaultAlignOptions()
		reportOpts.Alignments = []brimtext.Alignment{brimtext.Right, brimtext.Left}
		fmt.Fprint(output, brimtext.Align(report, reportOpts))
		return nil
	}
	return nil
}

// CLINode outputs a list of nodes in the ring or builder, with optional
// filtering and also allows setting attributes on those nodes; see the output
// of CLIHelp for detailed information.
//
// Provide either the ring or the builder, but not both; set the other to nil.
// Normally the results from RingOrBuilder.
func CLINode(r Ring, b *Builder, args []string, full bool, output io.Writer) (changed bool, err error) {
	var nodes NodeSlice
	if r != nil {
		nodes = r.Nodes()
	} else {
		bnodes := b.Nodes()
		nodes = make(NodeSlice, len(bnodes))
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
		if r != nil {
			return false, fmt.Errorf("cannot update nodes in a ring; use with a builder instead")
		}
		for _, n := range nodes {
			if err = CLIAddOrSet(b, setArgs, n.(BuilderNode), output); err != nil {
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
				fmt.Fprintln(output)
			}
			output.Write([]byte(CLINodeReport(n)))
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
	reportLine := func(n Node) []string {
		return []string{
			fmt.Sprintf("%d", n.ID()),
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
	fmt.Fprint(output, brimtext.Align(report, reportAlign))
	return
}

// CLITier outputs a list of tiers in the ring or builder; see the output of
// CLIHelp for detailed information.
//
// Provide either the ring or the builder, but not both; set the other to nil.
// Normally the results from RingOrBuilder.
func CLITier(r Ring, b *Builder, args []string, output io.Writer) error {
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
	fmt.Fprint(output, brimtext.Align(report, reportOpts))
	return nil
}

// CLIPartition outputs information about a partition's node assignments; see
// the output of CLIHelp for detailed information.
//
// Provide either the ring or the builder, but not both; set the other to nil.
// Normally the results from RingOrBuilder.
func CLIPartition(r Ring, b *Builder, args []string, output io.Writer) error {
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
			fmt.Fprintln(output)
		}
		output.Write([]byte(CLINodeReport(n)))
	}
	return nil
}

// CLICreate creates a new builder; see the output of CLIHelp for detailed
// information.
//
// Provide either the ring or the builder, but not both; set the other to nil.
// Normally the results from RingOrBuilder.
func CLICreate(filename string, args []string, output io.Writer) error {
	replicaCount := 3
	pointsAllowed := 1
	maxPartitionBitCount := 23
	moveWait := 60
	idBits := 64
	var config []byte
	var err error
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
		case "replicas":
			if replicaCount, err = strconv.Atoi(sarg[1]); err != nil {
				return err
			}
			if replicaCount < 1 {
				replicaCount = 1
			}
		case "points-allowed":
			if pointsAllowed, err = strconv.Atoi(sarg[1]); err != nil {
				return err
			}
			if pointsAllowed < 0 {
				pointsAllowed = 0
			} else if pointsAllowed > 255 {
				pointsAllowed = 255
			}
		case "max-partition-bits":
			if maxPartitionBitCount, err = strconv.Atoi(sarg[1]); err != nil {
				return err
			}
			if maxPartitionBitCount < 1 {
				maxPartitionBitCount = 1
			} else if maxPartitionBitCount > 64 {
				maxPartitionBitCount = 64
			}
		case "move-wait":
			if moveWait, err = strconv.Atoi(sarg[1]); err != nil {
				return err
			}
			if moveWait < 0 {
				moveWait = 0
			} else if moveWait > math.MaxUint16 {
				moveWait = math.MaxUint16
			}
		case "config":
			if sarg[1] == "" {
				config = nil
			} else {
				config = []byte(sarg[1])
			}
		case "config-file":
			config, err = ioutil.ReadFile(sarg[1])
			if err != nil {
				return fmt.Errorf("Error reading config file: %v", err)
			}
		case "id-bits":
			if idBits, err = strconv.Atoi(sarg[1]); err != nil {
				return err
			}
			if idBits < 1 || idBits > 64 {
				return fmt.Errorf("id-bits must be in the range 1-64; %d was given", idBits)
			}
		default:
			return fmt.Errorf("Invalid arg: %q in create cmd", arg)
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
	b := NewBuilder(idBits)
	b.SetConfig(config)
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

// CLIAddOrSet adds a new node or updates an existing node; see the output of
// CLIHelp for detailed information.
//
// Provide either the ring or the builder, but not both; set the other to nil.
// Normally the results from RingOrBuilder.
func CLIAddOrSet(b *Builder, args []string, n BuilderNode, output io.Writer) error {
	active := true
	capacity := uint32(1)
	var tiers []string
	var addresses []string
	var config []byte
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
		case "config":
			if sarg[1] == "" {
				config = nil
			} else {
				config = []byte(sarg[1])
			}
		case "config-file":
			var err error
			config, err = ioutil.ReadFile(sarg[1])
			if err != nil {
				return fmt.Errorf("Error reading config file: %v", err)
			}
			if n != nil {
				n.SetConfig(config)
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
			} else {
				return fmt.Errorf("Invalid arg %q in set.", sarg[0])
			}
		}
	}
	if n == nil {
		var err error
		n, err = b.AddNode(active, capacity, tiers, addresses, meta, config)
		if err != nil {
			return err
		}
		output.Write([]byte(CLINodeReport(n)))
	}
	return nil
}

// CLIRemove removes a node from the builder; see the output of CLIHelp for
// detailed information.
//
// Provide either the ring or the builder, but not both; set the other to nil.
// Normally the results from RingOrBuilder.
func CLIRemove(b *Builder, args []string, output io.Writer) error {
	if len(args) != 1 || !strings.HasPrefix(args[0], "id=") {
		return fmt.Errorf("must specify node to remove with id=<value>")
	}
	id, err := strconv.ParseUint(args[0][3:], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid id %#v", args[0][3:])
	}
	b.RemoveNode(id)
	return nil
}

// CLIRing writes a new ring file based on the builder; see the output of
// CLIHelp for detailed information.
//
// Provide either the ring or the builder, but not both; set the other to nil.
// Normally the results from RingOrBuilder.
func CLIRing(b *Builder, filename string, output io.Writer) error {
	r := b.Ring()
	if err := PersistRingOrBuilder(nil, b, filename); err != nil {
		return err
	}
	return PersistRingOrBuilder(r, nil, strings.TrimSuffix(filename, ".builder")+".ring")
}

// CLIPretendElapsed updates a builder, pretending some time has elapsed for
// testing purposes; see the output of CLIHelp for detailed information.
//
// Provide either the ring or the builder, but not both; set the other to nil.
// Normally the results from RingOrBuilder.
func CLIPretendElapsed(b *Builder, args []string, output io.Writer) error {
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

// CLIConfig displays or sets the top-level config in the ring or builder; see
// the output of CLIHelp for detailed information.
//
// Provide either the ring or the builder, but not both; set the other to nil.
// Normally the results from RingOrBuilder.
func CLIConfig(r Ring, b *Builder, args []string, output io.Writer) (changed bool, err error) {
	v := strings.Join(args, " ")
	if v != "" {
		if b == nil {
			return false, fmt.Errorf("cannot set config in a ring, only builder")
		} else {
			b.SetConfig([]byte(v))
		}
		return true, nil
	}
	if b == nil {
		fmt.Fprintf(output, "%q\n", string(r.Config()))
	} else {
		fmt.Fprintf(output, "%q\n", string(b.Config()))
	}
	return false, nil
}

// CLIConfigFile displays or sets the top-level config in the ring or builder;
// see the output of CLIHelp for detailed information.
//
// Provide either the ring or the builder, but not both; set the other to nil.
// Normally the results from RingOrBuilder.
func CLIConfigFile(r Ring, b *Builder, args []string, output io.Writer) (changed bool, err error) {
	if len(args) == 0 {
		if b == nil {
			output.Write(r.Config())
		} else {
			output.Write(b.Config())
		}
		return false, nil
	} else if len(args) == 1 {
		if b == nil {
			return false, fmt.Errorf("cannot set config in a ring, only builder")
		}
		config, err := ioutil.ReadFile(args[0])
		if err != nil {
			return false, fmt.Errorf("Error reading config file: %v", err)
		}
		b.SetConfig(config)
		return true, nil
	}
	return false, fmt.Errorf("too many arguments %v; should just be the file name or nothing", qStrings(args))
}

func qStrings(strings []string) []string {
	rv := make([]string, len(strings))
	for i, s := range strings {
		rv[i] = fmt.Sprintf("%q", s)
	}
	return rv
}

func CLINodeReport(n Node) string {
	report := [][]string{
		[]string{"ID:", fmt.Sprintf("%d", n.ID())},
		[]string{"Active:", fmt.Sprintf("%v", n.Active())},
		[]string{"Capacity:", fmt.Sprintf("%d", n.Capacity())},
		[]string{"Tiers:", strings.Join(qStrings(n.Tiers()), "\n")},
		[]string{"Addresses:", strings.Join(qStrings(n.Addresses()), "\n")},
		[]string{"Meta:", fmt.Sprintf("%q", n.Meta())},
		[]string{"Config:", fmt.Sprintf("%q", string(n.Config()))},
	}
	return brimtext.Align(report, nil)
}
