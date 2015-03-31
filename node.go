package ring

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type Node interface {
	// ID uniquely identifies this node; it will be non-zero as zero is used to
	// indicate "no node".
	ID() uint64
	Active() bool
	// Capacity indicates the amount of data that should be assigned to a node
	// relative to other nodes. It can be in any unit of designation as long as
	// all nodes use the same designation. Most commonly this is the number of
	// gigabytes the node can store, but could be based on CPU capacity or
	// another resource if that makes more sense to balance.
	Capacity() uint32
	// Tiers indicate the layout of the node with respect to other nodes. For
	// example, the lowest tier, tier 0, might be the server ip (where each
	// node represents a drive on that server). The next tier, 1, might then be
	// the power zone the server is in. The number of tiers is flexible, so
	// later an additional tier for geographic region could be added.
	Tiers() []string
	Tier(level int) string
	// Addresses give location information for the node; probably something
	// like ip:port. This is a list for those use cases where different
	// processes use different networks (such as replication using a
	// replication-only network).
	Addresses() []string
	Address(index int) string
	// Meta is additional information for the node; not defined or used by the
	// builder or ring directly.
	Meta() string
}

type BuilderNode interface {
	Node
	SetActive(value bool)
	SetCapacity(value uint32)
	SetTier(level int, value string)
	SetAddress(index int, value string)
	SetMeta(value string)
}

// Node is a single item assigned to a ring, usually a single device like a
// disk drive.
type node struct {
	tierBase *tierBase
	id       uint64
	inactive bool
	capacity uint32
	// Here the tier values are represented as indexes to the actual values
	// stored in tierBase.tiers. This is done for speed during rebalancing.
	tierIndexes []int32
	addresses   []string
	meta        string
}

func (n *node) ID() uint64 {
	return n.id
}

func (n *node) Active() bool {
	return !n.inactive
}

func (n *node) Capacity() uint32 {
	return n.capacity
}

func (n *node) Tiers() []string {
	tiers := make([]string, len(n.tierIndexes))
	for level := len(tiers) - 1; level >= 0; level-- {
		tiers[level] = n.Tier(level)
	}
	return tiers
}

func (n *node) Tier(level int) string {
	if len(n.tierIndexes) <= level {
		return ""
	}
	index := n.tierIndexes[level]
	if index == 0 {
		return ""
	}
	return n.tierBase.tiers[level][index]
}

func (n *node) Addresses() []string {
	addresses := make([]string, len(n.addresses))
	copy(addresses, n.addresses)
	return addresses
}

func (n *node) Address(index int) string {
	if len(n.addresses) <= index {
		return ""
	}
	return n.addresses[index]
}

func (n *node) Meta() string {
	return n.meta
}

func (n *node) SetActive(value bool) {
	n.inactive = !value
}

func (n *node) SetCapacity(value uint32) {
	n.capacity = value
}

func (n *node) SetTier(level int, value string) {
	if len(n.tierBase.tiers) <= level {
		tiers := make([][]string, level+1)
		copy(tiers, n.tierBase.tiers)
		n.tierBase.tiers = tiers
	}
	var index int
	for i, compare := range n.tierBase.tiers[level] {
		if compare == value {
			index = i
			break
		}
	}
	if index == 0 && value != "" {
		if len(n.tierBase.tiers[level]) == 0 {
			// First index should always be the empty string
			n.tierBase.tiers[level] = append(n.tierBase.tiers[level], "")
		}
		index = len(n.tierBase.tiers[level])
		n.tierBase.tiers[level] = append(n.tierBase.tiers[level], value)
	}
	if len(n.tierIndexes) <= level {
		tierIndexes := make([]int32, level+1)
		copy(tierIndexes, n.tierIndexes)
		n.tierIndexes = tierIndexes
	}
	n.tierIndexes[level] = int32(index)
}

func (n *node) SetAddress(index int, value string) {
	if len(n.addresses) <= index {
		addresses := make([]string, index+1)
		copy(addresses, n.addresses)
		n.addresses = addresses
	}
	n.addresses[index] = value
}

func (n *node) SetMeta(value string) {
	n.meta = value
}

type NodeSlice []Node

func (ns NodeSlice) Filter(filters []string) (NodeSlice, error) {
	nsB := ns
	for _, filter := range filters {
		sfilter := strings.SplitN(filter, "=", 2)
		if len(sfilter) != 2 {
			return nil, fmt.Errorf(`invalid expression %#v; needs "="`, filter)
		}
		if sfilter[0] == "" {
			return nil, fmt.Errorf(`invalid expression %#v; nothing was left of "="`, filter)
		}
		if sfilter[0] == "~" {
			return nil, fmt.Errorf(`invalid expression %#v; nothing was left of "~="`, filter)
		}
		if sfilter[1] == "" {
			return nil, fmt.Errorf(`invalid expression %#v; nothing was right of "="`, filter)
		}
		var re *regexp.Regexp
		if sfilter[0][len(sfilter[0])-1] == '~' {
			sfilter[0] = sfilter[0][:len(sfilter[0])-1]
			var err error
			re, err = regexp.Compile(sfilter[1])
			if err != nil {
				return nil, fmt.Errorf("invalid expression %#v; %s", filter, err)
			}
		}
		var matcher func(n Node) bool
		switch sfilter[0] {
		case "id":
			if re == nil {
				matcher = func(n Node) bool {
					return sfilter[1] == fmt.Sprintf("%08x", n.ID())
				}
			} else {
				matcher = func(n Node) bool {
					return re.MatchString(fmt.Sprintf("%08x", n.ID()))
				}
			}
		case "active":
			if re == nil {
				matcher = func(n Node) bool {
					return sfilter[1] == fmt.Sprintf("%v", n.Active())
				}
			} else {
				matcher = func(n Node) bool {
					return re.MatchString(fmt.Sprintf("%v", n.Active()))
				}
			}
		case "capacity":
			if re == nil {
				matcher = func(n Node) bool {
					return sfilter[1] == fmt.Sprintf("%d", n.Capacity())
				}
			} else {
				matcher = func(n Node) bool {
					return re.MatchString(fmt.Sprintf("%d", n.Capacity()))
				}
			}
		case "tier":
			if re == nil {
				matcher = func(n Node) bool {
					for _, tier := range n.Tiers() {
						if sfilter[1] == tier {
							return true
						}
					}
					return false
				}
			} else {
				matcher = func(n Node) bool {
					for _, tier := range n.Tiers() {
						if re.MatchString(tier) {
							return true
						}
					}
					return false
				}
			}
		case "address":
			if re == nil {
				matcher = func(n Node) bool {
					for _, address := range n.Addresses() {
						if sfilter[1] == address {
							return true
						}
					}
					return false
				}
			} else {
				matcher = func(n Node) bool {
					for _, address := range n.Addresses() {
						if re.MatchString(address) {
							return true
						}
					}
					return false
				}
			}
		case "meta":
			if re == nil {
				matcher = func(n Node) bool {
					return sfilter[1] == n.Meta()
				}
			} else {
				matcher = func(n Node) bool {
					return re.MatchString(n.Meta())
				}
			}
		default:
			if strings.HasPrefix(sfilter[0], "tier") {
				level, err := strconv.Atoi(sfilter[0][4:])
				if err != nil {
					return nil, fmt.Errorf("invalid expression %#v; %#v doesn't specify a number", filter, sfilter[0][4:])
				}
				if level < 0 {
					return nil, fmt.Errorf("invalid expression %#v; minimum level is 0", filter)
				}
				if re == nil {
					matcher = func(n Node) bool {
						return sfilter[1] == n.Tier(level)
					}
				} else {
					matcher = func(n Node) bool {
						return re.MatchString(n.Tier(level))
					}
				}
			} else if strings.HasPrefix(sfilter[0], "address") {
				index, err := strconv.Atoi(sfilter[0][7:])
				if err != nil {
					return nil, fmt.Errorf("invalid expression %#v; %#v doesn't specify a number", filter, sfilter[0][7:])
				}
				if re == nil {
					matcher = func(n Node) bool {
						return sfilter[1] == n.Address(index)
					}
				} else {
					matcher = func(n Node) bool {
						return re.MatchString(n.Address(index))
					}
				}
			} else {
				return nil, fmt.Errorf("invalid expression %#v; don't understand %#v", filter, sfilter[0])
			}
		}
		var nsC NodeSlice
		for _, n := range nsB {
			if matcher(n) {
				nsC = append(nsC, n)
			}
		}
		nsB = nsC
	}
	return nsB, nil
}

type BuilderNodeSlice []BuilderNode
