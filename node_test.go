package ring

import "testing"

type testSource struct {
	// The repeat part will cause an id == 0 when starting with repeat = false
	// and v = 0. Essentially, just tests that newNode won't actually return an
	// id == 0.
	repeat bool
	v      int64
}

func (s *testSource) Int63() int64 {
	if s.repeat {
		rv := s.v
		s.repeat = false
		s.v++
		return rv
	}
	s.repeat = true
	return s.v
}

func (s *testSource) Seed(seed int64) {
}

func TestNewNode(t *testing.T) {
	b := &tierBase{}
	var o []*node
	n1 := newNodeWithSource(b, o, &testSource{})
	if n1.ID() == 0 {
		t.Fatal("")
	}
	o = append(o, n1)
	n2 := newNodeWithSource(b, o, &testSource{})
	if n2.ID() == 0 {
		t.Fatal("")
	}
	if n1.ID() == n2.ID() {
		t.Fatal("")
	}
}

func TestNodeSimply(t *testing.T) {
	b := &tierBase{}
	n := newNode(b, nil)
	if !n.Active() {
		t.Fatal("")
	}
	n.SetActive(false)
	if n.Active() {
		t.Fatal("")
	}
	n.SetActive(true)
	if !n.Active() {
		t.Fatal("")
	}
	if n.Capacity() != 0 {
		t.Fatal("")
	}
	n.SetCapacity(100)
	if n.Capacity() != 100 {
		t.Fatal("")
	}
	if len(b.tiers) != 0 {
		t.Fatal("")
	}
	n.SetTier(0, "levelzero")
	if n.Tier(0) != "levelzero" {
		t.Fatal("")
	}
	if len(b.tiers) != 1 {
		t.Fatal("")
	}
	if len(b.tiers[0]) != 2 {
		t.Fatal("")
	}
	if len(n.tierIndexes) != 1 {
		t.Fatal("")
	}
	if n.tierIndexes[0] != 1 {
		t.Fatal("")
	}
	n.SetTier(10, "levelten")
	if n.Tier(10) != "levelten" {
		t.Fatal("")
	}
	if len(b.tiers) != 11 {
		t.Fatal("")
	}
	if len(b.tiers[10]) != 2 {
		t.Fatal("")
	}
	if len(n.tierIndexes) != 11 {
		t.Fatal("")
	}
	if n.tierIndexes[10] != 1 {
		t.Fatal("")
	}
	if n.Tier(5) != "" {
		t.Fatal("")
	}
	if n.Tier(15) != "" {
		t.Fatal("")
	}
	tiers := n.Tiers()
	if len(tiers) != 11 {
		t.Fatal("")
	}
	if tiers[0] != "levelzero" {
		t.Fatal("")
	}
	for i := 1; i < 10; i++ {
		if tiers[i] != "" {
			t.Fatal(i)
		}
	}
	if tiers[10] != "levelten" {
		t.Fatal("")
	}
	if len(n.Addresses()) != 0 {
		t.Fatal("")
	}
	if n.Address(5) != "" {
		t.Fatal("")
	}
	n.SetAddress(0, "addresszero")
	if n.Address(0) != "addresszero" {
		t.Fatal("")
	}
	n.SetAddress(10, "addressten")
	if n.Address(10) != "addressten" {
		t.Fatal("")
	}
	if n.Address(5) != "" {
		t.Fatal("")
	}
	if n.Address(15) != "" {
		t.Fatal("")
	}
	addresses := n.Addresses()
	if len(addresses) != 11 {
		t.Fatal("")
	}
	if addresses[0] != "addresszero" {
		t.Fatal("")
	}
	if addresses[10] != "addressten" {
		t.Fatal("")
	}
	for i := 1; i < 10; i++ {
		if addresses[i] != "" {
			t.Fatal("")
		}
	}
	if n.Meta() != "" {
		t.Fatal("")
	}
	n.SetMeta("meta value")
	if n.Meta() != "meta value" {
		t.Fatal("")
	}
}

func TestTierCoalescing(t *testing.T) {
	b := &tierBase{}
	n1 := newNode(b, nil)
	n2 := newNode(b, []*node{n1})
	n1.SetTier(0, "tierA")
	n2.SetTier(0, "tierA")
	n1.SetTier(1, "tierB")
	n2.SetTier(1, "tierA")
	if len(b.tiers) != 2 {
		t.Fatal("")
	}
	if len(b.tiers[0]) != 2 {
		t.Fatal("")
	}
	if b.tiers[0][0] != "" {
		t.Fatal("")
	}
	if b.tiers[0][1] != "tierA" {
		t.Fatal("")
	}
	if len(b.tiers[1]) != 3 {
		t.Fatal("")
	}
	if b.tiers[1][0] != "" {
		t.Fatal("")
	}
	if b.tiers[1][1] != "tierB" {
		t.Fatal("")
	}
	if b.tiers[1][2] != "tierA" {
		t.Fatal("")
	}
}

func TestNodeFilterCommonErrors(t *testing.T) {
	_, err := NodeSlice{}.Filter([]string{"randomstringwithoutequals"})
	if err == nil {
		t.Fatal("")
	}
	if err.Error() != `invalid expression "randomstringwithoutequals"; needs "="` {
		t.Fatal(err)
	}
	_, err = NodeSlice{}.Filter([]string{"trailingequals="})
	if err == nil {
		t.Fatal("")
	}
	if err.Error() != `invalid expression "trailingequals="; nothing was right of "="` {
		t.Fatal(err)
	}
	_, err = NodeSlice{}.Filter([]string{"=leadingequals"})
	if err == nil {
		t.Fatal("")
	}
	if err.Error() != `invalid expression "=leadingequals"; nothing was left of "="` {
		t.Fatal(err)
	}
	_, err = NodeSlice{}.Filter([]string{"~=leadingtildeequals"})
	if err == nil {
		t.Fatal("")
	}
	if err.Error() != `invalid expression "~=leadingtildeequals"; nothing was left of "~="` {
		t.Fatal(err)
	}
	_, err = NodeSlice{}.Filter([]string{"meta~=(broken regular expression"})
	if err == nil {
		t.Fatal("")
	}
	if err.Error() != "invalid expression \"meta~=(broken regular expression\"; error parsing regexp: missing closing ): `(broken regular expression`" {
		t.Fatal(err)
	}
	_, err = NodeSlice{}.Filter([]string{"unknown=value"})
	if err == nil {
		t.Fatal("")
	}
	if err.Error() != `invalid expression "unknown=value"; don't understand "unknown"` {
		t.Fatal(err)
	}
}

func TestNodeFilterID(t *testing.T) {
	b := &tierBase{}
	ns := NodeSlice{newNode(b, nil), newNode(b, nil), newNode(b, nil)}
	ns[0].(*node).id = 0x0000000000001230
	ns[1].(*node).id = 0x0000000000001231
	ns[2].(*node).id = 0x0000000000009999
	fns, err := ns.Filter([]string{"id=0000000000001230"})
	if err != nil {
		t.Fatal(err)
	}
	if len(fns) != 1 {
		t.Fatal("")
	}
	if fns[0].ID() != 0x0000000000001230 {
		t.Fatal("")
	}
	fns, err = ns.Filter([]string{"id~=123"})
	if err != nil {
		t.Fatal(err)
	}
	if len(fns) != 2 {
		t.Fatal("")
	}
	if fns[0].ID() != 0x0000000000001230 {
		t.Fatal("")
	}
	if fns[1].ID() != 0x0000000000001231 {
		t.Fatal("")
	}
}

func TestNodeFilterActive(t *testing.T) {
	b := &tierBase{}
	ns := NodeSlice{newNode(b, nil), newNode(b, nil), newNode(b, nil)}
	ns[0].(*node).SetActive(true)
	ns[1].(*node).SetActive(false)
	ns[2].(*node).SetActive(false)
	fns, err := ns.Filter([]string{"active=true"})
	if err != nil {
		t.Fatal(err)
	}
	if len(fns) != 1 {
		t.Fatal("")
	}
	if fns[0].ID() != ns[0].ID() {
		t.Fatal("")
	}
	fns, err = ns.Filter([]string{"active~=al"})
	if err != nil {
		t.Fatal(err)
	}
	if len(fns) != 2 {
		t.Fatal("")
	}
	if fns[0].Active() {
		t.Fatal("")
	}
	if fns[1].Active() {
		t.Fatal("")
	}
}

func TestNodeFilterCapacity(t *testing.T) {
	b := &tierBase{}
	ns := NodeSlice{newNode(b, nil), newNode(b, nil), newNode(b, nil)}
	ns[0].(*node).SetCapacity(123)
	ns[1].(*node).SetCapacity(321)
	ns[2].(*node).SetCapacity(999)
	fns, err := ns.Filter([]string{"capacity=123"})
	if err != nil {
		t.Fatal(err)
	}
	if len(fns) != 1 {
		t.Fatal("")
	}
	if fns[0].ID() != ns[0].ID() {
		t.Fatal("")
	}
	fns, err = ns.Filter([]string{"capacity~=1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(fns) != 2 {
		t.Fatal("")
	}
	if fns[0].ID() != ns[0].ID() {
		t.Fatal("")
	}
	if fns[1].ID() != ns[1].ID() {
		t.Fatal("")
	}
}

func TestNodeFilterTier(t *testing.T) {
	b := &tierBase{}
	ns := NodeSlice{newNode(b, nil), newNode(b, nil), newNode(b, nil)}
	ns[0].(*node).SetTier(0, "server1")
	ns[0].(*node).SetTier(1, "zone1")
	ns[1].(*node).SetTier(0, "server9")
	ns[2].(*node).SetTier(0, "server2")
	ns[2].(*node).SetTier(1, "zone1")
	fns, err := ns.Filter([]string{"tier=server2"})
	if err != nil {
		t.Fatal(err)
	}
	if len(fns) != 1 {
		t.Fatal("")
	}
	if fns[0].ID() != ns[2].ID() {
		t.Fatal("")
	}
	fns, err = ns.Filter([]string{"tier~=zone1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(fns) != 2 {
		t.Fatal("")
	}
	if fns[0].ID() != ns[0].ID() {
		t.Fatal("")
	}
	if fns[1].ID() != ns[2].ID() {
		t.Fatal("")
	}
}

func TestNodeFilterAddress(t *testing.T) {
	b := &tierBase{}
	ns := NodeSlice{newNode(b, nil), newNode(b, nil), newNode(b, nil)}
	ns[0].(*node).SetAddress(0, "1.2.3.4:5678")
	ns[0].(*node).SetAddress(1, "1.2.3.4:9876")
	ns[1].(*node).SetAddress(0, "5.6.7.8:5678")
	ns[2].(*node).SetAddress(0, "9.0.1.2:5678")
	ns[2].(*node).SetAddress(1, "9.0.1.2:9876")
	fns, err := ns.Filter([]string{"address=1.2.3.4:5678"})
	if err != nil {
		t.Fatal(err)
	}
	if len(fns) != 1 {
		t.Fatal("")
	}
	if fns[0].ID() != ns[0].ID() {
		t.Fatal("")
	}
	fns, err = ns.Filter([]string{"address~=:9876"})
	if err != nil {
		t.Fatal(err)
	}
	if len(fns) != 2 {
		t.Fatal("")
	}
	if fns[0].ID() != ns[0].ID() {
		t.Fatal("")
	}
	if fns[1].ID() != ns[2].ID() {
		t.Fatal("")
	}
}

func TestNodeFilterMeta(t *testing.T) {
	b := &tierBase{}
	ns := NodeSlice{newNode(b, nil), newNode(b, nil), newNode(b, nil)}
	ns[0].(*node).SetMeta("Sparkle Servers S8000, 128G RAM, 2x Northern Analog 2T PCIe SSD")
	ns[1].(*node).SetMeta("Sparkle Servers S9000, 128G RAM, 2x Northern Analog 2T PCIe SSD")
	ns[2].(*node).SetMeta("Sparkle Servers S10000, 128G RAM, 2x Northern Analog 3T PCIe SSD")
	fns, err := ns.Filter([]string{"meta=S9000"})
	if err != nil {
		t.Fatal(err)
	}
	fns, err = ns.Filter([]string{"meta=Sparkle Servers S9000, 128G RAM, 2x Northern Analog 2T PCIe SSD"})
	if err != nil {
		t.Fatal(err)
	}
	if len(fns) != 1 {
		t.Fatal("")
	}
	if fns[0].ID() != ns[1].ID() {
		t.Fatal("")
	}
	fns, err = ns.Filter([]string{"meta~=2T PCIe"})
	if err != nil {
		t.Fatal(err)
	}
	if len(fns) != 2 {
		t.Fatal("")
	}
	if fns[0].ID() != ns[0].ID() {
		t.Fatal("")
	}
	if fns[1].ID() != ns[1].ID() {
		t.Fatal("")
	}
	fns, err = ns.Filter([]string{"meta~=2t pcie"})
	if err != nil {
		t.Fatal(err)
	}
	if len(fns) != 0 {
		t.Fatal("")
	}
	// Quick example of case insensitivity
	fns, err = ns.Filter([]string{"meta~=(?i)2t pcie"})
	if err != nil {
		t.Fatal(err)
	}
	if len(fns) != 2 {
		t.Fatal("")
	}
	if fns[0].ID() != ns[0].ID() {
		t.Fatal("")
	}
	if fns[1].ID() != ns[1].ID() {
		t.Fatal("")
	}
}

func TestNodeFilterTierX(t *testing.T) {
	b := &tierBase{}
	ns := NodeSlice{newNode(b, nil), newNode(b, nil), newNode(b, nil)}
	ns[0].(*node).SetTier(0, "server1")
	ns[0].(*node).SetTier(1, "zone1")
	ns[1].(*node).SetTier(0, "server9")
	ns[2].(*node).SetTier(0, "server2")
	ns[2].(*node).SetTier(1, "zone1")
	fns, err := ns.Filter([]string{"tier0=server2"})
	if err != nil {
		t.Fatal(err)
	}
	if len(fns) != 1 {
		t.Fatal("")
	}
	if fns[0].ID() != ns[2].ID() {
		t.Fatal("")
	}
	fns, err = ns.Filter([]string{"tier1=server2"})
	if err != nil {
		t.Fatal(err)
	}
	if len(fns) != 0 {
		t.Fatal("")
	}
	fns, err = ns.Filter([]string{"tier1~=zone1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(fns) != 2 {
		t.Fatal("")
	}
	if fns[0].ID() != ns[0].ID() {
		t.Fatal("")
	}
	if fns[1].ID() != ns[2].ID() {
		t.Fatal("")
	}
	fns, err = ns.Filter([]string{"tier0~=zone1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(fns) != 0 {
		t.Fatal("")
	}
	fns, err = ns.Filter([]string{"tierA~=zone1"})
	if err == nil {
		t.Fatal("")
	}
	if err.Error() != `invalid expression "tierA~=zone1"; "A" doesn't specify a number` {
		t.Fatal(err)
	}
	fns, err = ns.Filter([]string{"tier-1~=zone1"})
	if err == nil {
		t.Fatal("")
	}
	if err.Error() != `invalid expression "tier-1~=zone1"; minimum level is 0` {
		t.Fatal(err)
	}
}

func TestNodeFilterAddressX(t *testing.T) {
	b := &tierBase{}
	ns := NodeSlice{newNode(b, nil), newNode(b, nil), newNode(b, nil)}
	ns[0].(*node).SetAddress(0, "1.2.3.4:5678")
	ns[0].(*node).SetAddress(1, "1.2.3.4:9876")
	ns[1].(*node).SetAddress(0, "5.6.7.8:5678")
	ns[2].(*node).SetAddress(0, "9.0.1.2:5678")
	ns[2].(*node).SetAddress(1, "9.0.1.2:9876")
	fns, err := ns.Filter([]string{"address0=1.2.3.4:5678"})
	if err != nil {
		t.Fatal(err)
	}
	if len(fns) != 1 {
		t.Fatal("")
	}
	if fns[0].ID() != ns[0].ID() {
		t.Fatal("")
	}
	fns, err = ns.Filter([]string{"address1=1.2.3.4:5678"})
	if err != nil {
		t.Fatal(err)
	}
	if len(fns) != 0 {
		t.Fatal("")
	}
	fns, err = ns.Filter([]string{"address1~=:9876"})
	if err != nil {
		t.Fatal(err)
	}
	if len(fns) != 2 {
		t.Fatal("")
	}
	if fns[0].ID() != ns[0].ID() {
		t.Fatal("")
	}
	if fns[1].ID() != ns[2].ID() {
		t.Fatal("")
	}
	fns, err = ns.Filter([]string{"address0~=:9876"})
	if err != nil {
		t.Fatal(err)
	}
	if len(fns) != 0 {
		t.Fatal("")
	}
	fns, err = ns.Filter([]string{"addressA~=zone1"})
	if err == nil {
		t.Fatal("")
	}
	if err.Error() != `invalid expression "addressA~=zone1"; "A" doesn't specify a number` {
		t.Fatal(err)
	}
	fns, err = ns.Filter([]string{"address-1~=zone1"})
	if err == nil {
		t.Fatal("")
	}
	if err.Error() != `invalid expression "address-1~=zone1"; minimum index is 0` {
		t.Fatal(err)
	}
}
