package ring

var PRETEND_BUILDER *Builder

func init() {
	PRETEND_BUILDER = NewBuilder(3)
	PRETEND_BUILDER.Add(&Node{ID: 1, Addresses: []string{"1.2.3.4:21212"}})
	PRETEND_BUILDER.Add(&Node{ID: 2, Addresses: []string{"5.6.7.8:21212"}})
	PRETEND_BUILDER.Add(&Node{ID: 3, Addresses: []string{"9.10.11.12:21211"}})
	// Example to get a ring with the local node bound to the first node:
	// ring := PRETEND_BUILDER.Ring(1)
}
