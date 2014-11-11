package ring

type Ring interface {
	ID() uint64
	PartitionPower() uint16
	NodeID() uint64
	Responsible(partition uint32) bool
}
