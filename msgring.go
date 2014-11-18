package ring

import "io"

const (
	_ MsgType = iota
	MSG_PULL_REPLICATION
	MSG_BULK_SET
	MSG_BULK_SET_ACK
)

type MsgRing interface {
	Ring
	SetMsgHandler(t MsgType, h MsgUnmarshaller)
	MsgToNode(nodeID uint64, msg Msg) bool
	MsgToOtherReplicas(ringID uint64, partition uint32, msg Msg) bool
}

type Msg interface {
	MsgType() MsgType
	MsgLength() uint64
	WriteContent(io.Writer) (uint64, error)
	Done()
}

type MsgUnmarshaller func(io.Reader, uint64) (uint64, error)

type MsgType uint64
