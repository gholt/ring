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

// MsgUnmarshaller will attempt to read desiredBytesToRead from the reader and will return
// the number of bytes actually read as well as any error that may have ocurred. If error
// is nil then actualBytesRead must equal desiredBytesToRead.
type MsgUnmarshaller func(io.Reader, desiredBytesToRead uint64) (actualBytesRead uint64, error)

type MsgType uint64
