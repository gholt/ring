package ring

import "io"

type MsgRing interface {
	Ring
	// MaxMsgLength indicates the maximum number of bytes the content of a
	// message may contain.
	MaxMsgLength() uint64
	SetMsgHandler(msgType uint64, handler MsgUnmarshaller)
	MsgToNode(nodeID uint64, msg Msg)
	MsgToOtherReplicas(ringVersion int64, partition uint32, msg Msg)
}

type Msg interface {
	MsgType() uint64
	MsgLength() uint64
	// WriteContent will send the contents of the message to the given writer;
	// note that WriteContent may be called multiple times and may be called
	// concurrently.
	WriteContent(io.Writer) (uint64, error)
	// Done will be called when the MsgRing is done processing the message and
	// allows the message to free any resources it may have.
	Done()
}

// MsgUnmarshaller will attempt to read desiredBytesToRead from the reader and
// will return the number of bytes actually read as well as any error that may
// have ocurred. If error is nil then actualBytesRead must equal
// desiredBytesToRead.
type MsgUnmarshaller func(reader io.Reader, desiredBytesToRead uint64) (actualBytesRead uint64, err error)
