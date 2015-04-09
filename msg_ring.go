package ring

import "io"

// MsgRing will send and receive Msg instances to and from ring nodes. See
// TCPMsgRing for a concrete implementation.
type MsgRing interface {
	Ring() Ring
	// MaxMsgLength indicates the maximum number of bytes the content of a
	// message may contain to be handled by this MsgRing.
	MaxMsgLength() uint64
	// SetMsgHandler associates a message type with a handler; any incoming
	// messages with the type will be delivered to the handler. Message types
	// just need to be unique uint64 values; usually picking 64 bits of a UUID
	// is fine.
	SetMsgHandler(msgType uint64, handler MsgUnmarshaller)
	// MsgToNode attempts to the deliver the message to the indicated node.
	MsgToNode(nodeID uint64, msg Msg)
	// MsgToNode attempts to the deliver the message to all other replicas of a
	// partition. If the ring is not bound to a specific node (LocalNode()
	// returns nil) then the delivery attempts will be to all replicas. The
	// ring version is used to short circuit any messages based on a different
	// ring version; if the ring version does not match Version(), the message
	// will simply be discarded.
	MsgToOtherReplicas(ringVersion int64, partition uint32, msg Msg)
}

// Msg is a single message to be sent to another node or nodes.
type Msg interface {
	// MsgType is the unique designator for the type of message content (such
	// as a pull replication request, a read request, etc.). Message types just
	// need to be unique values; usually picking 64 bits of a UUID is fine.
	MsgType() uint64
	// MsgLength returns the number of bytes for the content of the message
	// (the amount that will be written with a call to WriteContent).
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
// have occurred. If error is nil then actualBytesRead must equal
// desiredBytesToRead.
type MsgUnmarshaller func(reader io.Reader, desiredBytesToRead uint64) (actualBytesRead uint64, err error)
