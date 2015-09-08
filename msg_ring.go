package ring

import (
	"io"
	"time"
)

// MsgRing will send and receive Msg instances to and from ring nodes. See
// TCPMsgRing for a concrete implementation.
//
// The design is such that messages are not guaranteed delivery, or even
// transmission. Acknowledgement and retry logic is left outside to the
// producers and consumers of the messages themselves.
//
// For example, if a connection to a node is too slow to keep up with the
// messages wanting to be delivered to it, many of the messages will simply be
// dropped.
//
// For another example, if a message is ready to be delivered to a node that
// currently has no established connection, the connection process will be
// initiated (for future messages) but that current message will be dropped.
//
// This design is because much ring-related messaging is based on passes over
// the entire ring with many messages being sent constantly and conditions can
// change rapidly, making messages less useful as they age. It is better for
// the distributed system overall if a single message is just dropped if it
// can't be sent immediately as a similar message will be generated and
// attempted later on in the next pass.
//
// For messages that require guaranteed delivery, the sender's node ID can be
// embedded in the message and receiver would send an acknowledgement message
// back upon receipt. The exact retry logic could vary greatly and depends on
// the sender's implementation (e.g. background passes recording acks or
// dedicated goroutines for each message).
//
// In previous systems we've written, distributed algorithms could easily get
// hung up trying to communicate to one faulty node (for example) making the
// rest of the system suffer as well.
type MsgRing interface {
	// Ring returns the ring information used to determine messaging endpoints;
	// note that this method may return nil if no ring information is yet
	// available.
	Ring() Ring
	// MaxMsgLength indicates the maximum number of bytes the content of a
	// message may contain to be handled by this MsgRing.
	MaxMsgLength() uint64
	// SetMsgHandler associates a message type with a handler; any incoming
	// messages with the type will be delivered to the handler. Message types
	// just need to be unique uint64 values; usually picking 64 bits of a UUID
	// is fine.
	SetMsgHandler(msgType uint64, handler MsgUnmarshaller)
	// MsgToNode queues the message for delivery to the indicated node; the
	// timeout should be considered for queueing, not for actual delivery.
	//
	// When the msg has actually been sent or has been discarded due to
	// delivery errors or delays, msg.Free() will be called.
	MsgToNode(msg Msg, nodeID uint64, timeout time.Duration)
	// MsgToNode queues the message for delivery to all other replicas of a
	// partition; the timeout should be considered for queueing, not for actual
	// delivery.
	//
	// If the ring is not bound to a specific node (LocalNode() returns nil)
	// then the delivery attempts will be to all replicas.
	//
	// When the msg has actually been sent or has been discarded due to
	// delivery errors or delays, msg.Free() will be called.
	MsgToOtherReplicas(msg Msg, partition uint32, timeout time.Duration)
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
	// WriteContent will send the contents of the message to the given writer.
	//
	// Note that WriteContent may be called multiple times and may be called
	// concurrently.
	//
	// Also note that the content should be written as quickly as possible as
	// any delays may cause the message transmission to be aborted and dropped.
	// In other words, any significant processing to build the message content
	// should be done before the Msg is given to the MsgRing for delivery.
	WriteContent(io.Writer) (uint64, error)
	// Free will be called when the MsgRing no longer has any references to the
	// message and allows the message to free any resources it may have, or be
	// reused, etc.
	Free()
}

// MsgUnmarshaller will attempt to read desiredBytesToRead from the reader and
// will return the number of bytes actually read as well as any error that may
// have occurred. If error is nil then actualBytesRead must equal
// desiredBytesToRead.
//
// Note that the message content should be read as quickly as possible as any
// delays may cause the message transmission to be aborted. In other words, any
// significant processing of the message should be done after the contents are
// read and this reader function returns.
type MsgUnmarshaller func(reader io.Reader, desiredBytesToRead uint64) (actualBytesRead uint64, err error)
