package ring

import (
	"errors"
	"log"
	"math"
	"net"
	"sync"
	"time"
)

const _DEFAULT_CHUNK_SIZE int = 16 * 1024
const _DEFAULT_TIMEOUT time.Duration = 2 * time.Second
const _DEFAULT_TIMEOUT_NEXT time.Duration = 2 * time.Hour

type ringConn struct {
	Conn   net.Conn
	Writer *timeoutWriter
	sync.Mutex
}

func newRingConn(conn net.Conn, chunkSize int, timeout time.Duration) *ringConn {
	return &ringConn{
		Conn:   conn,
		Writer: newTimeoutWriter(conn, chunkSize, timeout),
	}
}

type TCPMsgRing struct {
	// AddressIndex is the index given to a Node's Address method to determine
	// the network address to connect to (see Node's Address method for more
	// information).
	AddressIndex int
	// ChunkSize is the size of network reads and writes.
	ChunkSize int
	// Timeout is the duration before network reads and writes expire.
	Timeout time.Duration
	// TimeoutNext is the duration to wait for the next command
	TimeoutNext time.Duration
	ring        Ring
	msgHandlers map[uint64]MsgUnmarshaller
	conns       map[string]*ringConn
}

func NewTCPMsgRing(r Ring) *TCPMsgRing {
	return &TCPMsgRing{
		ring:        r,
		msgHandlers: make(map[uint64]MsgUnmarshaller),
		conns:       make(map[string]*ringConn),
		ChunkSize:   _DEFAULT_CHUNK_SIZE,
		Timeout:     _DEFAULT_TIMEOUT,
		TimeoutNext: _DEFAULT_TIMEOUT_NEXT,
	}
}

func (m *TCPMsgRing) Ring() Ring {
	return m.ring
}

func (m *TCPMsgRing) MaxMsgLength() uint64 {
	return math.MaxUint64
}

func (m *TCPMsgRing) SetMsgHandler(msgType uint64, handler MsgUnmarshaller) {
	m.msgHandlers[uint64(msgType)] = handler
}

func (m *TCPMsgRing) MsgToNode(nodeID uint64, msg Msg) {
	m.msgToNode(nodeID, msg)
	msg.Done()
}

func (m *TCPMsgRing) msgToNode(nodeID uint64, msg Msg) {
	// TODO: Add retry functionality
	n := m.ring.Node(nodeID)
	if n == nil {
		return
	}
	// See if we have a connection already
	conn, ok := m.conns[n.Address(m.AddressIndex)]
	if !ok {
		// We need to open a connection
		// TODO: Handle connection timeouts
		tcpconn, err := net.DialTimeout("tcp", n.Address(m.AddressIndex), m.Timeout)
		if err != nil {
			log.Println("ERR: Trying to connect to", n.Address(m.AddressIndex), err)
			return
		}
		conn := newRingConn(tcpconn, m.ChunkSize, m.Timeout)
		m.conns[n.Address(m.AddressIndex)] = conn
	}
	conn.Lock() // Make sure we only have one writer at a time
	// TODO: Handle write timeouts
	// write the msg type
	msgType := msg.MsgType()
	for i := uint(0); i <= 56; i += 8 {
		_ = conn.Writer.WriteByte(byte(msgType >> i))
	}
	// Write the msg size
	msgLength := msg.MsgLength()
	for i := uint(0); i <= 56; i += 8 {
		_ = conn.Writer.WriteByte(byte(msgLength >> i))
	}
	// Write the msg
	length, err := msg.WriteContent(conn.Writer)
	// Make sure we flush the data
	conn.Writer.Flush()
	conn.Unlock()
	if err != nil {
		log.Println("ERR: Sending content - ", err)
		return
	}
	if length != msg.MsgLength() {
		log.Println("ERR: Didn't send enough data", length, msg.MsgLength())
		return
	}
}

func (m *TCPMsgRing) msgToNodeChan(nodeID uint64, msg Msg, retchan chan struct{}) {
	m.msgToNode(nodeID, msg)
	retchan <- struct{}{}
}

func (m *TCPMsgRing) MsgToOtherReplicas(partition uint32, msg Msg) {
	nodes := m.ring.ResponsibleNodes(partition)
	retchan := make(chan struct{}, 2)
	localNode := m.ring.LocalNode()
	var localID uint64
	if localNode != nil {
		localID = localNode.ID()
	}
	sent := 0
	for n := range nodes {
		if nodes[n].ID() != localID {
			go m.msgToNodeChan(nodes[n].ID(), msg, retchan)
			sent += 1
		}
	}
	for i := 0; i < sent; i++ {
		<-retchan
	}
	msg.Done()
}

func (m *TCPMsgRing) handleOne(reader *timeoutReader, wait bool) error {
	var length uint64 = 0
	var msgType uint64 = 0
	// for v.00002 we will store this in the fist 8 bytes
	for i := uint(0); i <= 56; i += 8 {
		if i == 0 && wait {
			// If this is the first read, and we are waiting, then
			// change the timeout
			reader.Timeout = m.TimeoutNext
		}
		b, err := reader.ReadByte()
		if i == 0 && wait {
			// Set the timeout back to normal
			reader.Timeout = m.Timeout
		}
		if err != nil {
			return err
		}
		msgType += uint64(b) << i
	}
	handle, ok := m.msgHandlers[msgType]
	if !ok {
		log.Println("ERR: Unknown message type", msgType)
		// TODO: Handle errors better
		return errors.New("Unknown message type")
	}
	// for v.00002 the msg length will be the next 8 bytes
	for i := uint(0); i <= 56; i += 8 {
		b, err := reader.ReadByte()
		if err != nil {
			return err
		}
		length += uint64(b) << i
	}
	// attempt to handle the message
	consumed, err := handle(reader, length)
	if err != nil {
		log.Println("ERR: Error handling message", err)
		// TODO: Handle errors better
		return err
	}
	if consumed != length {
		log.Println("ERR: Didn't consume whole message", length, consumed)
		// TODO: Handle errors better
		return errors.New("Didn't consume whole message")
	}
	// If we get here, everything is ok
	return nil
}

func (m *TCPMsgRing) handle(conn net.Conn) error {
	reader := newTimeoutReader(conn, m.ChunkSize, m.Timeout)
	err := m.handleOne(reader, false)
	for {
		if err != nil {
			log.Println("Closing connection")
			conn.Close()
			return err
		}
		err = m.handleOne(reader, true)
	}
}

func (m *TCPMsgRing) Listen() error {
	node := m.ring.LocalNode()
	tcpAddr, err := net.ResolveTCPAddr("tcp", node.Address(m.AddressIndex))
	if err != nil {
		return err
	}
	server, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return err
	}
	for {
		conn, err := server.AcceptTCP()
		if err != nil {
			// TODO: Not sure what types of errors occur here
			log.Println("Err accepting conn:", err)
			continue
		}
		go m.handle(conn)
	}
}
