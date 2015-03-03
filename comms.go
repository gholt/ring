package ring

import (
	"bufio"
	"encoding/binary"
	"errors"
	"log"
	"math"
	"net"
	"sync"
	"time"
)

var (
	default_chunksize int           = 1024 * 16       // 16Kb
	default_timeout   time.Duration = 2 * time.Second // 2 seconds
)

// TimeoutReader is a bufio.Reader that reads in chunks of ChunkSize and will
// return a timeout error if the chunk is not read in the Timeout time.
// TODO: Add chunking - or do we even need chunking at this layer?
// TODO: Add other bufio functions
type TimeoutReader struct {
	Timeout   time.Duration
	ChunkSize int
	reader    *bufio.Reader
	conn      net.Conn
}

func NewTimeoutReader(conn net.Conn) *TimeoutReader {
	return &TimeoutReader{
		Timeout:   default_timeout,
		ChunkSize: default_chunksize,
		reader:    bufio.NewReaderSize(conn, default_chunksize),
		conn:      conn,
	}
}

func (r *TimeoutReader) Read(p []byte) (n int, err error) {
	timeout := time.Now().Add(r.Timeout)
	r.conn.SetReadDeadline(timeout)
	count, err := r.reader.Read(p)
	r.conn.SetReadDeadline(time.Time{})
	return count, err
}

func (r *TimeoutReader) ReadByte() (c byte, err error) {
	timeout := time.Now().Add(r.Timeout)
	r.conn.SetReadDeadline(timeout)
	b, err := r.reader.ReadByte()
	r.conn.SetReadDeadline(time.Time{})
	return b, err
}

// TimeoutWriter is a bufio.Writer that reads in chunks of ChunkSize and will
// return a timeout error if the chunk is not read in the Timeout time.
// TODO: Add chunking
type TimeoutWriter struct {
	Timeout   time.Duration
	ChunkSize int
	writer    *bufio.Writer
	conn      net.Conn
}

func NewTimeoutWriter(conn net.Conn) *TimeoutWriter {
	return &TimeoutWriter{
		Timeout:   default_timeout,
		ChunkSize: default_chunksize,
		writer:    bufio.NewWriterSize(conn, default_chunksize),
		conn:      conn,
	}
}

func (w *TimeoutWriter) Write(p []byte) (n int, err error) {
	timeout := time.Now().Add(w.Timeout)
	w.conn.SetWriteDeadline(timeout)
	count, err := w.writer.Write(p)
	w.conn.SetWriteDeadline(time.Time{})
	return count, err
}

func (w *TimeoutWriter) WriteByte(c byte) error {
	timeout := time.Now().Add(w.Timeout)
	w.conn.SetReadDeadline(timeout)
	err := w.writer.WriteByte(c)
	w.conn.SetWriteDeadline(time.Time{})
	return err
}

func (w *TimeoutWriter) Flush() error {
	timeout := time.Now().Add(w.Timeout)
	w.conn.SetWriteDeadline(timeout)
	err := w.writer.Flush()
	w.conn.SetWriteDeadline(time.Time{})
	return err

}

type RingConn struct {
	Conn   net.Conn
	Writer *TimeoutWriter
	sync.Mutex
}

func NewRingConn(conn net.Conn) *RingConn {
	return &RingConn{
		Conn:   conn,
		Writer: NewTimeoutWriter(conn),
	}
}

type TCPMsgRing struct {
	Ring         Ring
	msg_handlers map[uint64]MsgUnmarshaller
	conns        map[string]*RingConn
}

func NewTCPMsgRing(r Ring) *TCPMsgRing {
	return &TCPMsgRing{
		Ring:         r,
		msg_handlers: make(map[uint64]MsgUnmarshaller),
		conns:        make(map[string]*RingConn),
	}
}

func (m *TCPMsgRing) GetAddressForNode(nodeID uint64) string {
	// Just a dummy function for now
	return "127.0.0.1:9999"
}

func (m *TCPMsgRing) GetNodesForPart(ringVersion int64, partition uint32) []uint64 {
	// Just a dummy function for now
	return []uint64{uint64(1), uint64(2)}
}

func (m *TCPMsgRing) MaxMsgLenght() uint64 {
	return math.MaxUint64
}

func (m *TCPMsgRing) SetMsgHandler(msg_type MsgType, handler MsgUnmarshaller) {
	m.msg_handlers[uint64(msg_type)] = handler
}

func (m *TCPMsgRing) MsgToNode(nodeID uint64, msg Msg) {
	m.msgToNode(nodeID, msg)
	msg.Done()
}

func (m *TCPMsgRing) msgToNode(nodeID uint64, msg Msg) {
	// TODO: Add retry functionality
	address := m.GetAddressForNode(nodeID)
	// See if we have a connection already
	conn, ok := m.conns[address]
	if !ok {
		// We need to open a connection
		// TODO: Handle connection timeouts
		tcpconn, err := net.DialTimeout("tcp", address, default_timeout)
		if err != nil {
			log.Println("ERR: Trying to connect to", address, err)
			return
		}
		conn := NewRingConn(tcpconn)
		m.conns[address] = conn
	}
	conn.Lock() // Make sure we only have one writer at a time
	// TODO: Handle write timeouts
	// write the msg type
	binary.Write(conn.Writer, binary.LittleEndian, msg.MsgType())
	// Write the msg size
	binary.Write(conn.Writer, binary.LittleEndian, msg.MsgLength())
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

func (m *TCPMsgRing) MsgToNodeChan(nodeID uint64, msg Msg, retchan chan struct{}) {
	m.msgToNode(nodeID, msg)
	retchan <- struct{}{}
}

func (m *TCPMsgRing) MsgToOtherReplicas(ringVersion int64, partition uint32, msg Msg) {
	nodes := m.GetNodesForPart(ringVersion, partition)
	retchan := make(chan struct{}, 2)
	for _, nodeID := range nodes {
		go m.MsgToNodeChan(nodeID, msg, retchan)
	}
	for i := 0; i < len(nodes); i++ {
		<-retchan
	}
	msg.Done()
}

func (m *TCPMsgRing) handle(conn net.Conn) error {
	reader := NewTimeoutReader(conn)
	var length uint64
	for {
		// for v.00002 we will store this in the fist 8 bytes
		var msg_type uint64
		err := binary.Read(reader, binary.LittleEndian, &msg_type)
		if err != nil {
			log.Println("Closing connection")
			conn.Close()
			return err
		}
		handle, ok := m.msg_handlers[msg_type]
		if !ok {
			log.Println("ERR: Unknown message type", msg_type)
			// TODO: Handle errors better
			log.Println("Closing connection")
			conn.Close()
			return errors.New("Unknown message type")
		}
		// for v.00002 the msg length will be the next 8 bytes
		err = binary.Read(reader, binary.LittleEndian, &length)
		if err != nil {
			log.Println("ERR: Error reading length")
			// TODO: Handle errors better
			log.Println("Closing connection")
			conn.Close()
			return err
		}
		// attempt to handle the message
		consumed, err := handle(reader, length)
		if err != nil {
			log.Println("ERR: Error handling message", err)
			// TODO: Handle errors better
			log.Println("Closing connection")
			conn.Close()
			return err
		}
		if consumed != length {
			log.Println("ERR: Didn't consume whole message", length, consumed)
			// TODO: Handle errors better
			log.Println("Closing connection")
			conn.Close()
			return errors.New("Didn't consume whole message")
		}
	}
}

func (m *TCPMsgRing) Listen(addr string) error {
	tcp_addr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return err
	}
	server, err := net.ListenTCP("tcp", tcp_addr)
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
