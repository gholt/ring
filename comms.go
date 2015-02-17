package ring

import (
	"bufio"
	"encoding/binary"
	"errors"
	"log"
	"net"
	"sync"
	"time"
)

var (
	default_chunksize int           = 1024 * 16       // 16Kb
	default_timeout   time.Duration = 2 * time.Second // 2 seconds
)

// TimeoutReader is an bufio.Reader that reads in chunks of ChunkSize and
// will return a timeout error if the chunk is not read in the Timeout
// time.
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

// TimeoutWriter is an bufio.Writer that reads in chunks of ChunkSize and
// will return a timeout error if the chunk is not read in the Timeout
// time.
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

type LockConn struct {
	// We need lockable connections
	conn net.Conn
	sync.Mutex
}

func NewLockConn(c net.Conn) *LockConn {
	return &LockConn{
		conn: c,
	}
}

type TCPMsgRing struct {
	Ring         Ring
	msg_handlers map[uint]MsgUnmarshaller
	conns        map[string]*LockConn
}

func NewTCPMsgRing(r Ring) *TCPMsgRing {
	return &TCPMsgRing{
		Ring:         r,
		msg_handlers: make(map[uint]MsgUnmarshaller),
		conns:        make(map[string]*LockConn),
	}
}

func (m *TCPMsgRing) GetAddressForNode(nodeID uint64) string {
	// Just a dummy function for now
	return "127.0.0.1:9999"
}

func (m *TCPMsgRing) GetNodesForPart(ringID uint64, partition uint32) []uint64 {
	// Just a dummy function for now
	return []uint64{uint64(1), uint64(2)}
}

func (m *TCPMsgRing) SetMsgHandler(msg_type MsgType, handler MsgUnmarshaller) {
	m.msg_handlers[uint(msg_type)] = handler
}

func (m *TCPMsgRing) MsgToNode(nodeID uint64, msg Msg) bool {
	// TODO: Add retry functionality
	address := m.GetAddressForNode(nodeID)
	// See if we have a connection already
	conn, ok := m.conns[address]
	if !ok {
		// We need to open a connection
		// TODO: Handle connection timeouts
		//var err error = nil
		tcpconn, err := net.DialTimeout("tcp", address, 3*time.Second)
		if err != nil {
			log.Println("ERR: Trying to connect to", address, err)
			return false
		}
		conn := NewLockConn(tcpconn)
		m.conns[address] = conn
	}
	conn.Lock() // Make sure we only have one writer at a time
	writer := NewTimeoutWriter(conn.conn)
	// TODO: Handle write timeouts
	// write the msg type
	writer.WriteByte(byte(msg.MsgType()))
	// Write the msg size
	binary.Write(writer, binary.LittleEndian, msg.MsgLength())
	// Write the msg
	length, err := msg.WriteContent(writer)
	// Make sure we flush the data
	writer.Flush()
	conn.Unlock()
	msg.Done()
	if err != nil {
		log.Println("ERR: Sending content - ", err)
		return false
	}
	if length != msg.MsgLength() {
		log.Println("ERR: Didn't send enough data", length, msg.MsgLength())
		return false
	}
	// If we get here then things must have gone alright
	return true
}

func (m *TCPMsgRing) MsgToNodeChan(nodeID uint64, msg Msg, retchan chan bool) {
	val := m.MsgToNode(nodeID, msg)
	retchan <- val
}

func (m *TCPMsgRing) MsgToOtherReplicas(ringID uint64, partition uint32, msg Msg) bool {
	nodes := m.GetNodesForPart(ringID, partition)
	retchan := make(chan bool, 2)
	for _, nodeID := range nodes {
		go m.MsgToNodeChan(nodeID, msg, retchan)
	}
	for i := 0; i < len(nodes); i++ {
		val := <-retchan
		if !val {
			// One of the failed, so return false
			return false
		}
	}
	// If we get here then things must have gone alright
	return true
}

func (m *TCPMsgRing) handle(conn net.Conn) error {
	reader := NewTimeoutReader(conn)
	var length uint64
	for {
		// for v.00001 of this we will just store the msg type in the 1st byte
		msg_type, err := reader.ReadByte()
		if err != nil {
			log.Println("Closing connection")
			conn.Close()
			return err
		}
		handle, ok := m.msg_handlers[uint(msg_type)]
		if !ok {
			log.Println("ERR: Unknown message type", msg_type)
			// TODO: Handle errors better
			log.Println("Closing connection")
			conn.Close()
			return errors.New("Unknown message type")
		}
		// for v.00001 the msg length will be the next 8 bytes
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
