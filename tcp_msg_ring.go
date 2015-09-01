package ring

import (
	"encoding/binary"
	"fmt"
	"log"
	"math"
	"net"
	"sync"
	"time"
)

type ringConn struct {
	addr       string
	conn       net.Conn
	reader     *timeoutReader
	writerLock sync.Mutex
	writer     *timeoutWriter
}

// This is used as a placeholder in TCPMsgring.conns to indicate a goroutine is
// currently working on a connection with that address.
var _WORKING = &ringConn{}

// These are constants for use with TCPMsgRing.state
const (
	_STOPPED = iota
	_RUNNING
	_STOPPING
)

type TCPMsgRing struct {
	lock sync.RWMutex
	// addressIndex is the index given to a Node's Address method to determine
	// the network address to connect to (see Node's Address method for more
	// information).
	addressIndex        int
	chunkSize           int
	connectionTimeout   time.Duration
	intraMessageTimeout time.Duration
	interMessageTimeout time.Duration
	ring                Ring
	msgHandlers         map[uint64]MsgUnmarshaller
	conns               map[string]*ringConn
	stateLock           sync.RWMutex
	state               int
	stateNowStoppedChan chan struct{}
}

func NewTCPMsgRing(r Ring) *TCPMsgRing {
	return &TCPMsgRing{
		ring:                r,
		msgHandlers:         make(map[uint64]MsgUnmarshaller),
		conns:               make(map[string]*ringConn),
		chunkSize:           16 * 1024,
		connectionTimeout:   60 * time.Second,
		intraMessageTimeout: 2 * time.Second,
		interMessageTimeout: 2 * time.Hour,
	}
}

func (m *TCPMsgRing) Ring() Ring {
	m.lock.RLock()
	r := m.ring
	m.lock.RUnlock()
	return r
}

func (m *TCPMsgRing) SetRing(ring Ring) {
	m.lock.Lock()
	m.ring = ring
	m.lock.Unlock()
}

func (m *TCPMsgRing) MaxMsgLength() uint64 {
	return math.MaxUint64
}

func (m *TCPMsgRing) SetMsgHandler(msgType uint64, handler MsgUnmarshaller) {
	m.lock.Lock()
	m.msgHandlers[uint64(msgType)] = handler
	m.lock.Unlock()
}

func (m *TCPMsgRing) MsgToNode(nodeID uint64, msg Msg) {
	for i := time.Second; i <= 4*time.Second && !m.Stopped(); i *= 2 {
		node := m.Ring().Node(nodeID)
		if node != nil && m.msgToNode(msg, node) == nil {
			break
		}
		time.Sleep(i)
	}
	msg.Done()
}

func (m *TCPMsgRing) connection(addr string) *ringConn {
	if m.Stopped() {
		return nil
	}
	m.lock.RLock()
	conn := m.conns[addr]
	m.lock.RUnlock()
	if conn != nil && conn != _WORKING {
		return conn
	}
	m.lock.Lock()
	conn = m.conns[addr]
	if conn != nil && conn != _WORKING {
		m.lock.Unlock()
		return conn
	}
	if m.Stopped() {
		m.lock.Unlock()
		return nil
	}
	m.conns[addr] = _WORKING
	m.lock.Unlock()
	go func() {
		tcpconn, err := net.DialTimeout("tcp", addr, m.connectionTimeout)
		if err != nil {
			m.disconnect(addr)
			// TODO: Log error.
		} else {
			go m.handleTCPConnection(addr, tcpconn)
		}
	}()
	return nil
}

func (m *TCPMsgRing) disconnect(addr string) {
	m.lock.Lock()
	conn := m.conns[addr]
	m.conns[addr] = _WORKING
	m.lock.Unlock()
	if conn != nil && conn != _WORKING {
		if err := conn.conn.Close(); err != nil {
			log.Println("tcp msg ring disconnect close err:", err)
		}
	}
	// TODO: Add a configurable sleep here to limit the quickness of reconnect
	// tries.
	m.lock.Lock()
	delete(m.conns, addr)
	m.lock.Unlock()
}

// handleTCPConnection will not return until the connection is closed; so be
// sure to run it in a background goroutine.
func (m *TCPMsgRing) handleTCPConnection(addr string, tcpconn net.Conn) {
	// TODO: trade version numbers and local ids
	conn := &ringConn{
		addr:   addr,
		conn:   tcpconn,
		reader: newTimeoutReader(tcpconn, m.chunkSize, m.intraMessageTimeout),
		writer: newTimeoutWriter(tcpconn, m.chunkSize, m.intraMessageTimeout),
	}
	m.lock.Lock()
	m.conns[addr] = conn
	m.lock.Unlock()
	m.handleConnection(conn)
	m.disconnect(addr)
}

func (m *TCPMsgRing) handleConnection(conn *ringConn) {
	for !m.Stopped() {
		if err := m.handleOneMessage(conn); err != nil {
			log.Println("handleOneMessage error:", err)
			break
		}
	}
}

func (m *TCPMsgRing) handleOneMessage(conn *ringConn) error {
	var msgType uint64
	conn.reader.Timeout = m.interMessageTimeout
	b, err := conn.reader.ReadByte()
	conn.reader.Timeout = m.intraMessageTimeout
	if err != nil {
		return err
	}
	msgType = uint64(b)
	for i := 1; i < 8; i++ {
		b, err = conn.reader.ReadByte()
		if err != nil {
			return err
		}
		msgType <<= 8
		msgType |= uint64(b)
	}
	handler := m.msgHandlers[msgType]
	if handler == nil {
		return fmt.Errorf("no handler for MsgType %x", msgType)
	}
	var length uint64
	for i := 0; i < 8; i++ {
		b, err = conn.reader.ReadByte()
		if err != nil {
			return err
		}
		length <<= 8
		length |= uint64(b)
	}
	consumed, err := handler(conn.reader, length)
	if consumed != length {
		if err == nil {
			err = fmt.Errorf("did not read %d bytes; only read %d", length, consumed)
		}
	}
	if err != nil {
		return err
	}
	return nil
}

func (m *TCPMsgRing) msgToNode(msg Msg, node Node) error {
	conn := m.connection(node.Address(m.addressIndex))
	if conn == nil {
		return fmt.Errorf("no connection")
	}
	conn.writerLock.Lock()
	disconnect := func(err error) error {
		log.Println("msgToNode error:", err)
		m.disconnect(node.Address(m.addressIndex))
		conn.writerLock.Unlock()
		return err
	}
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, msg.MsgType())
	_, err := conn.writer.Write(b)
	if err != nil {
		return disconnect(err)
	}
	binary.BigEndian.PutUint64(b, msg.MsgLength())
	_, err = conn.writer.Write(b)
	if err != nil {
		return disconnect(err)
	}
	length, err := msg.WriteContent(conn.writer)
	if err != nil {
		return disconnect(err)
	}
	err = conn.writer.Flush()
	if err != nil {
		return disconnect(err)
	}
	if length != msg.MsgLength() {
		return disconnect(fmt.Errorf("incorrect message length sent: %d != %d", length, msg.MsgLength()))
	}
	conn.writerLock.Unlock()
	return nil
}

func (m *TCPMsgRing) msgToNodeChan(msg Msg, node Node, retchan chan struct{}) {
	m.msgToNode(msg, node)
	retchan <- struct{}{}
}

func (m *TCPMsgRing) MsgToOtherReplicas(ringVersion int64, partition uint32, msg Msg) {
	r := m.Ring()
	if ringVersion != r.Version() {
		msg.Done()
		return
	}
	nodes := r.ResponsibleNodes(partition)
	retchan := make(chan struct{}, len(nodes))
	localNode := r.LocalNode()
	var localID uint64
	if localNode != nil {
		localID = localNode.ID()
	}
	sent := 0
	for _, node := range nodes {
		if node.ID() != localID {
			go m.msgToNodeChan(msg, node, retchan)
			sent++
		}
	}
	for ; sent > 0; sent-- {
		<-retchan
	}
	msg.Done()
}

// Start will launch a goroutine that will listen on the configured TCP port,
// accepting new connections and processing messages from those connections.
// The "chan error" that is returned may be read from to obtain any error the
// goroutine encounters or nil if the goroutine exits with no error due to
// Stop() being called. Note that if Stop() is never called and an error is
// never encountered, reading from this returned "chan error" will never
// return.
func (m *TCPMsgRing) Start() chan error {
	returnChan := make(chan error, 1)
	m.stateLock.Lock()
	switch m.state {
	case _STOPPED:
		m.state = _RUNNING
		go m.listen(returnChan)
	case _RUNNING:
		returnChan <- fmt.Errorf("already running")
	case _STOPPING:
		returnChan <- fmt.Errorf("stopping in progress")
	default:
		returnChan <- fmt.Errorf("unexpected state value %d", m.state)
	}
	m.stateLock.Unlock()
	return returnChan
}

func (m *TCPMsgRing) listen(returnChan chan error) {
	node := m.Ring().LocalNode()
	tcpAddr, err := net.ResolveTCPAddr("tcp", node.Address(m.addressIndex))
	if err != nil {
		returnChan <- err
		return
	}
	server, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		returnChan <- err
		return
	}
	var tcpconn net.Conn
	for !m.Stopped() {
		server.SetDeadline(time.Now().Add(time.Second))
		tcpconn, err = server.AcceptTCP()
		if err != nil {
			if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
				err = nil
				continue
			}
			log.Println("Listen/AcceptTCP error:", err)
			server.Close()
			break
		}
		addr := tcpconn.RemoteAddr().String()
		m.lock.Lock()
		c := m.conns[addr]
		if c != nil {
			c.conn.Close()
		}
		m.conns[addr] = _WORKING
		m.lock.Unlock()
		go m.handleTCPConnection(addr, tcpconn)
	}
	m.stateLock.Lock()
	m.state = _STOPPED
	if m.stateNowStoppedChan != nil {
		m.stateNowStoppedChan <- struct{}{}
		m.stateNowStoppedChan = nil
	}
	m.stateLock.Unlock()
	returnChan <- err
}

// Stop will send a stop signal the goroutine launched by Start(). When this
// method returns, the TCPMsgRing will not be listening for new incoming
// connections. It is okay to call Stop() even if the TCPMsgRing is already
// Stopped().
func (m *TCPMsgRing) Stop() {
	var c chan struct{}
	m.stateLock.Lock()
	if m.state == _RUNNING {
		m.state = _STOPPING
		c = make(chan struct{}, 1)
		m.stateNowStoppedChan = c
	}
	m.stateLock.Unlock()
	if c != nil {
		<-c
	}
}

// Stopped will be true if the TCPMsgRing is not currently listening for new
// connections.
func (m *TCPMsgRing) Stopped() bool {
	m.stateLock.RLock()
	s := m.state
	m.stateLock.RUnlock()
	return s == _STOPPED
}
