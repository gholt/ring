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
	// addressIndex is the index given to a Node's Address method to determine
	// the network address to connect to (see Node's Address method for more
	// information).
	addressIndex        int
	chunkSize           int
	connectionTimeout   time.Duration
	intraMessageTimeout time.Duration
	interMessageTimeout time.Duration
	ringLock            sync.RWMutex
	ring                Ring
	msgHandlersLock     sync.RWMutex
	msgHandlers         map[uint64]MsgUnmarshaller
	connsLock           sync.RWMutex
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
	m.ringLock.RLock()
	r := m.ring
	m.ringLock.RUnlock()
	return r
}

func (m *TCPMsgRing) SetRing(ring Ring) {
	m.ringLock.Lock()
	m.ring = ring
	m.ringLock.Unlock()
}

func (m *TCPMsgRing) MaxMsgLength() uint64 {
	return math.MaxUint64
}

func (m *TCPMsgRing) SetMsgHandler(msgType uint64, handler MsgUnmarshaller) {
	m.msgHandlersLock.Lock()
	m.msgHandlers[uint64(msgType)] = handler
	m.msgHandlersLock.Unlock()
}

func (m *TCPMsgRing) connection(addr string) *ringConn {
	if m.Stopped() {
		return nil
	}
	m.connsLock.RLock()
	conn := m.conns[addr]
	m.connsLock.RUnlock()
	if conn != nil && conn != _WORKING {
		return conn
	}
	m.connsLock.Lock()
	conn = m.conns[addr]
	if conn != nil && conn != _WORKING {
		m.connsLock.Unlock()
		return conn
	}
	if m.Stopped() {
		m.connsLock.Unlock()
		return nil
	}
	m.conns[addr] = _WORKING
	m.connsLock.Unlock()
	go func() {
		tcpconn, err := net.DialTimeout("tcp", addr, m.connectionTimeout)
		if err != nil {
			// TODO: Log error.
			// TODO: Add a configurable sleep here to limit the quickness of
			// reconnect tries.
			time.Sleep(time.Second)
			m.connsLock.Lock()
			delete(m.conns, addr)
			m.connsLock.Unlock()
		} else {
			go m.handleTCPConnection(addr, tcpconn)
		}
	}()
	return nil
}

func (m *TCPMsgRing) disconnect(addr string, conn *ringConn) {
	m.connsLock.Lock()
	if m.conns[addr] != conn {
		m.connsLock.Unlock()
		return
	}
	m.conns[addr] = _WORKING
	m.connsLock.Unlock()
	go func() {
		if err := conn.conn.Close(); err != nil {
			log.Println("tcp msg ring disconnect close err:", err)
		}
		// TODO: Add a configurable sleep here to limit the quickness of
		// reconnect tries.
		time.Sleep(time.Second)
		m.connsLock.Lock()
		delete(m.conns, addr)
		m.connsLock.Unlock()
	}()
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
	m.connsLock.Lock()
	m.conns[addr] = conn
	m.connsLock.Unlock()
	m.handleConnection(conn)
	m.disconnect(addr, conn)
}

func (m *TCPMsgRing) handleConnection(conn *ringConn) {
	for !m.Stopped() {
		if err := m.handleOneMessage(conn); err != nil {
			// TODO: We need better log handling. Some places are just a todo
			// and some places shoot stuff out the default logger, like here.
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
	m.msgHandlersLock.RLock()
	handler := m.msgHandlers[msgType]
	m.msgHandlersLock.RUnlock()
	if handler == nil {
		// TODO: This should read and discard the unknown message instead of
		// causing a disconnection.
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
	// NOTE: The conn.reader has a Timeout that would trigger on actual reads
	// the handler does, but if the handler goes off to do some long running
	// processing and does not attempt any reader operations for a while, the
	// timeout would have no effect. However, using time.After for every
	// message is overly expensive, so bad handler code is just an acceptable
	// risk here.
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

func (m *TCPMsgRing) MsgToNode(nodeID uint64, msg Msg) {
	node := m.Ring().Node(nodeID)
	if node != nil {
		m.msgToNode(msg, node)
	}
	msg.Done()
}

func (m *TCPMsgRing) msgToNode(msg Msg, node Node) {
	addr := node.Address(m.addressIndex)
	conn := m.connection(addr)
	if conn == nil {
		// TODO: Log, or count as a metric, or something.
		return
	}
	// NOTE: If there are a lot of messages to be sent to a node, this lock
	// wait could get significant. However, using a time.After is too
	// expensive. Perhaps we can refactor to place messages on a buffered
	// channel where we can more easily detect a full channel and immediately
	// drop additional messages until the channel has space.
	conn.writerLock.Lock()
	disconn := func(err error) {
		// TODO: Whatever we end up doing for logging, metrics, etc. should be
		// done very quickly or in the background.
		log.Println("msgToNode error:", err)
		m.disconnect(addr, conn)
		conn.writerLock.Unlock()
	}
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, msg.MsgType())
	_, err := conn.writer.Write(b)
	if err != nil {
		disconn(err)
		return
	}
	binary.BigEndian.PutUint64(b, msg.MsgLength())
	_, err = conn.writer.Write(b)
	if err != nil {
		disconn(err)
		return
	}
	length, err := msg.WriteContent(conn.writer)
	if err != nil {
		disconn(err)
		return
	}
	err = conn.writer.Flush()
	if err != nil {
		disconn(err)
		return
	}
	if length != msg.MsgLength() {
		disconn(fmt.Errorf("incorrect message length sent: %d != %d", length, msg.MsgLength()))
		return
	}
	conn.writerLock.Unlock()
}

func (m *TCPMsgRing) MsgToOtherReplicas(ringVersion int64, partition uint32, msg Msg) {
	r := m.Ring()
	if ringVersion != r.Version() {
		msg.Done()
		return
	}
	nodes := r.ResponsibleNodes(partition)
	retchan := make(chan struct{}, len(nodes))
	msgToNodeConc := func(n Node) {
		m.msgToNode(msg, n)
		retchan <- struct{}{}
	}
	localNode := r.LocalNode()
	var localID uint64
	if localNode != nil {
		localID = localNode.ID()
	}
	sent := 0
	for _, node := range nodes {
		if node.ID() != localID {
			go msgToNodeConc(node)
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
		m.connsLock.Lock()
		c := m.conns[addr]
		if c != nil {
			c.conn.Close()
		}
		m.conns[addr] = _WORKING
		m.connsLock.Unlock()
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
	return s == _STOPPED || s == _STOPPING
}
