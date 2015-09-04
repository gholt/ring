package ring

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"sync"
	"time"
)

type ringConn struct {
	// waitChan indicates this ringConn is placeholder while a connection is
	// being established. If waitChan != nil, a process should block, reading
	// from waitChan, and then discard the ringConn reference and repeat the
	// lookup.
	waitChan chan struct{}

	addr   string
	conn   net.Conn
	reader *timeoutReader
	writer *timeoutWriter
	// When this channel is closed, it signals any corresponding goroutines to
	// exit.
	done chan struct{}
}

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
	queuesLock          sync.RWMutex
	queuesIn            map[string]chan Msg
	queuesOut           map[string]chan Msg
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
		queuesIn:            make(map[string]chan Msg),
		queuesOut:           make(map[string]chan Msg),
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

func (m *TCPMsgRing) MsgToNode(nodeID uint64, msg Msg) {
	node := m.Ring().Node(nodeID)
	if node == nil {
		msg.Done()
		return
	}
	queueIn, _ := m.queuesForAddr(node.Address(m.addressIndex))
	queueIn <- msg
}

func (m *TCPMsgRing) MsgToOtherReplicas(ringVersion int64, partition uint32, msg Msg) {
	r := m.Ring()
	nodes := r.ResponsibleNodes(partition)
	mmsg := &multiMsg{msg: msg, freerChan: make(chan struct{}, len(nodes))}
	queuerChan := make(chan struct{}, len(nodes))
	queuer := func(n Node) {
		queueIn, _ := m.queuesForAddr(n.Address(m.addressIndex))
		queueIn <- mmsg
		queuerChan <- struct{}{}
	}
	var localID uint64
	if localNode := r.LocalNode(); localNode != nil {
		localID = localNode.ID()
	}
	queuers := 0
	for _, node := range nodes {
		if node.ID() != localID {
			go queuer(node)
			queuers++
		}
	}
	if queuers == 0 {
		msg.Done()
		return
	}
	for i := 0; i < queuers; i++ {
		<-queuerChan
	}
	go mmsg.freer(queuers)
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

func (m *TCPMsgRing) queuesForAddr(addr string) (chan Msg, chan Msg) {
	m.queuesLock.RLock()
	queueIn := m.queuesIn[addr]
	queueOut := m.queuesOut[addr]
	m.queuesLock.RUnlock()
	if queueIn != nil {
		return queueIn, queueOut
	}
	m.queuesLock.Lock()
	queueIn = m.queuesIn[addr]
	queueOut = m.queuesOut[addr]
	if queueIn != nil {
		m.queuesLock.Unlock()
		return queueIn, queueOut
	}
	queueIn = make(chan Msg, 10) // TODO: Make configurable
	queueOut = make(chan Msg)
	m.queuesIn[addr] = queueIn
	m.queuesOut[addr] = queueOut
	m.queuesLock.Unlock()
	go func() {
		for !m.Stopped() {
			var msg Msg
			select {
			case msg = <-queueIn:
				select {
				case queueOut <- msg:
				case <-time.After(time.Second): // TODO: Make configurable
				}
			case <-time.After(time.Second):
				// TODO: At some point I want to make this close down old,
				// unused connections. Right now, it's just here so that
				// Stopped is checked once a second.
			}
		}
		close(queueOut)
		m.queuesLock.Lock()
		currentQueueIn := m.queuesIn[addr]
		currentQueueOut := m.queuesOut[addr]
		if currentQueueIn == queueIn {
			delete(m.queuesIn, addr)
		}
		if currentQueueOut == queueOut {
			delete(m.queuesOut, addr)
		}
		m.queuesLock.Unlock()
	}()
	return queueIn, queueOut
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
		conn := m.establishConn(addr, tcpconn)
		for {
			m.connsLock.Lock()
			oldConn := m.conns[addr]
			if oldConn == nil {
				m.conns[addr] = conn
				m.connsLock.Unlock()
				break
			}
			if oldConn.waitChan == nil {
				m.conns[addr] = conn
				m.connsLock.Unlock()
				close(oldConn.done)
				break
			}
			m.connsLock.Unlock()
			<-oldConn.waitChan
		}
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

func (m *TCPMsgRing) ensureConn(addr string) {
	for !m.Stopped() {
		m.connsLock.RLock()
		conn := m.conns[addr]
		m.connsLock.RUnlock()
		if conn != nil {
			return
		}
		m.connsLock.Lock()
		conn = m.conns[addr]
		if conn != nil {
			m.connsLock.Unlock()
			return
		}
		waitChan := make(chan struct{})
		m.conns[addr] = &ringConn{waitChan: waitChan}
		m.connsLock.Unlock()
		tcpconn, err := net.DialTimeout("tcp", addr, m.connectionTimeout)
		if err == nil {
			conn = m.establishConn(addr, tcpconn)
			if conn != nil {
				m.connsLock.Lock()
				m.conns[addr] = conn
				m.connsLock.Unlock()
				close(waitChan)
				return
			}
		} else {
			log.Println("msgToNode error:", err)
		}
		// TODO: Add a configurable sleep here to limit the quickness of
		// reconnect tries.
		time.Sleep(time.Second)
		m.connsLock.Lock()
		delete(m.conns, addr)
		m.connsLock.Unlock()
		close(conn.waitChan)
	}
}

func (m *TCPMsgRing) establishConn(addr string, tcpconn net.Conn) *ringConn {
	// TODO: trade protocol version numbers and local ids etc.
	conn := &ringConn{
		addr:   addr,
		conn:   tcpconn,
		reader: newTimeoutReader(tcpconn, m.chunkSize, m.intraMessageTimeout),
		writer: newTimeoutWriter(tcpconn, m.chunkSize, m.intraMessageTimeout),
		done:   make(chan struct{}),
	}
	go m.ringConnReader(conn)
	go m.ringConnWriter(conn)
	return conn
}

func (m *TCPMsgRing) ringConnReader(conn *ringConn) {
RING_CONN_READER:
	for !m.Stopped() {
		select {
		case <-conn.done:
			break RING_CONN_READER
		default:
		}
		if err := m.ringConnReaderOneMessage(conn); err != nil {
			m.connsLock.Lock()
			if m.conns[conn.addr] == conn {
				delete(m.conns, conn.addr)
			}
			m.connsLock.Unlock()
			close(conn.done)
			// TODO: We need better log handling. Some places are just a todo
			// and some places shoot stuff out the default logger, like here.
			log.Println("ringConnReaderOneMessage error:", err)
			break
		}
	}
}

func (m *TCPMsgRing) ringConnReaderOneMessage(conn *ringConn) error {
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
		// TODO: This should read and discard the unknown message and
		// log/metric that occurrence instead of returning and error which
		// causes a disconnect.
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

func (m *TCPMsgRing) ringConnWriter(conn *ringConn) {
	_, queueOut := m.queuesForAddr(conn.addr)
RING_CONN_WRITER:
	for !m.Stopped() {
		select {
		case <-conn.done:
			break RING_CONN_WRITER
		default:
		}
		msg := <-queueOut
		if msg == nil {
			break
		}
		err := m.ringConnWriterOneMessage(conn, msg)
		msg.Done()
		if err != nil {
			m.connsLock.Lock()
			if m.conns[conn.addr] == conn {
				delete(m.conns, conn.addr)
			}
			m.connsLock.Unlock()
			close(conn.done)
			log.Println("ringConnWriterOneMessage error:", err)
			break
		}
	}
}

func (m *TCPMsgRing) ringConnWriterOneMessage(conn *ringConn, msg Msg) error {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, msg.MsgType())
	if _, err := conn.writer.Write(b); err != nil {
		return err
	}
	binary.BigEndian.PutUint64(b, msg.MsgLength())
	if _, err := conn.writer.Write(b); err != nil {
		return err
	}
	if length, err := msg.WriteContent(conn.writer); err != nil {
		return err
	} else if err = conn.writer.Flush(); err != nil {
		return err
	} else if length != msg.MsgLength() {
		return fmt.Errorf("incorrect message length sent: %d != %d", length, msg.MsgLength())
	}
	return nil
}

type multiMsg struct {
	msg       Msg
	freerChan chan struct{}
}

func (m *multiMsg) MsgType() uint64 {
	return m.msg.MsgType()
}

func (m *multiMsg) MsgLength() uint64 {
	return m.msg.MsgLength()
}

func (m *multiMsg) WriteContent(w io.Writer) (uint64, error) {
	return m.msg.WriteContent(w)
}

func (m *multiMsg) Done() {
	m.freerChan <- struct{}{}
}

func (m *multiMsg) freer(count int) {
	for i := 0; i < count; i++ {
		<-m.freerChan
	}
	m.msg.Done()
}
