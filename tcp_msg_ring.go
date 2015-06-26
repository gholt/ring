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

func (m *TCPMsgRing) MaxMsgLength() uint64 {
	return math.MaxUint64
}

func (m *TCPMsgRing) SetMsgHandler(msgType uint64, handler MsgUnmarshaller) {
	m.lock.Lock()
	m.msgHandlers[uint64(msgType)] = handler
	m.lock.Unlock()
}

func (m *TCPMsgRing) MsgToNode(nodeID uint64, msg Msg) {
	for i := time.Second; i <= 4*time.Second; i *= 2 {
		node := m.Ring().Node(nodeID)
		if node != nil && m.msgToNode(msg, node) == nil {
			break
		}
		time.Sleep(i)
	}
	msg.Done()
}

func (m *TCPMsgRing) connection(addr string) (*ringConn, error) {
	m.lock.RLock()
	conn := m.conns[addr]
	m.lock.RUnlock()
	if conn == nil {
		m.lock.Lock()
		conn = m.conns[addr]
		if conn == nil {
			tcpconn, err := net.DialTimeout("tcp", addr, m.connectionTimeout)
			if err != nil {
				return nil, fmt.Errorf("connection error with %s: %s", addr, err)
			}
			conn = &ringConn{
				addr:   addr,
				conn:   tcpconn,
				reader: newTimeoutReader(tcpconn, m.chunkSize, m.intraMessageTimeout),
				writer: newTimeoutWriter(tcpconn, m.chunkSize, m.intraMessageTimeout),
			}
			m.conns[addr] = conn
			go m.handleForever(conn)
		}
		m.lock.Unlock()
	}
	return conn, nil
}

func (m *TCPMsgRing) disconnection(addr string) {
	m.lock.Lock()
	conn := m.conns[addr]
	delete(m.conns, addr)
	m.lock.Unlock()
	if conn != nil {
		conn.conn.Close()
	}
}

func (m *TCPMsgRing) msgToNode(msg Msg, node Node) error {
	conn, err := m.connection(node.Address(m.addressIndex))
	if err != nil {
		return err
	}
	conn.writerLock.Lock()
	disconnect := func(err error) error {
		log.Println("msgToNode error:", err)
		m.disconnection(node.Address(m.addressIndex))
		conn.writerLock.Unlock()
		return err
	}
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, msg.MsgType())
	_, err = conn.writer.Write(b)
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

func (m *TCPMsgRing) handleOne(conn *ringConn) error {
	var msgType uint64 = 0
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
	var length uint64 = 0
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

func (m *TCPMsgRing) handleForever(conn *ringConn) {
	for {
		if err := m.handleOne(conn); err != nil {
			log.Println("handleForever error:", err)
			m.disconnection(conn.addr)
			break
		}
	}
}

func (m *TCPMsgRing) Listen() error {
	node := m.ring.LocalNode()
	tcpAddr, err := net.ResolveTCPAddr("tcp", node.Address(m.addressIndex))
	if err != nil {
		return err
	}
	server, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return err
	}
	for {
		tcpconn, err := server.AcceptTCP()
		if err != nil {
			log.Println("Listen/AcceptTCP error:", err)
			server.Close()
			return err
		}
		addr := tcpconn.RemoteAddr().String()
		conn := &ringConn{
			addr:   addr,
			conn:   tcpconn,
			reader: newTimeoutReader(tcpconn, m.chunkSize, m.intraMessageTimeout),
			writer: newTimeoutWriter(tcpconn, m.chunkSize, m.intraMessageTimeout),
		}
		m.lock.Lock()
		c := m.conns[addr]
		if c != nil {
			c.conn.Close()
		}
		m.conns[addr] = conn
		m.lock.Unlock()
		go m.handleForever(conn)
	}
}
