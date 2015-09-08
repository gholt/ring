package ring

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"os"
	"sync"
	"time"
)

type LogFunc func(format string, v ...interface{})

// TCPMsgRingConfig represents the set of values for configuring a TCPMsgRing.
// Note that changing the values (shallow changes) in this structure will have
// no effect on existing TCPMsgRings; but deep changes (such as reconfiguring
// an existing Logger) will.
type TCPMsgRingConfig struct {
	// LogCritical sets the func to use for critical messages. Defaults logging
	// to os.Stderr.
	LogCritical LogFunc
	// LogError sets the func to use for error messages. Defaults logging to
	// os.Stderr.
	LogError LogFunc
	// LogWarning sets the func to use for warning messages. Defaults logging
	// to os.Stderr.
	LogWarning LogFunc
	// LogInfo sets the func to use for info messages. Defaults logging to
	// os.Stdout.
	LogInfo LogFunc
	// LogDebug sets the func to use for debug messages. Defaults not logging
	// debug messages.
	LogDebug LogFunc
	// AddressIndex set the index to use with Node.Address(index) to lookup a
	// Node's TCP address.
	AddressIndex int
	// BufferedMessagesPerAddress indicates how many outgoing Msg instances can
	// be buffered before dropping additional ones. Defaults to 8.
	BufferedMessagesPerAddress int
	// ConnectTimeout indicates how many seconds before giving up on a TCP
	// connection establishment. Defaults to 60 seconds.
	ConnectTimeout int
	// ReconnectInterval indicates how many seconds to wait between connection
	// tries. Defaults to 10 seconds.
	ReconnectInterval int
	// ChunkSize indicates how many bytes to attempt to read at once with each
	// network read. Defaults to 16,384 bytes.
	ChunkSize int
	// WithinMessageTimeout indicates how many seconds before giving up on
	// reading data within a message. Defaults to 5 seconds.
	WithinMessageTimeout int
}

func resolveTCPMsgRingConfig(c *TCPMsgRingConfig) *TCPMsgRingConfig {
	cfg := &TCPMsgRingConfig{}
	if c != nil {
		*cfg = *c
	}
	if cfg.LogCritical == nil {
		cfg.LogCritical = log.New(os.Stderr, "TCPMsgRing ", log.LstdFlags).Printf
	}
	if cfg.LogError == nil {
		cfg.LogError = log.New(os.Stderr, "TCPMsgRing ", log.LstdFlags).Printf
	}
	if cfg.LogWarning == nil {
		cfg.LogWarning = log.New(os.Stderr, "TCPMsgRing ", log.LstdFlags).Printf
	}
	if cfg.LogInfo == nil {
		cfg.LogInfo = log.New(os.Stdout, "TCPMsgRing ", log.LstdFlags).Printf
	}
	// LogDebug set as nil is fine and shortcircuits any debug code.
	// AddressIndex defaulting to 0 is fine.
	if cfg.BufferedMessagesPerAddress < 1 {
		cfg.BufferedMessagesPerAddress = 8
	}
	if cfg.ConnectTimeout < 1 {
		cfg.ConnectTimeout = 60
	}
	if cfg.ReconnectInterval < 1 {
		cfg.ReconnectInterval = 10
	}
	if cfg.ChunkSize < 1 {
		cfg.ChunkSize = 16384
	}
	if cfg.WithinMessageTimeout < 1 {
		cfg.WithinMessageTimeout = 5
	}
	return cfg
}

type TCPMsgRing struct {
	logCritical                LogFunc
	logError                   LogFunc
	logWarning                 LogFunc
	logInfo                    LogFunc
	logDebug                   LogFunc
	controlChan                chan struct{}
	ringLock                   sync.RWMutex
	ring                       Ring
	addressIndex               int
	msgHandlersLock            sync.RWMutex
	msgHandlers                map[uint64]MsgUnmarshaller
	bufferedMessagesPerAddress int
	msgChansLock               sync.RWMutex
	msgChans                   map[string]chan Msg
	connectTimeout             time.Duration
	reconnectInterval          time.Duration
	chunkSize                  int
	withinMessageTimeout       time.Duration
}

// NewTCPMsgRing creates a new MsgRing that will use TCP to send and receive
// Msg instances.
func NewTCPMsgRing(c *TCPMsgRingConfig) *TCPMsgRing {
	cfg := resolveTCPMsgRingConfig(c)
	return &TCPMsgRing{
		logCritical:                cfg.LogCritical,
		logError:                   cfg.LogError,
		logWarning:                 cfg.LogWarning,
		logInfo:                    cfg.LogInfo,
		logDebug:                   cfg.LogDebug,
		controlChan:                make(chan struct{}),
		addressIndex:               cfg.AddressIndex,
		msgHandlers:                make(map[uint64]MsgUnmarshaller),
		bufferedMessagesPerAddress: cfg.BufferedMessagesPerAddress,
		msgChans:                   make(map[string]chan Msg),
		connectTimeout:             time.Duration(cfg.ConnectTimeout) * time.Second,
		reconnectInterval:          time.Duration(cfg.ReconnectInterval) * time.Second,
		chunkSize:                  cfg.ChunkSize,
		withinMessageTimeout:       time.Duration(cfg.WithinMessageTimeout) * time.Second,
	}
}

// Ring returns the ring information used to determine messaging endpoints;
// note that this method may return nil if no ring information is yet
// available.
func (t *TCPMsgRing) Ring() Ring {
	t.ringLock.RLock()
	r := t.ring
	t.ringLock.RUnlock()
	return r
}

// SetRing sets the ring whose information used to determine messaging
// endpoints.
func (t *TCPMsgRing) SetRing(ring Ring) {
	t.ringLock.Lock()
	t.ring = ring
	t.ringLock.Unlock()
}

// MaxMsgLength indicates the maximum number of bytes the content of a message
// may contain to be handled by this TCPMsgRing.
func (t *TCPMsgRing) MaxMsgLength() uint64 {
	return math.MaxUint64
}

// MsgHandler returns the handler for the given message type, if there is any
// set.
func (t *TCPMsgRing) MsgHandler(msgType uint64) MsgUnmarshaller {
	t.msgHandlersLock.RLock()
	handler := t.msgHandlers[msgType]
	t.msgHandlersLock.RUnlock()
	return handler
}

// SetMsgHandler associates a message type with a handler; any incoming
// messages with the type will be delivered to the handler. Message types just
// need to be unique uint64 values; usually picking 64 bits of a UUID is fine.
func (t *TCPMsgRing) SetMsgHandler(msgType uint64, handler MsgUnmarshaller) {
	t.msgHandlersLock.Lock()
	t.msgHandlers[uint64(msgType)] = handler
	t.msgHandlersLock.Unlock()
}

// MsgToNode queues the message for delivery to the indicated node; the timeout
// should be considered for queueing, not for actual delivery.
//
// When the msg has actually been sent or has been discarded due to delivery
// errors or delays, msg.Free() will be called.
func (t *TCPMsgRing) MsgToNode(msg Msg, nodeID uint64, timeout time.Duration) {
	ring := t.Ring()
	if ring == nil {
		msg.Free()
		return
	}
	node := ring.Node(nodeID)
	if node == nil {
		msg.Free()
		return
	}
	t.msgToAddr(msg, node.Address(t.addressIndex), timeout)
}

// MsgToNode queues the message for delivery to all other replicas of a
// partition; the timeout should be considered for queueing, not for actual
// delivery.
//
// If the ring is not bound to a specific node (LocalNode() returns nil) then
// the delivery attempts will be to all replicas.
//
// When the msg has actually been sent or has been discarded due to delivery
// errors or delays, msg.Free() will be called.
func (t *TCPMsgRing) MsgToOtherReplicas(msg Msg, partition uint32, timeout time.Duration) {
	ring := t.Ring()
	if ring == nil {
		msg.Free()
		return
	}
	nodes := ring.ResponsibleNodes(partition)
	mmsg := &multiMsg{msg: msg, freerChan: make(chan struct{}, len(nodes))}
	toAddrChan := make(chan struct{}, len(nodes))
	toAddr := func(addr string) {
		t.msgToAddr(mmsg, addr, timeout)
		toAddrChan <- struct{}{}
	}
	var localID uint64
	if localNode := ring.LocalNode(); localNode != nil {
		localID = localNode.ID()
	}
	toAddrs := 0
	for _, node := range nodes {
		if node.ID() != localID {
			go toAddr(node.Address(t.addressIndex))
			toAddrs++
		}
	}
	if toAddrs == 0 {
		msg.Free()
		return
	}
	for i := 0; i < toAddrs; i++ {
		<-toAddrChan
	}
	go mmsg.freer(toAddrs)
}

// Listen on the configured TCP port, accepting new connections and processing
// messages from those connections; this function will not return until
// t.Shutdown() is called.
func (t *TCPMsgRing) Listen() {
	var err error
OuterLoop:
	for {
		if err != nil {
			t.logCritical("listen: %s\n", err)
			time.Sleep(time.Second)
		}
		select {
		case <-t.controlChan:
			break OuterLoop
		default:
		}
		ring := t.Ring()
		if ring == nil {
			time.Sleep(time.Second)
			continue
		}
		node := ring.LocalNode()
		var tcpAddr *net.TCPAddr
		tcpAddr, err = net.ResolveTCPAddr("tcp", node.Address(t.addressIndex))
		if err != nil {
			continue
		}
		var server *net.TCPListener
		server, err = net.ListenTCP("tcp", tcpAddr)
		if err != nil {
			continue
		}
		for {
			select {
			case <-t.controlChan:
				break OuterLoop
			default:
			}
			// Deadline to force checking t.controlChan once a second.
			server.SetDeadline(time.Now().Add(time.Second))
			var conn net.Conn
			conn, err = server.AcceptTCP()
			if err != nil {
				if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
					continue
				}
				server.Close()
				continue OuterLoop
			}
			addr := conn.RemoteAddr().String()
			msgChan, created := t.msgChanForAddr(addr)
			// NOTE: If created is true, it'll indicate to connection that
			// redialing is okay. If created is false, once the connection has
			// terminated it won't be reestablished since there is already
			// another connection running that will redial.
			go t.connection(addr, conn, msgChan, created)
		}
	}
}

// Shutdown will signal the shutdown of all connections, listeners, etc.
// related to the TCPMsgRing; once Shutdown you must create a new TCPMsgRing to
// restart operations.
func (t *TCPMsgRing) Shutdown() {
	close(t.controlChan)
}

// msgChanForAddr returns the channel for the address as well as a bool
// indicating whether this call created the channel (true) or found it already
// existing (false).
func (t *TCPMsgRing) msgChanForAddr(addr string) (chan Msg, bool) {
	t.msgChansLock.RLock()
	msgChan := t.msgChans[addr]
	t.msgChansLock.RUnlock()
	if msgChan != nil {
		return msgChan, false
	}
	t.msgChansLock.Lock()
	msgChan = t.msgChans[addr]
	if msgChan != nil {
		t.msgChansLock.Unlock()
		return msgChan, false
	}
	msgChan = make(chan Msg, t.bufferedMessagesPerAddress)
	t.msgChans[addr] = msgChan
	t.msgChansLock.Unlock()
	return msgChan, true
}

func (t *TCPMsgRing) msgToAddr(msg Msg, addr string, timeout time.Duration) {
	msgChan, created := t.msgChanForAddr(addr)
	if created {
		go t.connection(addr, nil, msgChan, true)
	}
	timer := time.NewTimer(timeout)
	select {
	case <-t.controlChan:
		timer.Stop()
		msg.Free()
	case msgChan <- msg:
		timer.Stop()
	case <-timer.C:
		msg.Free()
	}
	// TODO: Uncertain the code block above is better than that below.
	//       Seems reasonable to Stop a timer if it won't be used; but
	//       perhaps that's more expensive than just letting it fire.
	//	select {
	//	case <-t.controlChan:
	//		msg.Free()
	//	case msgChan <- msg:
	//	case <-time.After(timeout):
	//		msg.Free()
	//	}
}

func (t *TCPMsgRing) connection(addr string, netConn net.Conn, msgChan chan Msg, dialOk bool) {
OuterLoop:
	for {
		select {
		case <-t.controlChan:
			break OuterLoop
		default:
		}
		var err error
		if netConn == nil {
			if !dialOk {
				break OuterLoop
			}
			netConn, err = net.DialTimeout("tcp", addr, t.connectTimeout)
			if err != nil {
				netConn = nil
				t.logError("connection: %s\n", err)
				time.Sleep(t.reconnectInterval)
				continue OuterLoop
			}
		}
		readerReturnChan := make(chan struct{}, 1)
		readerControlChan := make(chan struct{})
		go func() {
			t.readMsgs(readerControlChan, newTimeoutReader(netConn, t.chunkSize, t.withinMessageTimeout))
			readerReturnChan <- struct{}{}
		}()
		writerReturnChan := make(chan struct{}, 1)
		go func() {
			t.writeMsgs(newTimeoutWriter(netConn, t.chunkSize, t.withinMessageTimeout), msgChan)
			writerReturnChan <- struct{}{}
		}()
		select {
		case <-t.controlChan:
		case <-readerReturnChan:
		case <-writerReturnChan:
		}
		close(readerControlChan)
		netConn.Close()
		netConn = nil
	}
}

func (t *TCPMsgRing) readMsgs(readerControlChan chan struct{}, reader *timeoutReader) {
	var err error
OuterLoop:
	for {
		err = t.readMsg(reader)
		select {
		case <-readerControlChan:
			break OuterLoop
		default:
		}
		if err != nil {
			t.logError("readMsg: %s\n", err)
		}
	}
}

func (t *TCPMsgRing) readMsg(reader *timeoutReader) error {
	var msgType uint64
	timeout := reader.Timeout
	// Wait forever for the first byte or closed/eof error.
	reader.Timeout = math.MaxInt64
	b, err := reader.ReadByte()
	reader.Timeout = timeout
	if err != nil {
		return err
	}
	msgType = uint64(b)
	for i := 1; i < 8; i++ {
		b, err = reader.ReadByte()
		if err != nil {
			return err
		}
		msgType <<= 8
		msgType |= uint64(b)
	}
	handler := t.MsgHandler(msgType)
	if handler == nil {
		// TODO: This should read and discard the unknown message and
		// log/metric that occurrence instead of returning and error which
		// causes a disconnect.
		return fmt.Errorf("no handler for %x", msgType)
	}
	var length uint64
	for i := 0; i < 8; i++ {
		b, err = reader.ReadByte()
		if err != nil {
			return err
		}
		length <<= 8
		length |= uint64(b)
	}
	// CONSIDER: The reader has a timeout that would trigger on actual reads
	// the handler does, but if the handler goes off in an infinite loop and
	// does not attempt any reads, the timeout would have no effect. However,
	// using time.After or something similar for every message is probably
	// overly expensive, so bad handler code may be an acceptable risk here.
	consumed, err := handler(reader, length)
	if consumed != length {
		if err == nil {
			err = fmt.Errorf("handler %x did not read %d bytes; only read %d", msgType, length, consumed)
		}
	}
	if err != nil {
		return err
	}
	return nil
}

func (t *TCPMsgRing) writeMsgs(writer *timeoutWriter, msgChan chan Msg) {
	for msg := range msgChan {
		if err := t.writeMsg(writer, msg); err != nil {
			t.logError("writeMsg: %s\n", err)
			break
		}
	}
}

func (t *TCPMsgRing) writeMsg(writer *timeoutWriter, msg Msg) error {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, msg.MsgType())
	if _, err := writer.Write(b); err != nil {
		return err
	}
	binary.BigEndian.PutUint64(b, msg.MsgLength())
	if _, err := writer.Write(b); err != nil {
		return err
	}
	if length, err := msg.WriteContent(writer); err != nil {
		return err
	} else if err = writer.Flush(); err != nil {
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

func (m *multiMsg) Free() {
	m.freerChan <- struct{}{}
}

func (m *multiMsg) freer(count int) {
	for i := 0; i < count; i++ {
		<-m.freerChan
	}
	m.msg.Free()
}
