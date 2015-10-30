package ring

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"net"
	"os"
	"sync"
	"sync/atomic"
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
	// UseTLS enables use of TLS for server and client comms
	UseTLS     bool
	CertFile   string
	KeyFile    string
	SkipVerify bool
}

// newClientTLSFromFile constructs a TLS from the input certificate file for client.
func newClientTLSFromFile(certFile, serverName string, SkipVerify bool) (*tls.Config, error) {
	b, err := ioutil.ReadFile(certFile)
	if err != nil {
		return &tls.Config{}, err
	}
	cp := x509.NewCertPool()
	if !cp.AppendCertsFromPEM(b) {
		return &tls.Config{}, fmt.Errorf("failed to append certificates for client ca store")
	}
	return &tls.Config{ServerName: serverName, RootCAs: cp, InsecureSkipVerify: SkipVerify}, nil
}

// newServerTLSFromFile constructs a TLS from the input certificate file and key
// file for server.
func newServerTLSFromFile(certFile, keyFile string, SkipVerify bool) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}, InsecureSkipVerify: SkipVerify}, nil
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

	ringChanges               int32
	ringChangeCloses          int32
	msgToNodes                int32
	msgToNodeNoRings          int32
	msgToNodeNoNodes          int32
	msgToOtherReplicas        int32
	msgToOtherReplicasNoRings int32
	listenErrors              int32
	incomingConnections       int32
	dials                     int32
	dialErrors                int32
	outgoingConnections       int32
	msgChanCreations          int32
	msgToAddrs                int32
	msgToAddrQueues           int32
	msgToAddrTimeoutDrops     int32
	msgToAddrShutdownDrops    int32
	msgReads                  int32
	msgReadErrors             int32
	msgWrites                 int32
	msgWriteErrors            int32
	statsLock                 sync.Mutex

	chaosAddrOffsLock        sync.RWMutex
	chaosAddrOffs            map[string]bool
	chaosAddrDisconnectsLock sync.RWMutex
	chaosAddrDisconnects     map[string]bool

	useTLS          bool
	certFile        string
	keyFile         string
	skipVerify      bool
	serverTLSConfig *tls.Config
	clientTLSConfig *tls.Config
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
		chaosAddrOffs:              make(map[string]bool),
		chaosAddrDisconnects:       make(map[string]bool),
		useTLS:                     cfg.UseTLS,
		certFile:                   cfg.CertFile,
		keyFile:                    cfg.KeyFile,
		skipVerify:                 cfg.SkipVerify,
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
	atomic.AddInt32(&t.ringChanges, 1)
	t.ringLock.Lock()
	t.ring = ring
	t.ringLock.Unlock()
	addrs := make(map[string]bool)
	for _, n := range ring.Nodes() {
		addrs[n.Address(t.addressIndex)] = true
	}
	t.msgChansLock.Lock()
	for addr, msgChan := range t.msgChans {
		if !addrs[addr] {
			atomic.AddInt32(&t.ringChangeCloses, 1)
			close(msgChan)
			delete(t.msgChans, addr)
		}
	}
	t.msgChansLock.Unlock()
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
	atomic.AddInt32(&t.msgToNodes, 1)
	ring := t.Ring()
	if ring == nil {
		atomic.AddInt32(&t.msgToNodeNoRings, 1)
		msg.Free()
		return
	}
	node := ring.Node(nodeID)
	if node == nil {
		atomic.AddInt32(&t.msgToNodeNoNodes, 1)
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
	atomic.AddInt32(&t.msgToOtherReplicas, 1)
	ring := t.Ring()
	if ring == nil {
		atomic.AddInt32(&t.msgToOtherReplicasNoRings, 1)
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
	if t.useTLS {
		t.serverTLSConfig, err = newServerTLSFromFile(t.certFile, t.keyFile, t.skipVerify)
		if err != nil {
			t.logCritical("Unable to setup server tls config:", err)
		}
		t.clientTLSConfig, err = newServerTLSFromFile(t.certFile, t.keyFile, t.skipVerify)
		if err != nil {
			t.logCritical("Unable to setup client tls config:", err)
		}
	}
OuterLoop:
	for {
		if err != nil {
			atomic.AddInt32(&t.listenErrors, 1)
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
			var netConn net.Conn
			if t.useTLS {
				l := tls.NewListener(server, t.serverTLSConfig)
				netConn, err = l.Accept()
			} else {
				netConn, err = server.AcceptTCP()
			}
			if err != nil {
				if opErr, ok := err.(*net.OpError); ok && opErr.Timeout() {
					continue
				}
				server.Close()
				continue OuterLoop
			}
			atomic.AddInt32(&t.incomingConnections, 1)
			go func(netConn net.Conn) {
				addr := netConn.RemoteAddr().String()
				t.chaosAddrOffsLock.RLock()
				if t.chaosAddrOffs[addr] {
					t.logError("listen: %s chaosAddrOff\n", addr)
					netConn.Close()
					t.chaosAddrOffsLock.RUnlock()
					return
				}
				t.chaosAddrOffsLock.RUnlock()
				if err := t.handshake(netConn); err != nil {
					t.logError("listen: %s %s\n", addr, err)
					netConn.Close()
					return
				}
				msgChan, created := t.msgChanForAddr(addr)
				// NOTE: If created is true, it'll indicate to connection
				// that redialing is okay. If created is false, once the
				// connection has terminated it won't be reestablished
				// since there is already another connection running that
				// will redial.
				go t.connection(addr, netConn, msgChan, created)
			}(netConn)
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
	atomic.AddInt32(&t.msgChanCreations, 1)
	msgChan = make(chan Msg, t.bufferedMessagesPerAddress)
	t.msgChans[addr] = msgChan
	t.msgChansLock.Unlock()
	return msgChan, true
}

// lookupMsgChanForAddr returns the channel for the address or nil if there is
// none.
func (t *TCPMsgRing) lookupMsgChanForAddr(addr string) chan Msg {
	t.msgChansLock.RLock()
	msgChan := t.msgChans[addr]
	t.msgChansLock.RUnlock()
	return msgChan
}

func (t *TCPMsgRing) msgToAddr(msg Msg, addr string, timeout time.Duration) {
	atomic.AddInt32(&t.msgToAddrs, 1)
	msgChan, created := t.msgChanForAddr(addr)
	if created {
		go t.connection(addr, nil, msgChan, true)
	}
	timer := time.NewTimer(timeout)
	select {
	case <-t.controlChan:
		atomic.AddInt32(&t.msgToAddrShutdownDrops, 1)
		timer.Stop()
		msg.Free()
	case msgChan <- msg:
		atomic.AddInt32(&t.msgToAddrQueues, 1)
		timer.Stop()
	case <-timer.C:
		atomic.AddInt32(&t.msgToAddrTimeoutDrops, 1)
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

func (t *TCPMsgRing) handshake(netConn net.Conn) error {
	// TODO: Exchange protocol version numbers and whatever else.
	return nil
}

func (t *TCPMsgRing) connection(addr string, netConn net.Conn, msgChan chan Msg, dialOk bool) {
OuterLoop:
	for {
		select {
		case <-t.controlChan:
			break OuterLoop
		default:
			if msgChan != t.lookupMsgChanForAddr(addr) {
				// If the channel for this addr has changed or is no longer
				// set, this connection routine is no longer needed.
				break OuterLoop
			}
		}
		var err error
		if netConn == nil {
			if !dialOk {
				break OuterLoop
			}
			atomic.AddInt32(&t.dials, 1)
			t.chaosAddrOffsLock.RLock()
			if t.chaosAddrOffs[addr] {
				t.chaosAddrOffsLock.RUnlock()
				err = errors.New("chaosAddrOff")
			} else {
				t.chaosAddrOffsLock.RUnlock()
				var baseConn net.Conn
				baseConn, err = net.DialTimeout("tcp", addr, t.connectTimeout)
				if err == nil {
					if t.useTLS {
						c := t.clientTLSConfig
						c.ServerName = addr
						netConn = tls.Client(baseConn, c)
					} else {
						netConn = baseConn
					}
					err = t.handshake(netConn)
				}
			}
			if err != nil {
				atomic.AddInt32(&t.dialErrors, 1)
				if netConn != nil {
					netConn.Close()
					netConn = nil
				}
				t.logError("connection: %s %s\n", addr, err)
				time.Sleep(t.reconnectInterval)
				continue OuterLoop
			}
			atomic.AddInt32(&t.outgoingConnections, 1)
		}
		t.chaosAddrDisconnectsLock.RLock()
		if t.chaosAddrDisconnects[addr] {
			go func(netConn net.Conn) {
				time.Sleep(time.Duration((atomic.LoadInt32(&t.msgToAddrs)%61)+10) * time.Second)
				netConn.Close()
				t.logError("chaosAddrDisconnect %s\n", netConn.RemoteAddr())
			}(netConn)
		}
		t.chaosAddrDisconnectsLock.RUnlock()
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
OuterLoop:
	for {
		select {
		case <-readerControlChan:
			break OuterLoop
		default:
		}
		if err := t.readMsg(reader); err != nil {
			atomic.AddInt32(&t.msgReadErrors, 1)
			t.logError("readMsg: %s\n", err)
			break
		}
		atomic.AddInt32(&t.msgReads, 1)
	}
}

func (t *TCPMsgRing) readMsg(reader *timeoutReader) error {
	var msgType uint64
	timeout := reader.Timeout
	// Wait forever for the first byte or for closed/eof error.
	reader.Timeout = 0
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
			atomic.AddInt32(&t.msgWriteErrors, 1)
			t.logError("writeMsg: %s\n", err)
			msg.Free()
			break
		}
		atomic.AddInt32(&t.msgWrites, 1)
		msg.Free()
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

type TCPMsgRingStats struct {
	Shutdown                  bool
	RingChanges               int32
	RingChangeCloses          int32
	MsgToNodes                int32
	MsgToNodeNoRings          int32
	MsgToNodeNoNodes          int32
	MsgToOtherReplicas        int32
	MsgToOtherReplicasNoRings int32
	ListenErrors              int32
	IncomingConnections       int32
	Dials                     int32
	DialErrors                int32
	OutgoingConnections       int32
	MsgChanCreations          int32
	MsgToAddrs                int32
	MsgToAddrQueues           int32
	MsgToAddrTimeoutDrops     int32
	MsgToAddrShutdownDrops    int32
	MsgReads                  int32
	MsgReadErrors             int32
	MsgWrites                 int32
	MsgWriteErrors            int32
}

// Stats returns the current stat counters and resets those counters. In other
// words, if Stats().Dials gives the value 10 and no more dials occur before
// Stats() is called again, that second Stats().Dials will have the value 0.
//
// If debug=true, additional information (left undocumented because it is
// greatly subject to change) may be given when calling
// TCPMsgRingStats.String().
func (t *TCPMsgRing) Stats(debug bool) *TCPMsgRingStats {
	shutdown := false
	select {
	case <-t.controlChan:
		shutdown = true
	default:
	}
	t.statsLock.Lock()
	s := &TCPMsgRingStats{
		Shutdown:                  shutdown,
		RingChanges:               atomic.LoadInt32(&t.ringChanges),
		RingChangeCloses:          atomic.LoadInt32(&t.ringChangeCloses),
		MsgToNodes:                atomic.LoadInt32(&t.msgToNodes),
		MsgToNodeNoRings:          atomic.LoadInt32(&t.msgToNodeNoRings),
		MsgToNodeNoNodes:          atomic.LoadInt32(&t.msgToNodeNoNodes),
		MsgToOtherReplicas:        atomic.LoadInt32(&t.msgToOtherReplicas),
		MsgToOtherReplicasNoRings: atomic.LoadInt32(&t.msgToOtherReplicasNoRings),
		ListenErrors:              atomic.LoadInt32(&t.listenErrors),
		IncomingConnections:       atomic.LoadInt32(&t.incomingConnections),
		Dials:                     atomic.LoadInt32(&t.dials),
		DialErrors:                atomic.LoadInt32(&t.dialErrors),
		OutgoingConnections:       atomic.LoadInt32(&t.outgoingConnections),
		MsgChanCreations:          atomic.LoadInt32(&t.msgChanCreations),
		MsgToAddrs:                atomic.LoadInt32(&t.msgToAddrs),
		MsgToAddrQueues:           atomic.LoadInt32(&t.msgToAddrQueues),
		MsgToAddrTimeoutDrops:     atomic.LoadInt32(&t.msgToAddrTimeoutDrops),
		MsgToAddrShutdownDrops:    atomic.LoadInt32(&t.msgToAddrShutdownDrops),
		MsgReads:                  atomic.LoadInt32(&t.msgReads),
		MsgReadErrors:             atomic.LoadInt32(&t.msgReadErrors),
		MsgWrites:                 atomic.LoadInt32(&t.msgWrites),
		MsgWriteErrors:            atomic.LoadInt32(&t.msgWriteErrors),
	}
	atomic.AddInt32(&t.ringChanges, -s.RingChanges)
	atomic.AddInt32(&t.ringChangeCloses, -s.RingChangeCloses)
	atomic.AddInt32(&t.msgToNodes, -s.MsgToNodes)
	atomic.AddInt32(&t.msgToNodeNoRings, -s.MsgToNodeNoRings)
	atomic.AddInt32(&t.msgToNodeNoNodes, -s.MsgToNodeNoNodes)
	atomic.AddInt32(&t.msgToOtherReplicas, -s.MsgToOtherReplicas)
	atomic.AddInt32(&t.msgToOtherReplicasNoRings, -s.MsgToOtherReplicasNoRings)
	atomic.AddInt32(&t.listenErrors, -s.ListenErrors)
	atomic.AddInt32(&t.incomingConnections, -s.IncomingConnections)
	atomic.AddInt32(&t.dials, -s.Dials)
	atomic.AddInt32(&t.dialErrors, -s.DialErrors)
	atomic.AddInt32(&t.outgoingConnections, -s.OutgoingConnections)
	atomic.AddInt32(&t.msgChanCreations, -s.MsgChanCreations)
	atomic.AddInt32(&t.msgToAddrs, -s.MsgToAddrs)
	atomic.AddInt32(&t.msgToAddrQueues, -s.MsgToAddrQueues)
	atomic.AddInt32(&t.msgToAddrTimeoutDrops, -s.MsgToAddrTimeoutDrops)
	atomic.AddInt32(&t.msgToAddrShutdownDrops, -s.MsgToAddrShutdownDrops)
	atomic.AddInt32(&t.msgReads, -s.MsgReads)
	atomic.AddInt32(&t.msgReadErrors, -s.MsgReadErrors)
	atomic.AddInt32(&t.msgWrites, -s.MsgWrites)
	atomic.AddInt32(&t.msgWriteErrors, -s.MsgWriteErrors)
	t.statsLock.Unlock()
	return s
}

func (s *TCPMsgRingStats) String() string {
	return fmt.Sprintf("%#v", s)
}

// SetChaosAddrOff will disable all outgoing connections to addr and
// immediately close any incoming connections from addr.
func (t *TCPMsgRing) SetChaosAddrOff(addr string, off bool) {
	t.chaosAddrOffsLock.Lock()
	t.chaosAddrOffs[addr] = off
	t.chaosAddrOffsLock.Unlock()
}

// SetChaosAddrDisconnect will allow connections to and from addr but, after
// 10-70 seconds, it will abruptly close a connection.
func (t *TCPMsgRing) SetChaosAddrDisconnect(addr string, disconnect bool) {
	t.chaosAddrDisconnectsLock.Lock()
	t.chaosAddrDisconnects[addr] = disconnect
	t.chaosAddrDisconnectsLock.Unlock()
}
