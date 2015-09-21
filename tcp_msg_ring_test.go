package ring

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

/* TODO: Need to rework tests
func newRingConn(m *TCPMsgRing, addr string, netconn net.Conn) *ringConn {
	conn := &ringConn{
		addr:   addr,
		conn:   netconn,
		reader: newTimeoutReader(netconn, 16*1024, 2*time.Second),
		writer: newTimeoutWriter(netconn, 16*1024, 2*time.Second),
		done:   make(chan struct{}),
	}
	go m.ringConnReader(conn)
	go m.ringConnWriter(conn)
	return conn
}
*/

// Mock up a bunch of stuff

func newTestRing() (Ring, Node, Node) {
	b := NewBuilder(64)
	b.SetReplicaCount(3)
	nA := b.AddNode(true, 1, nil, []string{"127.0.0.1:9999"}, "", []byte("Conf"))
	nB := b.AddNode(true, 1, nil, []string{"127.0.0.1:8888"}, "", []byte("Conf"))
	r := b.Ring()
	r.SetLocalNode(nA.ID())
	return r, nA, nB
}

var testMsg = []byte("Testing")
var testStr = "Testing"

type TestMsg struct {
	done chan struct{}
}

func newTestMsg() *TestMsg {
	return &TestMsg{done: make(chan struct{}, 1)}
}

func (m *TestMsg) MsgType() uint64 {
	return 1
}

func (m *TestMsg) MsgLength() uint64 {
	return 7
}

func (m *TestMsg) WriteContent(writer io.Writer) (uint64, error) {
	count, err := writer.Write(testMsg)
	return uint64(count), err
}

func (m *TestMsg) Done() {
	m.done <- struct{}{}
}

// Following mock stuff borrowed from golang.org/src/net/http/serve_test.go
type dummyAddr string

func (a dummyAddr) Network() string {
	return string(a)
}

func (a dummyAddr) String() string {
	return string(a)
}

type noopConn struct{}

func (noopConn) LocalAddr() net.Addr                { return dummyAddr("local-addr") }
func (noopConn) RemoteAddr() net.Addr               { return dummyAddr("remote-addr") }
func (noopConn) SetDeadline(t time.Time) error      { return nil }
func (noopConn) SetReadDeadline(t time.Time) error  { return nil }
func (noopConn) SetWriteDeadline(t time.Time) error { return nil }

type testConn struct {
	readBuf  bytes.Buffer
	writeBuf bytes.Buffer
	noopConn
}

func (c *testConn) Read(b []byte) (int, error) {
	return c.readBuf.Read(b)
}

func (c *testConn) Write(b []byte) (int, error) {
	return c.writeBuf.Write(b)
}

func (c *testConn) Close() error {
	return nil
}

type testConnNoReads struct {
	writeBuf bytes.Buffer
	noopConn
}

func (c *testConnNoReads) Read(b []byte) (int, error) {
	time.Sleep(60)
	return 0, io.EOF
}

func (c *testConnNoReads) Write(b []byte) (int, error) {
	return c.writeBuf.Write(b)
}

func (c *testConnNoReads) Close() error {
	return nil
}

/***** Actual tests start here *****/

func TestTCPMsgRingIsMsgRing(t *testing.T) {
	tmr := NewTCPMsgRing(nil)
	r, _, _ := newTestRing()
	tmr.SetRing(r)
	func(mr MsgRing) {}(tmr)
}

func Test_NewTCPMsgRing(t *testing.T) {
	msgring := NewTCPMsgRing(nil)
	r, _, _ := newTestRing()
	msgring.SetRing(r)
	if msgring.Ring().LocalNode().Address(0) != "127.0.0.1:9999" {
		t.Error("Error initializing TCPMsgRing")
	}
}

func Test_TCPMsgRingSetRing(t *testing.T) {
	msgring := NewTCPMsgRing(nil)
	r, _, _ := newTestRing()
	msgring.SetRing(r)

	r2, _, _ := newTestRing()
	msgring.SetRing(r2)

	if msgring.Ring() != r2 {
		t.Error("Error setting TCPMsgRing Ring")
	}
}

func test_stringmarshaller(reader io.Reader, size uint64) (uint64, error) {
	buf := make([]byte, size)
	c, err := reader.Read(buf)
	if !bytes.Equal(buf, testMsg) {
		err = errors.New("Unmarshaller didn't read the correct value")
	}
	return uint64(c), err
}

/* TODO: Best I can tell, this wasn't really testing anything.
func Test_handle(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	conn := new(testConn)
	binary.Write(&conn.readBuf, binary.BigEndian, uint64(1))
	binary.Write(&conn.readBuf, binary.BigEndian, uint64(7))
	conn.readBuf.WriteString(testStr)
	r, _, _ := newTestRing()
	msgring := NewTCPMsgRing(r)
	msgring.SetMsgHandler(1, test_stringmarshaller)
	msgring.handleConnection(newRingConn(conn))
}
*/

/* TODO: Need to rework tests
func Test_MsgToNode(t *testing.T) {
	conn := new(testConnNoReads)
	r, _, nB := newTestRing()
	msgring := NewTCPMsgRing(r)
	msgring.state = _RUNNING
	msgring.conns[nB.Address(0)] = newRingConn(msgring, nB.Address(0), conn)
	msg := newTestMsg()
	msgring.MsgToNode(nB.ID(), msg)
	<-msg.done
	var msgtype uint64
	binary.Read(&conn.writeBuf, binary.BigEndian, &msgtype)
	if int(msgtype) != 1 {
		t.Error("Message type not written correctly")
	}
	var msgsize uint64
	binary.Read(&conn.writeBuf, binary.BigEndian, &msgsize)
	if msgsize != 7 {
		t.Error("Incorrect message size")
	}
	msgcontent := make([]byte, 7)
	conn.writeBuf.Read(msgcontent)
	if !bytes.Equal(msgcontent, testMsg) {
		t.Error("Incorrect message contents")
	}
}
*/

/* TODO: Need to rework tests
func Test_MsgToOtherReplicas(t *testing.T) {
	conn := new(testConnNoReads)
	r, _, nB := newTestRing()
	msgring := NewTCPMsgRing(r)
	msgring.state = _RUNNING
	msgring.conns[nB.Address(0)] = newRingConn(msgring, nB.Address(0), conn)
	msg := newTestMsg()
	msgring.MsgToOtherReplicas(r.Version(), uint32(1), msg)
	<-msg.done
	var msgtype uint64
	err := binary.Read(&conn.writeBuf, binary.BigEndian, &msgtype)
	if err != nil {
		t.Error(err)
	}
	if int(msgtype) != 1 {
		t.Errorf("Message type not written correctly was %d", msgtype)
	}
	var msgsize uint64
	binary.Read(&conn.writeBuf, binary.BigEndian, &msgsize)
	if msgsize != 7 {
		t.Error("Incorrect message size")
	}
	msgcontent := make([]byte, 7)
	conn.writeBuf.Read(msgcontent)
	if !bytes.Equal(msgcontent, testMsg) {
		t.Error("Incorrect message contents")
	}
}
*/
