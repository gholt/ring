package ring

import (
	"io"
	"io/ioutil"
	"log"
	"testing"
	"time"
)

func Benchmark_Time(b *testing.B) {
	for i := 0; i < b.N; i++ {
		time.Now()
	}
}

func Benchmark_MsgToNode(b *testing.B) {
	conn := new(testConn)
	r, _, nB := newTestRing()
	msgring := NewTCPMsgRing(r)
	addr := nB.Address(0)
	msgring.conns[addr] = newRingConn(conn, _DEFAULT_CHUNK_SIZE, _DEFAULT_TIMEOUT)
	msg := TestMsg{}
	msgId := uint64(1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgring.MsgToNode(msgId, &msg)
	}
}

func noopmarshaller(reader io.Reader, size uint64) (uint64, error) {
	return size, nil
}

func Benchmark_Handle(b *testing.B) {
	r, _, _ := newTestRing()
	msgring := NewTCPMsgRing(r)
	msgring.SetMsgHandler(1, noopmarshaller)
	msgring.ChunkSize = 16 // so we don't alloc a lot each iter
	data := [16]byte{1, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0}
	conns := make([]*testConn, b.N)
	for i := 0; i < b.N; i++ {
		conns[i] = new(testConn)
		conns[i].readBuf.Write(data[:])
	}
	log.SetOutput(ioutil.Discard)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgring.handle(conns[i])
	}
}
