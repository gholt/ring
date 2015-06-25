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
	msgring.conns[addr] = newRingConn(conn)
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

func Benchmark_HandleOne(b *testing.B) {
	r, _, _ := newTestRing()
	msgring := NewTCPMsgRing(r)
	msgring.SetMsgHandler(1, noopmarshaller)
	msgring.chunkSize = 16 // so we don't alloc too much
	data := [16]byte{1, 0, 0, 0, 0, 0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0}
	conns := make([]*ringConn, b.N)
	for i := 0; i < b.N; i++ {
		conn := new(testConn)
		conn.readBuf.Write(data[:])
		conns[i] = newRingConn(conn)
	}
	log.SetOutput(ioutil.Discard)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if !msgring.handleOne(conns[i]) {
			b.Error("handleOne")
		}
	}
}
