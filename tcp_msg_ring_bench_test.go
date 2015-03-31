package ring

import (
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
	r, _, nB := newCommsTestRing()
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
