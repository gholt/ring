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
	r := TestRing{}
	msgring := NewTCPMsgRing(&r)
	addr := msgring.GetAddressForNode(uint64(1))
	msgring.conns[addr] = NewRingConn(conn)
	msg := TestMsg{}
	msgId := uint64(1)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msgring.MsgToNode(msgId, &msg)
	}
}
