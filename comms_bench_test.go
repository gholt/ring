package ring

import (
	"testing"
)

func Benchmark_MsgToNode(b *testing.B) {
	conn := new(testConn)
	r := TestRing{}
	msgring := NewTCPMsgRing(&r)
	addr := msgring.GetAddressForNode(uint64(1))
	msgring.conns[addr] = NewRingConn(conn)
	msg := TestMsg{}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		success := msgring.MsgToNode(uint64(1), &msg)
		if !success {
			b.Error("MsgToNode failed")
		}
	}
}
