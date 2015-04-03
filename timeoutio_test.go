package ring

import (
	"bytes"
	"net"
	"testing"
	"time"
)

func isTimeout(err error) bool {
	e, ok := err.(net.Error)
	return ok && e.Timeout()
}

func Test_ReadTimeout(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	c, err := net.DialTCP("tcp", nil, ln.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	reader := newTimeoutReader(c, _DEFAULT_CHUNK_SIZE, -1*time.Second)
	_, err = reader.ReadByte()
	if err == nil {
		t.Error("Read didn't timeout")
	} else if !isTimeout(err) {
		t.Error("Error wasn't a timeout: ", err)
	}
	reader.Timeout = 10 * time.Millisecond
	_, err = reader.ReadByte()
	if err == nil {
		t.Error("Read didn't timeout")
	} else if !isTimeout(err) {
		t.Error("Error wasn't a timeout: ", err)
	}
}

func Test_WriteTimeout(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	c, err := net.DialTCP("tcp", nil, ln.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	writer := newTimeoutWriter(c, _DEFAULT_CHUNK_SIZE, -1*time.Second)
	writer.Write([]byte("Test"))
	err = writer.Flush()
	if err == nil {
		t.Error("Write didn't timeout")
	} else if !isTimeout(err) {
		t.Error("Error wasn't a timeout: ", err)
	}
	writer.Timeout = 10 * time.Millisecond
	err = writer.Flush()
	if err == nil {
		t.Error("Read didn't timeout")
	} else if !isTimeout(err) {
		t.Error("Error wasn't a timeout: ", err)
	}
}

func Test_ReadByte(t *testing.T) {
	c := new(testConn)
	c.readBuf.WriteString("ABCD")
	reader := newTimeoutReader(c, _DEFAULT_CHUNK_SIZE, _DEFAULT_TIMEOUT)
	b, err := reader.ReadByte()
	if err != nil {
		t.Error("Error reading byte: ", err)
	}
	if b != 'A' {
		t.Error("Read incorrect byte: ", string(b))
	}
}

func Test_Read(t *testing.T) {
	c := new(testConn)
	c.readBuf.WriteString("ABCD")
	reader := newTimeoutReader(c, _DEFAULT_CHUNK_SIZE, _DEFAULT_TIMEOUT)
	read := make([]byte, 3)
	n, err := reader.Read(read)
	if err != nil {
		t.Error("Error reading: ", err)
	}
	if n != 3 {
		t.Error("Read incorrect number of bytes: ", n)
	}
	if !bytes.Equal(read, []byte("ABC")) {
		t.Error("Read incorrect: ", string(read))
	}
}

func Test_WriteByte(t *testing.T) {
	c := new(testConn)
	writer := newTimeoutWriter(c, _DEFAULT_CHUNK_SIZE, _DEFAULT_TIMEOUT)
	err := writer.WriteByte('A')
	if err != nil {
		t.Error("Error writing byte: ", err)
	}
	writer.Flush()
	b, err := c.writeBuf.ReadByte()
	if err != nil {
		t.Error("Error reading written byte: ", err)
	}
	if b != 'A' {
		t.Error("Read incorrect byte: ", string(b))
	}
}

func Test_Write(t *testing.T) {
	c := new(testConn)
	writer := newTimeoutWriter(c, _DEFAULT_CHUNK_SIZE, _DEFAULT_TIMEOUT)
	n, err := writer.Write([]byte("ABCD"))
	if err != nil {
		t.Error("Error writing: ", err)
	}
	if n != 4 {
		t.Error("Wrote incorrect number of bytes: ", n)
	}
	writer.Flush()
	read := make([]byte, 4)
	n, err = c.writeBuf.Read(read)
	if err != nil {
		t.Error("Error reading writen data: ", err)
	}
	if n != 4 {
		t.Error("Read incorrect number of bytes: ", n)
	}
	if !bytes.Equal(read, []byte("ABCD")) {
		t.Error("Read incorrect: ", string(read))
	}
}
