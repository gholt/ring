package ring

import (
	"bufio"
	"net"
	"time"
)

// TODO: Maybe this should go into its own package if it's considered resuable
// by things other than just TCPMsgRing. For now, I'll just privatize it all.

// TimeoutReader is a bufio.Reader that reads in chunks and will return a
// timeout error if the chunk is not read in the Timeout time.
// TODO: Add other bufio functions
type TimeoutReader struct {
	Timeout time.Duration
	reader  *bufio.Reader
	conn    net.Conn
}

func NewTimeoutReader(conn net.Conn, chunkSize int, timeout time.Duration) *TimeoutReader {
	return &TimeoutReader{
		Timeout: timeout,
		reader:  bufio.NewReaderSize(conn, chunkSize),
		conn:    conn,
	}
}

func (r *TimeoutReader) Read(p []byte) (n int, err error) {
	deadline := false
	if r.reader.Buffered() == 0 {
		// Buffer is empty, so we will read from the network
		timeout := time.Now().Add(r.Timeout)
		r.conn.SetReadDeadline(timeout)
		deadline = true
	}
	count, err := r.reader.Read(p)
	if deadline {
		r.conn.SetReadDeadline(time.Time{})
	}
	return count, err
}

func (r *TimeoutReader) ReadByte() (c byte, err error) {
	deadline := false
	if r.reader.Buffered() == 0 {
		// Buffer is empty, so we will read from the network
		timeout := time.Now().Add(r.Timeout)
		r.conn.SetReadDeadline(timeout)
		deadline = true
	}
	b, err := r.reader.ReadByte()
	if deadline {
		r.conn.SetReadDeadline(time.Time{})
	}
	return b, err
}

// TimeoutWriter is a bufio.Writer that reads in chunks and will return a
// timeout error if the chunk is not read in the Timeout time.
type TimeoutWriter struct {
	Timeout time.Duration
	writer  *bufio.Writer
	conn    net.Conn
}

func NewTimeoutWriter(conn net.Conn, chunkSize int, timeout time.Duration) *TimeoutWriter {
	return &TimeoutWriter{
		Timeout: timeout,
		writer:  bufio.NewWriterSize(conn, chunkSize),
		conn:    conn,
	}
}

func (w *TimeoutWriter) Write(p []byte) (n int, err error) {
	deadline := false
	if len(p) > w.writer.Available() {
		// Write will flush(), so make sure we wrap in a timeout
		timeout := time.Now().Add(w.Timeout)
		w.conn.SetWriteDeadline(timeout)
		deadline = true
	}
	count, err := w.writer.Write(p)
	if deadline {
		w.conn.SetWriteDeadline(time.Time{})
	}
	return count, err
}

func (w *TimeoutWriter) WriteByte(c byte) error {
	deadline := false
	if w.writer.Available() <= 0 {
		// Write will flush(), so make sure we wrap in a timeout
		timeout := time.Now().Add(w.Timeout)
		w.conn.SetReadDeadline(timeout)
		deadline = true
	}
	err := w.writer.WriteByte(c)
	if deadline {
		w.conn.SetWriteDeadline(time.Time{})
	}
	return err
}

func (w *TimeoutWriter) Flush() error {
	timeout := time.Now().Add(w.Timeout)
	w.conn.SetWriteDeadline(timeout)
	err := w.writer.Flush()
	w.conn.SetWriteDeadline(time.Time{})
	return err

}
