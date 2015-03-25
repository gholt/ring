package ring

import (
	"bufio"
	"net"
	"time"
)

// TimeoutReader is a bufio.Reader that reads in chunks of ChunkSize and will
// return a timeout error if the chunk is not read in the Timeout time.
// TODO: Add chunking - or do we even need chunking at this layer?
// TODO: Add other bufio functions
type TimeoutReader struct {
	Timeout   time.Duration
	ChunkSize int
	reader    *bufio.Reader
	conn      net.Conn
}

func NewTimeoutReader(conn net.Conn) *TimeoutReader {
	return &TimeoutReader{
		Timeout:   DefaultTimeout,
		ChunkSize: DefaultChunksize,
		reader:    bufio.NewReaderSize(conn, DefaultChunksize),
		conn:      conn,
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

// TimeoutWriter is a bufio.Writer that reads in chunks of ChunkSize and will
// return a timeout error if the chunk is not read in the Timeout time.
// TODO: Add chunking
type TimeoutWriter struct {
	Timeout   time.Duration
	ChunkSize int
	writer    *bufio.Writer
	conn      net.Conn
}

func NewTimeoutWriter(conn net.Conn) *TimeoutWriter {
	return &TimeoutWriter{
		Timeout:   DefaultTimeout,
		ChunkSize: DefaultChunksize,
		writer:    bufio.NewWriterSize(conn, DefaultChunksize),
		conn:      conn,
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
