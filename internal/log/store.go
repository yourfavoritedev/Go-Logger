package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	lenWidth = 8 // binary.Write uses 8 bytes
)

// store: wrapper around a file (*os.File) and keeps track of its own size
type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

// newStore creates a new store from a file
func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// Append writes the given bytes (p) to the store. Returns the number of bytes written (n),
// the position where the store holds the record (pos),
// an error if one exists (err)
// the segment will make use of the returned pos when it creates an associated index entry for this record
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// new position starts from existing store size, the store size should be an accurate reference to where the last byte is written on the buffer.
	pos = s.size
	// binary.Write will use the next available 8 bytes on the buffer (will be used for indexing),
	// which is why we use pos+lenWidth to accurately return store size and position of the desired byte
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	w, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}
	// pad the written bytes by 8 (the number of bytes used for the encoding)
	w += lenWidth
	// update size of store with number of new bytes
	s.size += uint64(w)
	return uint64(w), pos, nil
}

// Read accepts the position (pos) of a record in a store.
// Leverages *os.File (ReadAt) method, given a starting point (pos+lenWidth), it will return the entire record given the
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	recordSize := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(recordSize, int64(pos)); err != nil {
		return nil, err
	}

	b := make([]byte, enc.Uint64(recordSize))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

// Read accepts a given range of bytes (b) and the position (pos) of a record in a store.
// Leverages *os.File (ReadAt) method, given a starting point (pos) and a specific amount of given bytes len(b),
// it returns the bytes within that range
func (s *store) ReadAt(b []byte, pos int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(b, pos)
}

// Close locks the store, writes all buffered data to the io.Writer and closes the file to stop usage
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}
