package server

import (
	"fmt"
	"sync"
)

// Log: mu is a mutually exclusive lock. records are a collection of all Records in the Log
type Log struct {
	mu      sync.Mutex
	records []Record
}

// Record: Value stores the written data. Offset refers to this record's position/index in the Log
type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}

// NewLog creates a new Log
func NewLog() *Log {
	return &Log{}
}

// Append sets the offset for the Record and pushes that record to the Log
func (c *Log) Append(record Record) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record.Offset = uint64(len(c.records))
	c.records = append(c.records, record)
	return record.Offset, nil
}

// Read returns a Record in a Log at the given offset
func (c *Log) Read(offset uint64) (Record, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if offset >= uint64(len(c.records)) {
		return Record{}, ErrOffsetNotFound
	}
	return c.records[offset], nil
}

var ErrOffsetNotFound = fmt.Errorf("offset not found")
