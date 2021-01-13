package server

import (
	"fmt"
	"sync"
)

type Log struct {
	// sync.Mutex provides 'mutual exclusion' to make sure only one goroutine can access a
	// variable at a time to avoid conflicts.
	mu      sync.Mutex
	records []Record
}

type Record struct {
	// Tag strings can be defined on struct fields. The key usually denotes the package
	// that the subsequent "value" is for. So 'json' key is processed with the
	// 'encoding/json' package.
	Value []byte `json:"value"`
	// uint64 is the set of all unsigned 64-bit integers
	Offset uint64 `json:"offset"`
}

var ErrOffsetNotFound = fmt.Errorf("offset not found")

// Recall that the & operator generates a pointer to it's operand.
// The * operator denotes the pointer's underlying value.
func NewLog() *Log {
	return &Log{}
}

// Recall that (c *Log) is the receiver on the Log type. So a variable of type Log will be able
// to call the Append() method on it.
func (c *Log) Append(record Record) (uint64, error) {
	c.mu.Lock()
	// A defer statement defers the execution of a function until the surrounding function
	// returns.
	defer c.mu.Unlock()
	record.Offset = uint64(len(c.records))
	c.records = append(c.records, record)
	return record.Offset, nil
}

func (c *Log) Read(offset uint64) (Record, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if offset >= uint64(len(c.records)) {
		return Record{}, ErrOffsetNotFound
	}
	return c.records[offset], nil
}
