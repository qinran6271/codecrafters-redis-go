package main

import (
	"sync"
	"time"
)

type valueKind int // Define a new type for value kinds based with constants

const (
	kindString valueKind = iota // index 0, string type, iota starts at 0 and increments by 1
	kindList // index 1, list type
	kindStream // index 2, stream type
)

type streamEntry struct {
	id string // Unique identifier for the stream entry
	fields map[string]string // Fields of the stream entry, key-value pairs
}

type entry struct {
	kind valueKind // Type of the value
	s string // string value (GET/SET)
	l []string // list value (LPUSH/RPUSH)
	streams []streamEntry // stream value (XADD/XREAD)
	expires time.Time // Expiration time for the entry, zero if no expiration
}

var kv struct {
	sync.RWMutex
	m map[string]entry
}

