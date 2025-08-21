package main

import (
	"errors"
	"sync"
	"time"
)

type valueKind int // Define a new type for value kinds based with constants

const (
	kindString valueKind = iota // index 0, iota starts at 0 and increments by 1
	kindList // index 1
)

var ErrWrongType = errors.New("WRONGTYPE of value for this operation")

type entry struct {
	kind valueKind // Type of the value
	s string // string value (GET/SET)
	l []string // list value (LPUSH/RPUSH)
	expires time.Time // Expiration time for the entry, zero if no expiration
}

var kv struct {
	sync.RWMutex
	m map[string]entry
}

