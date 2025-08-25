package main

import (
	"sync"
	"time"
)
//kv   (全局存储)
// │
// ├── m  (map[string]entry)   // Redis 的 keyspace
// │   │
// │   ├── "foo" → entry{kind: kindString, s: "bar"}
// │   ├── "list1" → entry{kind: kindList, l: ["a","b","c"]}
// │   └── "mystream" → entry{
// │         kind: kindStream,
// │         streams: [
// │           streamEntry{id:"1000-0", fields:{"temperature":"36"}},
// │           streamEntry{id:"1000-1", fields:{"humidity":"95"}},
// │         ]
// │      }
// │
// └── RWMutex (保证多 goroutine 并发安全)


type valueKind int // Define a new type for value kinds based with constants

const (
	kindString valueKind = iota // index 0, string type, iota starts at 0 and increments by 1
	kindList // index 1, list type
	kindStream // index 2, stream type
)



type streamEntry struct {
	id string // Unique identifier for the stream entry
	msTime int64 // Timestamp of the entry, can be used for ordering
	seqNum int64 // Sequence number for the entry, can be used for ordering
	fields map[string]string // Fields of the stream entry, key-value pairs
}

type entry struct {
	kind valueKind // Type of the value
	s string // string value (GET/SET)
	l []string // list value (LPUSH/RPUSH)
	streams []streamEntry // stream value (XADD/XREAD)
	expires time.Time // Expiration time for the entry, zero if no expiration
}

// 最外层的全局变量，表示整个 KV 存储
var kv struct {
	sync.RWMutex
	m map[string]entry // 全局 KV 表
}

