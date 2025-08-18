package main

import (
	"sync"
	"time"
	"errors"
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

func init() { 
	kv.m = make(map[string]entry) 
}


func isExpired(e entry, now time.Time) bool {
	return !e.expires.IsZero() && !now.Before(e.expires)
}

func setKey(key, value string, pxMs int64) {
	var exp time.Time
	if pxMs > 0 {
		exp = time.Now().Add(time.Duration(pxMs) * time.Millisecond)
	}
	kv.Lock()
	defer kv.Unlock()
	kv.m[key] = entry{
		kind: kindString,
		s: value,
		expires: exp,
	}
}

func getKey(key string) (string, bool) {
	now := time.Now()
	kv.Lock()
	defer kv.Unlock()
	e, exists := kv.m[key]
	if !exists {
		return "", false // Return empty string and false if key does not exist
	}
	if isExpired(e, now) {
		delete(kv.m, key) // Remove expired key
		return "", false // Return empty string and false if key is expired
	}
	if e.kind != kindString {
		return "", false // Return empty string and false if the value is not a string
	}
	return e.s, exists
}

func rpushKey(key string, values []string) (int, error){
	now := time.Now()

	kv.Lock()
	defer kv.Unlock()

	e, exists := kv.m[key]
	if exists {
		if isExpired(e, now) {
			delete(kv.m, key) // Remove expired key
			exists = false
		}
	}

	if !exists {
		newList := make([]string, 0, len(values))
		newList = append(newList, values...)
		kv.m[key] = entry{
			kind: kindList,
			l: newList,
			// No expiration for the new list
		}
		return len(newList), nil
	}

	if e.kind != kindList {
		return 0, ErrWrongType // Return error if the existing value is not a list
	}

	e.l = append(e.l, values...)
	kv.m[key] = e // Update the entry in the map
	return len(e.l), nil // Return the new length of the list

}