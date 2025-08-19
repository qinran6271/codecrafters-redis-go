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

func lrangeKey(key string, start int, end int) ([]string, error) {
	now := time.Now()
	kv.Lock()
	defer kv.Unlock()

	e, exists := kv.m[key]
	if !exists {
		return []string{}, nil // Return nil and false if key does not exist
	}
	if isExpired(e, now) {
		delete(kv.m, key) // Remove expired key
		return []string{}, nil  // Return nil and false if key is expired
	}
	if e.kind != kindList {
		return nil, ErrWrongType// Return nil and false if the value is not a list
	}

	
	listLen := len(e.l)
	if listLen == 0 {
		return []string{}, nil  // Return empty slice and true if the list is empty
	}

	// Handle negative indices for start and end, eg. -1 means last element
	if start < 0 {
		start = listLen + start // Handle negative start index
	}
	if end < 0 {
		end = listLen + end // Handle negative end index
	}

	// Handle out-of-bounds indices
	if start < 0 {
		start = 0 // Ensure start is not negative
	}
	if end >= listLen {
		end = listLen - 1 // Ensure end does not exceed the list length
	}

	// Empty list case
	if start > end || start >= listLen {
		return []string{}, nil  // Return empty slice and true if the range is invalid or out of bounds
	}

	return e.l[start:end+1], nil// Return the sub-slice from start to end (inclusive)
}

func lpushKey(key string, values []string) (int, error) {
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

	rev := make([]string, 0, len(values))
	for i := len(values) - 1; i >= 0; i-- {
		rev = append(rev, values[i]) // Reverse order for LPUSH
	}

	if !exists {	
		kv.m[key] = entry{
			kind: kindList,
			l: rev,
			// No expiration for the new list
		}
		return len(rev), nil
	}

	if e.kind != kindList {
		return 0, ErrWrongType // Return error if the existing value is not a list
	}

	newList := make([]string, 0, len(rev)+len(e.l))
	newList = append(newList, rev...)
	newList = append(newList, e.l...)

	e.l = newList // Update the list with the new values
	kv.m[key] = e // Update the entry in the map
	return len(e.l), nil // Return the new length of the list
}

func llenKey(key string) (int, error) {
	now := time.Now()
	kv.Lock()
	defer kv.Unlock()

	e, exists := kv.m[key]
	if !exists {
		return 0, nil // Return 0 and nil if key does not exist
	}
	if isExpired(e, now) {
		delete(kv.m, key) // Remove expired key
		return 0, nil // Return 0 and nil if key is expired
	}
	if e.kind != kindList {
		return 0, ErrWrongType // Return 0 and error if the value is not a list
	}
	return len(e.l), nil // Return the length of the list
}

func lpopKey(key string, count int) ([]string, error) {
	now := time.Now()
	kv.Lock()
	defer kv.Unlock()

	e, exists := kv.m[key]
	if !exists {
		return []string{}, nil // Return nil if key does not exist
	}
	if isExpired(e, now) {
		delete(kv.m, key) // Remove expired key
		return []string{}, nil // Return nil if key is expired
	}
	if e.kind != kindList {
		return []string{}, ErrWrongType // Return error if the value is not a list
	}

	n := len(e.l)
	if n == 0 {
		return []string{}, nil // Return nil if the list is empty
	}

	if count >= n {
		res := e.l
		delete(kv.m, key) // If count is greater than or equal to the list length, return the whole list
		return res, nil
	}

	// Pop the first element from the list
	res := e.l[:count] // Get the first 'count' elements
	e.l = e.l[count:] // Update the list by removing the popped elements
	kv.m[key] = e // Update the entry in the map
	return res, nil // Return the popped elements
	
}