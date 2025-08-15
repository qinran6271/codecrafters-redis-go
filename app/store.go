package main

import (
	"sync"
	"time"
)

type entry struct {
	val string
	expires time.Time
}

var kv struct {
	sync.RWMutex
	m map[string]entry
}

func init() { 
	kv.m = make(map[string]entry) 
}

func nowNs() int64 {
	return time.Now().UnixMilli()
}

func setKey(key, value string, pxMs int64) {
	var exp time.Time
	if pxMs > 0 {
		exp = time.Now().Add(time.Duration(pxMs) * time.Millisecond)
	}
	kv.Lock()
	defer kv.Unlock()
	kv.m[key] = entry{val: value, expires: exp}
}

func getKey(key string) (string, bool) {
	now := time.Now()
	kv.Lock()
	defer kv.Unlock()
	e, exists := kv.m[key]
	if !e.expires.IsZero() && !now.Before(e.expires) {
		delete(kv.m, key) // Remove expired key
		return "", false // Return empty string and false if key is expired
	}
	return e.val, exists
}