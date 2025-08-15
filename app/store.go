package main

import (
	"sync"
)


var kv struct {
	sync.RWMutex
	m map[string]string
}

func init() {
	kv.m = make(map[string]string)
}

func setKey(key, value string) {
	kv.Lock()
	defer kv.Unlock()
	kv.m[key] = value
}

func getKey(key string) (string, bool) {
	kv.RLock()
	defer kv.RUnlock()
	val, exists := kv.m[key]
	return val, exists
}