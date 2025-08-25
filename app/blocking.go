package main

import (
	"sync"
	"time"
	"net"
)

type blpopResult struct {
	key string
	value string
}

type blWaiter struct {
	ch chan blpopResult // 缓冲 1，避免发送阻塞
}

// var blq struct {
// 	sync.RWMutex
// 	m map[string][]*blWaiter // key -> waiters FIFO waiting list
// }

// func init() {
// 	blq.m = make(map[string][]*blWaiter)
// }

var blq = struct {
    sync.RWMutex
    m map[string][]*blWaiter
}{
    m: make(map[string][]*blWaiter),
}

func enqueueWaiter(key string, w *blWaiter) {
	blq.Lock()
	defer blq.Unlock()
	blq.m[key] = append(blq.m[key], w) // Append the waiter to the list for the key
}

func removeWaiter(key string, w *blWaiter) {
	blq.Lock()
	defer blq.Unlock()
	q := blq.m[key]
	for i:= range q {
		if q[i] == w {
			copy(q[i:], q[i+1:]) // Remove the waiter from the list
			q = q[:len(q)-1] // Resize the slice
			break
		}
	}
	if len(q) == 0 {
		delete(blq.m, key) // Remove the key if no waiters left
	} else {
		blq.m[key] = q // Update the list for the key
	}
}


// 必须在持有 kv.Lock() 的情况下调用。
// 如果成功把列表头元素交付给等待者，返回 true；否则 false。
func deliverToWaiterLocked (key string) bool {
	// 如果 key 不存在，或者不是列表类型，或者列表为空，直接返回 false。
	e, exit := kv.m[key]
	if !exit || e.kind != kindList || len(e.l) == 0 {
		return false // Return false if the key does not exist, is not a list, or is empty
	}

	// 如果没有等待者，直接返回 false。
	blq.Lock()
	q := blq.m[key]
	if len(q) == 0 {
		blq.Unlock()
		return false // Return false if there are no waiters for the key
	}

	// 取出第一个等待者，并从等待者列表中删除。
	w := q[0]
	if len(q) == 1 {
		delete(blq.m, key) // Remove the key if this is the last waiter
	}
	blq.Unlock()

	// 从列表中取出第一个元素，并把它交付给等待者
	item := e.l[0] // Get the first item from the list
	e.l = e.l[1:] // Remove the first item from the list
	if len(e.l) == 0 {
		delete(kv.m, key) // Remove the key if the list is now empty
	}else{
		kv.m[key] = e // Update the entry in the map
	}
	
	go func() {
		w.ch <- blpopResult{key: key, value: item} // Send the item to the waiter
	}()

	return true // Return true indicating the item was successfully delivered to the waiter

}

type xreadWaiter struct {
	conn net.Conn
	key string
	lastID string
	timeout time.Duration //表示一段时间的长度
	done chan struct{}
}

var xread = struct {
    sync.Mutex
    waiters map[string][]*xreadWaiter
}{
    waiters: make(map[string][]*xreadWaiter),
}

// 添加一个 XREAD 等待者
func addXReadWaiter(key string, w *xreadWaiter) {
	xread.Lock()
	defer xread.Unlock()
	xread.waiters[key] = append(xread.waiters[key], w)
}

// 唤醒等待者
func notifyXReadWaiters(key string, newEntry streamEntry) {
    xread.Lock()
    defer xread.Unlock()

	waiters := xread.waiters[key]
	if len(waiters) == 0 {
		return // No waiters to notify
	}	

	remaining := make([]*xreadWaiter, 0, len(waiters))
	for _, w := range waiters {
		ms, seq := splitStreamID(w.lastID)
		if newEntry.msTime > ms || (newEntry.msTime == ms && newEntry.seqNum > seq) {
			writeArrayHeader(w.conn, 1) // Number of streams
			writeArrayHeader(w.conn, 2) // Number of elements in this stream
			writeBulk(w.conn, key) // Stream key

			writeArrayHeader(w.conn, 1) // Number of entries
			writeArrayHeader(w.conn, 2) // Number of fields in this entry
			writeBulkString(w.conn, newEntry.id) // Entry ID

			writeArrayHeader(w.conn, len(newEntry.fields)*2) // Number of fields
			for field, value := range newEntry.fields {
				writeBulkString(w.conn, field) // Field name
				writeBulkString(w.conn, value) // Field value
			}
			close(w.done) // Signal that the response has been sent
		}else{
			remaining = append(remaining, w) // Keep the waiter if no new entries
		}
	}
	if len(remaining) == 0 {
		delete(xread.waiters, key) // Remove the key if no waiters left
	} else {
		xread.waiters[key] = remaining // Update the list of waiters
	}
}

