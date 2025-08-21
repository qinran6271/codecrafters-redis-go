package main
import (
	"sync"
)

type blpopResult struct {
	key string
	value string
}

type blWaiter struct {
	ch chan blpopResult // 缓冲 1，避免发送阻塞
}

var blq struct {
	sync.RWMutex
	m map[string][]*blWaiter // key -> waiters FIFO waiting list
}

func init() {
	blq.m = make(map[string][]*blWaiter)
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