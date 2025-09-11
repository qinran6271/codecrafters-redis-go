package main

import (
	"net"
	"sync"
)

// 每客户端 ：这个客户端订了哪些频道？
// 按客户端看（pubsubState）          
// Client A -> {foo, bar}            
// Client B -> {bar}             
// Client C -> {foo}   
type pubsubState struct {
	subs map[string]struct{} // 该客户端已订阅的唯一频道集合
}

// 每个客户端或者replica连接的上下文
type ClientCtx struct {
	tx *transactionState // 事务相关的状态
	isReplica bool
	offset int64 // 当前副本已经处理的字节数 只对 replica 自己有意义
	conn    net.Conn     // 在 handleConnection 里赋值：ctx.conn = conn
    writeMu sync.Mutex   // 保护对 conn 的写操作（多发布者并发推送时需要）
	pubsub *pubsubState
}

// 保存所有客户端的上下文
var (
    clientsMu sync.RWMutex
    clients   = make(map[net.Conn]*ClientCtx)
)

func getClientCtx(conn net.Conn) *ClientCtx {
    clientsMu.RLock()
    if ctx, ok := clients[conn]; ok {
        clientsMu.RUnlock()
        return ctx
    }
    clientsMu.RUnlock()

    clientsMu.Lock()
    ctx, ok := clients[conn]
    if !ok {
        ctx = &ClientCtx{
            tx:     &transactionState{},                 // ✅ 初始化
            pubsub: &pubsubState{subs: make(map[string]struct{})}, // ✅ 初始化
        }
        clients[conn] = ctx
    }
    clientsMu.Unlock()
    return ctx
}


func releaseClientCtx(conn net.Conn) {
    clientsMu.Lock()
    delete(clients, conn)
    clientsMu.Unlock()
}
