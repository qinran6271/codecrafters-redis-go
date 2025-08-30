package main

import (
	"net"
)

// 每个客户端连接的上下文
type ClientCtx struct {
	tx *transactionState // 事务相关的状态
	isReplica bool
	// 后续可以扩展更多，比如：
	// subscribedChannels []string
}

// 保存所有客户端的上下文
var clients = make(map[net.Conn]*ClientCtx)

// 获取或创建某个连接的上下文
func getClientCtx(conn net.Conn) *ClientCtx {
	if ctx, ok := clients[conn]; ok {
		return ctx
	}
	ctx := &ClientCtx{tx: &transactionState{}}
	clients[conn] = ctx
	return ctx
}