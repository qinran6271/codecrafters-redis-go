package main

// transaction.go 或 global.go 都可以

// 事务状态
type transactionState struct {
	inMulti bool
	queue   [][]string // 存放命令和参数
}

// 每个连接一个事务状态
// var transactions = make(map[net.Conn]*transactionState)