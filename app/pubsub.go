package main

import (
	"sync"
)

// 全局：频道 -> 订阅该频道的客户端集合
var subIndex = struct {
	sync.RWMutex
	chans map[string]map[*ClientCtx]struct{}
}{
	chans: make(map[string]map[*ClientCtx]struct{}),
}


func inSubscribedMode(ctx *ClientCtx) bool {
    return ctx != nil && ctx.pubsub != nil && len(ctx.pubsub.subs) > 0
}
func allowedInSubscribedMode(cmd string) bool {
    switch cmd {
    case "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE", "PUNSUBSCRIBE", "PING", "QUIT":
        return true
    default:
        return false
    }
}
