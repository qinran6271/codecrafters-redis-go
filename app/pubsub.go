package main

import (
	"sync"
	"log"
)

// 全局：频道 -> 订阅该频道的客户端集合
// 这个频道有哪些客户端在听？
// 按频道看（subIndex）
// foo -> {A, C}
// bar -> {A, B}
// ...
// 外层 map[string]...：按频道名索引（"foo", "bar"）
// 内层 map[*ClientCtx]struct{}：这个频道的订阅者集合（用 map 当 set，value 用 0 字节的 struct{} 省内存）
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

// 向订阅 ch 的所有客户端投递 payload；返回订阅者数量（用于 PUBLISH 的整数回复）
func deliverToSubscribers(channel, payload string) int {
    // 1) 快照订阅者集合
    subIndex.RLock()
    var receivers []*ClientCtx
    if set, ok := subIndex.chans[channel]; ok {
        receivers = make([]*ClientCtx, 0, len(set))
        for c := range set {
            receivers = append(receivers, c)
        }
    }
    subIndex.RUnlock()

    // 先记录订阅者数量，作为 PUBLISH 的返回值
    subsCount := len(receivers)

    // 2) 逐个写消息（有问题的连接跳过，但不影响返回值）
    for _, rc := range receivers {
        if rc == nil || rc.conn == nil { // 兜底，避免 panic
            continue
        }
        rc.writeMu.Lock()
        writeArrayHeader(rc.conn, 3)
        writeBulkString(rc.conn, "message")
        writeBulkString(rc.conn, channel)
        writeBulkString(rc.conn, payload)
        rc.writeMu.Unlock()
    }
	log.Printf("PUB %s -> receivers = %d", channel, len(receivers))

    return subsCount
}

// 在连接结束时调用，移除该客户端在所有频道的登记
// 移除端口对应的顾客
func cleanupSubscriptions(ctx *ClientCtx) {
    if ctx == nil || ctx.pubsub == nil {
        return
    }
    subIndex.Lock()
    for ch := range ctx.pubsub.subs {
        if set, ok := subIndex.chans[ch]; ok {
            delete(set, ctx)
            if len(set) == 0 {
                delete(subIndex.chans, ch)
            }
        }
    }
    subIndex.Unlock()

    // 可选：把客户端自己的订阅集合清空/置 nil，帮助 GC
    // ctx.pubsub.subs = nil
}

// 取消订阅单个频道（如本来没订，则什么都不做）
func unsubscribeChannel(ctx *ClientCtx, ch string) (wasSub bool, remaining int) {
    if ctx == nil || ctx.pubsub == nil {
        return false, 0
    }
    if _, ok := ctx.pubsub.subs[ch]; ok {
        delete(ctx.pubsub.subs, ch)
        // 同步更新全局索引
        subIndex.Lock()
        if set, ok2 := subIndex.chans[ch]; ok2 {
            delete(set, ctx)
            if len(set) == 0 {
                delete(subIndex.chans, ch)
            }
        }
        subIndex.Unlock()
        wasSub = true
    }
    return wasSub, len(ctx.pubsub.subs)
}

// 取消订阅全部频道：返回按“逐条取消”的顺序列表（频道名切片）
func unsubscribeAll(ctx *ClientCtx) []string {
    if ctx == nil || ctx.pubsub == nil || len(ctx.pubsub.subs) == 0 {
        return nil
    }
    // 先收集一份快照，避免边遍历边删 map
    channels := make([]string, 0, len(ctx.pubsub.subs))
    for ch := range ctx.pubsub.subs {
        channels = append(channels, ch)
    }
    // 逐个删除（会同步 subIndex）
    for _, ch := range channels {
        unsubscribeChannel(ctx, ch)
    }
    return channels
}

