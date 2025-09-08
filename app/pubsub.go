package main

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
