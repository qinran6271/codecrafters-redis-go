package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
	"log"
)

// **********************basic commands***********************
func replied(isWrite bool) CommandResult {
	return CommandResult{IsWrite: isWrite}
}

func cmdPING(conn net.Conn, args []string, ctx *ClientCtx) CommandResult {
	    // 订阅模式下：按要求返回 ["pong", ""]
    if inSubscribedMode(ctx) {
        writeArrayHeader(conn, 2)       // *2\r\n
        writeBulkString(conn, "pong")   // $4\r\npong\r\n
        writeBulkString(conn, "")       // $0\r\n\r\n
        return replied(false)
    }

	if len(args) == 1 {
		writeSimple(conn, "PONG")
	}else{
		writeBulk(conn, args[1])
	}
	return replied(false)
}

func cmdECHO(conn net.Conn, args []string, ctx *ClientCtx) CommandResult {
	if len(args) < 2 {
		writeError(conn, "wrong number of arguments for 'echo' command")
		return replied(false)
	}
	writeBulk(conn, args[1])
	return replied(false)
}

func cmdSET(conn net.Conn, args []string, ctx *ClientCtx) CommandResult {
	if len(args) < 3 {
		writeError(conn, "wrong number of arguments for 'set' command")
		return replied(false)
	}
	key, val := args[1], args[2]

	var pxMs int64 = 0
	seenPX := false
	for i:= 3; i < len(args);{
		switch strings.ToUpper(args[i]) {
		case "PX":
			if seenPX || i+1 >= len(args) {
				writeError(conn, "wrong number of arguments for 'set' command")
				return replied(false)
			}
			ms, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil || ms <= 0{
				writeError(conn, "invalid value for PX option")
				return replied(false)
			}
			pxMs = ms
			seenPX = true
			i += 2 // Skip the next argument as it's the value for PX
		default:
			writeError(conn, fmt.Sprintf("unknown option '%s' for 'set' command", args[i]))
			return replied(false)
		}
	}
	setKey(key, val, pxMs)
	writeSimple(conn, "OK")
	return replied(true)
}

func cmdGET(conn net.Conn, args []string, ctx *ClientCtx) CommandResult {
	if len(args) != 2 {
		writeError(conn, "wrong number of arguments for 'get' command")
		return replied(false)
	}
	
	if val, exists := getKey(args[1]); exists {
		writeBulk(conn, val)
	} else {
		writeNullBulk(conn) // RESP Null Bulk String for non-existing key
	}		
	return replied(false)
}

// **********************lists commands***********************

func cmdRPUSH(conn net.Conn, args []string, ctx *ClientCtx) CommandResult {
	if len(args) < 3 {
		writeError(conn, "wrong number of arguments for 'rpush' command")
		return replied(false)
	}
	key := args[1]
	values := args[2:]

	newLen, err := rpushKey(key, values)
	if err != nil {
		if err == ErrWrongType {
			writeError(conn, err.Error())
			return replied(false)
		}
		writeError(conn, "internal error")
		return replied(false)
	}
	writeInteger(conn, int64(newLen))
	return replied(true)
}



func cmdLRANGE(conn net.Conn, args []string, ctx *ClientCtx) CommandResult {
	if len(args) < 4 {
		writeError(conn, "wrong number of arguments for 'lrange' command")
		return replied(false)
	}
	key := args[1]
	
	start, err1 := strconv.Atoi(args[2])
	end, err2 := strconv.Atoi(args[3])
	if err1 != nil || err2 != nil {
		writeError(conn, "invalid start index for 'lrange' command")
		return replied(false)
	}

	items, err := lrangeKey(key, start, end)
	if err != nil {
		if err == ErrWrongType {
			writeError(conn, err.Error())
			return replied(false)
		} else {
			writeError(conn, "internal error")
		}
		return replied(false)
	}

	fmt.Fprintf(conn, "*%d\r\n", len(items)) // RESP Array with length
	for _, item := range items {
		writeBulkString(conn, item) // Write each item as a RESP Bulk String
	}
	return replied(false)
}

func cmdLPUSH(conn net.Conn, args []string, ctx *ClientCtx) CommandResult {
	if len(args) < 3 {
		writeError(conn, "wrong number of arguments for 'lpush' command")
		return replied(false)
	}
	key := args[1]
	values := args[2:]

	newLen, err := lpushKey(key, values)
	if err != nil {
		if err == ErrWrongType {
			writeError(conn, err.Error())
		} else {
			writeError(conn, "internal error")
		}
		return replied(false)
	}
	writeInteger(conn, int64(newLen))
	return replied(true)
}

func cmdLLEN(conn net.Conn, args []string, ctx *ClientCtx) CommandResult {
	if len(args) != 2 {
		writeError(conn, "wrong number of arguments for 'llen' command")
		return replied(false)
	}
	
	key := args[1]

	length, err := llenKey(key)
	if err != nil {
		if err == ErrWrongType {
			writeError(conn, err.Error())
			
		} else {
			writeError(conn, "internal error")// RESP Null Bulk String for non-existing key
		}
		return replied(false)
	}

	writeInteger(conn, int64(length)) // Return the length of the list
	return replied(false)
}

// 从列表（list）的 左边（头部）移除并返回元素
// 两种调用方式：
// 1) LPOP key → 返回被移除的元素
// 2) LPOP key count → 返回被移除的元素列表
func cmdLPOP(conn net.Conn, args []string, ctx *ClientCtx) CommandResult {
	if len(args) != 2  && len(args) != 3 {
		writeError(conn, "wrong number of arguments for 'lpop' command")
		return replied(false)
	}

	key := args[1]

	single := (len(args) == 2) // 判断是否是 LPOP key 还是 LPOP key count
	count := 1
	if !single {
		n, err := strconv.Atoi(args[2])
		if err != nil || n <= 0 {
			writeError(conn, "ERR value is not an integer or out of range")
			return replied(false)
		}
		count = n
	}

	items, err := lpopKey(key, count)
	if err != nil {
		if err == ErrWrongType {
			writeError(conn, err.Error())
		} else {
			writeError(conn, "internal error")
		}
		return replied(false)
	}

	if single {
		if len(items) == 0 {
			writeNullBulk(conn) // RESP Null Bulk String for empty list
		} else {
			writeBulkString(conn, items[0]) // Write the first item as a RESP Bulk String
		}
	} else{
		writeArrayHeader(conn, len(items)) // RESP Array with length
		for _, item := range items {
			writeBulkString(conn, item) // Write each item as a RESP Bulk String
		}
	}
	return replied(true)
	
}

func cmdBLPOP(conn net.Conn, args []string, ctx *ClientCtx) CommandResult {
	if len(args) < 3 {
		writeError(conn, "wrong number of arguments for 'blpop' command")
		return replied(false)
	}
	key := args[1]
	to, err := strconv.ParseFloat(args[2], 64)
	if err != nil || to < 0 {
		writeError(conn, "invalid timeout value for 'blpop' command")
		return replied(false)
	}

	// no blocking if key exists
	items, err := lpopKey(key, 1)
	if err != nil {
		if err == ErrWrongType {
			writeError(conn, err.Error())
			return replied(false)
		} else {
			writeError(conn, "internal error")
			return replied(false)
		}
	}

	if len(items) > 0 {
		writeArrayHeader(conn, 2) // RESP Array with length 2
		writeBulkString(conn, key) // Write the key as a RESP Bulk String
		writeBulkString(conn, items[0]) // Write the first item as a RESP Bulk String
		return replied(false)
	}

	// If no items were popped, we need to block
	waiter := &blWaiter{ch: make(chan blpopResult, 1)} // Create a new waiter with a buffered channel
	enqueueWaiter(key, waiter)

	
	if to == 0 {
		// wait indefinitely
		res := <-waiter.ch //如果channel是空的就在这一行阻塞
		writeArrayHeader(conn, 2) // RESP Array with length 2
		writeBulkString(conn, res.key) // Write the key as a RESP Bulk String
		writeBulkString(conn, res.value) // Write the value as a RESP Bulk String
		return replied(false)
	}

	// wait with timeout
	select {
	case res := <-waiter.ch:
		fmt.Println("hhhh", key)
		writeArrayHeader(conn, 2) // RESP Array with length 2
		writeBulkString(conn, res.key) // Write the key as a RESP Bulk String
		writeBulkString(conn, res.value) // Write the value as a RESP Bulk String
	case <-time.After(time.Duration(to * float64(time.Second))):
		fmt.Println("BLPOP timeout for key:", key)
		removeWaiter(key, waiter) // Remove the waiter if timeout occurs
		writeNullArray(conn) // RESP Null Bulk String for timeout
	}
	return replied(false)
}

// **********************Stream commands***********************
func cmdTYPE(conn net.Conn, args []string, ctx *ClientCtx) CommandResult {
	if len(args) != 2 {
		writeError(conn, "wrong number of arguments for 'type' command")
		return replied(false)
	}
	
	key := args[1]
	
	kv.RLock()
	e, exists := kv.m[key]
	kv.RUnlock()
	if !exists {
		writeSimple(conn, "none") // RESP Simple String for non-existing key
		return replied(false)
	}

	switch e.kind {
	case kindString:
		writeSimple(conn, "string") // RESP Simple String for string type
	case kindList:
		writeSimple(conn, "list") // RESP Simple String for list type
	// Add more cases for other types if needed
	// case kindSet:
	// 	writeSimple(conn, "set") // RESP Simple String for set type
	// case kindZSet:
	// 	writeSimple(conn, "zset") // RESP Simple String for sorted set type
	// case kindHash:
	// 	writeSimple(conn, "hash") // RESP Simple String for hash type
	case kindStream:
		writeSimple(conn, "stream") // RESP Simple String for stream type
	default:
		writeError(conn, "unknown type") // RESP Error for unknown type
	}
	return replied(false)
}


// cmdXADD handles the XADD command for adding entries to a stream
// eg. 
// $ redis-cli XADD stream_key 0-1 foo bar 
// "0-1"
// $ redis-cli XADD stream_key 1526919030474-0 temperature 36 humidity 95 
// "1526919030474-0" # (ID of the entry created)
func cmdXADD(conn net.Conn, args []string, ctx *ClientCtx) CommandResult {
	if len(args) < 5 || len(args)%2 != 1 {
		// fmt.Printf("args: %#v\n", args)
		writeError(conn, "wrong number of arguments for 'xadd' command")
		return replied(false)
	}
	key := args[1]
	id := args[2] // The ID of the entry, can be "0-0

	//parse the ID
	ms, seq, autoSeq, autoTime, err := parseStreamID(id)
	if err != nil {
		writeError(conn, err.Error())
		return replied(false)
	}

	// parse field-value pairs
	fields := make(map[string]string)
	for i := 3; i < len(args); i += 2 {
		fields[args[i]] = args[i+1] // Store field-value pairs in a map
	}

	kv.Lock()
	defer kv.Unlock()

	e, exists := kv.m[key]
	if !exists {
		// If the key does not exist, create a new stream entry
		e = entry{
			kind:    kindStream,
			streams: []streamEntry{},
		}
	}else{
		if e.kind != kindStream {
			writeError(conn, ErrWrongType.Error())
			return replied(false)
		}
	}

	// If the ID is "*", 完全自动生成时间戳
	if autoTime {
		ms = time.Now().UnixMilli() // Get the current time in milliseconds
		if len(e.streams) == 0 {
			seq = 0 // If the stream is empty, start with sequence 0
		} else {
			last := e.streams[len(e.streams)-1]
			if last.msTime == ms {
				seq = last.seqNum + 1 // Increment the sequence number if the timestamp is the same
			} else {
				seq = 0 // Reset sequence number if the timestamp is different
			}
		}
	}


	// autoSeq is true if the ID is "*" 只自动 seq 部分
	if autoSeq {
		if len(e.streams) == 0 {
			if ms == 0 {
				seq = 1
			} else {
				seq = 0
			}
		} else {
			last := e.streams[len(e.streams)-1]
			if last.msTime == ms {
				seq = last.seqNum + 1 // Increment the sequence number if the timestamp is the same
			} else {
				if ms == 0 {
					seq = 1 // If the timestamp is 0, start with sequence 1
				} else {						
				seq = 0 // Reset sequence number if the timestamp is different
				}
			}
		}
	}

	// Validate the stream ID
	if err := validateStreamID(ms, seq, e.streams); err != nil {
		writeError(conn, err.Error())
		return replied(false)
	}
	
	newID := fmt.Sprintf("%d-%d", ms, seq)

	// append the new stream entry
	newEntry := streamEntry{
        id:     newID,
        msTime: ms,
        seqNum: seq,
        fields: fields,
    }
	
	e.streams = append(e.streams, newEntry)
	kv.m[key] = e // Update the entry in the map
	writeBulk(conn, newID ) // Write the ID of the new entry as a RESP Bulk String
	notifyXReadWaiters(key, newEntry)

	return replied(true)

}

// It takes two arguments: start and end. 
// The command returns all entries in the stream with IDs between the start and end IDs.
//$ redis-cli XRANGE some_key 1526985054069 1526985054079
func cmdXRANGE(conn net.Conn, args []string, ctx *ClientCtx) CommandResult {
	if len(args) != 4 {
		writeError(conn, "wrong number of arguments for 'xrange' command")
		return replied(false)
	}
	key := args[1]
	startID := args[2]
	endID := args[3]


	kv.RLock()
	e, exists := kv.m[key]
	kv.RUnlock()

	if !exists {
		writeArrayHeader(conn, 0) // RESP Array with length 0 for non-existing key
		return replied(false)
	}

	// parse startID and endID
	startMs, startSeq := parseStreamIDForRange(startID, true)
	endMs, endSeq := parseStreamIDForRange(endID, false)

	// collect entries within the specified range
	var result []streamEntry
	for _, se := range e.streams {
		if (se.msTime > startMs || (se.msTime == startMs && se.seqNum >= startSeq)) &&
		   (se.msTime < endMs || (se.msTime == endMs && se.seqNum <= endSeq)) {
			result = append(result, se)
		}
	}

	// RESP
	writeArrayHeader(conn, len(result)) // RESP Array with length
	for _, se := range result {
		writeArrayHeader(conn, 2) // Each entry is an array of [ID, fields]
		writeBulkString(conn, se.id) // Write the ID as a RESP Bulk String

		writeArrayHeader(conn, len(se.fields)*2) // Fields are key-value pairs
		for field, value := range se.fields {
			writeBulkString(conn, field) // Write field name
			writeBulkString(conn, value) // Write field value
		}
	}
	return replied(false)
}

// XREAD STREAMS some_key 1526985054069-0
type keyResult struct {
	key     string
	entries []streamEntry
}


func cmdXREAD(conn net.Conn, args []string, ctx *ClientCtx) CommandResult {
    if len(args) < 4 {
        writeError(conn, "wrong number of arguments for 'xread' command")
        return replied(false)
    }

	var blockMs int64 = -1
	streamsIdx := 1

	// parse arguments
	i := 1
	for i < len(args) {
		switch strings.ToUpper(args[i]) {
		case "BLOCK": //args := []string{"XREAD", "BLOCK", "5000", "STREAMS", "mystream", "0"}
			if i+1 >= len(args) {
				writeError(conn, "syntax error")
				return replied(false)
			}
			ms, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				writeError(conn, "invalid block timeout")
				return replied(false)
			}
			blockMs = ms
			i += 2 // Skip the next argument as it's the value for BLOCK
		case "STREAMS": 
			streamsIdx = i
			i ++
			goto ParseStreams
		default:
			i ++
		}
	}

ParseStreams:
	// STREAMS 后面：一半是 keys，一半是 ids
	if streamsIdx == -1 || (len(args)-streamsIdx-1)%2 != 0 {
        writeError(conn, "syntax error")
        return replied(false)
    }

	pairs := (len(args) - streamsIdx - 1) / 2
	keys := args[streamsIdx+1 : streamsIdx+1+pairs]
	ids  := args[streamsIdx+1+pairs:]
	// fmt.Println("keys:", keys, "ids:", ids)

	// === 处理 $：替换为对应 key 的最新 ID ===
	kv.RLock()
	for i, id := range ids {
		if id == "$" {
			e, exists := kv.m[keys[i]]
			if exists && e.kind == kindStream && len(e.streams) > 0 {
				latest := e.streams[len(e.streams)-1]
				ids[i] = latest.id
			} else {
				ids[i] = "0-0" // stream 为空时，从 0-0 开始
			}
		}
	}
	kv.RUnlock()	

	kv.RLock()
	results := make([]keyResult, 0, len(keys))
	for i, key := range keys {
		id := ids[i]
		ms, seq := parseStreamIDForRange(id, true)	

		e, exists := kv.m[key]
		if !exists || e.kind != kindStream {
			results = append(results, keyResult{key: key, entries: nil})
			continue // Skip non-existing keys or keys that are not streams
		}
	
		var found []streamEntry
		for _, se := range e.streams {
			if se.msTime > ms || (se.msTime == ms && se.seqNum > seq) {
				found = append(found, se)
			}
		}
		results = append(results, keyResult{key: key, entries: found})
	}
    kv.RUnlock()

	// 如果有结果 → 按请求顺序返回
	hasData := false
	for _, r := range results {
		if len(r.entries) > 0 {
			hasData = true
			break
		}
	}
	if hasData {
		writeStreamResults(conn, results)
		return replied(false)
	}

	// If no results and BLOCK is specified, we need to wait
	if blockMs == -1 {
		writeNullArray(conn) // RESP Null Bulk String for no results and no blocking
		return replied(false)
	}

	//If no results & BLOCK > 0, we need to wait
	timeout := time.Duration(blockMs) * time.Millisecond
	for i, key := range keys {
		waiter := &xreadWaiter{
			conn: conn,
			key: key,
			lastID: ids[i],
			timeout: timeout,
			done: make(chan struct{}),		
		}
		addXReadWaiter(key, waiter)

		go func(w *xreadWaiter) {
			if blockMs == 0 {
				<- w.done // wait indefinitely
				return
			}
			select {
			case <- w.done: // 被关闭时（close(w.done)，<-w.done 的地方都会立即收到信号
				return
			case <- time.After(w.timeout): //等timeout的时间
				// fmt.Println("XREAD timeout for key:", w.key)
				writeNullArray(w.conn) // RESP Null Bulk String for timeout
			}
		}(waiter)
	}
	return replied(false)
}


func writeStreamResults(conn net.Conn, results []keyResult) {
	writeArrayHeader(conn, len(results)) // Number of streams
	for _, r := range results {
		writeArrayHeader(conn, 2)    // [key, entries]
		writeBulkString(conn, r.key) // stream key

		writeArrayHeader(conn, len(r.entries)) // Number of entries
		for _, se := range r.entries {
			writeArrayHeader(conn, 2)    // [id, fields]
			writeBulkString(conn, se.id) // entry ID

			writeArrayHeader(conn, len(se.fields)*2) // fields
			for field, value := range se.fields {
				writeBulkString(conn, field)
				writeBulkString(conn, value)
			}
		}
	}
}


// *********************Transactions**********************
//The INCR command is used to increment the value of a key by 1.
// 1. Key exists and has a numerical value (previous stages)
// 2. Key doesn't exist (This stage)
// 3. Key exists but doesn't have a numerical value (later stages)

//ex. redis-cli SET foo 5
func cmdINCR(conn net.Conn, args []string, ctx *ClientCtx) CommandResult {
	if len(args) != 2 {
		writeError(conn, "wrong number of arguments for 'incr' command")
		return replied(false)
	}

	key := args[1]

	val, exists := getKey(key)
	if !exists {
		setKey(key, "1", 0)
		writeInteger(conn, 1)
		return replied(true)
	}

	intVal, err := strconv.Atoi(val) // Convert the string value to an integer
	if err != nil {
		writeError(conn, "ERR value is not an integer or out of range")
		return replied(false)
	}
	intVal++
	setKey(key, strconv.Itoa(intVal), 0)
	writeInteger(conn, int64(intVal))
	return replied(true)
}

func cmdMULTI(conn net.Conn, args []string, ctx *ClientCtx) CommandResult {
	if len(args) != 1 {
		writeError(conn, "wrong number of arguments for 'multi' command")
		return replied(false)
	}

	ctx.tx.inMulti = true
	ctx.tx.queue = make([][]string, 0)

	writeSimple(conn, "OK")
	return replied(false)
}

func cmdEXEC(conn net.Conn, args []string, ctx *ClientCtx) CommandResult {
	if len(args) != 1 {
		writeError(conn, "wrong number of arguments for 'exec' command")
		return replied(false)
	}

	if !ctx.tx.inMulti {
		writeError(conn, "ERR EXEC without MULTI")
		return replied(false)
	}

	writeArrayHeader(conn, len(ctx.tx.queue)) // RESP Array with length of queued commands
	
	for _, queued := range ctx.tx.queue {
		cmd := strings.ToUpper(queued[0])
		if handler, ok := routs[cmd]; ok {
			handler(conn, queued, ctx) // 真正执行命令，把结果写回 conn
		} else {
			writeError(conn, "unknown command '"+queued[0]+"'")
		}
	}

	// 清理事务状态
	ctx.tx.inMulti = false
	ctx.tx.queue = nil
	return replied(false)
}

// 放弃事务，清理状态
func cmdDISCARD(conn net.Conn, args []string, ctx *ClientCtx) CommandResult {
	if len(args) != 1 {
		writeError(conn, "wrong number of arguments for 'discard' command")
		return replied(false)
	}

	if !ctx.tx.inMulti {
		writeError(conn, "ERR DISCARD without MULTI")
		return replied(false)
	}

	// 丢弃队列
	ctx.tx.inMulti = false
	ctx.tx.queue = nil

	writeSimple(conn, "OK")
	return replied(false)
}

// ********************** replication ***********************
func cmdINFO(conn net.Conn, args []string, ctx *ClientCtx) CommandResult {
	if len(args) < 2{
		writeError(conn, "wrong number of arguments for 'info' command")
		return replied(false)
	}

	section := strings.ToLower(args[1])
	if section == "replication" {
		writeBulkString(conn, GetReplicationInfo())
	}else {
		writeBulkString(conn, "")
	}
	return replied(false)
}

func cmdREPLCONF(conn net.Conn, args []string, ctx *ClientCtx) CommandResult {
    if len(args) < 2 {
        writeError(conn, "ERR wrong number of arguments for 'REPLCONF'")
        return CommandResult{IsWrite: false}
    }

    sub := strings.ToUpper(args[1])

    switch sub {
    // case "GETACK": 
	// 	_ = writeArrayBulk(conn, "REPLCONF", "ACK", "0")
    //     return CommandResult{IsWrite: false}

    case "LISTENING-PORT":
        if len(args) < 3 {
            writeError(conn, "ERR wrong number of arguments for 'REPLCONF'")
            return CommandResult{IsWrite: false}
        }
        // ctx.replicaPort = args[2] // 如果你要保存端口，可以在这里
        writeSimple(conn, "OK")
        return CommandResult{IsWrite: false}

    case "CAPA":
        writeSimple(conn, "OK")
        return CommandResult{IsWrite: false}

    default:
        writeError(conn, "ERR unknown REPLCONF subcommand")
        return CommandResult{IsWrite: false}
    }
}


// 客户端发送（replica） PSYNC <replid> <offset>
// ex. PSYNC ? -1
func cmdPSYNC(conn net.Conn, args []string, ctx *ClientCtx) CommandResult {
	// 标记当前连接为 replica
	// ctx.isReplica = true

	// PSYNC ? -1
	// 第一次全量同步：返回 FULLRESYNC
	reply := fmt.Sprintf("+FULLRESYNC %s %d\r\n", masterReplId, masterReplOffset)
	fmt.Fprint(conn, reply)

	// 发送空 RDB 文件
	fmt.Fprintf(conn, "$%d\r\n", len(emptyRdbDump))
	_, err := conn.Write(emptyRdbDump) // emptyRdbDump 已经自带结尾 \r\n
	if err != nil {
		fmt.Println("failed to send RDB:", err)
		return replied(false)
	}

	// 把这个连接注册为 replica（用于后续 propagate）
	addReplicaConn(conn)

	// PSYNC 本身不是写命令，不需要传播
	return replied(false)
}

func cmdWAIT(conn net.Conn, args []string, ctx *ClientCtx) CommandResult {
    if len(args) != 3 {
        writeError(conn, "ERR wrong number of arguments for 'WAIT'")
        return CommandResult{}
    }

    numReplicas, _ := strconv.Atoi(args[1])
    timeoutMs, _ := strconv.Atoi(args[2])

    targetOffset := masterOffset

    // 1. 广播 GETACK 给所有 replica
    for _, r := range snapshotReplicaConns() {
        r.conn.Write([]byte(buildRESPArray([]string{"REPLCONF", "GETACK", "*"})))
    }

    // 2. 等待 ACK 或超时
    deadline := time.Now().Add(time.Duration(timeoutMs) * time.Millisecond)
    for {
        acked := countReplicasAtLeast(targetOffset)
        if acked >= numReplicas {
            writeInteger(conn, int64(acked))
            return CommandResult{}
        }
        if time.Now().After(deadline) {
            writeInteger(conn, int64(acked))
            return CommandResult{}
        }
        time.Sleep(10 * time.Millisecond) // 简单轮询
    }
}


// ********************** RDB ***********************

func cmdCONFIG(conn net.Conn, args []string, ctx *ClientCtx) CommandResult {
    if len(args) < 3 || strings.ToUpper(args[1]) != "GET" {
        writeError(conn, "ERR only CONFIG GET is supported")
        return replied(false)
    }

    param := strings.ToLower(args[2])

    switch param {
    case "dir":
        writeArrayHeader(conn, 2)
        writeBulkString(conn, "dir")
        writeBulkString(conn, configDir)
    case "dbfilename":
        writeArrayHeader(conn, 2)
        writeBulkString(conn, "dbfilename")
        writeBulkString(conn, configDBFilename)
    default:
        // Redis 在参数不存在时返回空数组
        writeArrayHeader(conn, 0)
    }

   return replied(false)
}


func cmdKEYS(conn net.Conn, args []string, ctx *ClientCtx) CommandResult {
    if len(args) != 2 {
        writeError(conn, "ERR wrong number of arguments for 'keys' command")
        return replied(false)
    }

    pattern := args[1]
    if pattern != "*" {
        // 只支持 KEYS *
        writeArrayHeader(conn, 0)
        return replied(false)
    }

    kv.RLock()
    defer kv.RUnlock()

    // 收集所有没过期的 key
    keys := []string{}
    for k, e := range kv.m {
        if !e.expires.IsZero() && time.Now().After(e.expires) {
            continue // 已过期，跳过
        }
        keys = append(keys, k)
    }

    // RESP 数组返回
    writeArrayHeader(conn, len(keys))
    for _, k := range keys {
        writeBulkString(conn, k)
    }

    return replied(false) // KEYS 是读命令
}


// *********************** Pub/Sub ***********************

func cmdSUBSCRIBE(conn net.Conn, args []string, ctx *ClientCtx) CommandResult {
	if len(args) < 2 {
		writeError(conn, "wrong number of arguments for 'subscribe' command")
		return replied(false)
	}
	// 惰性初始化
	if ctx.pubsub == nil {
		ctx.pubsub = &pubsubState{subs: make(map[string]struct{})}
	}

	// 支持多个频道参数：SUBSCRIBE ch1 ch2 ...
   for i := 1; i < len(args); i++ {
        ch := args[i]

        if _, exists := ctx.pubsub.subs[ch]; !exists {
            ctx.pubsub.subs[ch] = struct{}{}

            // 加入频道→订阅者集合
            subIndex.Lock()
            set := subIndex.chans[ch]
            if set == nil {
                set = make(map[*ClientCtx]struct{})
                subIndex.chans[ch] = set
            }
            set[ctx] = struct{}{}
            subIndex.Unlock()
        }

        writeArrayHeader(conn, 3)
        writeBulkString(conn, "subscribe")
        writeBulkString(conn, ch)
        writeInteger(conn, int64(len(ctx.pubsub.subs)))
		log.Printf("SUB %s -> subIndex size = %d", ch, len(subIndex.chans[ch]))
    }

	


	// 不修改全局 DB，不需要 propagate
	return replied(false)
}


func cmdPUBLISH(conn net.Conn, args []string, ctx *ClientCtx) CommandResult {
	// 仅需检查参数；此阶段消息内容不用分发
	if len(args) != 3 {
		writeError(conn, "wrong number of arguments for 'publish' command")
		return replied(false)
	}
	ch := args[1]
	msg := args[2] // 先不使用（后续阶段才真正投递）

    n := deliverToSubscribers(ch, msg) // 先真正投递
    writeInteger(conn, int64(n))       // 然后回订阅者数量

	return replied(false)
}

func cmdUNSUBSCRIBE(conn net.Conn, args []string, ctx *ClientCtx) CommandResult {
    // 无订阅状态也要能优雅返回
    if ctx.pubsub == nil {
        ctx.pubsub = &pubsubState{subs: make(map[string]struct{})}
    }

    if len(args) == 1 {
        // 参数为空：取消当前客户端所有频道的订阅
        chans := unsubscribeAll(ctx)
        if len(chans) == 0 {
            // 与 Redis 保持兼容：没有任何订阅时也回一条占位
            writeArrayHeader(conn, 3)
            writeBulkString(conn, "unsubscribe")
            writeBulkString(conn, "")          // 空频道名
            writeInteger(conn, int64(0))       // 剩余为 0
            return replied(false)
        }
        // 按顺序逐条返回
        for i, ch := range chans {
            // 这里 remaining = len(ctx.pubsub.subs)，已经递减到当前值
            writeArrayHeader(conn, 3)
            writeBulkString(conn, "unsubscribe")
            writeBulkString(conn, ch)
            writeInteger(conn, int64(len(ctx.pubsub.subs)))
            _ = i // 仅避免未使用告警
        }
        return replied(false)
    }

    // 有参数：逐个处理
    for i := 1; i < len(args); i++ {
        ch := args[i]
        // 尝试取消；如果本来没订阅，则 remaining 不变
        _, remaining := unsubscribeChannel(ctx, ch)

        writeArrayHeader(conn, 3)
        writeBulkString(conn, "unsubscribe")
        writeBulkString(conn, ch)
        writeInteger(conn, int64(remaining))
    }
    return replied(false)
}


// ************************ Zset ***********************

func cmdZADD(conn net.Conn, args []string, ctx *ClientCtx) CommandResult {
    // 期望: ZADD key score member
    if len(args) != 4 {
        writeError(conn, "wrong number of arguments for 'zadd' command")
         return replied(false)
    }
    key := args[1]
    scoreStr := args[2]
    member := args[3]

    score, err := strconv.ParseFloat(scoreStr, 64)
    if err != nil {
        writeError(conn, "value is not a valid float")
         return replied(false)
    }

    // 如果 key 不存在，需要创建；如果存在且不是 zset -> WRONGTYPE
    zs, _, err := getZSetForZAdd(key)
    if err != nil {
        writeError(conn, err.Error())
        return replied(false)
    }

    // 写入逻辑：新成员 => 返回 1；已存在仅更新分数 => 返回 0
    added := 0
    kv.RLock()
    _, existed := zs.m[member]
    kv.RUnlock()
    if !existed {
        added = 1
    }

    // 更新分数（存在则覆盖，不计数）
    // 注意：这里需要写锁，因为 getZSetForZAdd 可能在创建时拿过一次锁
    // 为了简单，做一次独立的写锁更新
    kv.Lock()
    // 这里如果是刚创建的 zset，kv 已经插入；如果是已有 zset，entry 已存在
    zs.m[member] = score
    kv.Unlock()

    // RESP Integer
    writeInteger(conn, int64(added))
    return replied(true)
}
