package main

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
)

// **********************basic commands***********************

func cmdPING(conn net.Conn, args []string) {
	if len(args) == 1 {
		writeSimple(conn, "PONG")
		return 
	}
	writeBulk(conn, args[1])
	
}

func cmdECHO(conn net.Conn, args []string) {
	if len(args) < 2 {
		writeError(conn, "wrong number of arguments for 'echo' command")
		return
	}
	writeBulk(conn, args[1])
}

func cmdSET(conn net.Conn, args []string) {
	if len(args) < 3 {
		writeError(conn, "wrong number of arguments for 'set' command")
		return
	}
	key, val := args[1], args[2]

	var pxMs int64 = 0
	seenPX := false
	for i:= 3; i < len(args);{
		switch strings.ToUpper(args[i]) {
		case "PX":
			if seenPX || i+1 >= len(args) {
				writeError(conn, "wrong number of arguments for 'set' command")
				return
			}
			ms, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil || ms <= 0{
				writeError(conn, "invalid value for PX option")
				return
			}
			pxMs = ms
			seenPX = true
			i += 2 // Skip the next argument as it's the value for PX
		default:
			writeError(conn, fmt.Sprintf("unknown option '%s' for 'set' command", args[i]))
			return	
		}
	}
	setKey(key, val, pxMs)
	writeSimple(conn, "OK")
}

func cmdGET(conn net.Conn, args []string) {
	if len(args) != 2 {
		writeError(conn, "wrong number of arguments for 'get' command")
		return
	}
	
	if val, exists := getKey(args[1]); exists {
		writeBulk(conn, val)
	} else {
		writeNullBulk(conn) // RESP Null Bulk String for non-existing key
	}		
}

// **********************lists commands***********************

func cmdRPUSH(conn net.Conn, args []string) {
	if len(args) < 3 {
		writeError(conn, "wrong number of arguments for 'rpush' command")
		return
	}
	key := args[1]
	values := args[2:]

	newLen, err := rpushKey(key, values)
	if err != nil {
		if err == ErrWrongType {
			writeError(conn, err.Error())
			return
		}
		writeError(conn, "internal error")
		return
	}
	writeInteger(conn, int64(newLen))
}



func cmdLRANGE(conn net.Conn, args []string) {
	if len(args) < 4 {
		writeError(conn, "wrong number of arguments for 'lrange' command")
		return
	}
	key := args[1]
	
	start, err1 := strconv.Atoi(args[2])
	end, err2 := strconv.Atoi(args[3])
	if err1 != nil || err2 != nil {
		writeError(conn, "invalid start index for 'lrange' command")
		return
	}

	items, err := lrangeKey(key, start, end)
	if err != nil {
		if err == ErrWrongType {
			writeError(conn, err.Error())
			return
		} else {
			writeError(conn, "internal error")
		}
		return
	}

	fmt.Fprintf(conn, "*%d\r\n", len(items)) // RESP Array with length
	for _, item := range items {
		writeBulkString(conn, item) // Write each item as a RESP Bulk String
	}
}

func cmdLPUSH(conn net.Conn, args []string) {
	if len(args) < 3 {
		writeError(conn, "wrong number of arguments for 'lpush' command")
		return
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
		return
	}
	writeInteger(conn, int64(newLen))
}

func cmdLLEN(conn net.Conn, args []string) {
	if len(args) != 2 {
		writeError(conn, "wrong number of arguments for 'llen' command")
		return
	}
	
	key := args[1]

	length, err := llenKey(key)
	if err != nil {
		if err == ErrWrongType {
			writeError(conn, err.Error())
			
		} else {
			writeError(conn, "internal error")// RESP Null Bulk String for non-existing key
		}
		return
	}

	writeInteger(conn, int64(length)) // Return the length of the list
}

func cmdLPOP(conn net.Conn, args []string) {
	if len(args) != 2  && len(args) != 3 {
		writeError(conn, "wrong number of arguments for 'lpop' command")
		return
	}

	key := args[1]

	single := (len(args) == 2)
	count := 1
	if !single {
		n, err := strconv.Atoi(args[2])
		if err != nil || n <= 0 {
			writeError(conn, "ERR value is not an integer or out of range")
			return
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
		return
	}

	if single {
		if len(items) == 0 {
			writeNullBulk(conn) // RESP Null Bulk String for empty list
			return
		} else {
			writeBulkString(conn, items[0]) // Write the first item as a RESP Bulk String
			return
		}
	}

	writeArrayHeader(conn, len(items)) // RESP Array with length
	for _, item := range items {
		writeBulkString(conn, item) // Write each item as a RESP Bulk String
	}
	
}

func cmdBLPOP(conn net.Conn, args []string) {
	if len(args) < 3 {
		writeError(conn, "wrong number of arguments for 'blpop' command")
		return
	}
	key := args[1]
	to, err := strconv.ParseFloat(args[2], 64)
	if err != nil || to < 0 {
		writeError(conn, "invalid timeout value for 'blpop' command")
		return
	}

	// no blocking if key exists
	items, err := lpopKey(key, 1)
	if err != nil {
		if err == ErrWrongType {
			writeError(conn, err.Error())
			return
		} else {
			writeError(conn, "internal error")
			return
		}
	}

	if len(items) > 0 {
		writeArrayHeader(conn, 2) // RESP Array with length 2
		writeBulkString(conn, key) // Write the key as a RESP Bulk String
		writeBulkString(conn, items[0]) // Write the first item as a RESP Bulk String
		return
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
		return
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
		writeNullBulk(conn) // RESP Null Bulk String for timeout
	}
}

// **********************Stream commands***********************
func cmdTYPE(conn net.Conn, args []string) {
	if len(args) != 2 {
		writeError(conn, "wrong number of arguments for 'type' command")
		return
	}
	
	key := args[1]
	
	kv.RLock()
	e, exists := kv.m[key]
	kv.RUnlock()
	if !exists {
		writeSimple(conn, "none") // RESP Simple String for non-existing key
		return
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
}


// cmdXADD handles the XADD command for adding entries to a stream
// eg. 
// $ redis-cli XADD stream_key 0-1 foo bar 
// "0-1"
// $ redis-cli XADD stream_key 1526919030474-0 temperature 36 humidity 95 
// "1526919030474-0" # (ID of the entry created)

func cmdXADD(conn net.Conn, args []string) {
	if len(args) < 5 || len(args)%2 != 1 {
		fmt.Printf("args: %#v\n", args)
		writeError(conn, "wrong number of arguments for 'xadd' command")
		return
	}
	key := args[1]
	id := args[2] // The ID of the entry, can be "0-0

	//parse the ID
	ms, seq, autoSeq, autoTime, err := parseStreamID(id)
	if err != nil {
		writeError(conn, err.Error())
		return
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
			return
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
		return
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

}

// It takes two arguments: start and end. 
// The command returns all entries in the stream with IDs between the start and end IDs.
//$ redis-cli XRANGE some_key 1526985054069 1526985054079
func cmdXRANGE(conn net.Conn, args []string) {
	if len(args) != 4 {
		writeError(conn, "wrong number of arguments for 'xrange' command")
		return
	}
	key := args[1]
	startID := args[2]
	endID := args[3]


	kv.RLock()
	e, exists := kv.m[key]
	kv.RUnlock()

	if !exists {
		writeArrayHeader(conn, 0) // RESP Array with length 0 for non-existing key
		return
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

}

// XREAD STREAMS some_key 1526985054069-0
type keyResult struct {
	key     string
	entries []streamEntry
}


func cmdXREAD(conn net.Conn, args []string) {
    if len(args) < 4 {
        writeError(conn, "wrong number of arguments for 'xread' command")
        return
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
				return
			}
			ms, err := strconv.ParseInt(args[i+1], 10, 64)
			if err != nil {
				writeError(conn, "invalid block timeout")
				return
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
        return
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
		return
	}

	// If no results and BLOCK is specified, we need to wait
	if blockMs == -1 {
		writeNullBulk(conn) // RESP Null Bulk String for no results and no blocking
		return
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
				fmt.Println("XREAD timeout for key:", w.key)
				writeNullBulk(w.conn) // RESP Null Bulk String for timeout
			}
		}(waiter)
	}
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
func cmdINCR(conn net.Conn, args []string) {
	if len(args) != 2 {
		writeError(conn, "wrong number of arguments for 'incr' command")
		return
	}

	key := args[1]

	val, exists := getKey(key)
	if !exists {
		setKey(key, "1", 0)
		writeInteger(conn, 1)
		return
	}

	intVal, err := strconv.Atoi(val) // Convert the string value to an integer
	if err != nil {
		writeError(conn, "ERR value is not an integer or out of range")
		return
	}
	intVal++
	setKey(key, strconv.Itoa(intVal), 0)
	writeInteger(conn, int64(intVal))
}

func cmdMULTI(conn net.Conn, args []string) {
	if len(args) != 1 {
		writeError(conn, "wrong number of arguments for 'multi' command")
		return
	}
	transactions[conn] = &transactionState{
		inMulti: true,
		queue:   make([][]string, 0),
	}
	writeSimple(conn, "OK")
}

func cmdEXEC(conn net.Conn, args []string) {
	if len(args) != 1 {
		writeError(conn, "wrong number of arguments for 'exec' command")
		return
	}

	state, ok := transactions[conn]
	if !ok || !state.inMulti {
		writeError(conn, "ERR EXEC without MULTI")
		return
	}

	writeArrayHeader(conn, len(state.queue)) // RESP Array with length of queued commands
	
	for _, queued := range state.queue {
		cmd := strings.ToUpper(queued[0])
		if handler, ok := routs[cmd]; ok {
			handler(conn, queued) // 真正执行命令，把结果写回 conn
		} else {
			writeError(conn, "unknown command '"+queued[0]+"'")
		}
	}

	// 清理事务状态
	delete(transactions, conn)
}

func cmdDISCARD(conn net.Conn, args []string) {
	if len(args) != 1 {
		writeError(conn, "wrong number of arguments for 'discard' command")
		return
	}

	state, ok := transactions[conn]
	if !ok || !state.inMulti {
		writeError(conn, "ERR DISCARD without MULTI")
		return
	}

	delete(transactions, conn) // 清理事务状态
	writeSimple(conn, "OK")
}