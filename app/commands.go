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
		writeError(conn, "wrong number of arguments for 'xadd' command")
		return
	}
	key := args[1]
	id := args[2] // The ID of the entry, can be "0-0

	// field-value pairs
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

	// append the new stream entry
	e.streams = append(e.streams, streamEntry{
		id:     id,
		fields: fields,
	})
	kv.m[key] = e // Update the entry in the map
	writeBulk(conn, id) // Write the ID of the new entry as a RESP Bulk String

}