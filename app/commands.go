package main

import (
	"net"
	"strconv"
	"strings"
	"fmt"
)

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