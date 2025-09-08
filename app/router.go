package main

import (
	"net"
)

var routs map[string]func(net.Conn, []string, *ClientCtx) CommandResult // bool 表示是否是写命令传播给replica

func init() {
	routs = map[string]func(net.Conn, []string, *ClientCtx) CommandResult {
	"PING": cmdPING,
	"ECHO": cmdECHO,
	"SET":  cmdSET,
	"GET":  cmdGET,
	"RPUSH": cmdRPUSH,
	"LRANGE": cmdLRANGE,
	"LPUSH": cmdLPUSH,
	"LLEN": cmdLLEN,
	"LPOP": cmdLPOP,
	"BLPOP": cmdBLPOP,
	"TYPE": cmdTYPE,
	"XADD": cmdXADD,
	"XRANGE": cmdXRANGE,
	"XREAD": cmdXREAD,
	"INCR": cmdINCR,
	"MULTI": cmdMULTI,
	"EXEC": cmdEXEC,
	"DISCARD": cmdDISCARD,
	"INFO": cmdINFO,
	"REPLCONF": cmdREPLCONF,
	"PSYNC": cmdPSYNC,
	"WAIT": cmdWAIT,
	"CONFIG": cmdCONFIG,
	"KEYS": cmdKEYS,
	}
}
    