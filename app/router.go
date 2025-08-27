package main

import (
	"net"
)

var routs = map[string]func(net.Conn, []string){
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
}