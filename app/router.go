package main

import (
	"net"
)

var routs = map[string]func(net.Conn, []string){
	"PING": cmdPING,
	"ECHO": cmdECHO,
}