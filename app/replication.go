package main

import (
    "crypto/rand"
    "fmt"
    "strconv"
    "strings"
	"os"
	"net"
)

const replidChars = "abcdefghijklmnopqrstuvwxyz0123456789"

var masterReplId = generateReplId()
var masterReplOffset = 0
var masterHost string
var masterPort int

func generateReplId() string {
    b := make([]byte, 40)
    _, err := rand.Read(b)
    if err != nil {
        panic(err)
    }
    for i := 0; i < 40; i++ {
        b[i] = replidChars[int(b[i])%len(replidChars)]
    }
    return string(b)
}

func parseReplicaof(replicaof string) {
	parts := strings.Split(replicaof, " ")
	if len(parts) != 2 {
		fmt.Println("Invalid --replicaof argument, must be 'host port'")
		os.Exit(1)
	}
		masterHost = parts[0] // e.g. "localhost"
		var err error
		masterPort, err = strconv.Atoi(parts[1]) // e.g. 6379
		if err != nil {
		fmt.Println("Invalid port in --replicaof argument:", parts[1])
		os.Exit(1)
		}
}

func GetReplicationInfo() string {
    if role == "master" {
        return fmt.Sprintf(
            "# Replication\r\nrole:master\r\nmaster_replid:%s\r\nmaster_repl_offset:%d\r\n",
            masterReplId, masterReplOffset,
        )
    }
    return fmt.Sprintf(
        "# Replication\r\nrole:slave\r\nmaster_host:%s\r\nmaster_port:%d\r\n",
        masterHost, masterPort,
    )
}

func connectToMaster(host string, port int, replicaPort int) {
	addr := fmt.Sprintf("%s:%d", host, port)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Println("Failed to connect to master:", err)
		return
	}
	fmt.Println("Connected to master at", addr)

	// 第一步：发送 PING
    fmt.Fprintf(conn, "*1\r\n$4\r\nPING\r\n")
	readResponse(conn) // 应该是 "+PONG"

	// 第二步：发送两次 REPLCONF 命令
	// 1) REPLCONF listening-port <replicaPort>
    msg := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%d\r\n",
        len(strconv.Itoa(replicaPort)), replicaPort)
    fmt.Fprint(conn, msg)
    readResponse(conn) // 应该是 "+OK
	// 2) REPLCONF capa psync2
    fmt.Fprintf(conn, "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n")
    readResponse(conn) // 应该是 "+OK"

	// 第三步：发送 PSYNC <master_replid> -1
	msg3 := fmt.Sprintf("*3\r\n$5\r\nPSYNC\r\n$40\r\n%s\r\n$2\r\n-1\r\n", masterReplId)
    fmt.Fprint(conn, msg3)
    readResponse(conn) // 下一关会检查 +FULLRESYNC ...
}


func readResponse(conn net.Conn) {
    buf := make([]byte, 1024)
    n, _ := conn.Read(buf)
    fmt.Println("Master replied:", string(buf[:n]))
}

