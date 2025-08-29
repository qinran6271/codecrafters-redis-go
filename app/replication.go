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
	pingMsg := "*1\r\n$4\r\nPING\r\n"
    fmt.Fprint(conn, pingMsg)
	readResponse(conn) // 应该是 "+PONG"

	// 第二步：发送两次 REPLCONF 命令
	// 1) REPLCONF listening-port <replicaPort>
	portStr := strconv.Itoa(replicaPort)
	replconfPortMsg := fmt.Sprintf(
		"*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n",
		len(portStr), portStr,
	)
	fmt.Fprint(conn, replconfPortMsg)
	readResponse(conn) // 应该是 "+OK"
	// 2) REPLCONF capa psync2
	replconfCapaMsg := "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
    fmt.Fprint(conn, replconfCapaMsg)
    readResponse(conn) // 应该是 "+OK"

	// 第三步：发送 PSYNC <master_replid> -1
	psyncMsg := "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
	fmt.Fprint(conn, psyncMsg)

	reply := readResponse(conn) // master 应该在后续阶段返回 +FULLRESYNC ...
	fmt.Println("Master replied:", reply)

}


func readResponse(conn net.Conn) string {
    buf := make([]byte, 1024)
    n, err := conn.Read(buf)
    if err != nil {
        fmt.Println("Error reading from master:", err)
        return ""
    }
    reply := string(buf[:n])
    fmt.Print("Master replied:", reply)
    return reply
}

