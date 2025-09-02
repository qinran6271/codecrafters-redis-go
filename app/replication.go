package main

import (
    "crypto/rand"
    "fmt"
    "strconv"
    "strings"
	"os"
	"net"
	"encoding/base64"
	"sync"
	"bufio"
	"io"
)

const replidChars = "abcdefghijklmnopqrstuvwxyz0123456789"

var masterReplId = generateReplId()
var masterReplOffset = 0
var masterHost string
var masterPort int

var (
    replicaConns   []net.Conn // 保存所有 replica 的连接
    replicaConnsMu sync.RWMutex // 读写分离：读多写少时效率更高
)

var emptyRdbDump []byte
func init() {
    // 这是官方提供的空 RDB 文件的 base64
    data := "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog=="
    decoded, err := base64.StdEncoding.DecodeString(data)
    if err != nil {
        panic(err)
    }
    emptyRdbDump = decoded
}

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

	// 1. 和 master 建立 TCP 连接
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Println("Failed to connect to master:", err)
		return
	}
	fmt.Println("Connected to master at", addr)

	// 2. 进行复制握手（这一步要用真实 conn 回复 master）
    reader, ctx, err := doHandshakeWithMaster(conn, replicaPort)
    if err != nil {
        fmt.Println("Handshake failed:", err)
        return
    }

	//握手成功后处理rdb
	if err := consumeRDB(reader); err != nil {
    fmt.Println("Error reading RDB:", err)
    return
	}

	// 3. 握手完成后，进入 propagation 阶段 → 启动 goroutine 不断接收 master 的命令
	// go func() {

		for {
			args, consumed, err := readArray(reader)
			if err != nil {
				if err == io.EOF {
					fmt.Println("Master closed connection")
					return
				}
				fmt.Println("Error reading from master:", err)
				return
			}
			if len(args) == 0 {
				continue
			}

			cmd := strings.ToUpper(args[0])
			// ⚡ 处理 REPLCONF GETACK & PING
			if handled := processReplicaCommand(conn, cmd, args, consumed, ctx); handled {
				continue
			}

			if handler, ok := routs[cmd]; ok {
				dummy := &DummyConn{}
				handler(dummy, args, ctx) // 其他 propagate 的命令只更新 DB，不回 master
                
				fmt.Printf("[replica] applied propagated command: %v\n", args)
			} else {
				fmt.Printf("[replica] unknown propagated command: %v\n", args)
			}
			ctx.offset += int64(consumed)
		}
	// }()
}



func doHandshakeWithMaster(conn net.Conn, replicaPort int) (*bufio.Reader, *ClientCtx, error) {
    reader := bufio.NewReader(conn)
	// handshake 三部曲
    // 第一步：发送 PING
    fmt.Fprint(conn, "*1\r\n$4\r\nPING\r\n")
    line, _ := readResponse(reader) // 用同一个 reader
    fmt.Println("Master replied:", line)

    // 第二步：发送两次 REPLCONF 命令
	// 1) REPLCONF listening-port <replicaPort>
    portStr := strconv.Itoa(replicaPort)
    msg1 := fmt.Sprintf("*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%s\r\n",
        len(portStr), portStr)
    fmt.Fprint(conn, msg1)
    line, _ = readResponse(reader)
    fmt.Println("Master replied:", line)

    // 2) REPLCONF capa psync2
    msg2 := "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n"
    fmt.Fprint(conn, msg2)
    line, _ = readResponse(reader)
    fmt.Println("Master replied:", line)

	// 第三步：发送 PSYNC <master_replid> -1
    msg3 := "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"
    fmt.Fprint(conn, msg3)

	// 读 PSYNC 回
    // 1）读 FULLRESYNC 回复
    line, _ = readLine(reader) // +FULLRESYNC ...
    fmt.Println("Master replied:", line)

	// 解析 FULLRESYNC 回复
    // 6. 消费 RDB bulk string
	// if err := readRDBDump(reader); err != nil {
	// 	fmt.Println("Error reading RDB:", err)
	// 	return nil, nil, err
	// }
    // 返回 reader 和 ctx
    ctx := getClientCtx(conn)
    ctx.isReplica = true
    return reader, ctx, nil
}

func consumeRDB(reader *bufio.Reader) error {
    return readRDBDump(reader)
}


func readResponse(r *bufio.Reader) (string, error) {
    line, err := readLine(r)
    if err != nil {
        return "", err
    }
    return line, nil
}


// 添加新的 replica 连接
func addReplicaConn(conn net.Conn) {
    replicaConnsMu.Lock()
    defer replicaConnsMu.Unlock()
    replicaConns = append(replicaConns, conn)
}

func replicaCount() int {
	replicaConnsMu.Lock()
	defer replicaConnsMu.Unlock()
	return len(replicaConns)
}
// 移除失效的 replica 连接
func removeReplicaConn(bad net.Conn) {
    replicaConnsMu.Lock()
    defer replicaConnsMu.Unlock()
    for i, c := range replicaConns {
        if c == bad {
            replicaConns = append(replicaConns[:i], replicaConns[i+1:]...)
            break
        }
    }
}

// 获取 snapshot（避免遍历时长时间持锁）
func snapshotReplicaConns() []net.Conn {
    replicaConnsMu.RLock()
    defer replicaConnsMu.RUnlock()
    return append([]net.Conn(nil), replicaConns...)
}

// 把命令转发给所有已连接的 replicas
func propagateToReplicas(args []string) {
    resp := buildRESPArray(args)

    // 拷贝一份 snapshot，避免在锁里执行 I/O
    conns := snapshotReplicaConns()

    for _, rconn := range conns {
        _, err := rconn.Write(resp)
        if err != nil {
            fmt.Println("Error propagating to replica:", err)
            rconn.Close()
            removeReplicaConn(rconn) // 出错时从全局列表移除
        }
    }
}

// 把 args 转成 RESP2 Array 格式，比如 ["SET", "foo", "bar"]
// → *3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
func buildRESPArray(args []string) []byte {
    var sb strings.Builder
    sb.WriteString(fmt.Sprintf("*%d\r\n", len(args)))
    for _, arg := range args {
        sb.WriteString(fmt.Sprintf("$%d\r\n", len(arg)))
        sb.WriteString(arg)
        sb.WriteString("\r\n")
    }
    return []byte(sb.String())
}


// 处理 FULLRESYNC 后读取并丢弃 RDB 文件
func readRDBDump(r *bufio.Reader) error {
    // 读头: "$88\r\n"
    head, err := readLine(r)
    if err != nil {
        return err
    }
    if !strings.HasPrefix(head, "$") {
        return fmt.Errorf("expected bulk string, got %q", head)
    }

    // 解析长度
    length, err := strconv.Atoi(strings.TrimSpace(head[1:]))
    if err != nil {
        return fmt.Errorf("bad RDB length: %q", head)
    }

    // 只读 length 个字节（payload，包括末尾 CRLF）
    buf := make([]byte, length)
    if _, err := io.ReadFull(r, buf); err != nil {
        return fmt.Errorf("failed to read RDB payload: %v", err)
    }

    fmt.Printf("[replica] consumed RDB snapshot (%d bytes)\n", length)
    return nil
}

