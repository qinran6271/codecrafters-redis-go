package main

import (
	"bufio" // For easily reading commands from the client with buffered input.
	"fmt" // For printing debug messages or formatting output.
	"net" // For creating a TCP server and accepting client connections.
	"strings" // For processing and parsing command strings.
	"os"  // For handling errors and exiting the program gracefully.
	"io" // For reading input from the client.
	"flag" // For parsing command-line flags (like port number).
	// "strconv" // For converting strings to integers (like port number).
	"path/filepath" // For handling file paths (like RDB file path).
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit


var configDir string
var configDBFilename string

var role = "master" // "master" or "replica"

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// 定义一个参数 port，默认 6379
	port := flag.Int("port", 6379, "Port to listen on")
	replicaof := flag.String("replicaof", "", "host port of master")
    dir := flag.String("dir", "/tmp", "Directory to store RDB files")
    dbfilename := flag.String("dbfilename", "dump.rdb", "RDB file name")
	flag.Parse()

	// ⚠️ 保存到全局变量
	configDir = *dir
	configDBFilename = *dbfilename
	// 拼接路径并加载
	rdbPath := filepath.Join(configDir, configDBFilename)
	loadRDB(rdbPath)


    // 保存到全局变量，方便 CONFIG 命令读取
    configDir = *dir
    configDBFilename = *dbfilename

	// 如果有传 --replicaof，就切换角色
	// ./your_program.sh --port 6380 --replicaof "localhost 6379"
	if *replicaof != "" {
		role = "slave"
		parseReplicaof(*replicaof)
		// 启动一个 goroutine 去连接 master
		go connectToMaster(masterHost, masterPort, *port)
	}

	addr := fmt.Sprintf(":%d", *port)

	l, err := net.Listen("tcp",addr)
	if err != nil {
		fmt.Println("Failed to bind to port", *port, ":", err.Error())
		panic(err)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			continue
		}
		go handleConnection(conn) // Handle each connection in a separate goroutine
	}
	
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	// Read commands from the client
	r := bufio.NewReader(conn)
	for {
		args, consumed, err := readArray(r)

        if err != nil {
            if err == io.EOF {
                return // client closed connection
            }
            writeError(conn, fmt.Sprintf("ERR error reading command: %v", err))
            return
        }

		if len(args) == 0 { 
			fmt.Println("Received empty command, skipping...")
			continue  // Skip empty commands
		} 

		cmd := strings.ToUpper(args[0])
		ctx := getClientCtx(conn)

		// ⚡ 把 replica 特殊处理抽到一个函数
		if handled := processReplicaCommand(conn, cmd, args, consumed, ctx); handled {
			continue
		}

		// === 事务模式拦截 ===
		if ctx.tx.inMulti && cmd != "EXEC" && cmd != "DISCARD" && cmd != "MULTI" {
			ctx.tx.queue = append(ctx.tx.queue, args)
			if !ctx.isReplica {
				writeSimple(conn, "QUEUED")
			}
			ctx.offset += int64(consumed)
			continue
		}
		
		// === 正常执行命令 ===
		if handler, ok := routs[cmd]; ok {
			res := handler(conn, args, ctx) // Call the command handler

			// 如果当前是 master，且命令是写命令，且不是来自 replica 的连接，就传播给所有 replica
			if role == "master"  && res.IsWrite && !ctx.isReplica {
				 propagateToReplicas(args)
			}

		} else {
			if !ctx.isReplica {
			writeError(conn, fmt.Sprintf("unknown command '%s'", args[0]))
			}
		}
		fmt.Printf("args: %#v\n", args) // Debug 
		ctx.offset += int64(consumed)
	}

}

