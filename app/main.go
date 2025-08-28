package main

import (
	"bufio" // For easily reading commands from the client with buffered input.
	"fmt" // For printing debug messages or formatting output.
	"net" // For creating a TCP server and accepting client connections.
	"strings" // For processing and parsing command strings.
	"os"  // For handling errors and exiting the program gracefully.
	"io" // For reading input from the client.
	"flag" // For parsing command-line flags (like port number).
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// 定义一个参数 port，默认 6379
	port := flag.Int("port", 6379, "Port to listen on")
	flag.Parse()

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
		args, err := readArray(r)

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

		// === 新增：事务模式拦截 ===
		if state, ok := transactions[conn]; ok && state.inMulti {
			if cmd != "EXEC" && cmd != "DISCARD" && cmd != "MULTI" {
				state.queue = append(state.queue, args)
				writeSimple(conn, "QUEUED")
				continue
			}
		}
		
		if handler, ok := routs[cmd]; ok {
			handler(conn, args) // Call the command handler
		} else {
			writeError(conn, fmt.Sprintf("unknown command '%s'", args[0]))
		}
		fmt.Printf("args: %#v\n", args) // Debug 输出
	}

}


