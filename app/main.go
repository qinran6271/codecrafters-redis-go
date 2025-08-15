package main

import (
	"bufio" // For easily reading commands from the client with buffered input.
	"fmt" // For printing debug messages or formatting output.
	"net" // For creating a TCP server and accepting client connections.
	"strings" // For processing and parsing command strings.
	"os"  // For handling errors and exiting the program gracefully.
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
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
			// if err.Error() == "EOF" {
			// 	fmt.Println("Client disconnected")
			// 	return // Exit the loop if the client disconnects
			// }
			writeError(conn, fmt.Sprintf("error reading command: %v", err))
			return
		}
		if len(args) == 0 { 
			fmt.Println("Received empty command, skipping...")
			continue  // Skip empty commands
		} 
		
		if cmd, ok := routs[strings.ToUpper(args[0])]; ok {
			cmd(conn, args) // Call the command handler
		} else {
			writeError(conn, fmt.Sprintf("unknown command '%s'", args[0]))
		}

	}

}

