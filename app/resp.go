package main

import (
    "bufio"
    "fmt"
    "io"
    "strconv"
    "strings"
    "net"
)

// // 读取一行，去掉结尾的 CRLF
// func readLine(r *bufio.Reader) (string, error) {
// 	s, err := r.ReadString('\n')
// 	if err != nil {
// 		return "", err
// 	}
// 	return strings.TrimRight(s, "\r\n"), nil
// }

// // 读取 RESP Bulk String：$<len>\r\n<payload>\r\n
// func readBulk(r *bufio.Reader) (string, error) {
// 	lenLine, err := readLine(r) // 例如 "$3"
// 	if err != nil {
// 		return "", err
// 	}
// 	if !strings.HasPrefix(lenLine, "$") {
// 		return "", fmt.Errorf("expect $, got %q", lenLine)
// 	}
// 	n, err := strconv.Atoi(lenLine[1:])
// 	if err != nil || n < 0 {
// 		return "", fmt.Errorf("bad bulk len: %v", lenLine)
// 	}
// 	buf := make([]byte, n)
// 	if _, err := io.ReadFull(r, buf); err != nil {
// 		return "", err
// 	}
// 	// 丢弃结尾的 \r\n
// 	if _, err := r.ReadByte(); err != nil { return "", err }
// 	if _, err := r.ReadByte(); err != nil { return "", err }
// 	return string(buf), nil
// }

// // 读取 RESP Array：*<n>\r\n 后跟 n 个 Bulk
// func readArray(r *bufio.Reader) ([]string, error) {
// 	head, err := readLine(r) // 例如 "*2"
// 	if err != nil {
// 		return nil, err
// 	}
// 	if !strings.HasPrefix(head, "*") {
// 		return nil, fmt.Errorf("expect *, got %q", head)
// 	}
// 	n, err := strconv.Atoi(head[1:])
// 	if err != nil || n < 0 {
// 		return nil, fmt.Errorf("bad array len: %v", head)
// 	}
// 	args := make([]string, 0, n)
// 	for i := 0; i < n; i++ {
// 		s, err := readBulk(r)
// 		if err != nil {
// 			return nil, err
// 		}
// 		args = append(args, s)
// 	}
// 	return args, nil
// }

// 读取一行，必须以 \r\n 结尾
func readLine(r *bufio.Reader) (string, error) {
	s, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	// 检查协议，必须 \r\n 结尾
	if len(s) < 2 || s[len(s)-2] != '\r' || s[len(s)-1] != '\n' {
		return "", fmt.Errorf("protocol error: %q", s)
	}
	return s[:len(s)-2], nil
}

// 读取 Bulk String
func readBulk(r *bufio.Reader) (string, error) {
	lenLine, err := readLine(r) // e.g. "$3"
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(lenLine, "$") {
		return "", fmt.Errorf("expect $, got %q", lenLine)
	}

	// Bulk string 长度
	n, err := strconv.Atoi(lenLine[1:])
	if err != nil || n < 0 {
		return "", fmt.Errorf("bad bulk len: %v", lenLine)
	}

	// 读出内容
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}

	// 丢掉结尾的 \r\n
	if _, err := r.ReadByte(); err != nil {
		return "", err
	}
	if _, err := r.ReadByte(); err != nil {
		return "", err
	}

	return string(buf), nil
}

// 读取数组（命令参数）
func readArray(r *bufio.Reader) ([]string, error) {
	head, err := readLine(r) // e.g. "*3"
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(head, "*") {
		return nil, fmt.Errorf("expect *, got %q", head)
	}

	// 数组长度
	n, err := strconv.Atoi(head[1:])
	if err != nil || n < 0 {
		return nil, fmt.Errorf("bad array len: %v", head)
	}

	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		s, err := readBulk(r)
		if err != nil {
			return nil, err
		}
		args = append(args, s)
	}
	return args, nil
}

func readRESP(r *bufio.Reader) ([]string, error) {
    prefix, err := r.ReadByte()
    if err != nil {
        return nil, err
    }

    switch prefix {
    case '*': // Array
        line, _ := r.ReadString('\n')
        n, _ := strconv.Atoi(strings.TrimSpace(line))
        result := make([]string, 0, n)
        for i := 0; i < n; i++ {
            elem, err := readRESP(r) // 递归解析子元素
            if err != nil {
                return nil, err
            }
            result = append(result, elem...)
        }
        return result, nil

    case '$': // Bulk string
        line, _ := r.ReadString('\n')
        length, _ := strconv.Atoi(strings.TrimSpace(line))
        buf := make([]byte, length+2) // 包含 \r\n
        io.ReadFull(r, buf)
        return []string{string(buf[:length])}, nil

    case '+', '-', ':': // simple string / error / integer
        line, _ := r.ReadString('\n')
        return []string{strings.TrimSuffix(line, "\r\n")}, nil

    default:
        return nil, fmt.Errorf("expect RESP type, got %q", prefix)
    }
}



// Simple RESP String：+<payload>\r\n
func writeSimple(conn net.Conn, s string) {
    fmt.Fprintf(conn, "+%s\r\n", s)
}

// Bulk RESP String：$<len>\r\n<payload>\r\n
func writeBulk(conn net.Conn, s string) {
    fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(s), s)
}

// Error RESP String：-<error message>\r\n
func writeError(conn net.Conn, errMsg string) {
    fmt.Fprintf(conn, "-%s\r\n", errMsg)
}

// Null Bulk RESP String：$-1\r\n
func writeNullBulk(conn net.Conn) {
    conn.Write([]byte("$-1\r\n"))
}

// Integer RESP String：:<number>\r\n
func writeInteger(conn net.Conn, n int64) {
    fmt.Fprintf(conn, ":%d\r\n", n)
}

// Bulk RESP String with length: $<len>\r\n<payload>\r\n
func writeBulkString(conn net.Conn, s string) {
    fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(s), s)
}

// RESP Array header: *<n>\r\n
func writeArrayHeader(conn net.Conn, n int) {
    fmt.Fprintf(conn, "*%d\r\n", n)
}

func writeNullArray(conn net.Conn) {
    fmt.Fprint(conn, "*-1\r\n")
}

// writeArrayBulk 写一个由纯 Bulk String 组成的 RESP Array
// 每个元素都会被编码为 $<len>\r\n<payload>\r\n
func writeArrayBulk(conn net.Conn, elems ...string) error {
    // 先写数组头：*<n>\r\n
    if _, err := fmt.Fprintf(conn, "*%d\r\n", len(elems)); err != nil {
        return err
    }

    // 逐个写 bulk string
    for _, e := range elems {
        if _, err := fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(e), e); err != nil {
            return err
        }
    }
    return nil
}
