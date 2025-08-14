package main

import (
    "bufio"
    "fmt"
    "io"
    "strconv"
    "strings"
)

// 读取一行，去掉结尾的 CRLF
func readLine(r *bufio.Reader) (string, error) {
	s, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(s, "\r\n"), nil
}

// 读取 RESP Bulk String：$<len>\r\n<payload>\r\n
func readBulk(r *bufio.Reader) (string, error) {
	lenLine, err := readLine(r) // 例如 "$3"
	if err != nil {
		return "", err
	}
	if !strings.HasPrefix(lenLine, "$") {
		return "", fmt.Errorf("expect $, got %q", lenLine)
	}
	n, err := strconv.Atoi(lenLine[1:])
	if err != nil || n < 0 {
		return "", fmt.Errorf("bad bulk len: %v", lenLine)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return "", err
	}
	// 丢弃结尾的 \r\n
	if _, err := r.ReadByte(); err != nil { return "", err }
	if _, err := r.ReadByte(); err != nil { return "", err }
	return string(buf), nil
}

// 读取 RESP Array：*<n>\r\n 后跟 n 个 Bulk
func readArray(r *bufio.Reader) ([]string, error) {
	head, err := readLine(r) // 例如 "*2"
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(head, "*") {
		return nil, fmt.Errorf("expect *, got %q", head)
	}
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


