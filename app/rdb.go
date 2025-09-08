package main

import (
    "encoding/binary"
    "fmt"
    "io"
    "os"
	"time"
)

// 加载 RDB 文件
func loadRDB(path string) {
	f, err := os.Open(path)
	if err != nil {
		fmt.Println("RDB file not found, starting with empty DB")
		return
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		fmt.Println("Failed to read RDB:", err)
		return
	}

	// 1. 校验 header
	if len(data) < 9 || string(data[:5]) != "REDIS" {
		fmt.Println("Invalid RDB header")
		return
	}

	// 从 header 之后开始
	i := 9
	var expireTime time.Time

	for i < len(data) {
		b := data[i]
		i++

		switch b {
		case 0xFE: // Database selector
			_, n := parseLength(data[i:])
			i += n

		case 0xFB: // Hash table sizes
			_, n1 := parseLength(data[i:])
			i += n1
			_, n2 := parseLength(data[i:])
			i += n2

		case 0xFC: // Expire time in ms
			ts := binary.LittleEndian.Uint64(data[i : i+8])
			expireTime = time.UnixMilli(int64(ts))
			i += 8

		case 0xFD: // Expire time in s
			ts := binary.LittleEndian.Uint32(data[i : i+4])
			expireTime = time.Unix(int64(ts), 0)
			i += 4

		case 0x00: // Value type: string
			key, n1 := parseString(data[i:])
			i += n1
			val, n2 := parseString(data[i:])
			i += n2

			kv.Lock()
			kv.m[key] = entry{
				kind:    kindString,
				s:       val,
				expires: expireTime,
			}
			kv.Unlock()

			// reset expireTime，避免影响下一个 key
			expireTime = time.Time{}

		case 0xFF: // End of file
			return

		default:
			// 其他类型暂时不处理
		}
	}
}

// 解析 length 编码
func parseLength(data []byte) (int, int) {
	first := data[0]
	mode := (first & 0xC0) >> 6
	if mode == 0 { // 6 bits
		return int(first & 0x3F), 1
	} else if mode == 1 { // 14 bits
		return int(first&0x3F)<<8 | int(data[1]), 2
	} else if mode == 2 { // 32-bit
		return int(binary.BigEndian.Uint32(data[1:5])), 5
	}
	return 0, 1
}

// 解析 string 编码
func parseString(data []byte) (string, int) {
	length, n := parseLength(data)
	start := n
	end := n + length
	return string(data[start:end]), end
}
