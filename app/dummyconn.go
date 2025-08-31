package main

import (
    "io"
    "net"
    "time"
)

// DummyConn 是一个实现了 net.Conn 的“黑洞连接”
// 所有写操作都会被丢弃，不会真正发送
type DummyConn struct{}

func (d *DummyConn) Read(b []byte) (int, error)         { return 0, io.EOF }
func (d *DummyConn) Write(b []byte) (int, error)        { return len(b), nil }
func (d *DummyConn) Close() error                       { return nil }
func (d *DummyConn) LocalAddr() net.Addr                { return nil }
func (d *DummyConn) RemoteAddr() net.Addr               { return nil }
func (d *DummyConn) SetDeadline(t time.Time) error      { return nil }
func (d *DummyConn) SetReadDeadline(t time.Time) error  { return nil }
func (d *DummyConn) SetWriteDeadline(t time.Time) error { return nil }