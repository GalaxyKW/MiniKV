package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

const (
	maxKeySize   = 4096
	maxValueSize = 1024 * 1024
	opPut        = 1
	opGet        = 2
	opDelete     = 3
	statusOK     = 0
	statusValue  = 1
	statusMiss   = 2
	statusBad    = 3
	statusIO     = 4
	statusBusy   = 5
)

var errClientClosed = errors.New("RPC client is closed")

type rpcRequest struct {
	op         byte
	key, value string
}

type rpcResponse struct {
	status byte
	value  string
}

type rpcConn struct {
	net.Conn
	idleSince time.Time
}

type rpcClient struct {
	mu          sync.Mutex
	closed      bool
	connections map[*rpcConn]struct{}
	idle        chan *rpcConn
	slots       chan struct{}
	done        chan struct{}
	timeout     time.Duration
	idleTimeout time.Duration
	dial        func(context.Context) (net.Conn, error)
}

func newRPCClient(address string, size int, timeout time.Duration) *rpcClient {
	dialer := &net.Dialer{Timeout: 500 * time.Millisecond, KeepAlive: 30 * time.Second}
	return &rpcClient{
		connections: make(map[*rpcConn]struct{}),
		idle:        make(chan *rpcConn, size),
		slots:       make(chan struct{}, size),
		done:        make(chan struct{}),
		timeout:     timeout,
		idleTimeout: 15 * time.Second,
		dial: func(ctx context.Context) (net.Conn, error) {
			return dialer.DialContext(ctx, "tcp", address)
		},
	}
}

func (c *rpcClient) acquire(ctx context.Context) (*rpcConn, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, errClientClosed
	case c.slots <- struct{}{}:
	}
	// A slot covers both dialing and an in-flight RPC, so idle + active
	// connections can never exceed the configured capacity.
	if err := ctx.Err(); err != nil {
		<-c.slots
		return nil, err
	}
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		<-c.slots
		return nil, errClientClosed
	}
	for {
		select {
		case conn := <-c.idle:
			if time.Since(conn.idleSince) < c.idleTimeout {
				return conn, nil
			}
			c.discard(conn)
		default:
			conn, err := c.dial(ctx)
			if err != nil {
				<-c.slots
				return nil, err
			}
			result := &rpcConn{Conn: conn}
			c.mu.Lock()
			if c.closed {
				c.mu.Unlock()
				_ = conn.Close()
				<-c.slots
				return nil, errClientClosed
			}
			c.connections[result] = struct{}{}
			c.mu.Unlock()
			return result, nil
		}
	}
}

func (c *rpcClient) discard(conn *rpcConn) {
	c.mu.Lock()
	delete(c.connections, conn)
	c.mu.Unlock()
	_ = conn.Close()
}

func (c *rpcClient) release(conn *rpcConn, healthy bool) {
	defer func() { <-c.slots }()
	c.mu.Lock()
	defer c.mu.Unlock()
	if healthy && !c.closed {
		conn.idleSince = time.Now()
		select {
		case c.idle <- conn:
			return
		default:
		}
	}
	delete(c.connections, conn)
	_ = conn.Close()
}

func (c *rpcClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	c.closed = true
	close(c.done)
	for conn := range c.connections {
		_ = conn.Close()
		delete(c.connections, conn)
	}
}

func encodeRequest(request rpcRequest) ([]byte, error) {
	if len(request.key) == 0 || len(request.key) > maxKeySize || len(request.value) > maxValueSize ||
		(request.op != opPut && request.op != opGet && request.op != opDelete) ||
		(request.op != opPut && request.value != "") {
		return nil, errors.New("invalid RPC operation or key/value length")
	}
	bytes := make([]byte, 16+len(request.key)+len(request.value))
	copy(bytes, "MKV1")
	bytes[4] = request.op
	binary.BigEndian.PutUint32(bytes[8:12], uint32(len(request.key)))
	binary.BigEndian.PutUint32(bytes[12:16], uint32(len(request.value)))
	copy(bytes[16:], request.key)
	copy(bytes[16+len(request.key):], request.value)
	return bytes, nil
}

func readResponse(reader io.Reader) (rpcResponse, error) {
	var header [12]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return rpcResponse{}, err
	}
	size := binary.BigEndian.Uint32(header[8:12])
	if string(header[:4]) != "MKR1" || header[4] > statusBusy || header[5] != 0 || header[6] != 0 || header[7] != 0 ||
		size > maxValueSize || ((header[4] == statusOK || header[4] == statusMiss) && size != 0) {
		return rpcResponse{}, errors.New("invalid RPC response frame")
	}
	value := make([]byte, size)
	if _, err := io.ReadFull(reader, value); err != nil {
		return rpcResponse{}, err
	}
	return rpcResponse{status: header[4], value: string(value)}, nil
}

func writeAll(writer io.Writer, bytes []byte) error {
	for len(bytes) > 0 {
		n, err := writer.Write(bytes)
		if err != nil {
			return err
		}
		if n <= 0 {
			return io.ErrShortWrite
		}
		bytes = bytes[n:]
	}
	return nil
}

func (c *rpcClient) exchange(ctx context.Context, bytes []byte) (response rpcResponse, err error) {
	conn, err := c.acquire(ctx)
	if err != nil {
		return rpcResponse{}, err
	}
	healthy := false
	deadline, _ := ctx.Deadline()
	if err := conn.SetDeadline(deadline); err != nil {
		c.release(conn, false)
		return rpcResponse{}, err
	}
	// Wait for the cancellation callback before returning a connection to the
	// pool; a late callback must not change the next borrower's deadline.
	cancelFinished := make(chan struct{})
	stopCancel := context.AfterFunc(ctx, func() {
		_ = conn.SetDeadline(time.Now())
		close(cancelFinished)
	})
	defer func() {
		if !stopCancel() {
			<-cancelFinished
		}
		if ctx.Err() != nil {
			err = ctx.Err()
			healthy = false
		} else if !deadline.IsZero() && !time.Now().Before(deadline) {
			// The socket deadline can fire before the context timer's callback.
			var timeout net.Error
			if errors.As(err, &timeout) && timeout.Timeout() {
				err = context.DeadlineExceeded
				healthy = false
			}
		}
		c.release(conn, healthy)
	}()
	if err := writeAll(conn, bytes); err != nil {
		return rpcResponse{}, err
	}
	response, err = readResponse(conn)
	healthy = err == nil
	return response, err
}

func (c *rpcClient) execute(parent context.Context, request rpcRequest) (rpcResponse, error) {
	bytes, err := encodeRequest(request)
	if err != nil {
		return rpcResponse{}, err
	}
	ctx, cancel := context.WithTimeout(parent, c.timeout)
	defer cancel()
	response, err := c.exchange(ctx, bytes)
	// Replaying a timed-out write can overwrite a newer value or repeat a
	// deletion. Only reads may retry, within the original request deadline.
	if err != nil && request.op == opGet && ctx.Err() == nil &&
		!errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, errClientClosed) {
		response, err = c.exchange(ctx, bytes)
	}
	if err != nil {
		return rpcResponse{}, fmt.Errorf("RPC failed: %w", err)
	}
	return response, nil
}
