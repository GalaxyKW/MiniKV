package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

func readTestRequest(reader io.Reader) (rpcRequest, error) {
	var header [16]byte
	if _, err := io.ReadFull(reader, header[:]); err != nil {
		return rpcRequest{}, err
	}
	keySize := int(binary.BigEndian.Uint32(header[8:12]))
	valueSize := int(binary.BigEndian.Uint32(header[12:16]))
	if string(header[:4]) != "MKV1" || keySize > maxKeySize || valueSize > maxValueSize {
		return rpcRequest{}, errors.New("invalid request")
	}
	body := make([]byte, keySize+valueSize)
	if _, err := io.ReadFull(reader, body); err != nil {
		return rpcRequest{}, err
	}
	return rpcRequest{op: header[4], key: string(body[:keySize]), value: string(body[keySize:])}, nil
}

func responseFrame(status byte, value string) []byte {
	frame := make([]byte, 12+len(value))
	copy(frame, "MKR1")
	frame[4] = status
	binary.BigEndian.PutUint32(frame[8:12], uint32(len(value)))
	copy(frame[12:], value)
	return frame
}

func TestBinaryRPCAndConnectionReuse(t *testing.T) {
	client := newRPCClient("unused", 1, time.Second)
	defer client.Close()
	key, value := "tenant:1\n\x00", " \nline1\nDEL victim\n\x00\t "
	var dials atomic.Int32
	done := make(chan error, 1)
	client.dial = func(context.Context) (net.Conn, error) {
		dials.Add(1)
		local, remote := net.Pipe()
		go func() {
			defer remote.Close()
			put, err := readTestRequest(remote)
			if err != nil || put.op != opPut || put.key != key || put.value != value {
				done <- errors.New("PUT frame changed data")
				return
			}
			if err = writeAll(remote, responseFrame(statusOK, "")); err != nil {
				done <- err
				return
			}
			get, err := readTestRequest(remote)
			if err != nil || get.op != opGet || get.key != key {
				done <- errors.New("GET frame changed key")
				return
			}
			// Exercise partial response headers and bodies.
			for _, b := range responseFrame(statusValue, value) {
				if err = writeAll(remote, []byte{b}); err != nil {
					done <- err
					return
				}
			}
			done <- nil
		}()
		return local, nil
	}
	if _, err := client.execute(context.Background(), rpcRequest{op: opPut, key: key, value: value}); err != nil {
		t.Fatal(err)
	}
	got, err := client.execute(context.Background(), rpcRequest{op: opGet, key: key})
	if err != nil || got.status != statusValue || got.value != value || dials.Load() != 1 {
		t.Fatalf("round trip/reuse failed: %#v, %v, dials=%d", got, err, dials.Load())
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestPoolCapacityIncludesInFlightConnections(t *testing.T) {
	client := newRPCClient("unused", 2, 3*time.Second)
	defer client.Close()
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	var dials atomic.Int32
	client.dial = func(context.Context) (net.Conn, error) {
		dials.Add(1)
		local, remote := net.Pipe()
		go func() {
			defer remote.Close()
			if _, err := readTestRequest(remote); err != nil {
				return
			}
			started <- struct{}{}
			<-release
			_ = writeAll(remote, responseFrame(statusOK, ""))
		}()
		return local, nil
	}
	finished := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			_, err := client.execute(context.Background(), rpcRequest{op: opPut, key: "key", value: "value"})
			finished <- err
		}()
	}
	<-started
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	_, err := client.execute(ctx, rpcRequest{op: opPut, key: "third"})
	close(release)
	if !errors.Is(err, context.DeadlineExceeded) || dials.Load() != 2 {
		t.Fatalf("pool did not enforce its total cap/deadline: err=%v dials=%d", err, dials.Load())
	}
	for i := 0; i < 2; i++ {
		if err := <-finished; err != nil {
			t.Fatal(err)
		}
	}
}

func TestCancellationInterruptsRPC(t *testing.T) {
	client := newRPCClient("unused", 1, 5*time.Second)
	defer client.Close()
	started := make(chan struct{})
	remoteDone := make(chan struct{})
	client.dial = func(context.Context) (net.Conn, error) {
		local, remote := net.Pipe()
		go func() {
			defer remote.Close()
			defer close(remoteDone)
			_, _ = readTestRequest(remote)
			close(started)
			var one [1]byte
			_, _ = remote.Read(one[:])
		}()
		return local, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := client.execute(ctx, rpcRequest{op: opPut, key: "key"})
		done <- err
	}()
	<-started
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected cancellation, got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancel did not interrupt blocked response read")
	}
	<-remoteDone
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.connections) != 0 || len(client.slots) != 0 {
		t.Fatal("canceled connection/slot leaked")
	}
}

func TestOnlyReadsRetryAfterUncertainOutcome(t *testing.T) {
	for _, operation := range []byte{opPut, opDelete, opGet} {
		t.Run(string(rune('0'+operation)), func(t *testing.T) {
			client := newRPCClient("unused", 1, time.Second)
			defer client.Close()
			var dials atomic.Int32
			client.dial = func(context.Context) (net.Conn, error) {
				attempt := dials.Add(1)
				local, remote := net.Pipe()
				go func() {
					defer remote.Close()
					_, _ = readTestRequest(remote)
					if operation == opGet && attempt == 2 {
						_ = writeAll(remote, responseFrame(statusMiss, ""))
					}
				}()
				return local, nil
			}
			response, err := client.execute(context.Background(), rpcRequest{op: operation, key: "key"})
			if operation == opGet {
				if err != nil || response.status != statusMiss || dials.Load() != 2 {
					t.Fatalf("read retry failed: %v, dials=%d", err, dials.Load())
				}
			} else if err == nil || dials.Load() != 1 {
				t.Fatalf("uncertain write was replayed: err=%v, dials=%d", err, dials.Load())
			}
		})
	}
}

func TestMalformedResponsesAreRejected(t *testing.T) {
	oversized := responseFrame(statusValue, "")
	binary.BigEndian.PutUint32(oversized[8:12], maxValueSize+1)
	badMagic := responseFrame(statusOK, "")
	badMagic[0] = 'X'
	for _, frame := range [][]byte{oversized, badMagic, responseFrame(99, ""), responseFrame(statusOK, "extra"), responseFrame(statusValue, "value")[:14]} {
		if _, err := readResponse(bytes.NewReader(frame)); err == nil {
			t.Fatalf("accepted malformed response: %x", frame)
		}
	}
}

func TestClosedClientUnblocksPoolWaiters(t *testing.T) {
	client := newRPCClient("unused", 1, time.Second)
	client.slots <- struct{}{}
	client.Close()
	if _, err := client.acquire(context.Background()); !errors.Is(err, errClientClosed) {
		t.Fatalf("expected closed-client error, got %v", err)
	}
}

// Model the legal race where the socket deadline fires before the context's
// timer callback has changed Err(). The parent remains cancellable for cleanup.
type delayedDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (ctx delayedDeadlineContext) Deadline() (time.Time, bool) { return ctx.deadline, true }

func TestSocketDeadlineIsReportedAsRequestTimeout(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx := delayedDeadlineContext{Context: parent, deadline: time.Now().Add(-time.Second)}
	client := newRPCClient("unused", 1, time.Second)
	defer client.Close()
	local, remote := net.Pipe()
	defer remote.Close()
	client.dial = func(context.Context) (net.Conn, error) { return local, nil }
	_, err := client.exchange(ctx, []byte("request"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("socket deadline became a generic backend error: %v", err)
	}
}
