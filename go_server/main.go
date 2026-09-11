package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

type KVRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type commandClient interface {
	execute(context.Context, rpcRequest) (rpcResponse, error)
}

func envInt(name string, fallback, minimum, maximum int) (int, error) {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < minimum || value > maximum {
		return 0, fmt.Errorf("invalid %s", name)
	}
	return value, nil
}

func envString(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func newKVHandler(client commandClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var request rpcRequest
		switch r.Method {
		case http.MethodPost:
			// JSON escaping can expand one data byte into six source bytes.
			r.Body = http.MaxBytesReader(w, r.Body, 6*(maxKeySize+maxValueSize)+1024)
			var body KVRequest
			decoder := json.NewDecoder(r.Body)
			decoder.DisallowUnknownFields()
			err := decoder.Decode(&body)
			if err == nil {
				var extra any
				if next := decoder.Decode(&extra); next != io.EOF {
					if next == nil {
						next = errors.New("multiple JSON values")
					}
					err = next
				}
			}
			if err != nil {
				var oversized *http.MaxBytesError
				if errors.As(err, &oversized) {
					http.Error(w, "Request body is too large", http.StatusRequestEntityTooLarge)
				} else {
					http.Error(w, "Invalid JSON body", http.StatusBadRequest)
				}
				return
			}
			request = rpcRequest{op: opPut, key: body.Key, value: body.Value}
		case http.MethodGet:
			request = rpcRequest{op: opGet, key: r.URL.Query().Get("key")}
		case http.MethodDelete:
			request = rpcRequest{op: opDelete, key: r.URL.Query().Get("key")}
		default:
			w.Header().Set("Allow", "POST, GET, DELETE")
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if len(request.key) == 0 || len(request.key) > maxKeySize {
			http.Error(w, "Key must contain 1 to 4096 bytes", http.StatusBadRequest)
			return
		}
		if len(request.value) > maxValueSize {
			http.Error(w, "Value exceeds 1 MiB", http.StatusRequestEntityTooLarge)
			return
		}
		response, err := client.execute(r.Context(), request)
		if err != nil {
			code := http.StatusBadGateway
			if errors.Is(err, context.DeadlineExceeded) {
				code = http.StatusGatewayTimeout
			} else if errors.Is(err, errClientClosed) {
				code = http.StatusServiceUnavailable
			}
			http.Error(w, "Backend request failed; a write may already have executed", code)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		switch response.status {
		case statusOK:
			if request.op == opGet {
				http.Error(w, "Unexpected backend response", http.StatusBadGateway)
				return
			}
			_, _ = io.WriteString(w, "OK\n")
		case statusValue:
			if request.op != opGet {
				http.Error(w, "Unexpected backend response", http.StatusBadGateway)
				return
			}
			// Preserve every value byte; the final newline belongs to the HTTP
			// envelope, as in the original text API.
			_, _ = io.WriteString(w, "VALUE "+response.value+"\n")
		case statusMiss:
			if request.op == opPut {
				http.Error(w, "Unexpected backend response", http.StatusBadGateway)
				return
			}
			w.WriteHeader(http.StatusNotFound)
			_, _ = io.WriteString(w, "NOT_FOUND\n")
		case statusBad:
			http.Error(w, "Backend rejected the request", http.StatusBadRequest)
		case statusIO, statusBusy:
			http.Error(w, "Storage unavailable", http.StatusServiceUnavailable)
		default:
			http.Error(w, "Unexpected backend response", http.StatusBadGateway)
		}
	}
}

func run() error {
	size, err := envInt("MINIKV_RPC_POOL_SIZE", 64, 1, 65536)
	if err != nil {
		return err
	}
	timeoutMS, err := envInt("MINIKV_RPC_TIMEOUT_MS", 2000, 1, 60000)
	if err != nil {
		return err
	}
	client := newRPCClient(envString("MINIKV_ENGINE_ADDR", "127.0.0.1:9090"), size, time.Duration(timeoutMS)*time.Millisecond)
	defer client.Close()
	mux := http.NewServeMux()
	mux.HandleFunc("/kv", newKVHandler(client))
	server := &http.Server{
		Addr:              envString("MINIKV_HTTP_ADDR", ":8080"),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      time.Duration(timeoutMS)*time.Millisecond + 5*time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    64 * 1024,
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	done := make(chan error, 1)
	go func() {
		log.Printf("MiniKV HTTP gateway listening on %s", server.Addr)
		done <- server.ListenAndServe()
	}()
	select {
	case err := <-done:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			_ = server.Close()
			return err
		}
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
