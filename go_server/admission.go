package main

import (
	"net/http"
	"sync/atomic"
	"time"
)

type admissionStats struct {
	Capacity      int    `json:"capacity"`
	Inflight      int    `json:"inflight"`
	RejectedTotal uint64 `json:"rejected_total"`
}

type gatewayHTTPStats struct {
	Data  admissionStats `json:"data"`
	Stats admissionStats `json:"stats"`
}

// Admission precedes body decoding and RPC frame allocation. The token covers
// the handler, including a blocked response Write, but not net/http's subsequent
// flushing/body cleanup or connections still reading their request headers.
type requestAdmission struct {
	slots    chan struct{}
	rejected atomic.Uint64
}

func newRequestAdmission(capacity int) *requestAdmission {
	if capacity < 1 {
		panic("HTTP admission capacity must be positive")
	}
	return &requestAdmission{slots: make(chan struct{}, capacity)}
}

func (a *requestAdmission) stats() admissionStats {
	return admissionStats{Capacity: cap(a.slots), Inflight: len(a.slots), RejectedTotal: a.rejected.Load()}
}

func (a *requestAdmission) wrap(next http.Handler) http.Handler {
	return a.wrapWithRejection(next, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "Gateway request capacity exhausted", http.StatusServiceUnavailable)
	})
}

func (a *requestAdmission) wrapWithRejection(next http.Handler, reject http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case a.slots <- struct{}{}:
			defer func() { <-a.slots }()
			next.ServeHTTP(w, r)
		default:
			a.rejected.Add(1)
			// Otherwise net/http can try to consume a slow unread body before
			// writing the rejection. Do not call Body.Close here: that can drain
			// too. The server's read timeout bounds later connection cleanup.
			if r.ProtoMajor == 1 {
				w.Header().Set("Connection", "close")
			}
			w.Header().Set("Cache-Control", "no-store")
			reject.ServeHTTP(w, r)
		}
	})
}

func newGatewayHandler(dataClient *rpcClient, statsClient commandClient, started time.Time, httpMaxInflight int) http.Handler {
	dataAdmission := newRequestAdmission(httpMaxInflight)
	statsAdmission := newRequestAdmission(1)
	localStats := func() gatewayStats {
		return gatewayStats{
			UptimeSeconds: time.Since(started).Seconds(), RPC: dataClient.stats(),
			HTTP: gatewayHTTPStats{Data: dataAdmission.stats(), Stats: statsAdmission.stats()},
		}
	}
	mux := http.NewServeMux()
	mux.Handle("/kv", dataAdmission.wrap(newKVHandler(dataClient)))
	mux.Handle("/stats", statsAdmission.wrapWithRejection(newStatsHandler(statsClient, localStats),
		func(w http.ResponseWriter, r *http.Request) {
			writeRuntimeStats(w, http.StatusServiceUnavailable,
				runtimeStats{SchemaVersion: 1, Error: "gateway_overloaded"}, localStats())
		}))
	return mux
}
