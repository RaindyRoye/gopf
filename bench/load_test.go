package main

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunLoadConnections(t *testing.T) {
	for _, keepAlive := range []bool{false, true} {
		name := "short"
		wantConnections := int64(4)
		if keepAlive {
			name = "keepalive"
			wantConnections = 1
		}
		t.Run(name, func(t *testing.T) {
			var connections atomic.Int64
			server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = io.WriteString(w, "ok")
			}))
			server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
				if state == http.StateNew {
					connections.Add(1)
				}
			}
			server.Start()
			defer server.Close()
			result := runLoad(context.Background(), strings.TrimPrefix(server.URL, "http://"), 4, 1, keepAlive)
			if result.Successful != 4 || result.Failed != 0 || connections.Load() != wantConnections {
				t.Fatalf("result=%+v connections=%d; want 4 successes, 0 failures, %d connections", result, connections.Load(), wantConnections)
			}
		})
	}
}

func TestRunLoadRejectsFailedResponses(t *testing.T) {
	for _, empty := range []bool{false, true} {
		name := "HTTP 500"
		if empty {
			name = "empty response"
		}
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if empty {
					conn, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					_ = conn.Close()
					return
				}
				w.WriteHeader(http.StatusInternalServerError)
			}))
			defer server.Close()
			result := runLoad(context.Background(), strings.TrimPrefix(server.URL, "http://"), 4, 2, false)
			if result.Successful != 0 || result.Failed != 4 || result.Mean != 0 || result.P50 != 0 || result.P99 != 0 {
				t.Fatalf("failed responses counted as successes: %+v", result)
			}
		})
	}
}

func TestRunLoadCancellation(t *testing.T) {
	started := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan loadResult, 1)
	go func() {
		finished <- runLoad(ctx, strings.TrimPrefix(server.URL, "http://"), 20, 1, true)
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("request did not reach backend")
	}
	cancel()
	select {
	case result := <-finished:
		if result.Successful != 0 || result.Failed != 1 {
			t.Fatalf("unexpected results after cancellation: %+v", result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("load did not stop after cancellation")
	}
}
