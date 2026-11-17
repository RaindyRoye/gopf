package main

import (
	"log"
	"net"
	"net/http"
	"strings"
)

// Adapted from agent-repro-gopf/bench_backend.go.
func startBackend() (*http.Server, error) {
	listener, err := net.Listen("tcp", backendAddress)
	if err != nil {
		return nil, err
	}
	body := []byte(strings.Repeat("x", 396))
	big := []byte(strings.Repeat("y", 1<<20))
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write(body)
	})
	mux.HandleFunc("/big", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(big)
	})
	server := &http.Server{Handler: mux}
	go func() {
		if err := server.Serve(listener); err != http.ErrServerClosed {
			log.Printf("bench backend: %v", err)
		}
	}()
	return server, nil
}
