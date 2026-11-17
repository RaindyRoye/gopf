package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"
)

type Rule struct {
	Listen string `json:"listen"`
	Target string `json:"target"`
}

type Config struct {
	Forwards []Rule `json:"forwards"`
}

func readConfig(path string) (Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("open config: %w", err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	var config Config
	if err := decoder.Decode(&config); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return Config{}, errors.New("config must contain a single JSON object")
	}
	if config.Forwards == nil {
		return Config{}, errors.New("config requires a forwards array")
	}
	seen := make(map[string]bool, len(config.Forwards))
	for _, rule := range config.Forwards {
		if err := validateAddress(rule.Listen, true); err != nil {
			return Config{}, fmt.Errorf("invalid listen address %q: %w", rule.Listen, err)
		}
		if err := validateAddress(rule.Target, false); err != nil {
			return Config{}, fmt.Errorf("invalid target address %q: %w", rule.Target, err)
		}
		if seen[rule.Listen] {
			return Config{}, fmt.Errorf("duplicate listen address %q", rule.Listen)
		}
		seen[rule.Listen] = true
	}
	return config, nil
}

func validateAddress(address string, listen bool) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 0 || number > 65535 || (!listen && (number == 0 || host == "")) {
		return errors.New("expected host:port with a valid TCP port")
	}
	return nil
}

type portForwarder struct {
	listener net.Listener
	target   atomic.Value // string; initialized before the accept loop starts
}

// reload and close are owned by the main signal loop.
type proxyManager struct {
	forwards map[string]*portForwarder
}

func (manager *proxyManager) reload(path string) error {
	config, err := readConfig(path)
	if err != nil {
		return err
	}
	next := make(map[string]*portForwarder, len(config.Forwards))
	var created []*portForwarder
	for _, rule := range config.Forwards {
		entry, exists := manager.forwards[rule.Listen]
		if !exists {
			listenConfig := net.ListenConfig{KeepAlive: 60 * time.Second}
			listener, err := listenConfig.Listen(context.Background(), "tcp", rule.Listen)
			if err != nil {
				for _, pending := range created {
					pending.listener.Close()
				}
				return fmt.Errorf("listen on %s: %w", rule.Listen, err)
			}
			entry = &portForwarder{listener: listener}
			created = append(created, entry)
		}
		next[rule.Listen] = entry
	}

	// Commit only after every new listener has bound successfully.
	for _, rule := range config.Forwards {
		next[rule.Listen].target.Store(rule.Target)
	}
	previous := manager.forwards
	manager.forwards = next
	for _, entry := range created {
		go entry.serve()
	}
	for address, entry := range previous {
		if _, retained := next[address]; !retained {
			entry.listener.Close()
			log.Printf("Stopped forwarding %s", address)
		}
	}
	for _, rule := range config.Forwards {
		log.Printf("Forwarding %s -> %s", next[rule.Listen].listener.Addr(), rule.Target)
	}
	return nil
}

func (manager *proxyManager) close() {
	for _, entry := range manager.forwards {
		entry.listener.Close()
	}
	manager.forwards = nil
}

func (entry *portForwarder) serve() {
	for {
		conn, err := entry.listener.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) {
				return
			}
			log.Printf("Accept error on %s: %v", entry.listener.Addr(), err)
			if errors.Is(err, syscall.EMFILE) || errors.Is(err, syscall.ENFILE) || errors.Is(err, syscall.ETIMEDOUT) {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			return
		}
		target := entry.target.Load().(string)
		go handleConn(conn.(*net.TCPConn), target)
	}
}
