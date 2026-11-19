// Package main forwards multiple TCP ports using reloadable configuration.
package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"syscall"
	"time"
)

const (
	SIG_RELOAD = syscall.SIGHUP
	SIG_STATUS = syscall.SIGUSR1
)

// forward copies data bidirectionally between two TCP connections.
// Each copy closes its destination's write side so the other direction can finish independently.
func forward(conn1, conn2 *net.TCPConn) {
	// Use a WaitGroup to wait for both directions to finish copying.
	var wg sync.WaitGroup
	wg.Add(1)

	// Copy from conn1 to conn2
	go func() {
		defer wg.Done()
		// Close the write side of conn2 when copying from conn1 finishes.
		defer conn2.CloseWrite()
		// Copy data from conn1's reader to conn2's writer.
		_, err := io.Copy(conn2, conn1)
		if err != nil && err != io.EOF {
			// Log errors during copy, but EOF is normal when one side closes.
			log.Printf("Forwarding error (conn1->conn2): %v", err)
		}
	}()

	// Reuse the handler goroutine for the reverse direction.
	_, err := io.Copy(conn1, conn2)
	conn1.CloseWrite()
	if err != nil && err != io.EOF {
		log.Printf("Forwarding error (conn2->conn1): %v", err)
	}

	// Wait for both copy operations to complete.
	wg.Wait()
}

// handleConn uses the target captured by the accepting listener.
func handleConn(clientConn *net.TCPConn, target string) {
	defer clientConn.Close()
	dialer := net.Dialer{Timeout: 5 * time.Second, KeepAlive: 60 * time.Second}
	backendConn, err := dialer.Dial("tcp", target)
	if err != nil {
		log.Printf("Failed to connect to target %q: %v", target, err)
		return
	}
	defer backendConn.Close()
	forward(clientConn, backendConn.(*net.TCPConn))
}

// status logs runtime statistics like the number of goroutines and the current stack trace.
func status() {
	numGoroutines := runtime.NumGoroutine()
	log.Printf("Current number of goroutines: %d", numGoroutines)

	// Capture and log the stack trace.
	stackBuf := make([]byte, 32768) // Buffer size for stack trace
	n := runtime.Stack(stackBuf, true)
	log.Printf("Goroutine Stack Trace:\n%s", stackBuf[:n])
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s config_file\n", os.Args[0])
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() != 1 {
		flag.Usage()
		os.Exit(1)
	}
	if err := run(flag.Arg(0)); err != nil {
		log.Printf("gopf: %v", err)
		os.Exit(1)
	}
}

func run(configPath string) error {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, SIG_RELOAD, SIG_STATUS, syscall.SIGTERM, syscall.SIGINT)
	defer signal.Stop(signals)
	var manager proxyManager
	if err := manager.reload(configPath); err != nil {
		return err
	}
	defer manager.close()
	for {
		switch sig := <-signals; sig {
		case SIG_RELOAD:
			if err := manager.reload(configPath); err != nil {
				log.Printf("Configuration reload failed: %v", err)
			} else {
				log.Println("Configuration reloaded successfully.")
			}
		case SIG_STATUS:
			status()
		case syscall.SIGTERM, syscall.SIGINT:
			log.Printf("Received signal %v, shutting down...", sig)
			return nil
		}
	}
}
