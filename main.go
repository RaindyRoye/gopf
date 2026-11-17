// Package main forwards TCP connections to a configured target.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"time"
)

// Config identifies the fixed forwarding target.
type Config struct {
	Target string `json:"target"`
}

// Options holds the configuration used by new connections.
type Options struct {
	configFile string
	config     Config
	mutex      sync.RWMutex
}

var options Options

// usage prints the command-line usage information and exits.
func usage() {
	fmt.Fprintf(os.Stderr, "Usage: %s [options] config_file\n", os.Args[0])
	flag.PrintDefaults()
	os.Exit(1)
}

func readConfig(configPath string) (Config, error) {
	file, err := os.Open(configPath)
	if err != nil {
		return Config{}, fmt.Errorf("open config %q: %w", configPath, err)
	}
	defer file.Close()

	var config Config
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return Config{}, fmt.Errorf("parse config %q: %w", configPath, err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return Config{}, fmt.Errorf("config %q must contain one JSON object", configPath)
	}
	host, port, err := net.SplitHostPort(config.Target)
	if err != nil || host == "" {
		return Config{}, fmt.Errorf("invalid target %q: expected host:port", config.Target)
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 1 || number > 65535 {
		return Config{}, fmt.Errorf("invalid target port %q", port)
	}
	return config, nil
}

// reloadConfig publishes the target only after the new configuration is valid.
func reloadConfig(configPath string) error {
	config, err := readConfig(configPath)
	if err != nil {
		return err
	}
	options.mutex.Lock()
	options.config = config
	options.mutex.Unlock()
	log.Printf("Loaded target: %s", config.Target)
	return nil
}

func getTarget() string {
	options.mutex.RLock()
	defer options.mutex.RUnlock()
	return options.config.Target
}

// forward copies data bidirectionally between two TCP connections.
// It closes the read side of one connection and the write side of the other upon completion.
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
	conn1.CloseRead()
	if err != nil && err != io.EOF {
		log.Printf("Forwarding error (conn2->conn1): %v", err)
	}

	// Wait for both copy operations to complete.
	wg.Wait()
}

// handleConn captures the current target before forwarding the connection.
func handleConn(clientConn *net.TCPConn) {
	defer clientConn.Close()
	target := getTarget()
	dialer := net.Dialer{Timeout: 5 * time.Second, KeepAlive: 60 * time.Second}
	backendConn, err := dialer.Dial("tcp", target)
	if err != nil {
		log.Printf("Failed to connect to target %q: %v", target, err)
		return
	}
	defer backendConn.Close()
	forward(clientConn, backendConn.(*net.TCPConn))
}

// Constants for custom signals (platform-dependent, consider using SIGHUP/SIGUSR1/USR2)
// Using syscall.Signal(35) and syscall.Signal(36) is not portable.
// Standard signals like SIGHUP (1), SIGUSR1 (10/30), SIGUSR2 (12/31) are more common.
// For this refactored version, we'll stick to common signals.
const (
	SIG_RELOAD = syscall.SIGHUP  // Common signal for reloading config
	SIG_STATUS = syscall.SIGUSR1 // Common signal for status report
)

// status logs runtime statistics like the number of goroutines and the current stack trace.
func status() {
	numGoroutines := runtime.NumGoroutine()
	log.Printf("Current number of goroutines: %d", numGoroutines)

	// Capture and log the stack trace.
	stackBuf := make([]byte, 32768) // Buffer size for stack trace
	n := runtime.Stack(stackBuf, true)
	log.Printf("Goroutine Stack Trace:\n%s", stackBuf[:n])
}

// reload triggers a reload of the configuration file.
func reload() {
	log.Println("Reloading configuration...")
	if err := reloadConfig(options.configFile); err != nil {
		log.Printf("Configuration reload failed: %v", err)
	} else {
		log.Println("Configuration reloaded successfully.")
	}
}

func main() {
	// Define command-line flags
	listenAddr := flag.String("listen", ":1248", "Local address to listen on (e.g., ':1248' or '127.0.0.1:1248')")
	flag.Usage = usage
	flag.Parse()

	// Get the config file path from the first positional argument
	args := flag.Args()
	if len(args) < 1 {
		log.Println("Error: Configuration file path is required as the first argument.")
		usage() // This will print usage and exit
	}
	configFile := args[0]

	// Initialize global options
	options.configFile = configFile

	// Load the initial configuration
	if err := reloadConfig(configFile); err != nil {
		log.Fatalf("Failed to load initial configuration from '%s': %v", configFile, err)
	}

	// Create the TCP listener
	listenConfig := net.ListenConfig{KeepAlive: 60 * time.Second}
	listener, err := listenConfig.Listen(context.Background(), "tcp", *listenAddr)
	if err != nil {
		log.Fatalf("Failed to create listener on '%s': %v", *listenAddr, err)
	}
	defer listener.Close()
	log.Printf("TCP forwarder started, listening on %s", *listenAddr)

	// Set up signal handling
	go func() {
		sigChan := make(chan os.Signal, 1)
		// Listen for reload, status, and termination signals
		signal.Notify(sigChan, SIG_RELOAD, SIG_STATUS, syscall.SIGTERM, syscall.SIGINT)

		for sig := range sigChan {
			switch sig {
			case SIG_RELOAD: // Handles SIGHUP
				reload()
			case SIG_STATUS: // Handles SIGUSR1
				status()
			case syscall.SIGTERM, syscall.SIGINT:
				log.Printf("Received signal %v, shutting down gracefully...", sig)
				// In a more complex app, you'd wait for connections to finish here.
				os.Exit(0)
			default:
				log.Printf("Caught unexpected signal: %v (ignored)", sig)
				// Ignore other signals
			}
		}
	}()

	// Main accept loop
	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Accept error: %v", err)
			// Check if the error is temporary (e.g., too many open files temporarily).
			// net.OpError is deprecated; use errors.As with net.Error directly.
			if netErr, ok := err.(net.Error); ok && netErr.Temporary() {
				log.Println("Temporary network error during accept, continuing...")
				continue
			} else {
				// Permanent error (e.g., listener closed by signal handler)
				log.Printf("Permanent error during accept, stopping: %v", err)
				break
			}
		}

		// Cast to *net.TCPConn for further processing (forwarding, keepalive).
		tcpConn := conn.(*net.TCPConn)

		// Handle the connection in a new goroutine.
		go handleConn(tcpConn)
	}

	log.Println("Main accept loop stopped.")
}
