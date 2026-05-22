// Package main implements a simple TCP load balancer/proxy.
// It listens on a local address, reads backend configuration from a JSON file,
// and forwards incoming connections to selected backend hosts based on weighted round-robin.
package main

import (
        "encoding/json"
        "flag"
        "fmt"
        "io"
        "log"
        "math/rand"
        "net"
        "os"
        "os/signal"
        "runtime"
        "sync"
        "syscall"
        "time"
)

// Host represents a single backend server.
type Host struct {
        Addr   string `json:"addr"`   // Address of the backend server (e.g., "192.168.1.10:8080")
        Weight int    `json:"weight"` // Weight for load balancing (higher weight means more traffic)
}

// Backend holds the configuration for all backend servers.
type Backend struct {
        TrafficUrl string `json:"traffic_url"` // Not used in this proxy implementation
        Hosts      []Host `json:"hosts"`       // List of backend hosts
        weightSum  int    // Pre-calculated sum of all host weights for efficient selection
}

// Options holds the application's runtime configuration.
type Options struct {
        configFile string       // Path to the JSON configuration file
        backend    Backend      // Current backend configuration
        mutex      sync.RWMutex // Protects concurrent access to 'backend'
}

var options Options

// usage prints the command-line usage information and exits.
func usage() {
        fmt.Fprintf(os.Stderr, "Usage: %s [options] config_file\n", os.Args[0])
        flag.PrintDefaults()
        os.Exit(1)
}

// reloadConfig loads the backend configuration from the specified JSON file.
// It updates the global 'options.backend' atomically using a mutex.
func reloadConfig(configPath string) error {
        file, err := os.Open(configPath)
        if err != nil {
                return fmt.Errorf("failed to open config file '%s': %w", configPath, err)
        }
        defer file.Close()

        var newBackend Backend
        decoder := json.NewDecoder(file)
        if err := decoder.Decode(&newBackend); err != nil {
                return fmt.Errorf("failed to parse config file '%s': %w", configPath, err)
        }

        // Calculate the total weight for the new configuration.
        weightSum := 0
        for i := range newBackend.Hosts {
                host := &newBackend.Hosts[i]
                // Validate host address format
                if _, _, err := net.SplitHostPort(host.Addr); err != nil {
                        return fmt.Errorf("invalid host address '%s' in config: %w", host.Addr, err)
                }
                // Ensure weight is positive (or zero for disabled host)
                if host.Weight < 0 {
                        return fmt.Errorf("invalid negative weight %d for host '%s'", host.Weight, host.Addr)
                }
                weightSum += host.Weight
        }
        newBackend.weightSum = weightSum

        log.Printf("Loaded new config: %+v", newBackend)

        // Atomically update the global backend configuration.
        options.mutex.Lock()
        options.backend = newBackend
        options.mutex.Unlock()

        return nil
}

// getBackend retrieves a copy of the current backend configuration.
// It uses a read lock to allow concurrent access.
func getBackend() Backend {
        options.mutex.RLock()
        defer options.mutex.RUnlock()
        return options.backend
}

// chooseHost selects a backend host based on weighted random selection.
// It uses the provided Backend struct which contains pre-calculated weightSum.
func chooseHost(backend Backend) *Host {
        if backend.weightSum <= 0 {
                return nil // No valid hosts
        }

        // Generate a random value within the total weight range.
        randomValue := rand.Intn(backend.weightSum)

        // Iterate through hosts and select based on cumulative weight.
        cumulativeWeight := 0
        for _, host := range backend.Hosts {
                cumulativeWeight += host.Weight
                if randomValue < cumulativeWeight {
                        return &host // Found the selected host
                }
        }
        // Fallback: This should theoretically not happen if weightSum calculation is correct,
        // but return the last host if it does.
        if len(backend.Hosts) > 0 {
                return &backend.Hosts[len(backend.Hosts)-1]
        }
        return nil
}

// forward copies data bidirectionally between two TCP connections.
// It closes the read side of one connection and the write side of the other upon completion.
func forward(conn1, conn2 *net.TCPConn) {
        // Use a WaitGroup to wait for both directions to finish copying.
        var wg sync.WaitGroup
        wg.Add(2)

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

        // Copy from conn2 to conn1
        go func() {
                defer wg.Done()
                // Close the read side of conn1 when copying from conn2 finishes.
                defer conn1.CloseRead()
                // Copy data from conn2's reader to conn1's writer.
                _, err := io.Copy(conn1, conn2)
                if err != nil && err != io.EOF {
                        // Log errors during copy, but EOF is normal when one side closes.
                        log.Printf("Forwarding error (conn2->conn1): %v", err)
                }
        }()

        // Wait for both copy operations to complete.
        wg.Wait()
}

// handleConn manages the lifecycle of a single incoming connection.
// It selects a backend host, connects to it, and starts bidirectional forwarding.
func handleConn(clientConn *net.TCPConn) {
        defer clientConn.Close() // Ensure the client connection is closed when this function exits.

        // Get the current backend configuration.
        backend := getBackend()

        // Select a backend host based on weights.
        host := chooseHost(backend)
        if host == nil {
                log.Println("No available backend hosts to forward connection.")
                return
        }

        // Dial the selected backend host.
        backendConn, err := net.DialTimeout("tcp", host.Addr, 5*time.Second) // Add a dial timeout
        if err != nil {
                log.Printf("Failed to connect to backend '%s': %v", host.Addr, err)
                return
        }
        defer backendConn.Close() // Ensure the backend connection is closed.

        // Assert types for TCP-specific operations (SetKeepAlive, CloseRead/CloseWrite).
        backendTCPConn := backendConn.(*net.TCPConn)

        // Configure keep-alive for both connections to maintain long-lived sessions.
        clientConn.SetKeepAlive(true)
        clientConn.SetKeepAlivePeriod(60 * time.Second)
        backendTCPConn.SetKeepAlive(true)
        backendTCPConn.SetKeepAlivePeriod(60 * time.Second)

        // Start bidirectional forwarding between client and backend.
        forward(clientConn, backendTCPConn)
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
        listener, err := net.Listen("tcp", *listenAddr)
        if err != nil {
                log.Fatalf("Failed to create listener on '%s': %v", *listenAddr, err)
        }
        defer listener.Close()
        log.Printf("Load balancer started, listening on %s", *listenAddr)

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
                log.Printf("Accepted connection from %s", tcpConn.RemoteAddr())

                // Handle the connection in a new goroutine.
                go handleConn(tcpConn)
        }

        log.Println("Main accept loop stopped.")
}
