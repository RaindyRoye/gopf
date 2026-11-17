// Command bench compares direct requests with requests through gopf.
package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

const (
	backendAddress = "127.0.0.1:18080"
	proxyAddress   = "127.0.0.1:1248"
)

func main() {
	requests := flag.Int("n", 10000, "number of requests per target")
	concurrency := flag.Int("c", 100, "number of concurrent requests")
	keepAlive := flag.Bool("k", false, "use HTTP Keep-Alive")
	flag.Parse()
	if flag.NArg() != 0 || *requests < 1 || *concurrency < 1 || *concurrency > *requests {
		fmt.Fprintln(os.Stderr, "bench: require no positional arguments and n >= c >= 1")
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *requests, *concurrency, *keepAlive); err != nil {
		fmt.Fprintln(os.Stderr, "bench:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, requests, concurrency int, keepAlive bool) error {
	if _, err := os.Stat("go.mod"); err != nil {
		return fmt.Errorf("run from the repository root: %w", err)
	}
	dir, err := os.MkdirTemp("", "gopf-bench-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	binary := filepath.Join(dir, "gopf")
	build := exec.CommandContext(ctx, "go", "build", "-o", binary, ".")
	build.Stdout, build.Stderr = os.Stderr, os.Stderr
	if err := build.Run(); err != nil {
		return fmt.Errorf("build gopf: %w", err)
	}
	backend, err := startBackend()
	if err != nil {
		return fmt.Errorf("start backend on %s: %w", backendAddress, err)
	}
	defer backend.Close()

	config := fmt.Sprintf(`{"forwards":[{"listen":%q,"target":%q}]}`, proxyAddress, backendAddress)
	configPath := filepath.Join(dir, "settings.json")
	if err := os.WriteFile(configPath, []byte(config), 0o600); err != nil {
		return err
	}
	proxy := exec.Command(binary, configPath)
	proxy.Stdout = os.Stderr
	stderr, err := proxy.StderrPipe()
	if err != nil {
		return err
	}
	if err := proxy.Start(); err != nil {
		return fmt.Errorf("start gopf: %w", err)
	}
	ready := make(chan struct{}, 1)
	done := make(chan struct{})
	var proxyErr error
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			line := scanner.Text()
			fmt.Fprintln(os.Stderr, line)
			// Wait for this process to bind, rather than probing another listener.
			if strings.Contains(line, "Forwarding "+proxyAddress+" -> ") {
				ready <- struct{}{}
			}
		}
		scanErr := scanner.Err()
		proxyErr = proxy.Wait()
		if proxyErr == nil && scanErr != nil {
			proxyErr = fmt.Errorf("read gopf stderr: %w", scanErr)
		}
		close(done)
	}()
	defer func() {
		_ = proxy.Process.Kill()
		<-done
	}()
	select {
	case <-ready:
	case <-done:
		return fmt.Errorf("start gopf on %s: %v", proxyAddress, proxyErr)
	case <-time.After(5 * time.Second):
		return fmt.Errorf("gopf did not start on %s within 5 seconds", proxyAddress)
	case <-ctx.Done():
		return ctx.Err()
	}

	failed := false
	for _, target := range []struct{ name, address string }{
		{"direct", backendAddress},
		{"proxy", proxyAddress},
	} {
		result := runLoad(ctx, target.address, requests, concurrency, keepAlive)
		fmt.Printf("%s: success=%d failed=%d concurrency=%d elapsed=%.3fs rps=%.0f mean=%.2fms p50=%.2fms p99=%.2fms\n",
			target.name, result.Successful, result.Failed, concurrency, result.Elapsed.Seconds(),
			float64(result.Successful)/result.Elapsed.Seconds(),
			float64(result.Mean)/float64(time.Millisecond),
			float64(result.P50)/float64(time.Millisecond),
			float64(result.P99)/float64(time.Millisecond))
		if err := ctx.Err(); err != nil {
			return err
		}
		failed = failed || result.Failed != 0
	}
	if failed {
		return fmt.Errorf("some requests failed; see the results above")
	}
	return nil
}
