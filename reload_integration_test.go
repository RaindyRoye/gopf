package main

import (
	"bufio"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func startReloadProxy(t *testing.T, binary, config string) (*os.Process, func(string) string) {
	t.Helper()
	cmd := exec.Command(binary, config)
	stderr, err := cmd.StderrPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	lines := make(chan string, 16)
	done := make(chan struct{})
	stopping := make(chan struct{})
	var scanErr, waitErr error
	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			select {
			case lines <- scanner.Text():
			case <-stopping:
			}
		}
		scanErr = scanner.Err()
		waitErr = cmd.Wait()
		close(lines)
		close(done)
	}()
	t.Cleanup(func() {
		close(stopping)
		select {
		case <-done:
			t.Errorf("gopf exited before cleanup: %v", waitErr)
			return
		default:
		}
		if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
			t.Errorf("stop gopf: %v", err)
		}
		select {
		case <-done:
			if waitErr != nil || scanErr != nil {
				t.Errorf("gopf shutdown: wait=%v stderr=%v", waitErr, scanErr)
			}
		case <-time.After(5 * time.Second):
			t.Error("gopf did not exit after SIGTERM")
			if err := cmd.Process.Kill(); err != nil {
				t.Errorf("kill gopf: %v", err)
			}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("gopf did not exit after Kill")
			}
		}
	})
	var history []string
	waitFor := func(want string) string {
		t.Helper()
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		for {
			select {
			case line, ok := <-lines:
				if !ok {
					t.Fatalf("gopf exited waiting for %q: wait=%v stderr=%v\n%s", want, waitErr, scanErr, strings.Join(history, "\n"))
				}
				history = append(history, line)
				if strings.Contains(line, want) {
					return line
				}
			case <-timer.C:
				t.Fatalf("timed out waiting for gopf log %q\n%s", want, strings.Join(history, "\n"))
			}
		}
	}
	return cmd.Process, waitFor
}

func loggedForwardAddress(t *testing.T, line string) string {
	t.Helper()
	_, message, ok := strings.Cut(line, "Forwarding ")
	if !ok {
		t.Fatalf("expected forwarding log, got %q", line)
	}
	address, _, ok := strings.Cut(message, " -> ")
	if !ok {
		t.Fatalf("expected listen and target in log, got %q", line)
	}
	return address
}

func TestReloadIntegration(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "gopf")
	if output, err := exec.Command("go", "build", "-race", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("build gopf: %v\n%s", err, output)
	}
	t.Run("same listen changes target", func(t *testing.T) {
		targetA := responseBackend(t, "a")
		targetB := responseBackend(t, "b")
		path := filepath.Join(t.TempDir(), "settings.json")
		const listen = "127.0.0.1:0"
		writeRules(t, path, []Rule{{Listen: listen, Target: targetA}})
		process, waitFor := startReloadProxy(t, binary, path)
		address := loggedForwardAddress(t, waitFor(" -> "+targetA))
		existing := forwardingClient(t, true)
		requireResponse(t, existing, address, "a")

		writeRules(t, path, []Rule{{Listen: listen, Target: targetB}})
		if err := process.Signal(syscall.SIGHUP); err != nil {
			t.Fatal(err)
		}
		updatedAddress := loggedForwardAddress(t, waitFor(" -> "+targetB))
		waitFor("Configuration reloaded successfully.")
		if updatedAddress != address {
			t.Fatalf("retained listener moved from %s to %s", address, updatedAddress)
		}
		requireResponse(t, existing, address, "a")
		requireResponse(t, forwardingClient(t, false), address, "b")
	})
	t.Run("new listen retains target", func(t *testing.T) {
		target := responseBackend(t, "a")
		path := filepath.Join(t.TempDir(), "settings.json")
		writeRules(t, path, []Rule{{Listen: "127.0.0.1:0", Target: target}})
		process, waitFor := startReloadProxy(t, binary, path)
		oldAddress := loggedForwardAddress(t, waitFor(" -> "+target))
		existing := forwardingClient(t, true)
		fresh := forwardingClient(t, false)
		requireResponse(t, existing, oldAddress, "a")

		writeRules(t, path, []Rule{{Listen: "localhost:0", Target: target}})
		if err := process.Signal(syscall.SIGHUP); err != nil {
			t.Fatal(err)
		}
		newAddress := loggedForwardAddress(t, waitFor(" -> "+target))
		waitFor("Configuration reloaded successfully.")
		requireResponse(t, fresh, newAddress, "a")
		requireListenerClosed(t, oldAddress)
		requireResponse(t, existing, oldAddress, "a")

		occupied, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { occupied.Close() })
		writeRules(t, path, []Rule{{Listen: occupied.Addr().String(), Target: target}})
		if err := process.Signal(syscall.SIGHUP); err != nil {
			t.Fatal(err)
		}
		waitFor("Configuration reload failed:")
		requireResponse(t, fresh, newAddress, "a")
		requireResponse(t, existing, oldAddress, "a")
	})
}
