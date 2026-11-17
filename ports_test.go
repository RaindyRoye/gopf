package main

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func writeRules(t *testing.T, path string, rules []Rule) {
	t.Helper()
	data, err := json.Marshal(Config{Forwards: rules})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
}

func responseBackend(t *testing.T, body string) string {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(server.Close)
	return strings.TrimPrefix(server.URL, "http://")
}

func forwardingClient(t *testing.T, keepAlive bool) *http.Client {
	t.Helper()
	transport := &http.Transport{
		Proxy:               nil,
		DisableKeepAlives:   !keepAlive,
		MaxIdleConnsPerHost: 1,
		MaxConnsPerHost:     1,
	}
	t.Cleanup(transport.CloseIdleConnections)
	return &http.Client{Transport: transport, Timeout: 2 * time.Second}
}

func requireResponse(t *testing.T, client *http.Client, address, want string) {
	t.Helper()
	response, err := client.Get("http://" + address + "/")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || string(body) != want {
		t.Fatalf("%s returned status=%d body=%q; want 200 %q", address, response.StatusCode, body, want)
	}
}

func requireListenerClosed(t *testing.T, address string) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", address, time.Second)
	if err == nil {
		conn.Close()
		t.Fatalf("removed listener %s still accepts new connections", address)
	}
}

func TestReloadAddsChangesAndRemovesForwards(t *testing.T) {
	targetA := responseBackend(t, "a")
	targetB := responseBackend(t, "b")
	manager := &proxyManager{}
	t.Cleanup(manager.close)
	path := filepath.Join(t.TempDir(), "settings.json")
	const listenA = "127.0.0.1:0"
	const listenB = "localhost:0"

	writeRules(t, path, []Rule{{Listen: listenA, Target: targetA}})
	if err := manager.reload(path); err != nil {
		t.Fatal(err)
	}
	addressA := manager.forwards[listenA].listener.Addr().String()
	existingA := forwardingClient(t, true)
	fresh := forwardingClient(t, false)
	requireResponse(t, existingA, addressA, "a")

	writeRules(t, path, []Rule{
		{Listen: listenA, Target: targetA},
		{Listen: listenB, Target: targetB},
	})
	if err := manager.reload(path); err != nil {
		t.Fatal(err)
	}
	if got := manager.forwards[listenA].listener.Addr().String(); got != addressA {
		t.Fatalf("retained listener moved from %s to %s", addressA, got)
	}
	addressB := manager.forwards[listenB].listener.Addr().String()
	existingB := forwardingClient(t, true)
	requireResponse(t, existingA, addressA, "a")
	requireResponse(t, existingB, addressB, "b")

	writeRules(t, path, []Rule{{Listen: listenA, Target: targetB}})
	if err := manager.reload(path); err != nil {
		t.Fatal(err)
	}
	requireResponse(t, fresh, addressA, "b")
	requireResponse(t, existingA, addressA, "a")
	requireListenerClosed(t, addressB)
	requireResponse(t, existingB, addressB, "b")

	writeRules(t, path, []Rule{})
	if err := manager.reload(path); err != nil {
		t.Fatal(err)
	}
	requireListenerClosed(t, addressA)
	requireResponse(t, existingA, addressA, "a")
	requireResponse(t, existingB, addressB, "b")
}

func TestFailedReloadKeepsActiveForwards(t *testing.T) {
	targetA := responseBackend(t, "a")
	targetB := responseBackend(t, "b")
	manager := &proxyManager{}
	t.Cleanup(manager.close)
	path := filepath.Join(t.TempDir(), "settings.json")
	const listen = "127.0.0.1:0"
	writeRules(t, path, []Rule{{Listen: listen, Target: targetA}})
	if err := manager.reload(path); err != nil {
		t.Fatal(err)
	}
	address := manager.forwards[listen].listener.Addr().String()
	client := forwardingClient(t, false)

	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { occupied.Close() })
	writeRules(t, path, []Rule{
		{Listen: listen, Target: targetB},
		{Listen: "localhost:0", Target: targetB},
		{Listen: occupied.Addr().String(), Target: targetB},
	})
	if err := manager.reload(path); err == nil {
		t.Fatal("reload accepted an occupied port")
	}
	if len(manager.forwards) != 1 {
		t.Fatalf("failed reload changed active listener count to %d", len(manager.forwards))
	}
	requireResponse(t, client, address, "a")

	if err := os.WriteFile(path, []byte(`{"forwards":[],"unexpected":true}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := manager.reload(path); err == nil {
		t.Fatal("reload accepted an unknown config field")
	}
	requireResponse(t, client, address, "a")
}

func TestReadConfigRejectsInvalidRules(t *testing.T) {
	for name, body := range map[string]string{
		"missing forwards": `{}`,
		"duplicate listen": `{"forwards":[{"listen":":1248","target":"127.0.0.1:80"},{"listen":":1248","target":"127.0.0.1:81"}]}`,
		"empty target":     `{"forwards":[{"listen":":1248","target":""}]}`,
		"invalid port":     `{"forwards":[{"listen":":1248","target":"127.0.0.1:70000"}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "settings.json")
			if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := readConfig(path); err == nil {
				t.Fatal("invalid config was accepted")
			}
		})
	}
}

type acceptErrorOnceListener struct {
	net.Listener
	err error
}

func (listener *acceptErrorOnceListener) Accept() (net.Conn, error) {
	if listener.err != nil {
		err := listener.err
		listener.err = nil
		return nil, err
	}
	return listener.Listener.Accept()
}

func TestAcceptRecoversFromRetryableErrors(t *testing.T) {
	for _, errno := range []syscall.Errno{syscall.EMFILE, syscall.ENFILE, syscall.ETIMEDOUT} {
		t.Run(errno.Error(), func(t *testing.T) {
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			entry := &portForwarder{listener: &acceptErrorOnceListener{
				Listener: listener,
				err:      &net.OpError{Op: "accept", Err: os.NewSyscallError("accept", errno)},
			}}
			entry.target.Store(responseBackend(t, "ok"))
			done := make(chan struct{})
			go func() {
				entry.serve()
				close(done)
			}()
			t.Cleanup(func() {
				listener.Close()
				select {
				case <-done:
				case <-time.After(2 * time.Second):
					t.Error("accept loop did not exit after listener closed")
				}
			})
			requireResponse(t, forwardingClient(t, false), listener.Addr().String(), "ok")
		})
	}
}
