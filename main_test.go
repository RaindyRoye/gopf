package main

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"testing"
	"time"
)

func startTestForward(t *testing.T) (client, backend *net.TCPConn, done <-chan struct{}) {
	t.Helper()
	pair := func() (*net.TCPConn, *net.TCPConn) {
		t.Helper()
		listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		client, err := net.DialTCP("tcp", nil, listener.Addr().(*net.TCPAddr))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { client.Close() })
		server, err := listener.AcceptTCP()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { server.Close() })
		for _, conn := range []*net.TCPConn{client, server} {
			if err := conn.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
		}
		return client, server
	}
	client, proxyClient := pair()
	proxyBackend, backend := pair()
	forwarded := make(chan struct{})
	go func() {
		forward(proxyClient, proxyBackend)
		// handleConn closes its connections after forward returns.
		proxyClient.Close()
		proxyBackend.Close()
		close(forwarded)
	}()
	return client, backend, forwarded
}

func TestForwardPreservesDataAfterHalfClose(t *testing.T) {
	client, backend, forwarded := startTestForward(t)
	request := make([]byte, 96<<10)
	for i := range request {
		request[i] = byte(i*i/127 + i*31)
	}
	response := append([]byte("response:"), request[1000:70000]...)
	backendResult := make(chan error, 1)
	go func() {
		got, err := io.ReadAll(backend)
		if err != nil {
			backendResult <- err
			return
		}
		if !bytes.Equal(got, request) {
			backendResult <- fmt.Errorf("backend received %d bytes with unexpected contents", len(got))
			return
		}
		if _, err := backend.Write(response); err != nil {
			backendResult <- err
			return
		}
		backendResult <- backend.CloseWrite()
	}()
	if _, err := client.Write(request); err != nil {
		t.Fatal(err)
	}
	if err := client.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(client)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, response) {
		t.Fatalf("client received %d bytes with unexpected contents", len(got))
	}
	if err := <-backendResult; err != nil {
		t.Fatal(err)
	}
	select {
	case <-forwarded:
	case <-time.After(time.Second):
		t.Fatal("forward did not return after both sides finished")
	}
}

func TestForwardClientCanUploadAfterBackendHalfClose(t *testing.T) {
	client, backend, forwarded := startTestForward(t)
	const response = "ready"
	const upload = "upload-after-fin"

	if _, err := io.WriteString(backend, response); err != nil {
		t.Fatal(err)
	}
	if err := backend.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	gotResponse, err := io.ReadAll(client)
	if err != nil {
		t.Fatalf("read response through EOF: %v", err)
	}
	if string(gotResponse) != response {
		t.Fatalf("client received %q, want %q", gotResponse, response)
	}

	if _, err := io.WriteString(client, upload); err != nil {
		t.Fatalf("upload after EOF: %v", err)
	}
	if err := client.CloseWrite(); err != nil {
		t.Fatal(err)
	}
	gotUpload, err := io.ReadAll(backend)
	if err != nil {
		t.Fatalf("read upload through EOF: %v", err)
	}
	if string(gotUpload) != upload {
		t.Fatalf("backend received %q, want %q", gotUpload, upload)
	}
	select {
	case <-forwarded:
	case <-time.After(time.Second):
		t.Fatal("forward did not return after both sides finished")
	}
}
