package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "settings.json")
	for _, test := range []struct {
		name string
		data string
		want string
	}{
		{"fixed target", `{"target":"127.0.0.1:8080"}`, "127.0.0.1:8080"},
		{"old weights", `{"hosts":[{"addr":"127.0.0.1:8080","weight":1}]}`, ""},
		{"missing target", `{}`, ""},
		{"invalid address", `{"target":"127.0.0.1"}`, ""},
		{"empty host", `{"target":":8080"}`, ""},
		{"invalid port", `{"target":"127.0.0.1:0"}`, ""},
		{"trailing JSON", `{"target":"127.0.0.1:8080"} {}`, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := os.WriteFile(path, []byte(test.data), 0o600); err != nil {
				t.Fatal(err)
			}
			config, err := readConfig(path)
			if test.want == "" {
				if err == nil {
					t.Fatal("invalid configuration was accepted")
				}
				return
			}
			if err != nil || config.Target != test.want {
				t.Fatalf("got config=%+v err=%v; want target %q", config, err, test.want)
			}
		})
	}
}

func TestReloadConfigPreservesTargetOnFailure(t *testing.T) {
	options.mutex.Lock()
	previous := options.config
	options.mutex.Unlock()
	t.Cleanup(func() {
		options.mutex.Lock()
		options.config = previous
		options.mutex.Unlock()
	})
	path := filepath.Join(t.TempDir(), "settings.json")
	write := func(data string) {
		t.Helper()
		if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(`{"target":"127.0.0.1:8080"}`)
	if err := reloadConfig(path); err != nil {
		t.Fatal(err)
	}
	write(`{"target":"invalid"}`)
	if err := reloadConfig(path); err == nil {
		t.Fatal("invalid reload succeeded")
	}
	if got := getTarget(); got != "127.0.0.1:8080" {
		t.Fatalf("invalid reload changed target to %q", got)
	}
	write(`{"target":"127.0.0.1:8081"}`)
	if err := reloadConfig(path); err != nil {
		t.Fatal(err)
	}
	if got := getTarget(); got != "127.0.0.1:8081" {
		t.Fatalf("valid reload left target at %q", got)
	}
}
