package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadConfig(t *testing.T) {
	for _, test := range []struct {
		name string
		data string
		want int
	}{
		{"two fixed forwards", `{"forwards":[{"listen":":1248","target":"127.0.0.1:80"},{"listen":":1249","target":"127.0.0.1:81"}]}`, 2},
		{"explicit empty forwards", `{"forwards":[]}`, 0},
		{"old single target", `{"target":"127.0.0.1:80"}`, -1},
		{"trailing JSON", `{"forwards":[]} {}`, -1},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "settings.json")
			if err := os.WriteFile(path, []byte(test.data), 0o600); err != nil {
				t.Fatal(err)
			}
			config, err := readConfig(path)
			if test.want < 0 {
				if err == nil {
					t.Fatal("invalid configuration was accepted")
				}
				return
			}
			if err != nil || len(config.Forwards) != test.want {
				t.Fatalf("got config=%+v err=%v; want %d forwards", config, err, test.want)
			}
		})
	}
}
