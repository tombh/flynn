package main_test

import (
	"bytes"
	"errors"
	"io"
	"os"
	"reflect"
	"testing"

	"github.com/flynn/flynn/appliance/discoverd-wrapper"
)

const (
	DefaultExternalIP = "127.0.0.1"
	DefaultPort0      = "1110"
	DefaultPort1      = "1111"
)

// Ensure options can be parsed from environment variables.
func TestMain_ParseFlags(t *testing.T) {
	SetDefaultEnv()
	defer UnsetEnv()

	opt, err := NewMain().ParseFlags(nil)
	if err != nil {
		t.Fatal(err)
	} else if opt.IP != "127.0.0.1" {
		t.Fatalf("unexpected ip: %s", opt.IP)
	} else if opt.RaftAddr != ":1110" {
		t.Fatalf("unexpected raft addr: %s", opt.RaftAddr)
	} else if opt.HTTPAddr != ":1111" {
		t.Fatalf("unexpected http addr: %s", opt.HTTPAddr)
	} else if opt.DNSAddr != "" {
		t.Fatalf("unexpected dns addr: %s", opt.DNSAddr)
	}
}

// Ensure options can parse DNS bind address, if specified.
func TestMain_ParseFlags_DNS(t *testing.T) {
	SetDefaultEnv()
	os.Setenv("PORT_2", "53")
	defer UnsetEnv()

	opt, err := NewMain().ParseFlags(nil)
	if err != nil {
		t.Fatal(err)
	} else if opt.DNSAddr != ":53" {
		t.Fatalf("unexpected dns addr: %s", opt.DNSAddr)
	}
}

// Ensure errors is returned if EXTERNAL_IP is undefined.
func TestMain_ParseFlags_ErrExternalIPUndefined(t *testing.T) {
	SetDefaultEnv()
	os.Unsetenv("EXTERNAL_IP")
	defer UnsetEnv()

	_, err := NewMain().ParseFlags(nil)
	if err == nil || err.Error() != `EXTERNAL_IP is undefined` {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure errors is returned if PORT_0 is undefined.
func TestMain_ParseFlags_ErrPort0Undefined(t *testing.T) {
	SetDefaultEnv()
	os.Unsetenv("PORT_0")
	defer UnsetEnv()

	_, err := NewMain().ParseFlags(nil)
	if err == nil || err.Error() != `PORT_0 is undefined` {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure errors is returned if PORT_1 is undefined.
func TestMain_ParseFlags_ErrPort1Undefined(t *testing.T) {
	SetDefaultEnv()
	os.Unsetenv("PORT_1")
	defer UnsetEnv()

	_, err := NewMain().ParseFlags(nil)
	if err == nil || err.Error() != `PORT_1 is undefined` {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure discoverd process can be executed with the correct arguments.
func TestMain_Run(t *testing.T) {
	SetDefaultEnv()
	defer UnsetEnv()

	m := NewMain()

	// Mock path resolution.
	var lookPathInvoked bool
	m.LookPath = func(file string) (string, error) {
		lookPathInvoked = true
		return "/my/path/discoverd", nil
	}

	// Mock process execution.
	var execInvoked bool
	m.Exec = func(argv0 string, argv []string, envv []string) error {
		execInvoked = true
		if argv0 != "/my/path/discoverd" {
			t.Fatalf("unexpected argv0: %s", argv0)
		} else if !reflect.DeepEqual(argv, []string{
			"/my/path/discoverd",
			"-data-dir=/data",
			"-name=f528764d624db129b32c21fbca0cb8d6",
			"-host=127.0.0.1",
			"-raft-addr=:1110",
			"-http-addr=:1111",
			"-dns-addr=",
		}) {
			t.Fatalf("unexpected argv: %+v", argv)
		}
		return nil
	}

	if err := m.Run(); err != nil {
		t.Fatal(err)
	} else if !lookPathInvoked {
		t.Fatal("LookPath was not invoked")
	} else if !execInvoked {
		t.Fatal("Exec was not invoked")
	}
}

// Ensure error is returned if discoverd cannot be found in PATH.
func TestMain_Run_ErrDiscoverdNotFound(t *testing.T) {
	SetDefaultEnv()
	defer UnsetEnv()
	m := NewMain()
	m.LookPath = func(file string) (string, error) { return "", errors.New("marker") }
	if err := m.Run(); err == nil || err.Error() != `cannot find discoverd: marker` {
		t.Fatalf("unexpected errors: %s", err)
	}
}

// Ensure options can return a hash of the IP.
func TestOptions_Name(t *testing.T) {
	opt := main.Options{IP: "127.0.0.1"}
	if name := opt.Name(); name != `f528764d624db129b32c21fbca0cb8d6` {
		t.Fatalf("unexpected name: %s", name)
	}
}

// Ensure options return a blank name if there is no IP.
func TestOptions_Name_MissingIP(t *testing.T) {
	opt := main.Options{}
	if name := opt.Name(); name != `` {
		t.Fatalf("unexpected name: %s", name)
	}
}

// Main represents a test wrapper for main.Main.
type Main struct {
	*main.Main
	Stdout bytes.Buffer
	Stderr bytes.Buffer
}

// NewMain returns a new test wrapper.
func NewMain() *Main {
	m := &Main{Main: main.NewMain()}

	if testing.Verbose() {
		m.Main.Stdout = io.MultiWriter(os.Stdout, &m.Stdout)
		m.Main.Stderr = io.MultiWriter(os.Stderr, &m.Stderr)
	} else {
		m.Main.Stdout = &m.Stdout
		m.Main.Stderr = &m.Stderr
	}

	return m
}

// SetDefaultEnv sets the required environment variables to their default values.
func SetDefaultEnv() {
	os.Setenv("EXTERNAL_IP", DefaultExternalIP)
	os.Setenv("PORT_0", DefaultPort0)
	os.Setenv("PORT_1", DefaultPort1)
}

// UnsetEnv unsets all related environment variables.
func UnsetEnv() {
	os.Unsetenv("EXTERNAL_IP")
	os.Unsetenv("PORT_0")
	os.Unsetenv("PORT_1")
	os.Unsetenv("PORT_2")
}
