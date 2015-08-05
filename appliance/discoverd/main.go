package main

import (
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"syscall"
)

func main() {
	m := NewMain()
	if err := m.Run(os.Args[1:]...); err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}
}

// Main representations the main program execution.
type Main struct {
	Exec     func(argv0 string, argv []string, envv []string) error
	LookPath func(file string) (string, error)

	Stdout io.Writer
	Stderr io.Writer
}

// NewMain returns a new instance of Main.
func NewMain() *Main {
	return &Main{
		Exec:     syscall.Exec,
		LookPath: exec.LookPath,

		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

// Run executes the program.
func (m *Main) Run(args ...string) error {
	// Parse flags and environment variables.
	opt, err := m.ParseFlags(args)
	if err != nil {
		return err
	}

	// Find the discoverd path.
	path, err := m.LookPath("discoverd")
	if err != nil {
		return fmt.Errorf("cannot find discoverd: %s", err)
	}

	// Execute discoverd process.
	return m.Exec(path, []string{
		path,
		"-data-dir=/data",
		"-name=" + opt.Name(),
		"-host=" + opt.IP,
		"-raft-addr=" + opt.RaftAddr,
		"-http-addr=" + opt.HTTPAddr,
		"-dns-addr=" + opt.DNSAddr,
	}, os.Environ())
}

// ParseFlags returns options parsed from args and environment variables.
func (m *Main) ParseFlags(args []string) (Options, error) {
	var opt Options

	// Retrieve the externally visible IP address of the container.
	opt.IP = os.Getenv("EXTERNAL_IP")
	if opt.IP == "" {
		return opt, errors.New("EXTERNAL_IP is undefined")
	}

	// Retrieve the raft port.
	port0 := os.Getenv("PORT_0")
	if port0 == "" {
		return opt, errors.New("PORT_0 is undefined")
	}
	opt.RaftAddr = ":" + port0

	// Construct the process's URL.
	opt.URL = url.URL{
		Scheme: "http",
		Host:   net.JoinHostPort(opt.IP, port0),
	}

	// Retrieve the HTTP port.
	port1 := os.Getenv("PORT_1")
	if port1 == "" {
		return opt, errors.New("PORT_1 is undefined")
	}
	opt.HTTPAddr = ":" + port1

	// Retrieve the DNS port.
	if port := os.Getenv("PORT_2"); port != "" {
		opt.DNSAddr = ":" + port
	}

	return opt, nil
}

// Options represents the options passed into the program execution.
type Options struct {
	IP       string  // external IP
	URL      url.URL // external URL
	RaftAddr string  // raft bind address
	HTTPAddr string  // HTTP bind address
	DNSAddr  string  // DNS bind address
}

// Name returns a unique, deterministic name based on the IP address.
func (opt *Options) Name() string {
	if opt.IP == "" {
		return ""
	}

	sum := md5.Sum([]byte(opt.IP))
	return hex.EncodeToString(sum[:])
}
