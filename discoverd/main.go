package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/flynn/flynn/discoverd/client"
	"github.com/flynn/flynn/discoverd/server"
	"github.com/flynn/flynn/host/types"
	"github.com/flynn/flynn/pkg/cluster"
	"github.com/flynn/flynn/pkg/shutdown"
)

const (
	// LeaderTimeout is the amount of time discoverd will wait for a leader.
	LeaderTimeout = 5 * time.Second
)

func main() {
	defer shutdown.Exit()

	// Initialize main program and execute.
	m := NewMain()
	if err := m.Run(os.Args[1:]...); err != nil {
		fmt.Fprintln(m.Stderr, err.Error())
		os.Exit(1)
	}

	// Wait indefinitely.
	<-(chan struct{})(nil)
}

// Main represents the main program execution.
type Main struct {
	mu           sync.Mutex
	status       host.DiscoverdConfig
	store        *server.Store
	dnsServer    *server.DNSServer
	httpListener net.Listener

	logger *log.Logger

	Stdout io.Writer
	Stderr io.Writer
}

// NewMain returns a new instance of Main.
func NewMain() *Main {
	return &Main{
		status: host.DiscoverdConfig{JobID: os.Getenv("FLYNN_JOB_ID")},

		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}

// Run executes the program.
func (m *Main) Run(args ...string) error {
	// Create logger.
	m.logger = log.New(m.Stdout, "", log.LstdFlags)

	// Parse command line flags.
	opt, err := m.ParseFlags(args...)
	if err != nil {
		return err
	}

	// Open store & servers.
	if err := m.openStore(opt.DataDir, opt.RaftAddr, MergeHostPort(opt.Host, opt.RaftAddr), opt.Join); err != nil {
		return fmt.Errorf("Failed to open store: %s", err)
	}

	// If we have a DNS address, start a DNS server right away, otherwise
	// wait for the host network to come up and then start a DNS server.
	if opt.DNSAddr != "" {
		if err := m.openDNSServer(opt.DNSAddr, opt.Recursors); err != nil {
			return fmt.Errorf("Failed to start DNS server: %s", err)
		}
		m.logger.Printf("discoverd listening for DNS on %s", opt.DNSAddr)
	} else if opt.WaitNetDNS {
		go func() {
			// Wait for the host network.
			status, err := cluster.WaitForHostStatus(func(status *host.HostStatus) bool {
				return status.Network != nil && status.Network.Subnet != ""
			})
			if err != nil {
				log.Fatal(err)
			}

			// Parse network subnet to determine bind address.
			ip, _, err := net.ParseCIDR(status.Network.Subnet)
			if err != nil {
				log.Fatal(err)
			}
			addr := net.JoinHostPort(ip.String(), "53")

			if err := m.openDNSServer(addr, status.Network.Resolvers); err != nil {
				log.Fatalf("Failed to start DNS server: %s", err)
			}
			m.logger.Printf("discoverd listening for DNS on %s", opt.DNSAddr)

			// Notify webhook.
			if opt.Notify != "" {
				m.Notify(opt.Notify, "", addr)
			}
		}()
	}

	if err := m.openHTTPServer(opt.HTTPAddr); err != nil {
		return fmt.Errorf("Failed to start HTTP server: %s", err)
	}

	// Notify user that the servers are listening.
	m.logger.Printf("discoverd listening for HTTP on %s", opt.HTTPAddr)

	// FIXME(benbjohnson): Join to cluster.

	// Wait for leadership.
	// FIXME(benbjohnson): Wait for any leader, not just local leader.
	select {
	case <-time.After(LeaderTimeout):
		return errors.New("timed out waiting for leader")
	case <-m.store.LeaderCh():
	}

	// Notify URL that discoverd is running.
	httpAddr := m.httpListener.Addr().String()
	host, port, _ := net.SplitHostPort(httpAddr)
	if host == "0.0.0.0" {
		httpAddr = net.JoinHostPort(os.Getenv("EXTERNAL_IP"), port)
	}
	m.Notify(opt.Notify, "http://"+httpAddr, opt.DNSAddr)
	go discoverd.NewClientWithURL("http://"+httpAddr).AddServiceAndRegister("discoverd", httpAddr)

	return nil
}

// Close shuts down all open servers.
func (m *Main) Close() error {
	if m.store != nil {
		m.store.Close()
		m.store = nil
	}
	if m.dnsServer != nil {
		m.dnsServer.Close()
		m.dnsServer = nil
	}
	if m.httpListener != nil {
		m.httpListener.Close()
		m.httpListener = nil
	}
	return nil
}

// openStore initializes and opens the store.
func (m *Main) openStore(path, bindAddress, advertise, join string) error {
	// Resolve advertised address.
	addr, err := net.ResolveTCPAddr("tcp", advertise)
	if err != nil {
		return err
	}

	// Initialize store.
	s := server.NewStore(path)
	s.BindAddress = bindAddress
	s.Advertise = addr

	// Allow single node if there's no peers set.
	s.EnableSingleNode = (join == "")

	// Open store.
	if err := s.Open(); err != nil {
		return err
	}
	m.store = s

	// If join is specified then request to join.
	if join != "" {
		u := url.URL{
			Scheme:   "http",
			Host:     join,
			Path:     "/raft/nodes",
			RawQuery: (&url.Values{"addr": {advertise}}).Encode(),
		}
		resp, err := http.Post(u.String(), "text/plain", nil)
		if err != nil {
			return fmt.Errorf("joining: %s", err)
		}
		defer resp.Body.Close()

		// Return error if necessary.
		if resp.StatusCode != http.StatusOK {
			return fmt.Errorf("join error: code=%d", resp.StatusCode)
		}
	}

	return nil
}

// openDNSServer initializes and opens the DNS server.
// The store must already be open.
func (m *Main) openDNSServer(addr string, recursors []string) error {
	s := &server.DNSServer{
		UDPAddr:   addr,
		TCPAddr:   addr,
		Recursors: recursors,
		Store:     m.store,
	}
	if err := s.ListenAndServe(); err != nil {
		return err
	}
	m.dnsServer = s
	return nil
}

// openHTTPServer initializes and opens the HTTP server.
// The store must already be open.
func (m *Main) openHTTPServer(addr string) error {
	// Open HTTP API.
	ln, err := net.Listen("tcp4", addr)
	if err != nil {
		return err
	}
	m.httpListener = ln

	// Run HTTP server.
	h := server.NewHandler()
	h.Store = m.store
	go http.Serve(m.httpListener, h)

	return nil
}

// Notify sends a POST to notifyURL to let it know that addr is accessible.
func (m *Main) Notify(notifyURL, httpURL, dnsAddr string) {
	m.mu.Lock()
	if httpURL != "" {
		m.status.URL = httpURL
	}
	if dnsAddr != "" {
		m.status.DNS = dnsAddr
	}
	payload, _ := json.Marshal(m.status)
	m.mu.Unlock()

	res, err := http.Post(notifyURL, "application/json", bytes.NewReader(payload))
	if err != nil {
		m.logger.Printf("failed to notify: %s", err)
	} else {
		res.Body.Close()
	}
}

// MergeHostPort joins host to the port in portAddr.
func MergeHostPort(host, portAddr string) string {
	_, port, _ := net.SplitHostPort(portAddr)
	return net.JoinHostPort(host, port)
}

// ParseFlags parses the command line flags.
func (m *Main) ParseFlags(args ...string) (Options, error) {
	var opt Options
	var recursors string

	fs := flag.NewFlagSet("discoverd", flag.ContinueOnError)
	fs.SetOutput(m.Stderr)
	fs.StringVar(&opt.DataDir, "data-dir", "", "data directory")
	fs.StringVar(&opt.Host, "host", "", "advertised hostname")
	fs.StringVar(&opt.RaftAddr, "raft-addr", ":1110", "address to serve raft cluster from")
	fs.StringVar(&opt.HTTPAddr, "http-addr", ":1111", "address to serve HTTP API from")
	fs.StringVar(&opt.DNSAddr, "dns-addr", "", "address to service DNS from")
	fs.StringVar(&recursors, "recursors", "8.8.8.8,8.8.4.4", "upstream recursive DNS servers")
	fs.StringVar(&opt.Notify, "notify", "", "url to send webhook to after starting listener")
	fs.BoolVar(&opt.WaitNetDNS, "wait-net-dns", false, "start DNS server after host network is configured")
	fs.StringVar(&opt.Join, "join", "", "address of existing cluster to join")
	if err := fs.Parse(args); err != nil {
		return Options{}, err
	}

	// Split recursors into slice.
	if recursors != "" {
		opt.Recursors = strings.Split(recursors, ",")
	}

	// Validate options.
	if opt.DataDir == "" {
		return opt, errors.New("data directory required")
	} else if opt.Host == "" {
		return opt, errors.New("host required")
	}

	return opt, nil
}

// Options represents the command line options.
type Options struct {
	DataDir    string   // data directory
	Host       string   // hostname
	RaftAddr   string   // raft bind address
	HTTPAddr   string   // http bind address
	DNSAddr    string   // dns bind address
	Recursors  []string // dns recursors
	Notify     string   // notify URL
	WaitNetDNS bool     // wait for the network DNS
	Join       string   // host to join to
}
