package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/flynn/flynn/discoverd/client"
	hh "github.com/flynn/flynn/pkg/httphelper"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

// DefaultTTL is the length of time after a heartbeat from an instance before it expires.
const DefaultTTL = 10 * time.Second

type Backend interface {
	AddService(service string, config *discoverd.ServiceConfig) error
	RemoveService(service string) error
	AddInstance(service string, inst *discoverd.Instance) error
	RemoveInstance(service, id string) error
	SetServiceMeta(service string, meta *discoverd.ServiceMeta) error
	SetLeader(service, id string) error
	StartSync() error
	Close() error
	Ping() error
}

// backend represents a storage backend using the raft protocol.
type backend struct {
	mu          sync.RWMutex
	path        string // root store path
	raft        *raft.Raft
	transport   *raft.NetworkTransport
	peerStore   raft.PeerStore
	stableStore *raftboltdb.BoltStore

	data *raftData

	// The address the raft TCP port binds to.
	BindAddress string

	// The address the raft server uses to represent itself in the peer list.
	Advertise net.Addr

	// Raft settings.
	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	LeaderLeaseTimeout time.Duration
	CommitTimeout      time.Duration

	// Returns the current time.
	// This defaults to time.Now and can be changed for mocking.
	Now func() time.Time
}

// NewBackend returns an instance of Backend.
func NewBackend(path, bindAddress string, advertise net.Addr) Backend {
	return &backend{
		path:               path,
		data:               newRaftData(),
		BindAddress:        bindAddress,
		Advertise:          advertise,
		HeartbeatTimeout:   1000 * time.Millisecond,
		ElectionTimeout:    1000 * time.Millisecond,
		LeaderLeaseTimeout: 500 * time.Millisecond,
		CommitTimeout:      50 * time.Millisecond,
		Now:                time.Now,
	}
}

// StartSync starts the raft server.
func (b *backend) StartSync() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Create raft configuration.
	config := raft.DefaultConfig()
	config.HeartbeatTimeout = b.HeartbeatTimeout
	config.ElectionTimeout = b.ElectionTimeout
	config.LeaderLeaseTimeout = b.LeaderLeaseTimeout
	config.CommitTimeout = b.CommitTimeout
	config.EnableSingleNode = true // FIXME(benbjohnson): allow peers

	// Begin listening to TCP port.
	trans, err := raft.NewTCPTransport(b.BindAddress, b.Advertise, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return fmt.Errorf("opening tcp transport: %s", err)
	}
	b.transport = trans

	// Setup storage layers.
	b.peerStore = raft.NewJSONPeers(b.path, b.transport)
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(b.path, "raft.db"))
	if err != nil {
		return fmt.Errorf("stable store: %s", err)
	}
	b.stableStore = stableStore

	// Create the snapshot store.
	ss, err := raft.NewFileSnapshotStore(b.path, 2, os.Stderr)
	if err != nil {
		return fmt.Errorf("snapshot store: %s", err)
	}

	// Create raft log.
	r, err := raft.NewRaft(config, b, stableStore, stableStore, ss, b.peerStore, b.transport)
	if err != nil {
		return fmt.Errorf("raft: %s", err)
	}
	b.raft = r

	return nil
}

// AddService creates a service with a configuration.
// Returns an error if the service already exists.
func (b *backend) AddService(service string, config *discoverd.ServiceConfig) error {
	if config == nil {
		config = DefaultServiceConfig
	}

	// Serialize command.
	cmd, err := json.Marshal(&addServiceCommand{
		Service: service,
		Config:  config,
	})
	if err != nil {
		return err
	}

	return b.raftApply(addServiceCommandType, cmd)
}

func (b *backend) applyAddServiceCommand(cmd []byte) error {
	var c addServiceCommand
	if err := json.Unmarshal(cmd, &c); err != nil {
		return err
	}

	// Verify that the service doesn't already exist.
	if b.data.Services[c.Service] != nil {
		return ServiceExistsError(c.Service)
	}

	// Create new named service with configuration.
	b.data.Services[c.Service] = c.Config

	return nil
}

// RemoveService deletes the service from the backend.
func (b *backend) RemoveService(service string) error {
	// Serialize command.
	cmd, err := json.Marshal(&removeServiceCommand{Service: service})
	if err != nil {
		return err
	}

	return b.raftApply(removeServiceCommandType, cmd)
}

func (b *backend) applyRemoveServiceCommand(cmd []byte) error {
	var c removeServiceCommand
	if err := json.Unmarshal(cmd, &c); err != nil {
		return err
	}

	// Verify that the service exists.
	if b.data.Services[c.Service] == nil {
		return NotFoundError{Service: c.Service}
	}

	// Remove the service.
	delete(b.data.Services, c.Service)

	return nil
}

func (b *backend) AddInstance(service string, inst *discoverd.Instance) error {
	// Serialize command.
	cmd, err := json.Marshal(&addInstanceCommand{
		Service:  service,
		Instance: inst,
	})
	if err != nil {
		return err
	}

	return b.raftApply(addInstanceCommandType, cmd)
}

func (b *backend) applyAddInstanceCommand(cmd []byte) error {
	var c addInstanceCommand
	if err := json.Unmarshal(cmd, &c); err != nil {
		return err
	}

	// Verify that the service exists.
	if b.data.Services[c.Service] == nil {
		return NotFoundError{Service: c.Service}
	}

	// Save the instance data.
	if b.data.Instances[c.Service] == nil {
		b.data.Instances[c.Service] = make(map[string]instanceEntry)
	}
	b.data.Instances[c.Service][c.Instance.ID] = instanceEntry{
		Instance:   c.Instance,
		ExpiryTime: b.Now().Add(DefaultTTL),
	}

	return nil
}

func (b *backend) RemoveInstance(service, id string) error {
	// Serialize command.
	cmd, err := json.Marshal(&removeInstanceCommand{
		Service: service,
		ID:      id,
	})
	if err != nil {
		return err
	}

	return b.raftApply(removeInstanceCommandType, cmd)
}

func (b *backend) applyRemoveInstanceCommand(cmd []byte) error {
	var c removeInstanceCommand
	if err := json.Unmarshal(cmd, &c); err != nil {
		return err
	}

	// Verify that the service exists.
	if b.data.Instances[c.Service] == nil {
		return NotFoundError{Service: c.Service}
	}

	// Remove instance data.
	delete(b.data.Instances[c.Service], c.ID)

	return nil
}

func (b *backend) SetServiceMeta(service string, meta *discoverd.ServiceMeta) error {
	// Serialize command.
	cmd, err := json.Marshal(&setServiceMetaCommand{
		Service: service,
		Meta:    meta,
	})
	if err != nil {
		return err
	}

	return b.raftApply(setServiceMetaCommandType, cmd)
}

func (b *backend) applySetServiceMetaCommand(cmd []byte, index uint64) error {
	var c setServiceMetaCommand
	if err := json.Unmarshal(cmd, &c); err != nil {
		return err
	}

	// Verify that the service exists.
	s := b.data.Services[c.Service]
	if s == nil {
		return NotFoundError{Service: c.Service}
	}

	// If no index is provided then the meta should not be set.
	curr := b.data.Metas[c.Service]
	if c.Meta.Index == 0 {
		if curr != nil {
			return hh.ObjectExistsErr(fmt.Sprintf("Service metadata for %q already exists, use index=n to set", c.Service))
		}
	} else {
		if curr == nil {
			return hh.PreconditionFailedErr(fmt.Sprintf("Service metadata for %q does not exist, use index=0 to set", c.Service))
		} else if curr.Index != c.Meta.Index {
			return hh.PreconditionFailedErr(fmt.Sprintf("Service metadata for %q exists, but wrong index provided", c.Service))
		}
	}

	// Update the meta and set the index.
	c.Meta.Index = index
	b.data.Metas[c.Service] = c.Meta

	return nil
}

// SetLeader manually sets the leader for a service.
func (b *backend) SetLeader(service, id string) error {
	// Serialize command.
	cmd, err := json.Marshal(&setLeaderCommand{
		Service: service,
		ID:      id,
	})
	if err != nil {
		return err
	}

	return b.raftApply(setLeaderCommandType, cmd)
}

func (b *backend) applySetLeaderCommand(cmd []byte) error {
	var c setLeaderCommand
	if err := json.Unmarshal(cmd, &c); err != nil {
		return err
	}

	b.data.Leaders[c.Service] = c.ID

	return nil
}

func (b *backend) Close() error {
	if b.transport != nil {
		b.transport.Close()
	}
	if b.stableStore != nil {
		b.stableStore.Close()
	}
	return nil
}

// raftApply joins typ and cmd and applies it to raft.
// This call blocks until the apply completes and returns the error.
func (b *backend) raftApply(typ byte, cmd []byte) error {
	// Join the command type and data into one message.
	buf := append([]byte{typ}, cmd...)

	// Apply to raft and receive an ApplyFuture back.
	f := b.raft.Apply(buf, 5*time.Second)
	if err := f.Error(); err != nil {
		return err
	} else if err, ok := f.Response().(error); ok {
		return err
	}

	return nil
}

func (b *backend) Apply(l *raft.Log) interface{} {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Require at least a "command type" header byte.
	if len(l.Data) == 0 {
		return errors.New("no log data found")
	}

	// Extract the command type and data.
	typ, cmd := l.Data[0], l.Data[1:]

	// Determine the command type by the first byte.
	switch typ {
	case addServiceCommandType:
		return b.applyAddServiceCommand(cmd)
	case removeServiceCommandType:
		return b.applyRemoveServiceCommand(cmd)
	case setServiceMetaCommandType:
		return b.applySetServiceMetaCommand(cmd, l.Index)
	case setLeaderCommandType:
		return b.applySetLeaderCommand(cmd)
	case addInstanceCommandType:
		return b.applyAddInstanceCommand(cmd)
	case removeInstanceCommandType:
		return b.applyRemoveInstanceCommand(cmd)
	default:
		return fmt.Errorf("invalid command type: %d", typ)
	}
}

// Snapshot implements raft.FSM.
func (b *backend) Snapshot() (raft.FSMSnapshot, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	buf, err := json.Marshal(b.data)
	if err != nil {
		return nil, err
	}
	return &raftSnapshot{data: buf}, nil
}

// Restore implements raft.FSM.
func (b *backend) Restore(r io.ReadCloser) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	data := &raftData{}
	if err := json.NewDecoder(r).Decode(data); err != nil {
		return err
	}
	b.data = data
	return nil
}

// raftSnapshot implements raft.FSMSnapshot.
// The FSM is serialized on snapshot creation so this simply writes to the sink.
type raftSnapshot struct {
	data []byte
}

// Persist writes the snapshot to the sink.
func (ss *raftSnapshot) Persist(sink raft.SnapshotSink) error {
	// Write data to sink.
	if _, err := sink.Write(ss.data); err != nil {
		sink.Cancel()
		return err
	}

	// Close and exit.
	return sink.Close()
}

// Release implements raft.FSMSnapshot. This is a no-op.
func (ss *raftSnapshot) Release() {}

// instanceEntry represents an instance with a TTL.
type instanceEntry struct {
	Instance   *discoverd.Instance
	ExpiryTime time.Time
}

// Command type header bytes.
const (
	addServiceCommandType     = byte(0)
	removeServiceCommandType  = byte(1)
	setServiceMetaCommandType = byte(2)
	setLeaderCommandType      = byte(3)
	addInstanceCommandType    = byte(4)
	removeInstanceCommandType = byte(5)
)

// addServiceCommand represents a command object to create a service.
type addServiceCommand struct {
	Service string
	Config  *discoverd.ServiceConfig
}

// removeServiceCommand represents a command object to delete a service.
type removeServiceCommand struct {
	Service string
}

// setServiceMetaCommand represents a command object to set meta on a service.
type setServiceMetaCommand struct {
	Service string
	Meta    *discoverd.ServiceMeta
}

// setLeaderCommand represents a command object to manually assign a leader to a service.
type setLeaderCommand struct {
	Service string
	ID      string
}

// addInstanceCommand represents a command object to add an instance.
type addInstanceCommand struct {
	Service  string
	Instance *discoverd.Instance
}

// removeInstanceCommand represents a command object to remove an instance.
type removeInstanceCommand struct {
	Service string
	ID      string
}

// raftData represents the root data structure for the raft backend.
type raftData struct {
	Services  map[string]*discoverd.ServiceConfig `json:"services,omitempty"`
	Metas     map[string]*discoverd.ServiceMeta   `json:"metas,omitempty"`
	Leaders   map[string]string                   `json:"leaders,omitempty"`
	Instances map[string]map[string]instanceEntry `json:"instances,omitempty"`
}

func newRaftData() *raftData {
	return &raftData{
		Services:  make(map[string]*discoverd.ServiceConfig),
		Metas:     make(map[string]*discoverd.ServiceMeta),
		Leaders:   make(map[string]string),
		Instances: make(map[string]map[string]instanceEntry),
	}
}

type NotFoundError struct {
	Service  string
	Instance string
}

func (e NotFoundError) Error() string {
	if e.Instance == "" {
		return fmt.Sprintf("discoverd: service %q not found", e.Service)
	}
	return fmt.Sprintf("discoverd: instance %s/%s not found", e.Service, e.Instance)
}

func IsNotFound(err error) bool {
	_, ok := err.(NotFoundError)
	return ok
}

type ServiceExistsError string

func (e ServiceExistsError) Error() string {
	return fmt.Sprintf("discoverd: service %q already exists", string(e))
}

func IsServiceExists(err error) bool {
	_, ok := err.(ServiceExistsError)
	return ok
}

type SyncHandler interface {
	AddService(service string, config *discoverd.ServiceConfig)
	RemoveService(service string)
	AddInstance(service string, inst *discoverd.Instance)
	RemoveInstance(service, id string)
	SetService(service string, config *discoverd.ServiceConfig, data []*discoverd.Instance)
	SetServiceMeta(service string, meta []byte, index uint64)
	SetLeader(service, id string)
	ListServices() []string
}
