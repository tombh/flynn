package server

import (
	"container/list"
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
	"github.com/flynn/flynn/pkg/stream"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
)

// DefaultTTL is the length of time after a heartbeat from an instance before it expires.
const DefaultTTL = 10 * time.Second

// DefaultServiceConfig is the default configuration for a service when one is not specified.
var DefaultServiceConfig = &discoverd.ServiceConfig{
	LeaderType: discoverd.LeaderTypeOldest,
}

var (
	ErrUnsetService = errors.New("discoverd: service name must not be empty")

	ErrInvalidService = errors.New("discoverd: service must be lowercase alphanumeric plus dash")

	ErrSendBlocked = errors.New("discoverd: channel send failed due to blocked receiver")

	ErrBindAddressRequired = errors.New("discoverd: bind address required")

	ErrAdvertiseRequired = errors.New("discoverd: advertised address required")
)

// Store represents a storage backend using the raft protocol.
type Store struct {
	mu          sync.RWMutex
	path        string // root store path
	raft        *raft.Raft
	transport   *raft.NetworkTransport
	peerStore   raft.PeerStore
	stableStore *raftboltdb.BoltStore

	data        *raftData
	subscribers map[string]*list.List

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

// NewStore returns an instance of Store.
func NewStore(path string) *Store {
	return &Store{
		path:               path,
		data:               newRaftData(),
		HeartbeatTimeout:   1000 * time.Millisecond,
		ElectionTimeout:    1000 * time.Millisecond,
		LeaderLeaseTimeout: 500 * time.Millisecond,
		CommitTimeout:      50 * time.Millisecond,
		Now:                time.Now,
	}
}

// Open starts the raft consensus and opens the store.
func (s *Store) Open() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Require bind address & advertise address.
	if s.BindAddress == "" {
		return ErrBindAddressRequired
	} else if s.Advertise == nil {
		return ErrAdvertiseRequired
	}

	// Create raft configuration.
	config := raft.DefaultConfig()
	config.HeartbeatTimeout = s.HeartbeatTimeout
	config.ElectionTimeout = s.ElectionTimeout
	config.LeaderLeaseTimeout = s.LeaderLeaseTimeout
	config.CommitTimeout = s.CommitTimeout
	config.EnableSingleNode = true // FIXME(benbjohnson): allow peers

	// Begin listening to TCP port.
	trans, err := raft.NewTCPTransport(s.BindAddress, s.Advertise, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return fmt.Errorf("opening tcp transport: %s", err)
	}
	s.transport = trans

	// Setup storage layers.
	s.peerStore = raft.NewJSONPeers(s.path, s.transport)
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(s.path, "raft.db"))
	if err != nil {
		return fmt.Errorf("stable store: %s", err)
	}
	s.stableStore = stableStore

	// Create the snapshot store.
	ss, err := raft.NewFileSnapshotStore(s.path, 2, os.Stderr)
	if err != nil {
		return fmt.Errorf("snapshot store: %s", err)
	}

	// Create raft log.
	r, err := raft.NewRaft(config, s, stableStore, stableStore, ss, s.peerStore, s.transport)
	if err != nil {
		return fmt.Errorf("raft: %s", err)
	}
	s.raft = r

	return nil
}

// ServiceNames returns a sorted list of existing service names.
func (s *Store) ServiceNames() []string {
	panic("not yet implemented") // FIXME(benbjohnson)
}

// AddService creates a service with a configuration.
// Returns an error if the service already exists.
func (s *Store) AddService(service string, config *discoverd.ServiceConfig) error {
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

	return s.raftApply(addServiceCommandType, cmd)
}

func (s *Store) applyAddServiceCommand(cmd []byte) error {
	var c addServiceCommand
	if err := json.Unmarshal(cmd, &c); err != nil {
		return err
	}

	// Verify that the service doesn't already exist.
	if s.data.Services[c.Service] != nil {
		return ServiceExistsError(c.Service)
	}

	// Create new named service with configuration.
	s.data.Services[c.Service] = c.Config

	return nil
}

// Config returns the configuration for service.
func (s *Store) Config(service string) *discoverd.ServiceConfig {
	panic("not yet implemented") // FIXME(benbjohnson): Return config
}

// RemoveService deletes the service from the store.
func (s *Store) RemoveService(service string) error {
	// Serialize command.
	cmd, err := json.Marshal(&removeServiceCommand{Service: service})
	if err != nil {
		return err
	}

	return s.raftApply(removeServiceCommandType, cmd)
}

func (s *Store) applyRemoveServiceCommand(cmd []byte) error {
	var c removeServiceCommand
	if err := json.Unmarshal(cmd, &c); err != nil {
		return err
	}

	// Verify that the service exists.
	if s.data.Services[c.Service] == nil {
		return NotFoundError{Service: c.Service}
	}

	// Remove the service.
	delete(s.data.Services, c.Service)

	// FIXME(benbjohnson): Broadcast EventKindDown for all instances on the service.

	return nil
}

func (s *Store) Instances(service string) []*discoverd.Instance {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.instances(service)
}

func (s *Store) instances(service string) []*discoverd.Instance {
	panic("not yet implemented") // FIXME(benbjohnson): Return service instances.
}

func (s *Store) AddInstance(service string, inst *discoverd.Instance) error {
	// Serialize command.
	cmd, err := json.Marshal(&addInstanceCommand{
		Service:  service,
		Instance: inst,
	})
	if err != nil {
		return err
	}

	return s.raftApply(addInstanceCommandType, cmd)
}

func (s *Store) applyAddInstanceCommand(cmd []byte) error {
	var c addInstanceCommand
	if err := json.Unmarshal(cmd, &c); err != nil {
		return err
	}

	// Verify that the service exists.
	if s.data.Services[c.Service] == nil {
		return NotFoundError{Service: c.Service}
	}

	// Save the instance data.
	if s.data.Instances[c.Service] == nil {
		s.data.Instances[c.Service] = make(map[string]instanceEntry)
	}
	s.data.Instances[c.Service][c.Instance.ID] = instanceEntry{
		Instance:   c.Instance,
		ExpiryTime: s.Now().Add(DefaultTTL),
	}

	// FIXME(benbjohnson): maybeSetLeader

	// FIXME(benbjohnson): Broadcast EventKindUp event if new instance.
	// FIXME(benbjohnson): Broadcast EventKindUpdate event if instance changed.

	// FIXME(benbjohnson): Broadcast leader.

	return nil
}

func (s *Store) RemoveInstance(service, id string) error {
	// Serialize command.
	cmd, err := json.Marshal(&removeInstanceCommand{
		Service: service,
		ID:      id,
	})
	if err != nil {
		return err
	}

	return s.raftApply(removeInstanceCommandType, cmd)
}

func (s *Store) applyRemoveInstanceCommand(cmd []byte) error {
	var c removeInstanceCommand
	if err := json.Unmarshal(cmd, &c); err != nil {
		return err
	}

	// Verify that the service exists.
	if s.data.Instances[c.Service] == nil {
		return NotFoundError{Service: c.Service}
	}

	// Remove instance data.
	delete(s.data.Instances[c.Service], c.ID)

	// FIXME(benbjohnson): Pick leader if service is not manual.
	// FIXME(benbjohnson): Broadcast EventKindDown event.
	// FIXME(benbjohnson): Broadcast leader change.

	return nil
}

// ServiceMeta returns the meta data for a service.
func (s *Store) ServiceMeta(service string) *discoverd.ServiceMeta {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.serviceMeta(service)
}

func (s *Store) serviceMeta(service string) *discoverd.ServiceMeta {
	return s.data.Metas[service]
}

func (s *Store) SetServiceMeta(service string, meta *discoverd.ServiceMeta) error {
	// Serialize command.
	cmd, err := json.Marshal(&setServiceMetaCommand{
		Service: service,
		Meta:    meta,
	})
	if err != nil {
		return err
	}

	return s.raftApply(setServiceMetaCommandType, cmd)
}

func (s *Store) applySetServiceMetaCommand(cmd []byte, index uint64) error {
	var c setServiceMetaCommand
	if err := json.Unmarshal(cmd, &c); err != nil {
		return err
	}

	// Verify that the service exists.
	service := s.data.Services[c.Service]
	if service == nil {
		return NotFoundError{Service: c.Service}
	}

	// If no index is provided then the meta should not be set.
	curr := s.data.Metas[c.Service]
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
	s.data.Metas[c.Service] = c.Meta

	// FIXME(benbjohnson): Broadcast EventKindServiceMeta event.

	return nil
}

// SetLeader manually sets the leader for a service.
func (s *Store) SetLeader(service, id string) error {
	// Serialize command.
	cmd, err := json.Marshal(&setLeaderCommand{
		Service: service,
		ID:      id,
	})
	if err != nil {
		return err
	}

	return s.raftApply(setLeaderCommandType, cmd)
}

func (s *Store) applySetLeaderCommand(cmd []byte) error {
	var c setLeaderCommand
	if err := json.Unmarshal(cmd, &c); err != nil {
		return err
	}

	s.data.Leaders[c.Service] = c.ID

	// FIXME(benbjohnson): Notify new leadership.

	return nil
}

func (s *Store) Leader(service string) *discoverd.Instance {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.leader(service)
}

func (s *Store) leader(service string) *discoverd.Instance {
	panic("not yet implemented") // FIXME(benbjohnson)
}

func (s *Store) Close() error {
	if s.transport != nil {
		s.transport.Close()
	}
	if s.stableStore != nil {
		s.stableStore.Close()
	}
	return nil
}

// raftApply joins typ and cmd and applies it to raft.
// This call blocks until the apply completes and returns the error.
func (s *Store) raftApply(typ byte, cmd []byte) error {
	// Join the command type and data into one message.
	buf := append([]byte{typ}, cmd...)

	// Apply to raft and receive an ApplyFuture back.
	f := s.raft.Apply(buf, 5*time.Second)
	if err := f.Error(); err != nil {
		return err
	} else if err, ok := f.Response().(error); ok {
		return err
	}

	return nil
}

func (s *Store) Apply(l *raft.Log) interface{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Require at least a "command type" header byte.
	if len(l.Data) == 0 {
		return errors.New("no log data found")
	}

	// Extract the command type and data.
	typ, cmd := l.Data[0], l.Data[1:]

	// Determine the command type by the first byte.
	switch typ {
	case addServiceCommandType:
		return s.applyAddServiceCommand(cmd)
	case removeServiceCommandType:
		return s.applyRemoveServiceCommand(cmd)
	case setServiceMetaCommandType:
		return s.applySetServiceMetaCommand(cmd, l.Index)
	case setLeaderCommandType:
		return s.applySetLeaderCommand(cmd)
	case addInstanceCommandType:
		return s.applyAddInstanceCommand(cmd)
	case removeInstanceCommandType:
		return s.applyRemoveInstanceCommand(cmd)
	default:
		return fmt.Errorf("invalid command type: %d", typ)
	}
}

// Snapshot implements raft.FSM.
func (s *Store) Snapshot() (raft.FSMSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	buf, err := json.Marshal(s.data)
	if err != nil {
		return nil, err
	}
	return &raftSnapshot{data: buf}, nil
}

// Restore implements raft.FSM.
func (s *Store) Restore(r io.ReadCloser) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	data := &raftData{}
	if err := json.NewDecoder(r).Decode(data); err != nil {
		return err
	}
	s.data = data
	return nil
}

// Subscribe creates a subscription to events on a given service.
func (s *Store) Subscribe(service string, sendCurrent bool, kinds discoverd.EventKind, ch chan *discoverd.Event) stream.Stream {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Create service subscription list if it doesn't exist yet.
	if _, ok := s.subscribers[service]; !ok {
		s.subscribers[service] = list.New()
	}

	// Create and add subscription.
	sub := &subscription{
		kinds:   kinds,
		ch:      ch,
		store:   s,
		service: service,
	}
	sub.el = s.subscribers[service].PushBack(sub)

	// Send current instances.
	if sendCurrent && kinds.Any(discoverd.EventKindUp) {
		for _, inst := range s.instances(service) {
			ch <- &discoverd.Event{
				Service:  service,
				Kind:     discoverd.EventKindUp,
				Instance: inst,
			}
			// TODO: add a timeout to sends so that clients can't slow things down too much
		}
	}

	// Send current leader.
	if leader := s.leader(service); sendCurrent && kinds&discoverd.EventKindLeader != 0 && leader != nil {
		ch <- &discoverd.Event{
			Service:  service,
			Kind:     discoverd.EventKindLeader,
			Instance: leader,
		}
	}

	// Send current service meta data.
	if meta := s.serviceMeta(service); sendCurrent && kinds.Any(discoverd.EventKindServiceMeta) && meta != nil {
		ch <- &discoverd.Event{
			Service:     service,
			Kind:        discoverd.EventKindServiceMeta,
			ServiceMeta: meta,
		}
	}

	// Send current service.
	if sendCurrent && kinds.Any(discoverd.EventKindCurrent) {
		ch <- &discoverd.Event{
			Service: service,
			Kind:    discoverd.EventKindCurrent,
		}
	}

	return sub
}

// Broadcast sends an event to all subscribers.
func (s *Store) Broadcast(event *discoverd.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Retrieve list of subscribers for the service.
	l, ok := s.subscribers[event.Service]
	if !ok {
		return
	}

	// Iterate over each subscriber in the list.
	for el := l.Front(); el != nil; el = el.Next() {
		sub := el.Value.(*subscription)

		// Skip if event type is not subscribed to.
		if sub.kinds&event.Kind == 0 {
			continue
		}

		// Send event to subscriber.
		// If subscriber is blocked then close it.
		select {
		case sub.ch <- event:
		default:
			// run in a goroutine as it requires a lock on subscribersMtx
			sub.err = ErrSendBlocked
			go sub.Close()
		}
	}
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

// raftData represents the root data structure for the raft store.
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

// subscription represents a listener to one or more kinds of events.
type subscription struct {
	kinds discoverd.EventKind
	ch    chan *discoverd.Event
	err   error

	// the following fields are used by Close to clean up
	el      *list.Element
	store   *Store
	service string
	closed  bool
}

func (s *subscription) Err() error { return s.err }

func (s *subscription) Close() error {
	go func() {
		// drain channel to prevent deadlocks
		for range s.ch {
		}
	}()

	s.close()
	return nil
}

func (s *subscription) close() {
	s.store.mu.Lock()
	defer s.store.mu.Unlock()

	if s.closed {
		return
	}

	l := s.store.subscribers[s.service]
	l.Remove(s.el)
	if l.Len() == 0 {
		delete(s.store.subscribers, s.service)
	}
	close(s.ch)

	s.closed = true
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

// ValidServiceName returns nil if service is valid. Otherwise returns an error.
func ValidServiceName(service string) error {
	// Blank service names are not allowed.
	if service == "" {
		return ErrUnsetService
	}

	// Service names must consist of the characters [a-z0-9-]
	for _, r := range service {
		if (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-' {
			return ErrInvalidService
		}
	}

	return nil
}
