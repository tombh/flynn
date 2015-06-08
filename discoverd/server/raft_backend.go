package server

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/flynn/flynn/discoverd/client"
	"github.com/hashicorp/raft"
)

// raftBackend represents a storage backend using the raft protocol.
type raftBackend struct {
	Data struct {
		Services map[string]*discoverd.ServiceConfig `json:"services,omitempty"`
	}

	Raft interface {
		Apply(cmd []byte) error
		Close() error
	}
}

// NewRaftBackend returns an instance of RaftBackend.
func NewRaftBackend() Backend {
	b := &raftBackend{}
	b.Data.Services = make(map[string]*discoverd.ServiceConfig)

	// FIXME(benbjohnson): Configure raft.

	return b
}

// AddService creates a service with a configuration.
// Returns an error if the service already exists.
func (b *raftBackend) AddService(service string, config *discoverd.ServiceConfig) error {
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

	return b.Raft.Apply(cmd)
}

func (b *raftBackend) applyAddServiceCommand(cmd []byte) error {
	var c addServiceCommand
	if err := json.Unmarshal(cmd, &c); err != nil {
		return err
	}

	// Verify that the service doesn't already exist.
	if b.Data.Services[c.Service] != nil {
		return ServiceExistsError(c.Service)
	}

	// Create new named service with configuration.
	b.Data.Services[c.Service] = c.Config

	return nil
}

func (b *raftBackend) RemoveService(service string) error {
	panic("not yet implemented")
}

func (b *raftBackend) AddInstance(service string, inst *discoverd.Instance) error {
	panic("not yet implemented")
}

func (b *raftBackend) RemoveInstance(service, id string) error {
	panic("not yet implemented")
}

func (b *raftBackend) SetServiceMeta(service string, meta *discoverd.ServiceMeta) error {
	panic("not yet implemented")
}

func (b *raftBackend) SetLeader(service, id string) error {
	panic("not yet implemented")
}

func (b *raftBackend) StartSync() error {
	panic("not yet implemented")
}

func (b *raftBackend) Close() error {
	return b.Raft.Close()
}

func (b raftBackend) Apply(l *raft.Log) interface{} {
	// Require at least a "command type" header byte.
	if len(l.Data) == 0 {
		return errors.New("no log data found")
	}

	// Determine the command type by the first byte.
	switch l.Data[0] {
	case addServiceCommandType:
		return b.applyAddServiceCommand(l.Data[1:])
	default:
		return fmt.Errorf("invalid command type: %d", l.Data[0])
	}
}

// Command type header bytes.
const (
	addServiceCommandType = byte(0)
)

// addServiceCommand represents a command object to create a service.
type addServiceCommand struct {
	Service string
	Config  *discoverd.ServiceConfig
}
