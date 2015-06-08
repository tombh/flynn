package server

import (
	"fmt"

	. "github.com/flynn/flynn/Godeps/_workspace/src/github.com/flynn/go-check"
	"github.com/flynn/flynn/discoverd/client"
)

var _ = Suite(&RaftSuite{})

type RaftSuite struct {
	backend  *raftBackend
	MockRaft MockRaft
}

func (s *RaftSuite) SetUpTest(c *C) {
	s.backend = NewRaftBackend().(*raftBackend)
	s.backend.Raft = &s.MockRaft
	s.MockRaft.CloseFn = func() error { return nil }
}

func (s *RaftSuite) TearDownTest(c *C) {
	if s.backend != nil {
		s.backend.Close()
	}
}

// Ensure the raft backend can add a service.
func (s *RaftSuite) TestAddService(c *C) {
	s.MockRaft.ApplyFn = func(cmd []byte) error {
		fmt.Println("APPLY!")
		return nil
	}
	c.Assert(s.backend.AddService("new-service", &discoverd.ServiceConfig{}), IsNil)
}

type MockRaft struct {
	ApplyFn func(cmd []byte) error
	CloseFn func() error
}

func (m *MockRaft) Apply(cmd []byte) error { return m.ApplyFn(cmd) }
func (m *MockRaft) Close() error           { return m.CloseFn() }
