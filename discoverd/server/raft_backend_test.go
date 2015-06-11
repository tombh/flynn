package server

import (
	"io/ioutil"
	"net"
	"time"

	. "github.com/flynn/flynn/Godeps/_workspace/src/github.com/flynn/go-check"
	"github.com/flynn/flynn/discoverd/client"
	hh "github.com/flynn/flynn/pkg/httphelper"
)

var _ = Suite(&RaftSuite{})

type RaftSuite struct {
	backend *raftBackend
}

func (s *RaftSuite) SetUpTest(c *C) {
	path, _ := ioutil.TempDir("", "raft-")
	addr, _ := net.ResolveTCPAddr("tcp", "localhost:20000")
	s.backend = NewRaftBackend(path, ":20000", addr).(*raftBackend)
	s.backend.HeartbeatTimeout = 50 * time.Millisecond
	s.backend.ElectionTimeout = 50 * time.Millisecond
	s.backend.LeaderLeaseTimeout = 50 * time.Millisecond
	s.backend.CommitTimeout = 5 * time.Millisecond
	c.Assert(s.backend.StartSync(), IsNil)

	// Wait for leadership.
	<-s.backend.raft.LeaderCh()
}

func (s *RaftSuite) TearDownTest(c *C) {
	if s.backend != nil {
		s.backend.Close()
	}
}

// Ensure the raft backend can add a service.
func (s *RaftSuite) TestAddService(c *C) {
	c.Assert(s.backend.AddService("new-service", &discoverd.ServiceConfig{LeaderType: discoverd.LeaderTypeManual}), IsNil)
	c.Assert(s.backend.Data.Services["new-service"], DeepEquals, &discoverd.ServiceConfig{LeaderType: discoverd.LeaderTypeManual})
}

// Ensure the raft backend can remove an existing service.
func (s *RaftSuite) TestRemoveService(c *C) {
	c.Assert(s.backend.AddService("service0", &discoverd.ServiceConfig{LeaderType: discoverd.LeaderTypeManual}), IsNil)
	c.Assert(s.backend.RemoveService("service0"), IsNil)
	c.Assert(s.backend.Data.Services["service0"], IsNil)
}

// Ensure the raft backend can set service metadata.
func (s *RaftSuite) TestSetServiceMeta(c *C) {
	c.Assert(s.backend.AddService("service0", &discoverd.ServiceConfig{LeaderType: discoverd.LeaderTypeManual}), IsNil)

	// Set initial meta with index=0
	c.Assert(s.backend.SetServiceMeta("service0", &discoverd.ServiceMeta{Data: []byte(`"foo"`), Index: 0}), IsNil)
	c.Assert(s.backend.Data.ServiceMetas["service0"], DeepEquals, &discoverd.ServiceMeta{Data: []byte(`"foo"`), Index: 3})

	// Update meta using current index.
	c.Assert(s.backend.SetServiceMeta("service0", &discoverd.ServiceMeta{Data: []byte(`"bar"`), Index: 3}), IsNil)
	c.Assert(s.backend.Data.ServiceMetas["service0"], DeepEquals, &discoverd.ServiceMeta{Data: []byte(`"bar"`), Index: 4})

	// Disallow update if index doesn't match previous.
	c.Assert(s.backend.SetServiceMeta("service0", &discoverd.ServiceMeta{Data: []byte(`"baz"`), Index: 100}), DeepEquals, hh.PreconditionFailedErr(`Service metadata for "service0" exists, but wrong index provided`))
	c.Assert(s.backend.Data.ServiceMetas["service0"], DeepEquals, &discoverd.ServiceMeta{Data: []byte(`"bar"`), Index: 4})
}
