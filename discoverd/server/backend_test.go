package server

import (
	"io/ioutil"
	"net"
	"testing"
	"time"

	. "github.com/flynn/flynn/Godeps/_workspace/src/github.com/flynn/go-check"
	"github.com/flynn/flynn/discoverd/client"
	hh "github.com/flynn/flynn/pkg/httphelper"
)

// Hook gocheck up to the "go test" runner
func Test(t *testing.T) { TestingT(t) }

var _ = Suite(&BackendSuite{})

type BackendSuite struct {
	backend *backend
}

func (s *BackendSuite) SetUpTest(c *C) {
	path, _ := ioutil.TempDir("", "raft-")
	addr, _ := net.ResolveTCPAddr("tcp", "localhost:20000")
	s.backend = NewBackend(path, ":20000", addr).(*backend)
	s.backend.HeartbeatTimeout = 50 * time.Millisecond
	s.backend.ElectionTimeout = 50 * time.Millisecond
	s.backend.LeaderLeaseTimeout = 50 * time.Millisecond
	s.backend.CommitTimeout = 5 * time.Millisecond
	c.Assert(s.backend.StartSync(), IsNil)

	// Wait for leadership.
	<-s.backend.raft.LeaderCh()
}

func (s *BackendSuite) TearDownTest(c *C) {
	if s.backend != nil {
		s.backend.Close()
	}
}

// Ensure the raft backend can add a service.
func (s *BackendSuite) TestAddService(c *C) {
	// Add a service.
	c.Assert(s.backend.AddService("service0", &discoverd.ServiceConfig{LeaderType: discoverd.LeaderTypeManual}), IsNil)

	// Validate that the data has been applied.
	c.Assert(s.backend.data.Services["service0"], DeepEquals, &discoverd.ServiceConfig{LeaderType: discoverd.LeaderTypeManual})
}

// Ensure the raft backend can remove an existing service.
func (s *BackendSuite) TestRemoveService(c *C) {
	// Add and remove the service.
	c.Assert(s.backend.AddService("service0", &discoverd.ServiceConfig{LeaderType: discoverd.LeaderTypeManual}), IsNil)
	c.Assert(s.backend.RemoveService("service0"), IsNil)

	// Validate that the service is actually removed.
	c.Assert(s.backend.data.Services["service0"], IsNil)
}

// Ensure the raft backend can set service metadata.
func (s *BackendSuite) TestSetServiceMeta(c *C) {
	c.Assert(s.backend.AddService("service0", &discoverd.ServiceConfig{LeaderType: discoverd.LeaderTypeManual}), IsNil)

	// Set initial meta with index=0
	c.Assert(s.backend.SetServiceMeta("service0", &discoverd.ServiceMeta{Data: []byte(`"foo"`), Index: 0}), IsNil)
	c.Assert(s.backend.data.Metas["service0"], DeepEquals, &discoverd.ServiceMeta{Data: []byte(`"foo"`), Index: 3})

	// Update meta using current index.
	c.Assert(s.backend.SetServiceMeta("service0", &discoverd.ServiceMeta{Data: []byte(`"bar"`), Index: 3}), IsNil)
	c.Assert(s.backend.data.Metas["service0"], DeepEquals, &discoverd.ServiceMeta{Data: []byte(`"bar"`), Index: 4})

	// Disallow update if index doesn't match previous.
	c.Assert(s.backend.SetServiceMeta("service0", &discoverd.ServiceMeta{Data: []byte(`"baz"`), Index: 100}), DeepEquals, hh.PreconditionFailedErr(`Service metadata for "service0" exists, but wrong index provided`))
	c.Assert(s.backend.data.Metas["service0"], DeepEquals, &discoverd.ServiceMeta{Data: []byte(`"bar"`), Index: 4})
}

// Ensure the raft backend can set the leader of a service.
func (s *BackendSuite) TestSetLeader(c *C) {
	// Add service and set the current leader.
	c.Assert(s.backend.AddService("service0", &discoverd.ServiceConfig{LeaderType: discoverd.LeaderTypeManual}), IsNil)
	c.Assert(s.backend.SetLeader("service0", "node0"), IsNil)

	// Validate that the service's leader has been set.
	c.Assert(s.backend.data.Leaders["service0"], Equals, "node0")
}

// Ensure the raft backend can add a new instance.
func (s *BackendSuite) TestAddInstance(c *C) {
	// Mock the current time so we can test it.
	now := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	s.backend.Now = func() time.Time { return now }

	// Add service and instance.
	c.Assert(s.backend.AddService("service0", &discoverd.ServiceConfig{LeaderType: discoverd.LeaderTypeManual}), IsNil)
	c.Assert(s.backend.AddInstance("service0", &discoverd.Instance{ID: "node0", Addr: "0.0.0.0:0"}), IsNil)

	// Validate that the instance map is created, contains data, and has a valid expiration.
	c.Assert(s.backend.data.Instances["service0"], NotNil)
	c.Assert(s.backend.data.Instances["service0"]["node0"], DeepEquals, instanceEntry{
		Instance:   &discoverd.Instance{ID: "node0", Addr: "0.0.0.0:0"},
		ExpiryTime: now.Add(10 * time.Second),
	})
}

// Ensure the raft backend can remove an existing instance.
func (s *BackendSuite) TestRemoveInstance(c *C) {
	// Add service and instance. Then remove it.
	c.Assert(s.backend.AddService("service0", &discoverd.ServiceConfig{LeaderType: discoverd.LeaderTypeManual}), IsNil)
	c.Assert(s.backend.AddInstance("service0", &discoverd.Instance{ID: "node0", Addr: "0.0.0.0:0"}), IsNil)
	c.Assert(s.backend.RemoveInstance("service0", "node0"), IsNil)

	// Validate that the instance has been removed.
	c.Assert(s.backend.data.Instances["service0"], NotNil)
	_, ok := s.backend.data.Instances["service0"]["node0"]
	c.Assert(ok, Equals, false)
}
