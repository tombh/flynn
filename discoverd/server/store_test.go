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

var _ = Suite(&StoreSuite{})

type StoreSuite struct {
	store *Store
}

func (s *StoreSuite) SetUpTest(c *C) {
	path, _ := ioutil.TempDir("", "raft-")
	addr, _ := net.ResolveTCPAddr("tcp", "localhost:20000")
	s.store = NewStore(path, ":20000", addr)
	s.store.HeartbeatTimeout = 50 * time.Millisecond
	s.store.ElectionTimeout = 50 * time.Millisecond
	s.store.LeaderLeaseTimeout = 50 * time.Millisecond
	s.store.CommitTimeout = 5 * time.Millisecond
	c.Assert(s.store.StartSync(), IsNil)

	// Wait for leadership.
	<-s.store.raft.LeaderCh()
}

func (s *StoreSuite) TearDownTest(c *C) {
	if s.store != nil {
		s.store.Close()
	}
}

// Ensure the raft store can add a service.
func (s *StoreSuite) TestAddService(c *C) {
	// Add a service.
	c.Assert(s.store.AddService("service0", &discoverd.ServiceConfig{LeaderType: discoverd.LeaderTypeManual}), IsNil)

	// Validate that the data has been applied.
	c.Assert(s.store.data.Services["service0"], DeepEquals, &discoverd.ServiceConfig{LeaderType: discoverd.LeaderTypeManual})
}

// Ensure the raft store can remove an existing service.
func (s *StoreSuite) TestRemoveService(c *C) {
	// Add and remove the service.
	c.Assert(s.store.AddService("service0", &discoverd.ServiceConfig{LeaderType: discoverd.LeaderTypeManual}), IsNil)
	c.Assert(s.store.RemoveService("service0"), IsNil)

	// Validate that the service is actually removed.
	c.Assert(s.store.data.Services["service0"], IsNil)
}

// Ensure the raft store can set service metadata.
func (s *StoreSuite) TestSetServiceMeta(c *C) {
	c.Assert(s.store.AddService("service0", &discoverd.ServiceConfig{LeaderType: discoverd.LeaderTypeManual}), IsNil)

	// Set initial meta with index=0
	c.Assert(s.store.SetServiceMeta("service0", &discoverd.ServiceMeta{Data: []byte(`"foo"`), Index: 0}), IsNil)
	c.Assert(s.store.data.Metas["service0"], DeepEquals, &discoverd.ServiceMeta{Data: []byte(`"foo"`), Index: 3})

	// Update meta using current index.
	c.Assert(s.store.SetServiceMeta("service0", &discoverd.ServiceMeta{Data: []byte(`"bar"`), Index: 3}), IsNil)
	c.Assert(s.store.data.Metas["service0"], DeepEquals, &discoverd.ServiceMeta{Data: []byte(`"bar"`), Index: 4})

	// Disallow update if index doesn't match previous.
	c.Assert(s.store.SetServiceMeta("service0", &discoverd.ServiceMeta{Data: []byte(`"baz"`), Index: 100}), DeepEquals, hh.PreconditionFailedErr(`Service metadata for "service0" exists, but wrong index provided`))
	c.Assert(s.store.data.Metas["service0"], DeepEquals, &discoverd.ServiceMeta{Data: []byte(`"bar"`), Index: 4})
}

// Ensure the raft store can set the leader of a service.
func (s *StoreSuite) TestSetLeader(c *C) {
	// Add service and set the current leader.
	c.Assert(s.store.AddService("service0", &discoverd.ServiceConfig{LeaderType: discoverd.LeaderTypeManual}), IsNil)
	c.Assert(s.store.SetLeader("service0", "node0"), IsNil)

	// Validate that the service's leader has been set.
	c.Assert(s.store.data.Leaders["service0"], Equals, "node0")
}

// Ensure the raft store can add a new instance.
func (s *StoreSuite) TestAddInstance(c *C) {
	// Mock the current time so we can test it.
	now := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	s.store.Now = func() time.Time { return now }

	// Add service and instance.
	c.Assert(s.store.AddService("service0", &discoverd.ServiceConfig{LeaderType: discoverd.LeaderTypeManual}), IsNil)
	c.Assert(s.store.AddInstance("service0", &discoverd.Instance{ID: "node0", Addr: "0.0.0.0:0"}), IsNil)

	// Validate that the instance map is created, contains data, and has a valid expiration.
	c.Assert(s.store.data.Instances["service0"], NotNil)
	c.Assert(s.store.data.Instances["service0"]["node0"], DeepEquals, instanceEntry{
		Instance:   &discoverd.Instance{ID: "node0", Addr: "0.0.0.0:0"},
		ExpiryTime: now.Add(10 * time.Second),
	})
}

// Ensure the raft store can remove an existing instance.
func (s *StoreSuite) TestRemoveInstance(c *C) {
	// Add service and instance. Then remove it.
	c.Assert(s.store.AddService("service0", &discoverd.ServiceConfig{LeaderType: discoverd.LeaderTypeManual}), IsNil)
	c.Assert(s.store.AddInstance("service0", &discoverd.Instance{ID: "node0", Addr: "0.0.0.0:0"}), IsNil)
	c.Assert(s.store.RemoveInstance("service0", "node0"), IsNil)

	// Validate that the instance has been removed.
	c.Assert(s.store.data.Instances["service0"], NotNil)
	_, ok := s.store.data.Instances["service0"]["node0"]
	c.Assert(ok, Equals, false)
}
