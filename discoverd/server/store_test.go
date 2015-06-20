package server

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"net"
	"reflect"
	"testing"
	"time"

	. "github.com/flynn/flynn/Godeps/_workspace/src/github.com/flynn/go-check"
	"github.com/flynn/flynn/discoverd/client"
	hh "github.com/flynn/flynn/pkg/httphelper"
	"github.com/flynn/flynn/pkg/random"
)

// Hook gocheck up to the "go test" runner
func Test(t *testing.T) { TestingT(t) }

var _ = Suite(&StoreSuite{})

type StoreSuite struct {
	store *Store
}

func (s *StoreSuite) SetUpTest(c *C) {
	path, _ := ioutil.TempDir("", "raft-")
	s.store = NewStore(path)
	s.store.BindAddress = ":20000"
	s.store.Advertise, _ = net.ResolveTCPAddr("tcp", "localhost:20000")
	s.store.HeartbeatTimeout = 50 * time.Millisecond
	s.store.ElectionTimeout = 50 * time.Millisecond
	s.store.LeaderLeaseTimeout = 50 * time.Millisecond
	s.store.CommitTimeout = 5 * time.Millisecond
	c.Assert(s.store.Open(), IsNil)

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

func fakeInstance() *discoverd.Instance {
	octet := func() int { return random.Math.Intn(255) + 1 }
	inst := &discoverd.Instance{
		Addr:  fmt.Sprintf("%d.%d.%d.%d:%d", octet(), octet(), octet(), octet(), random.Math.Intn(65535)+1),
		Proto: "tcp",
		Meta:  map[string]string{"foo": "bar"},
	}
	inst.ID = md5sum(inst.Proto + "-" + inst.Addr)
	return inst
}

func assertHasInstance(c *C, list []*discoverd.Instance, want ...*discoverd.Instance) {
	for _, want := range want {
		for _, have := range list {
			if reflect.DeepEqual(have, want) {
				return
			}
		}
		c.Fatalf("couldn't find %#v in %#v", want, list)
	}
}

func assertNoEvent(c *C, events chan *discoverd.Event) {
	select {
	case e, ok := <-events:
		if !ok {
			c.Fatal("channel closed")
		}
		c.Fatalf("unexpected event %v %#v", e, e.Instance)
	default:
	}
}

func assertEvent(c *C, events chan *discoverd.Event, service string, kind discoverd.EventKind, instance *discoverd.Instance) {
	var event *discoverd.Event
	var ok bool
	select {
	case event, ok = <-events:
		if !ok {
			c.Fatal("channel closed")
		}
	case <-time.After(10 * time.Second):
		c.Fatalf("timed out waiting for %s %#v", kind, instance)
	}

	assertEventEqual(c, event, &discoverd.Event{
		Service:  service,
		Kind:     kind,
		Instance: instance,
	})
}

func assertMetaEvent(c *C, events chan *discoverd.Event, service string, meta *discoverd.ServiceMeta) {
	var event *discoverd.Event
	var ok bool
	select {
	case event, ok = <-events:
		if !ok {
			c.Fatal("channel closed")
		}
	case <-time.After(10 * time.Second):
		c.Fatalf("timed out waiting for meta event %s", string(meta.Data))
	}

	assertEventEqual(c, event, &discoverd.Event{
		Service:     service,
		Kind:        discoverd.EventKindServiceMeta,
		ServiceMeta: meta,
	})
}

func assertEventEqual(c *C, actual, expected *discoverd.Event) {
	c.Assert(actual.Service, Equals, expected.Service)
	c.Assert(actual.Kind, Equals, expected.Kind)
	if expected.Kind == discoverd.EventKindServiceMeta {
		c.Assert(actual.ServiceMeta.Data, DeepEquals, expected.ServiceMeta.Data)
	}
	if expected.Instance == nil {
		c.Assert(actual.Instance, IsNil)
		return
	}
	c.Assert(actual.ServiceMeta, IsNil)
	c.Assert(actual.Instance, NotNil)
	assertInstanceEqual(c, actual.Instance, expected.Instance)
}

func assertInstanceEqual(c *C, actual, expected *discoverd.Instance) {
	// zero out the index for comparison purposes
	eInst := *expected
	eInst.Index = 0
	aInst := *actual
	aInst.Index = 0
	// initialize internal cache fields
	eInst.Host()
	aInst.Host()
	c.Assert(aInst, DeepEquals, eInst)
}

func receiveEvents(c *C, events chan *discoverd.Event, count int) map[string][]*discoverd.Event {
	res := receiveSomeEvents(c, events, count)
	assertNoEvent(c, events)
	return res
}

func receiveSomeEvents(c *C, events chan *discoverd.Event, count int) map[string][]*discoverd.Event {
	res := make(map[string][]*discoverd.Event, count)
	for i := 0; i < count; i++ {
		select {
		case e := <-events:
			c.Logf("+ event %s", e)
			res[e.Instance.ID] = append(res[e.Instance.ID], e)
		case <-time.After(10 * time.Second):
			c.Fatalf("expected %d events, got %d", count, len(res))
		}
	}
	return res
}

func md5sum(data string) string {
	digest := md5.Sum([]byte(data))
	return hex.EncodeToString(digest[:])
}
