package main

import (
	c "github.com/flynn/flynn/Godeps/_workspace/src/github.com/flynn/go-check"
)

type DiscoverdSuite struct {
	Helper
}

var _ = c.ConcurrentSuite(&DiscoverdSuite{})

func (s *DiscoverdSuite) TestMultiNodeRestart(t *c.C) {
	// FIXME: Retrieve image ID of discoverd: imageURIs["discoverd"]
	// FIXME: exec.Run 3 instances: image uri, args
}
