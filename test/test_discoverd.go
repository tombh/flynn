package main

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	c "github.com/flynn/flynn/Godeps/_workspace/src/github.com/flynn/go-check"
	"github.com/flynn/flynn/appliance/postgresql/state"
	ct "github.com/flynn/flynn/controller/types"
	"github.com/flynn/flynn/discoverd/client"
	"github.com/flynn/flynn/pkg/postgres"
)

type DiscoverdSuite struct {
	Helper
}

var _ = c.ConcurrentSuite(&DiscoverdSuite{})

func (s *DiscoverdSuite) TestMultiNodeRestart(t *c.C) {
	// FIXME: Retrieve image ID of discoverd: imageURIs["discoverd"]
	// FIXME: exec.Run 3 instances: image uri, args
}
