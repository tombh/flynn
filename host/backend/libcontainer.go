// +build linux

package backend

import (
	"fmt"

	"github.com/flynn/flynn/host/logmux"
	"github.com/flynn/flynn/host/types"
	"github.com/flynn/flynn/host/volume/manager"
	"github.com/flynn/flynn/pinkerton"

	lc "github.com/flynn/flynn/Godeps/_workspace/src/github.com/docker/libcontainer"
)

func init() {
	backends["libcontainer"] = newLibcontainer
}

type libcontainer struct {
	factory lc.Factory
}

func newLibcontainer(state *State, vman *volumemanager.Manager, volPath, logPath, initPath string, mux *logmux.LogMux) (Backend, error) {
	factory, err := lc.New("/var/lib/flynn")
	if err != nil {
		return nil, err
	}

	pinkertonCtx, err := pinkerton.BuildContext("aufs", imageRoot)
	if err != nil {
		return nil, err
	}

	fmt.Printf("factory=%#v\n", factory)
	fmt.Printf("pinkertonCtx=%#v\n", pinkertonCtx)

	return &libcontainer{
		factory: factory,
	}, nil
}

func (l *libcontainer) Attach(req *AttachRequest) error {
	panic("TODO")
	return nil
}

func (l *libcontainer) Cleanup() error {
	panic("TODO")
	return nil
}

func (l *libcontainer) ConfigureNetworking(strategy NetworkStrategy, job string) (*NetworkInfo, error) {
	panic("TODO")
	return nil, nil
}

func (l *libcontainer) ResizeTTY(id string, height, width uint16) error {
	panic("TODO")
	return nil
}

func (l *libcontainer) Run(job *host.Job, runConfig *RunConfig) error {
	panic("TODO")
	return nil
}

func (l *libcontainer) Signal(id string, sig int) error {
	panic("TODO")
	return nil
}

func (c *libcontainer) Stop(id string) error {
	panic("TODO")
	return nil
}

func (l *libcontainer) UnmarshalState(jobs map[string]*host.ActiveJob, jobBackendStates map[string][]byte, backendGlobalState []byte) error {
	panic("TODO")
	return nil
}
