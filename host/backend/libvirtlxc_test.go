// +build linux

package backend

import (
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"

	. "github.com/flynn/flynn/Godeps/_workspace/src/github.com/flynn/go-check"
	zfs "github.com/flynn/flynn/Godeps/_workspace/src/github.com/mistifyio/go-zfs"
	"github.com/flynn/flynn/host/logmux"
	"github.com/flynn/flynn/host/types"
	"github.com/flynn/flynn/host/volume"
	"github.com/flynn/flynn/host/volume/manager"
	zfsVolume "github.com/flynn/flynn/host/volume/zfs"
	"github.com/flynn/flynn/pkg/random"
)

type LibvirtLXCSuite struct {
	id, runDir string
	backend    Backend
	job        *host.Job
	tty        io.ReadWriteCloser
}

var _ = Suite(&LibvirtLXCSuite{})

func (s *LibvirtLXCSuite) SetUpSuite(c *C) {
	if os.Getuid() != 0 {
		c.Skip("backend tests must be run as root")
	}

	var err error
	s.id = random.String(12)

	s.runDir, err = ioutil.TempDir("", fmt.Sprintf("flynn-test-%s.", s.id))
	c.Assert(err, IsNil)

	vdevFile := filepath.Join(s.runDir, fmt.Sprintf("flynn-test-zpool-%s.vdev", s.id))

	vman, err := volumemanager.New(
		filepath.Join(s.runDir, "volumes.bolt"),
		func() (volume.Provider, error) {
			return zfsVolume.NewProvider(&zfsVolume.ProviderConfig{
				DatasetName: fmt.Sprintf("flynn-test-zpool-%s", s.id),
				Make: &zfsVolume.MakeDev{
					BackingFilename: vdevFile,
					Size:            int64(math.Pow(2, float64(30))),
				},
				WorkingDir: filepath.Join(s.runDir, "zfs"),
			})
		})
	c.Assert(err, IsNil)

	pwd, err := os.Getwd()
	c.Assert(err, IsNil)

	state := NewState("test-host", filepath.Join(s.runDir, "host-state.bolt"))

	s.backend, err = New("libvirt-lxc", Config{
		State:    state,
		Manager:  vman,
		VolPath:  filepath.Join(s.runDir, "host-volumes"),
		LogPath:  filepath.Join(s.runDir, "host-logs"),
		InitPath: filepath.Join(pwd, "../bin/flynn-init"),
		Mux:      logmux.New(1000),
	})
	c.Assert(err, IsNil)

	s.job = &host.Job{
		ID: s.id,
		Artifact: host.Artifact{
			URI: "https://registry.hub.docker.com?name=busybox",
		},
		Config: host.ContainerConfig{
			Entrypoint:  []string{"/bin/sh", "-"},
			HostNetwork: true,
			TTY:         true,
			Stdin:       true,
		},
	}

	attachWait := make(chan struct{})
	state.AddAttacher(s.job.ID, attachWait)

	err = s.backend.Run(s.job, nil)
	c.Assert(err, IsNil)

	stdinr, stdinw := io.Pipe()
	stdoutr, stdoutw := io.Pipe()

	s.tty = struct {
		io.WriteCloser
		io.Reader
	}{stdinw, stdoutr}

	<-attachWait
	job := state.GetJob(s.job.ID)

	attached := make(chan struct{})
	attachReq := &AttachRequest{
		Job:      job,
		Height:   80,
		Width:    80,
		Attached: attached,
		Stdin:    stdinr,
		Stdout:   stdoutw,
	}

	go s.backend.Attach(attachReq)
	<-attached
}

func (s *LibvirtLXCSuite) TearDownSuite(c *C) {
	if os.Getuid() != 0 {
		return
	}

	c.Assert(s.backend.Stop(s.job.ID), IsNil)
	c.Assert(s.backend.Cleanup(), IsNil)

	zpool, err := zfs.GetZpool(fmt.Sprintf("flynn-test-zpool-%s", s.id))
	c.Assert(err, IsNil)

	err = zpool.Destroy()
	c.Assert(err, IsNil)
}
func (s *LibvirtLXCSuite) TestLibvirtBackendContainerinit(c *C) {
	s.tty.Write([]byte("file /.containerinit\n"))
	buf := make([]byte, 4096)

	n, err := s.tty.Read(buf)
	c.Assert(err, IsNil)

	fmt.Printf("%q\n", string(buf[:n]))
}

/*
func (s *LibvirtLXCSuite) TestLibvirtBackendContainerinit(c *C) {
	c.Assert(s.container, HasFile, "/.containerinit")
	c.Assert(s.container, HasFile, "/.containerenv")
	c.Assert(s.container, HasFile, "/.container-shared")
}

func (s *LibvirtLXCSuite) TestLibvirtBackendNetworking(c *C) {
	c.Assert(s.container, HasFile, "/etc/hosts")
	c.Assert(s.container, HasFile, "/etc/resolv.conf")
}

func (s *LibvirtLXCSuite) TestLibvirtLXCBackendDevices(c *C) {
	c.Assert(s.container, HasDevice, "/dev/console")

	// documented as available but not actually provided: https://libvirt.org/drvlxc.html#devnodes
	c.Assert(s.container, Not(HasDevice), "/dev/zero")
	c.Assert(s.container, Not(HasDevice), "/dev/null")
	c.Assert(s.container, Not(HasDevice), "/dev/full")
	c.Assert(s.container, Not(HasDevice), "/dev/random")
	c.Assert(s.container, Not(HasDevice), "/dev/urandom")
	c.Assert(s.container, Not(HasDevice), "/dev/stdin")
	c.Assert(s.container, Not(HasDevice), "/dev/stdout")
	c.Assert(s.container, Not(HasDevice), "/dev/fd")
	c.Assert(s.container, Not(HasDevice), "/dev/ptmx")

	time.Sleep(300 * time.Second)
}
*/
