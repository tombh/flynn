package main

import (
	"runtime"

	"github.com/codegangsta/cli"
	log "github.com/flynn/flynn/Godeps/_workspace/src/github.com/Sirupsen/logrus"
	"github.com/flynn/flynn/Godeps/_workspace/src/github.com/docker/libcontainer"
	_ "github.com/flynn/flynn/Godeps/_workspace/src/github.com/docker/libcontainer/nsenter"
)

var initCommand = cli.Command{
	Name:  "init",
	Usage: "runs the init process inside the namespace",
	Action: func(context *cli.Context) {
		log.SetLevel(log.DebugLevel)
		runtime.GOMAXPROCS(1)
		runtime.LockOSThread()
		factory, err := libcontainer.New("")
		if err != nil {
			fatal(err)
		}
		if err := factory.StartInitialization(3); err != nil {
			fatal(err)
		}
		panic("This line should never been executed")
	},
}
