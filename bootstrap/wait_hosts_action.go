package bootstrap

import (
	"fmt"
	"log"
	"time"

	"github.com/flynn/flynn/pkg/cluster"
)

type WaitHostsAction struct{}

func init() {
	Register("wait-hosts", &WaitHostsAction{})
}

func (a *WaitHostsAction) Run(s *State) error {
	const waitMax = time.Minute
	const waitInterval = 500 * time.Millisecond

	hosts := make(map[*cluster.Host]struct{}, len(s.Hosts))
	for _, h := range s.Hosts {
		hosts[h] = struct{}{}
	}

	start := time.Now()
	up := 0
outer:
	for {
		for h := range hosts {
			status, err := h.GetStatus()
			log.Printf("wait-hosts: host: %q: status=%#v, err=%s", h.ID(), status, err)
			if err != nil {
				log.Printf("wait-hosts: ->continue")
				continue
			}
			if status.Network != nil && status.Network.Subnet != "" && status.Discoverd != nil && status.Discoverd.URL != "" {
				log.Printf("wait-hosts: ->up")
				delete(hosts, h)
				up++
			}
		}
		log.Printf("wait-hosts: min-hosts: %d", s.MinHosts)
		if up >= s.MinHosts {
			break outer
		}

		if time.Now().Sub(start) >= waitMax {
			return fmt.Errorf("bootstrap: timed out waiting for hosts to come up")
		}
		time.Sleep(waitInterval)
	}
	return nil
}
