package domain_migration

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/flynn/flynn/Godeps/_workspace/src/github.com/flynn/que-go"
	"github.com/flynn/flynn/Godeps/_workspace/src/gopkg.in/inconshreveable/log15.v2"
	"github.com/flynn/flynn/controller/client"
	ct "github.com/flynn/flynn/controller/types"
	"github.com/flynn/flynn/pkg/postgres"
	"github.com/flynn/flynn/pkg/tlscert"
	"github.com/flynn/flynn/router/types"
)

type context struct {
	db     *postgres.DB
	client *controller.Client
	logger log15.Logger
}

func JobHandler(db *postgres.DB, client *controller.Client, logger log15.Logger) func(*que.Job) error {
	return (&context{db, client, logger}).HandleDomainMigration
}

func (c *context) HandleDomainMigration(job *que.Job) (err error) {
	log := c.logger.New("fn", "HandleDomainMigration")
	log.Info("handling domain migration", "job_id", job.ID, "error_count", job.ErrorCount)

	var dm *ct.DomainMigration
	if err := json.Unmarshal(job.Args, &dm); err != nil {
		log.Error("error unmarshaling job", "err", err)
		return err
	}

	log = log.New("domain_migration", dm.ID)

	if err := c.db.QueryRow("SELECT old_domain, domain, old_tls_cert, tls_cert, created_at, finished_at FROM domain_migrations WHERE migration_id = $1", dm.ID).Scan(&dm.OldDomain, &dm.Domain, &dm.OldTLSCert, &dm.TLSCert, &dm.CreatedAt, &dm.FinishedAt); err != nil {
		log.Error("error fetching postgres record", "err", err)
		c.createEvent(dm, err)
		return err
	}

	if dm.FinishedAt != nil {
		// Already done
		return nil
	}

	// Generate TLS Cert if not already present
	var cert *tlscert.Cert
	if dm.TLSCert == nil {
		var err error
		cert, err = c.generateTLSCert(dm.ID, dm.Domain)
		if err != nil {
			log.Error("error generating TLS cert", "err", err)
			c.createEvent(dm, err)
			return err
		}
	} else {
		cert = dm.TLSCert
	}
	oldCert := dm.OldTLSCert

	if err := c.maybeDeployController(oldCert, cert, dm.OldDomain, dm.Domain); err != nil {
		log.Error("error deploying controller", "err", err)
		c.createEvent(dm, err)
		return err
	}

	if err := c.maybeDeployRouter(oldCert, cert, dm.OldDomain, dm.Domain); err != nil {
		log.Error("error deploying router", "err", err)
		c.createEvent(dm, err)
		return err
	}

	if err := c.maybeDeployDashboard(oldCert, cert, dm.OldDomain, dm.Domain); err != nil {
		log.Error("error deploying dashboard", "err", err)
		c.createEvent(dm, err)
		return err
	}

	if err := c.createMissingRoutes(oldCert, cert, dm.OldDomain, dm.Domain); err != nil {
		log.Error("error creating missing routes", "err", err)
		c.createEvent(dm, err)
		return err
	}

	if err := c.db.QueryRow("UPDATE domain_migrations SET finished_at = now() RETURNING finished_at").Scan(&dm.FinishedAt); err != nil {
		log.Error("error setting finished_at", "err", err)
		c.createEvent(dm, err)
		return err
	}

	log.Info("domain migration complete")

	c.createEvent(dm, nil)

	return nil
}

func (c *context) generateTLSCert(id, domain string) (*tlscert.Cert, error) {
	hosts := []string{
		domain,
		fmt.Sprintf("*.%s", domain),
	}
	cert, err := tlscert.Generate(hosts)
	if err != nil {
		return nil, err
	}
	if _, err := c.db.Query("UPDATE domain_migrations SET tls_cert = $1 WHERE migration_id = $2", cert, id); err != nil {
		return nil, err
	}
	return cert, nil
}

func dupRelease(release *ct.Release) *ct.Release {
	return &ct.Release{
		ArtifactID: release.ArtifactID,
		Env:        release.Env,
		Meta:       release.Meta,
		Processes:  release.Processes,
	}
}

func (c *context) waitForDeployment(app string) error {
	list, err := c.client.DeploymentList(app)
	if err != nil || len(list) == 0 {
		return err
	}
	d := list[0]
	if d.Status != "pending" && d.Status != "running" {
		return nil
	}

	events := make(chan *ct.Event)
	stream, err := c.client.StreamEvents(controller.StreamEventsOptions{
		AppID:       d.AppID,
		ObjectID:    d.ID,
		ObjectTypes: []ct.EventType{ct.EventTypeDeployment},
	}, events)
	if err != nil {
		return err
	}
	defer stream.Close()

	timeout := time.After(time.Minute)
	for {
		select {
		case event := <-events:
			var data *ct.DeploymentEvent
			if err := json.Unmarshal(event.Data, &data); err != nil {
				return err
			}
			if data.Status == "complete" {
				return nil
			}
			if data.Status == "failed" {
				return errors.New(data.Error)
			}
		case <-timeout:
			return errors.New("timed out waiting for deployment")
		}
	}
}

func (c *context) maybeDeployController(oldCert, cert *tlscert.Cert, oldDomain, newDomain string) error {
	const appName = "controller"
	if err := c.waitForDeployment(appName); err != nil {
		return err
	}
	release, err := c.client.GetAppRelease(appName)
	if err != nil {
		return err
	}
	if release.Env["DEFAULT_ROUTE_DOMAIN"] != oldDomain {
		return nil
	}
	release = dupRelease(release)
	release.Env["DEFAULT_ROUTE_DOMAIN"] = newDomain
	release.Env["CA_CERT"] = cert.CACert
	if err := c.client.CreateRelease(release); err != nil {
		return err
	}
	return c.client.DeployAppRelease(appName, release.ID)
}

func (c *context) maybeDeployRouter(oldCert, cert *tlscert.Cert, oldDomain, newDomain string) error {
	const appName = "router"
	if err := c.waitForDeployment(appName); err != nil {
		return err
	}
	release, err := c.client.GetAppRelease(appName)
	if err != nil {
		return err
	}
	if release.Env["TLSCERT"] != oldCert.Cert {
		return nil
	}
	release = dupRelease(release)
	release.Env["TLSCERT"] = cert.Cert
	release.Env["TLSKEY"] = cert.PrivateKey
	if err := c.client.CreateRelease(release); err != nil {
		return err
	}
	return c.client.DeployAppRelease(appName, release.ID)
}

func (c *context) maybeDeployDashboard(oldCert, cert *tlscert.Cert, oldDomain, newDomain string) error {
	const appName = "dashboard"
	if err := c.waitForDeployment(appName); err != nil {
		return err
	}
	release, err := c.client.GetAppRelease(appName)
	if err != nil {
		return err
	}
	if release.Env["DEFAULT_ROUTE_DOMAIN"] != oldDomain {
		return nil
	}
	release = dupRelease(release)
	release.Env["CA_CERT"] = cert.CACert
	release.Env["DEFAULT_ROUTE_DOMAIN"] = newDomain
	release.Env["CONTROLLER_DOMAIN"] = fmt.Sprintf("controller.%s", newDomain)
	release.Env["URL"] = fmt.Sprintf("dashboard.%s", newDomain)
	if err := c.client.CreateRelease(release); err != nil {
		return err
	}
	return c.client.DeployAppRelease(appName, release.ID)
}

func (c *context) createMissingRoutes(oldCert, cert *tlscert.Cert, oldDomain, newDomain string) error {
	apps, err := c.client.AppList()
	if err != nil {
		return err
	}
	errChan := make(chan error, len(apps))
	createMissingRoutes := func(app *ct.App) {
		errChan <- c.appCreateMissingRoutes(app.ID, oldCert, cert, oldDomain, newDomain)
	}
	for _, app := range apps {
		go createMissingRoutes(app)
	}
	defer close(errChan)
	for range apps {
		if err := <-errChan; err != nil {
			return err
		}
	}
	return nil
}

func (c *context) appCreateMissingRoutes(appID string, oldCert, cert *tlscert.Cert, oldDomain, newDomain string) error {
	routes, err := c.client.RouteList(appID)
	if err != nil {
		return err
	}
	for _, route := range routes {
		if route.Type != "http" {
			continue
		}
		if strings.HasSuffix(route.Domain, oldDomain) && route.TLSCert == oldCert.Cert {
			if err := c.appMaybeCreateRoute(appID, oldCert, cert, route, strings.TrimSuffix(route.Domain, oldDomain), newDomain, routes); err != nil {
				return err
			}
		}
	}
	return nil
}

func (c *context) appMaybeCreateRoute(appID string, oldCert, cert *tlscert.Cert, oldRoute *router.Route, prefix, domain string, routes []*router.Route) error {
	for _, route := range routes {
		if strings.HasPrefix(route.Domain, prefix) && strings.HasSuffix(route.Domain, domain) {
			// Route already exists
			return nil
		}
	}
	route := &router.Route{
		Type:    "http",
		Domain:  strings.Join([]string{prefix, domain}, ""),
		Sticky:  oldRoute.Sticky,
		Service: oldRoute.Service,
	}
	if oldRoute.TLSCert == oldCert.Cert {
		route.TLSCert = cert.Cert
		route.TLSKey = cert.PrivateKey
	} else {
		route.TLSCert = oldRoute.TLSCert
		route.TLSKey = oldRoute.TLSKey
	}
	err := c.client.CreateRoute(appID, route)
	if err != nil && err.Error() == "conflict: Duplicate route" {
		return nil
	}
	return err
}

func (c *context) createEvent(dm *ct.DomainMigration, err error) error {
	e := ct.DomainMigrationEvent{DomainMigration: dm}
	if err != nil {
		e.Error = err.Error()
	}
	query := "INSERT INTO events (object_id, object_type, data) VALUES ($1, $2, $3)"
	return c.db.Exec(query, dm.ID, string(ct.EventTypeDomainMigration), e)
}
