package server

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/flynn/flynn/Godeps/_workspace/src/github.com/julienschmidt/httprouter"
	"github.com/flynn/flynn/discoverd/client"
	hh "github.com/flynn/flynn/pkg/httphelper"
	"github.com/flynn/flynn/pkg/sse"
	"github.com/flynn/flynn/pkg/stream"
)

// StreamBufferSize is the size of the channel buffer used for event subscription.
const StreamBufferSize = 64 // TODO: Figure out how big this buffer should be.

// NewHandler returns a new instance of Handler.
func NewHandler() *Handler {
	h := &Handler{router: httprouter.New()}

	h.router.PUT("/services/:service", h.servePutService)
	h.router.DELETE("/services/:service", h.serveDeleteService)
	h.router.GET("/services/:service", h.serveGetService)

	h.router.PUT("/services/:service/meta", h.servePutServiceMeta)
	h.router.GET("/services/:service/meta", h.serveGetServiceMeta)

	h.router.PUT("/services/:service/instances/:instance_id", h.servePutInstance)
	h.router.DELETE("/services/:service/instances/:instance_id", h.serveDeleteInstance)
	h.router.GET("/services/:service/instances", h.serveGetInstances)

	h.router.PUT("/services/:service/leader", h.servePutLeader)
	h.router.GET("/services/:service/leader", h.serveGetLeader)

	h.router.POST("/raft/nodes", h.servePostRaftNodes)
	h.router.DELETE("/raft/nodes", h.serveDeleteRaftNodes)

	h.router.GET("/ping", h.servePing)
	h.router.GET(status.Path, status.SimpleHandler(func() error { return nil }))

	return h
}

// Handler represents an HTTP handler for the Store.
type Handler struct {
	router *httprouter.Router
	Store  interface {
		Leader() string
		AddService(service string, config *discoverd.ServiceConfig) error
		RemoveService(service string) error
		SetServiceMeta(service string, meta *discoverd.ServiceMeta) error
		ServiceMeta(service string) *discoverd.ServiceMeta
		AddInstance(service string, inst *discoverd.Instance) error
		RemoveInstance(service, id string) error
		Instances(service string) []*discoverd.Instance
		Config(service string) *discoverd.ServiceConfig
		SetServiceLeader(service, id string) error
		ServiceLeader(service string) *discoverd.Instance
		Subscribe(service string, sendCurrent bool, kinds discoverd.EventKind, ch chan *discoverd.Event) stream.Stream

		AddPeer(peer string) error
		RemovePeer(peer string) error
	}
}

// ServeHTTP handles HTTP requests and responds to the response writer.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	h.router.ServeHTTP(w, r)
}

// servePutService creates a service.
func (h *Handler) servePutService(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	// Retrieve the path parameter.
	service := params.ByName("service")
	if err := ValidServiceName(service); err != nil {
		hh.ValidationError(w, "", err.Error())
		return
	}

	// Read config from the request.
	config := &discoverd.ServiceConfig{}
	if err := hh.DecodeJSON(r, config); err != nil {
		hh.Error(w, err)
		return
	}

	// Add the service to the store.
	if err := h.Store.AddService(service, config); err == ErrNotLeader {
		h.redirectToLeader(w, r)
		return
	} else if IsServiceExists(err) {
		hh.ObjectExistsError(w, err.Error())
		return
	} else if err != nil {
		hh.Error(w, err)
		return
	}
}

// serveDeleteService removes a service from the store by name.
func (h *Handler) serveDeleteService(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	// Retrieve the path parameter.
	service := params.ByName("service")
	if err := ValidServiceName(service); err != nil {
		hh.ValidationError(w, "", err.Error())
		return
	}

	// Delete from the store.
	if err := h.Store.RemoveService(params.ByName("service")); err == ErrNotLeader {
		h.redirectToLeader(w, r)
		return
	} else if IsNotFound(err) {
		hh.ObjectNotFoundError(w, err.Error())
		return
	} else if err != nil {
		hh.Error(w, err)
		return
	}
}

// serveGetService streams service events to the client.
func (h *Handler) serveGetService(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	if strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
		h.serveStream(w, params, discoverd.EventKindAll)
		return
	}
}

// serveServiceMeta sets the metadata for a service.
func (h *Handler) servePutServiceMeta(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	// Read the metadata from the request.
	meta := &discoverd.ServiceMeta{}
	if err := hh.DecodeJSON(r, meta); err != nil {
		hh.Error(w, err)
		return
	}

	// Update the meta in the store.
	if err := h.Store.SetServiceMeta(params.ByName("service"), meta); err == ErrNotLeader {
		h.redirectToLeader(w, r)
		return
	} else if IsNotFound(err) {
		hh.ObjectNotFoundError(w, err.Error())
		return
	} else if err != nil {
		hh.Error(w, err)
		return
	}

	// Write meta back to response.
	hh.JSON(w, 200, meta)
}

// serveGetServiceMeta returns the metadata for a service.
func (h *Handler) serveGetServiceMeta(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	// Read path parameter.
	service := params.ByName("service")

	// Read meta from the store.
	meta := h.Store.ServiceMeta(service)
	if meta == nil {
		hh.ObjectNotFoundError(w, "service meta not found")
		return
	}

	// Write meta to the response.
	hh.JSON(w, 200, meta)
}

// servePutInstance adds an instance to a service.
func (h *Handler) servePutInstance(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	// Read path parameter.
	service := params.ByName("service")

	// Read instance from request.
	inst := &discoverd.Instance{}
	if err := json.NewDecoder(r.Body).Decode(inst); err != nil {
		hh.Error(w, err)
		return
	}

	// Ensure instance is valid.
	if err := inst.Valid(); err != nil {
		hh.ValidationError(w, "", err.Error())
		return
	}

	// Add instance to service in the store.
	if err := h.Store.AddInstance(service, inst); err == ErrNotLeader {
		h.redirectToLeader(w, r)
		return
	} else if IsNotFound(err) {
		hh.ObjectNotFoundError(w, err.Error())
		return
	} else if err != nil {
		hh.Error(w, err)
		return
	}
}

// serveDeleteInstance removes an instance from the store by name.
func (h *Handler) serveDeleteInstance(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	// Retrieve path parameters.
	service := params.ByName("service")
	instanceID := params.ByName("instance_id")

	// Remove instance from the store.
	if err := h.Store.RemoveInstance(service, instanceID); err == ErrNotLeader {
		h.redirectToLeader(w, r)
		return
	} else if IsNotFound(err) {
		hh.ObjectNotFoundError(w, err.Error())
		return
	} else if err != nil {
		hh.Error(w, err)
		return
	}
}

// serveGetInstances returns a list of all instances for a service.
func (h *Handler) serveGetInstances(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	// If the client is requesting a stream, then handle as a stream.
	if strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
		h.serveStream(w, params, discoverd.EventKindUp|discoverd.EventKindUpdate|discoverd.EventKindDown)
		return
	}

	// Otherwise read instances from the store.
	instances := h.Store.Instances(params.ByName("service"))
	if instances == nil {
		hh.ObjectNotFoundError(w, "service not found")
		return
	}

	// Write instances to the response.
	hh.JSON(w, 200, instances)
}

// servePutLeader sets the leader for a service.
func (h *Handler) servePutLeader(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	// Retrieve path parameters.
	service := params.ByName("service")

	// Check if the service allows manual leader election.
	config := h.Store.Config(service)
	if config == nil || config.LeaderType != discoverd.LeaderTypeManual {
		hh.ValidationError(w, "", "service leader election type is not manual")
		return
	}

	// Read instance from the request.
	inst := &discoverd.Instance{}
	if err := hh.DecodeJSON(r, inst); err != nil {
		hh.Error(w, err)
		return
	}

	// Manually set the leader on the service.
	if err := h.Store.SetServiceLeader(service, inst.ID); err == ErrNotLeader {
		h.redirectToLeader(w, r)
		return
	} else if err != nil {
		hh.Error(w, err)
		return
	}
}

// serveGetLeader returns the current leader for a service.
func (h *Handler) serveGetLeader(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	// Process as a stream if that's what the client wants.
	if strings.Contains(r.Header.Get("Accept"), "text/event-stream") {
		h.serveStream(w, params, discoverd.EventKindLeader)
		return
	}

	// Otherwise retrieve the current leader.
	service := params.ByName("service")
	leader := h.Store.ServiceLeader(service)
	if leader == nil {
		hh.ObjectNotFoundError(w, "no leader found")
		return
	}

	// Write leader to the response.
	hh.JSON(w, 200, leader)
}

// servePing returns a 200 OK.
func (h *Handler) servePing(w http.ResponseWriter, r *http.Request, params httprouter.Params) {}

// serveStream creates a subscription and streams out events in SSE format.
func (h *Handler) serveStream(w http.ResponseWriter, params httprouter.Params, kind discoverd.EventKind) {
	// Create a buffered channel to receive events.
	ch := make(chan *discoverd.Event, StreamBufferSize)

	// Subscribe to events on the store.
	service := params.ByName("service")
	stream := h.Store.Subscribe(service, true, kind, ch)

	// Create and serve an SSE stream.
	s := sse.NewStream(w, ch, nil)
	s.Serve()
	s.Wait()
	stream.Close()

	// Check if there was an error while closing.
	if err := stream.Err(); err != nil {
		s.CloseWithError(err)
	}
}

// servePostRaftNodes joins a node to the store cluster.
func (h *Handler) servePostRaftNodes(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	addr := r.FormValue("addr")
	if err := h.Store.AddPeer(addr); err == ErrNotLeader {
		h.redirectToLeader(w, r)
		return
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// serveDeleteRaftNodes removes a node to the store cluster.
func (h *Handler) serveDeleteRaftNodes(w http.ResponseWriter, r *http.Request, params httprouter.Params) {
	addr := r.FormValue("addr")
	if err := h.Store.RemovePeer(addr); err == ErrNotLeader {
		h.redirectToLeader(w, r)
		return
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

// redirectToLeader redirects the request to the current known leader.
func (h *Handler) redirectToLeader(w http.ResponseWriter, r *http.Request) {
	// Find the current leader.
	leader := h.Store.Leader()
	if leader == "" {
		hh.Error(w, ErrNoKnownLeader)
		return
	}

	// Redirect request to leader node.
	u := *r.URL
	u.Host = leader
	http.Redirect(w, r, u.String(), http.StatusTemporaryRedirect)
}
