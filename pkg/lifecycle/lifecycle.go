// Package lifecycle implements the module side of the module-registration
// (liveness) control-plane contract. A module is the RPC *client* for these
// operations and core is the RPC server: the module announces itself with
// module.register, proves liveness with module.heartbeat on an interval, and
// leaves cleanly with module.deregister on shutdown.
//
// The contract is specified in the docs repo under
// contracts/module-lifecycle.md and adr/0003-module-registration-lifecycle.md.
// Transport details (exchange logos.core.rpc, routing key core, direct reply-to)
// live in AMQPPublisher; the Client owns envelope construction and the liveness
// state machine and is transport-agnostic via the Publisher interface.
package lifecycle

import (
	"context"
	"encoding/json"
	"time"

	coreerrors "github.com/logoscore/logos-golang-channel-core/pkg/errors"
	protocol "github.com/logoscore/logos-golang-protocol/protocol"
)

// Operation type identifiers for the module lifecycle contract.
const (
	TypeRegister   = "module.register"
	TypeHeartbeat  = "module.heartbeat"
	TypeDeregister = "module.deregister"
)

// Status is a module's self-reported liveness state, sent on each heartbeat.
type Status string

const (
	StatusHealthy  Status = "healthy"
	StatusDegraded Status = "degraded"
	StatusDraining Status = "draining"
)

// Defaults applied when core's register ack omits or zeroes a liveness value.
const (
	defaultHeartbeatInterval = 30 * time.Second
	defaultGraceMisses       = 3
	defaultDeregisterTimeout = 5 * time.Second
)

// Identity is a module instance's static self-description, sent at registration.
// ModuleName is the hardcoded kind shared by every instance of a module (e.g.
// "http"); Instance is the unique id of one deployed instance and is the key
// core addresses for heartbeat, deregister, and management RPC.
type Identity struct {
	ModuleType  string // "channel" | "minion-factory"
	ModuleName  string // hardcoded module kind, e.g. "http"
	Instance    string // unique deployed-instance id, self-assigned
	Version     string // module build/version
	RPCQueue    string // queue core uses to call back into this module
	Description string // optional free text, surfaced on the admin Modules page
}

func (id Identity) validate() error {
	switch {
	case id.ModuleType == "":
		return coreerrors.New(coreerrors.CodeInvalidInput, "module_type is required")
	case id.ModuleName == "":
		return coreerrors.New(coreerrors.CodeInvalidInput, "module_name is required")
	case id.Instance == "":
		return coreerrors.New(coreerrors.CodeInvalidInput, "instance is required")
	case id.RPCQueue == "":
		return coreerrors.New(coreerrors.CodeInvalidInput, "rpc_queue is required")
	}
	return nil
}

// BootstrapConfig is the registration ack: liveness parameters plus the opaque
// per-module bootstrap config core returns.
type BootstrapConfig struct {
	HeartbeatInterval time.Duration
	GraceMisses       int
	Config            map[string]json.RawMessage
}

// HeartbeatResult is the outcome of a single heartbeat. When ConfigChanged is
// true the module should re-register to pull updated bootstrap config.
type HeartbeatResult struct {
	ConfigChanged bool
}

// Publisher transmits a control-plane RPC request envelope to core and returns
// the reply. Implementations own AMQP transport; the Client owns envelope
// construction and payload semantics. The Client serializes its calls, so a
// Publisher need not be safe for concurrent use.
type Publisher interface {
	Call(ctx context.Context, req protocol.Envelope) (protocol.ReplyEnvelope, error)
}

// MetricsFunc returns an optional lightweight liveness snapshot for a heartbeat.
type MetricsFunc func() map[string]any

// Client drives the module side of the lifecycle contract.
type Client struct {
	pub     Publisher
	id      Identity
	source  protocol.Source
	status  func() Status
	metrics MetricsFunc

	deregisterTimeout time.Duration
	// newTicker is injectable so the Run loop is deterministically testable.
	newTicker func(time.Duration) (<-chan time.Time, func())
}

// Option configures a Client.
type Option func(*Client)

// WithStatus sets the callback used to report heartbeat status. Without it the
// Client reports StatusHealthy.
func WithStatus(fn func() Status) Option {
	return func(c *Client) {
		if fn != nil {
			c.status = fn
		}
	}
}

// WithMetrics sets the callback supplying the optional heartbeat metrics
// snapshot.
func WithMetrics(fn MetricsFunc) Option {
	return func(c *Client) { c.metrics = fn }
}

// WithDeregisterTimeout overrides how long Run waits for the shutdown
// deregister RPC (default 5s).
func WithDeregisterTimeout(d time.Duration) Option {
	return func(c *Client) {
		if d > 0 {
			c.deregisterTimeout = d
		}
	}
}

// NewClient builds a lifecycle Client for id, transmitting over pub.
func NewClient(pub Publisher, id Identity, opts ...Option) *Client {
	c := &Client{
		pub:               pub,
		id:                id,
		source:            protocol.Source{Service: id.ModuleType, Instance: id.Instance},
		status:            func() Status { return StatusHealthy },
		deregisterTimeout: defaultDeregisterTimeout,
		newTicker: func(d time.Duration) (<-chan time.Time, func()) {
			t := time.NewTicker(d)
			return t.C, t.Stop
		},
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// --- wire payloads -------------------------------------------------------

type registerRequest struct {
	ModuleType  string `json:"module_type"`
	ModuleName  string `json:"module_name"`
	Instance    string `json:"instance"`
	Version     string `json:"version"`
	RPCQueue    string `json:"rpc_queue"`
	Description string `json:"description,omitempty"`
}

type registerReply struct {
	Instance              string                     `json:"instance"`
	Registered            bool                       `json:"registered"`
	HeartbeatIntervalSecs int                        `json:"heartbeat_interval_seconds"`
	HeartbeatGraceMisses  int                        `json:"heartbeat_grace_misses"`
	Config                map[string]json.RawMessage `json:"config"`
}

type heartbeatRequest struct {
	Instance string         `json:"instance"`
	Status   Status         `json:"status"`
	Metrics  map[string]any `json:"metrics,omitempty"`
}

type heartbeatReply struct {
	Instance      string `json:"instance"`
	Ack           bool   `json:"ack"`
	ConfigChanged bool   `json:"config_changed"`
}

type deregisterRequest struct {
	Instance string `json:"instance"`
	Reason   string `json:"reason,omitempty"`
}

// --- operations ----------------------------------------------------------

// Register announces this module instance and returns the bootstrap config from
// core's ack. Re-registration is idempotent on the core side (takeover).
func (c *Client) Register(ctx context.Context) (BootstrapConfig, error) {
	if err := c.id.validate(); err != nil {
		return BootstrapConfig{}, err
	}
	var rep registerReply
	err := c.call(ctx, TypeRegister, registerRequest{
		ModuleType:  c.id.ModuleType,
		ModuleName:  c.id.ModuleName,
		Instance:    c.id.Instance,
		Version:     c.id.Version,
		RPCQueue:    c.id.RPCQueue,
		Description: c.id.Description,
	}, &rep)
	if err != nil {
		return BootstrapConfig{}, err
	}
	return bootstrapFrom(rep), nil
}

// Heartbeat sends a single liveness beat and reports whether core signaled a
// config change.
func (c *Client) Heartbeat(ctx context.Context) (HeartbeatResult, error) {
	var metrics map[string]any
	if c.metrics != nil {
		metrics = c.metrics()
	}
	var rep heartbeatReply
	err := c.call(ctx, TypeHeartbeat, heartbeatRequest{
		Instance: c.id.Instance,
		Status:   c.status(),
		Metrics:  metrics,
	}, &rep)
	if err != nil {
		return HeartbeatResult{}, err
	}
	return HeartbeatResult{ConfigChanged: rep.ConfigChanged}, nil
}

// Deregister tells core to drop this instance from the active registry. reason
// is free-form (e.g. "shutdown", "redeploy").
func (c *Client) Deregister(ctx context.Context, reason string) error {
	return c.call(ctx, TypeDeregister, deregisterRequest{Instance: c.id.Instance, Reason: reason}, nil)
}

// Run registers, then heartbeats until ctx is cancelled, then deregisters
// gracefully. It re-registers when core reports the instance unknown
// (unknown_instance) or signals config_changed. Run returns the error that
// ended it; a clean shutdown via ctx cancellation returns nil.
func (c *Client) Run(ctx context.Context) error {
	boot, err := c.Register(ctx)
	if err != nil {
		return err
	}

	tick, stop := c.newTicker(boot.HeartbeatInterval)
	defer stop()

	for {
		select {
		case <-ctx.Done():
			c.gracefulDeregister()
			return nil
		case <-tick:
			hb, err := c.Heartbeat(ctx)
			if err != nil {
				// On an unknown instance, re-register and resume. Other errors
				// (transient transport, etc.) are retried on the next beat;
				// core declares an instance dead only after grace misses.
				if coreerrors.Code(err) == protocol.CodeUnknownInstance {
					_, _ = c.Register(ctx)
				}
				continue
			}
			if hb.ConfigChanged {
				_, _ = c.Register(ctx)
			}
		}
	}
}

// gracefulDeregister deregisters with a fresh, bounded context since Run's ctx
// is already cancelled by the time this runs.
func (c *Client) gracefulDeregister() {
	ctx, cancel := context.WithTimeout(context.Background(), c.deregisterTimeout)
	defer cancel()
	_ = c.Deregister(ctx, "shutdown")
}

// --- internals -----------------------------------------------------------

// call builds a request envelope of the given type, transmits it via the
// Publisher, maps an error reply to a typed error, and decodes a success
// payload into out (when non-nil).
func (c *Client) call(ctx context.Context, typ string, payload, out any) error {
	env, err := c.newRequest(typ, payload)
	if err != nil {
		return coreerrors.Wrap(coreerrors.CodeInternal, "marshal "+typ+" request", err)
	}
	reply, err := c.pub.Call(ctx, env)
	if err != nil {
		return err
	}
	if reply.Status == protocol.StatusError {
		code, msg := protocol.CodeInternalError, "lifecycle rpc failed"
		if reply.Error != nil {
			code, msg = reply.Error.Code, reply.Error.Message
		}
		return coreerrors.New(code, msg)
	}
	if out != nil {
		if err := json.Unmarshal(reply.Payload, out); err != nil {
			return coreerrors.Wrap(coreerrors.CodeInternal, "decode "+typ+" reply", err)
		}
	}
	return nil
}

// newRequest builds a control-plane request envelope. correlation_id equals
// message_id on an originating request, per the envelope contract.
func (c *Client) newRequest(typ string, payload any) (protocol.Envelope, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return protocol.Envelope{}, err
	}
	id := protocol.NewULID()
	return protocol.Envelope{
		MessageID:     id,
		CorrelationID: id,
		Type:          typ,
		Version:       protocol.EnvelopeVersionV1,
		Timestamp:     protocol.NowTimestamp(),
		Source:        c.source,
		Payload:       raw,
	}, nil
}

func bootstrapFrom(rep registerReply) BootstrapConfig {
	interval := defaultHeartbeatInterval
	if rep.HeartbeatIntervalSecs > 0 {
		interval = time.Duration(rep.HeartbeatIntervalSecs) * time.Second
	}
	grace := defaultGraceMisses
	if rep.HeartbeatGraceMisses > 0 {
		grace = rep.HeartbeatGraceMisses
	}
	return BootstrapConfig{HeartbeatInterval: interval, GraceMisses: grace, Config: rep.Config}
}
