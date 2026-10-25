// Package module runs a module's whole control-plane session with core: it
// connects to RabbitMQ, serves the management server (if any) on the module's
// rpc_queue, registers, heartbeats, and deregisters on shutdown — reconnecting
// with backoff whenever the broker connection or the initial register fails.
//
// It is plumbing only. What a module can be managed on (properties, actions,
// status values) is declared by the module through its mgmt.Server; this
// package never adds behavior of its own to that surface.
package module

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/logoscore/logos-golang-channel-core/v2/pkg/lifecycle"
	"github.com/logoscore/logos-golang-channel-core/v2/pkg/mgmt"
	protocol "github.com/logoscore/logos-golang-protocol/protocol"
	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	minBackoff = time.Second
	maxBackoff = 30 * time.Second
)

// RPCQueue is the conventional rpc_queue for instance: the queue core calls
// it back on, bound to mgmt.ChannelRPCExchange.
func RPCQueue(instance string) string { return mgmt.ChannelRPCExchange + "." + instance }

// Config describes one module instance's control-plane session.
type Config struct {
	// AMQPURL is the RabbitMQ URL core's control plane runs on.
	AMQPURL string
	// Identity is sent at registration. An empty RPCQueue defaults to
	// RPCQueue(Identity.Instance).
	Identity lifecycle.Identity
	// Management, when set, is served on the rpc_queue and declared at
	// registration. Nil runs lifecycle only; core then cannot manage the
	// instance.
	Management *mgmt.Server
	// Options are extra lifecycle options (e.g. WithStatus, WithMetrics).
	Options []lifecycle.Option
	// Logf receives session events (registered, rejected, reconnecting).
	// Defaults to log.Printf.
	Logf func(format string, args ...any)
}

// Run keeps the instance registered until ctx is cancelled, then deregisters
// and returns nil. It returns an error only for an invalid Config.
//
// lifecycle.Client gives up on a failed initial register and does not recover
// a dropped broker connection, so Run supervises it: every failure reconnects
// with exponential backoff (1s doubling to 30s, reset after a session that
// stayed up longer than that).
//
// Core rejects sync traffic from unregistered instances, so stop Run only
// after the module has drained its in-flight requests: give it a context of
// its own rather than the one that starts the shutdown.
func Run(ctx context.Context, cfg Config) error {
	if cfg.AMQPURL == "" {
		return errors.New("module: AMQPURL is required")
	}
	if cfg.Identity.RPCQueue == "" {
		cfg.Identity.RPCQueue = RPCQueue(cfg.Identity.Instance)
	}
	if cfg.Logf == nil {
		cfg.Logf = log.Printf
	}

	backoff := minBackoff
	for {
		started := time.Now()
		err := runSession(ctx, cfg)
		if ctx.Err() != nil {
			return nil
		}
		// A session that stayed up for a while was healthy; start over fast.
		if time.Since(started) > maxBackoff {
			backoff = minBackoff
		}
		cfg.Logf("control plane: %v; retrying in %s", err, backoff)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

// runSession runs one broker connection's worth of lifecycle. It returns nil
// only on ctx cancellation (after deregistering).
func runSession(ctx context.Context, cfg Config) error {
	id := cfg.Identity
	conn, err := amqp.Dial(cfg.AMQPURL)
	if err != nil {
		return fmt.Errorf("connect to RabbitMQ: %w", err)
	}
	defer conn.Close()

	pub, err := lifecycle.NewAMQPPublisher(conn)
	if err != nil {
		return fmt.Errorf("open lifecycle publisher: %w", err)
	}
	defer pub.Close()

	// Core may call the rpc_queue as soon as it acks the register, so the
	// consumer must be up first. It is closed after the deregister.
	opts := cfg.Options
	var mgmtDone <-chan struct{}
	if cfg.Management != nil {
		consumer, err := mgmt.NewAMQPConsumer(conn, id.RPCQueue, id.Instance, cfg.Management)
		if err != nil {
			return fmt.Errorf("serve management on %q: %w", id.RPCQueue, err)
		}
		defer consumer.Close()
		mgmtDone = consumer.Done()
		opts = append([]lifecycle.Option{lifecycle.WithManagement(cfg.Management)}, opts...)
	}

	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	connLost := make(chan error, 1)
	closed := conn.NotifyClose(make(chan *amqp.Error, 1))
	go func() {
		select {
		case e := <-closed:
			connLost <- fmt.Errorf("RabbitMQ connection closed: %v", e)
			cancel()
		case <-mgmtDone: // nil without management: never ready
			connLost <- fmt.Errorf("management consumer on %q stopped", id.RPCQueue)
			cancel()
		case <-sessionCtx.Done():
		}
	}()

	logged := loggingPublisher{pub: pub, instance: id.Instance, connClosed: conn.IsClosed, logf: cfg.Logf}
	if err := lifecycle.NewClient(logged, id, opts...).Run(sessionCtx); err != nil {
		return err
	}
	select {
	case err := <-connLost:
		return err
	default:
		return nil
	}
}

// loggingPublisher logs lifecycle outcomes. lifecycle.Client swallows heartbeat
// errors and exposes no hooks, so without this a rejected or unreachable
// instance would fail silently.
type loggingPublisher struct {
	pub      lifecycle.Publisher
	instance string
	// connClosed suppresses failures of the deregister lifecycle.Client
	// attempts after the connection is already gone; Run reports that loss.
	connClosed func() bool
	logf       func(format string, args ...any)
}

func (p loggingPublisher) Call(ctx context.Context, req protocol.Envelope) (protocol.ReplyEnvelope, error) {
	reply, err := p.pub.Call(ctx, req)
	switch {
	case err != nil:
		if ctx.Err() == nil && !p.connClosed() {
			p.logf("control plane: %s failed: %v", req.Type, err)
		}
	case reply.Status == protocol.StatusError:
		code := "unknown"
		if reply.Error != nil {
			code = reply.Error.Code + ": " + reply.Error.Message
		}
		p.logf("control plane: %s rejected by core (%s)", req.Type, code)
	case req.Type == lifecycle.TypeRegister:
		p.logf("control plane: registered with core as %q", p.instance)
	case req.Type == lifecycle.TypeDeregister:
		p.logf("control plane: deregistered %q", p.instance)
	}
	return reply, err
}
