package lifecycle

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	coreerrors "github.com/logoscore/logos-golang-channel-core/pkg/errors"
	protocol "github.com/logoscore/logos-golang-protocol/protocol"
)

func testIdentity() Identity {
	return Identity{
		ModuleType:  "channel",
		ModuleName:  "http",
		Instance:    "http-1",
		Version:     "1.2.0",
		RPCQueue:    "logos.channel.rpc.http-1",
		Description: "HTTP Logos channel",
	}
}

// fakePublisher records sent envelopes and replies via a scripted function.
type fakePublisher struct {
	mu      sync.Mutex
	sent    []protocol.Envelope
	replyFn func(req protocol.Envelope) (protocol.ReplyEnvelope, error)
}

func (f *fakePublisher) Call(_ context.Context, req protocol.Envelope) (protocol.ReplyEnvelope, error) {
	f.mu.Lock()
	f.sent = append(f.sent, req)
	fn := f.replyFn
	f.mu.Unlock()
	return fn(req)
}

func (f *fakePublisher) calls() []protocol.Envelope {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]protocol.Envelope, len(f.sent))
	copy(out, f.sent)
	return out
}

func (f *fakePublisher) hasType(t string) bool {
	for _, e := range f.calls() {
		if e.Type == t {
			return true
		}
	}
	return false
}

func okReply(t *testing.T, req protocol.Envelope, payload any) protocol.ReplyEnvelope {
	t.Helper()
	r, err := protocol.NewReply(req, protocol.Source{Service: "core", Instance: "core-1"}, payload)
	if err != nil {
		t.Fatalf("build reply: %v", err)
	}
	return r
}

func TestRegisterSuccessBuildsEnvelopeAndAppliesDefaults(t *testing.T) {
	fp := &fakePublisher{replyFn: func(req protocol.Envelope) (protocol.ReplyEnvelope, error) {
		// interval omitted -> client applies default
		return okReply(t, req, registerReply{Instance: "http-1", Registered: true}), nil
	}}
	c := NewClient(fp, testIdentity())

	boot, err := c.Register(context.Background())
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if boot.HeartbeatInterval != defaultHeartbeatInterval {
		t.Errorf("interval = %v, want default %v", boot.HeartbeatInterval, defaultHeartbeatInterval)
	}
	if boot.GraceMisses != defaultGraceMisses {
		t.Errorf("grace = %d, want %d", boot.GraceMisses, defaultGraceMisses)
	}

	sent := fp.calls()
	if len(sent) != 1 {
		t.Fatalf("sent %d envelopes, want 1", len(sent))
	}
	env := sent[0]
	if env.Type != TypeRegister {
		t.Errorf("type = %q, want %q", env.Type, TypeRegister)
	}
	if env.Version != protocol.EnvelopeVersionV1 {
		t.Errorf("version = %q, want %q", env.Version, protocol.EnvelopeVersionV1)
	}
	if env.MessageID == "" || env.CorrelationID != env.MessageID {
		t.Errorf("correlation_id %q must equal message_id %q", env.CorrelationID, env.MessageID)
	}
	if env.Source.Service != "channel" || env.Source.Instance != "http-1" {
		t.Errorf("source = %+v, want {channel http-1}", env.Source)
	}
	var rr registerRequest
	if err := json.Unmarshal(env.Payload, &rr); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if rr.ModuleName != "http" || rr.RPCQueue != "logos.channel.rpc.http-1" {
		t.Errorf("register payload = %+v", rr)
	}
}

func TestRegisterHonorsCoreProvidedInterval(t *testing.T) {
	fp := &fakePublisher{replyFn: func(req protocol.Envelope) (protocol.ReplyEnvelope, error) {
		return okReply(t, req, registerReply{
			Instance: "http-1", Registered: true,
			HeartbeatIntervalSecs: 15, HeartbeatGraceMisses: 5,
		}), nil
	}}
	boot, err := NewClient(fp, testIdentity()).Register(context.Background())
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if boot.HeartbeatInterval != 15*time.Second {
		t.Errorf("interval = %v, want 15s", boot.HeartbeatInterval)
	}
	if boot.GraceMisses != 5 {
		t.Errorf("grace = %d, want 5", boot.GraceMisses)
	}
}

func TestRegisterValidationFailsBeforePublish(t *testing.T) {
	fp := &fakePublisher{replyFn: func(req protocol.Envelope) (protocol.ReplyEnvelope, error) {
		t.Fatal("publisher should not be called when identity is invalid")
		return protocol.ReplyEnvelope{}, nil
	}}
	id := testIdentity()
	id.Instance = ""
	if _, err := NewClient(fp, id).Register(context.Background()); err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if len(fp.calls()) != 0 {
		t.Fatalf("publisher called %d times, want 0", len(fp.calls()))
	}
}

func TestErrorReplyMapsToTypedCode(t *testing.T) {
	fp := &fakePublisher{replyFn: func(req protocol.Envelope) (protocol.ReplyEnvelope, error) {
		return protocol.NewErrorReply(req, protocol.Source{Service: "core", Instance: "core-1"},
			protocol.CodeValidationFailed, "missing rpc_queue"), nil
	}}
	_, err := NewClient(fp, testIdentity()).Register(context.Background())
	if err == nil {
		t.Fatal("expected error reply to surface as error")
	}
	if got := coreerrors.Code(err); got != protocol.CodeValidationFailed {
		t.Errorf("code = %q, want %q", got, protocol.CodeValidationFailed)
	}
}

func TestHeartbeatSendsStatusAndMetrics(t *testing.T) {
	fp := &fakePublisher{replyFn: func(req protocol.Envelope) (protocol.ReplyEnvelope, error) {
		return okReply(t, req, heartbeatReply{Instance: "http-1", Ack: true, ConfigChanged: true}), nil
	}}
	c := NewClient(fp, testIdentity(),
		WithStatus(func() Status { return StatusDraining }),
		WithMetrics(func() map[string]any { return map[string]any{"profiles_enabled": 4} }),
	)

	res, err := c.Heartbeat(context.Background())
	if err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	if !res.ConfigChanged {
		t.Error("ConfigChanged = false, want true")
	}
	var hr heartbeatRequest
	if err := json.Unmarshal(fp.calls()[0].Payload, &hr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if hr.Status != StatusDraining {
		t.Errorf("status = %q, want draining", hr.Status)
	}
	if hr.Metrics["profiles_enabled"] != float64(4) {
		t.Errorf("metrics = %+v, want profiles_enabled=4", hr.Metrics)
	}
}

func TestDeregisterSendsReason(t *testing.T) {
	fp := &fakePublisher{replyFn: func(req protocol.Envelope) (protocol.ReplyEnvelope, error) {
		return okReply(t, req, map[string]any{"instance": "http-1", "deregistered": true}), nil
	}}
	if err := NewClient(fp, testIdentity()).Deregister(context.Background(), "redeploy"); err != nil {
		t.Fatalf("Deregister: %v", err)
	}
	var dr deregisterRequest
	if err := json.Unmarshal(fp.calls()[0].Payload, &dr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if dr.Reason != "redeploy" || dr.Instance != "http-1" {
		t.Errorf("deregister payload = %+v", dr)
	}
}

func TestRunRegistersHeartbeatsAndDeregisters(t *testing.T) {
	hbSeen := make(chan struct{}, 1)
	fp := &fakePublisher{}
	fp.replyFn = func(req protocol.Envelope) (protocol.ReplyEnvelope, error) {
		switch req.Type {
		case TypeRegister:
			return okReply(t, req, registerReply{Instance: "http-1", Registered: true, HeartbeatIntervalSecs: 1}), nil
		case TypeHeartbeat:
			select {
			case hbSeen <- struct{}{}:
			default:
			}
			return okReply(t, req, heartbeatReply{Instance: "http-1", Ack: true}), nil
		case TypeDeregister:
			return okReply(t, req, map[string]any{"deregistered": true}), nil
		}
		return protocol.ReplyEnvelope{}, fmt.Errorf("unexpected type %q", req.Type)
	}

	c := NewClient(fp, testIdentity())
	tickCh := make(chan time.Time)
	c.newTicker = func(time.Duration) (<-chan time.Time, func()) { return tickCh, func() {} }

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	tickCh <- time.Time{} // drive exactly one heartbeat
	select {
	case <-hbSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeat not observed")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run returned %v, want nil on clean shutdown", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after ctx cancel")
	}

	for _, typ := range []string{TypeRegister, TypeHeartbeat, TypeDeregister} {
		if !fp.hasType(typ) {
			t.Errorf("missing lifecycle op %q; sent=%v", typ, fp.calls())
		}
	}
}

func TestRunReregistersOnUnknownInstance(t *testing.T) {
	var mu sync.Mutex
	registers := 0
	reregistered := make(chan struct{}, 1)
	fp := &fakePublisher{}
	fp.replyFn = func(req protocol.Envelope) (protocol.ReplyEnvelope, error) {
		switch req.Type {
		case TypeRegister:
			mu.Lock()
			registers++
			n := registers
			mu.Unlock()
			if n >= 2 {
				select {
				case reregistered <- struct{}{}:
				default:
				}
			}
			return okReply(t, req, registerReply{Instance: "http-1", Registered: true, HeartbeatIntervalSecs: 1}), nil
		case TypeHeartbeat:
			// Core forgot us -> force a re-register.
			return protocol.NewErrorReply(req, protocol.Source{Service: "core", Instance: "core-1"},
				protocol.CodeUnknownInstance, "no active registration"), nil
		case TypeDeregister:
			return okReply(t, req, map[string]any{"deregistered": true}), nil
		}
		return protocol.ReplyEnvelope{}, fmt.Errorf("unexpected %q", req.Type)
	}

	c := NewClient(fp, testIdentity())
	tickCh := make(chan time.Time)
	c.newTicker = func(time.Duration) (<-chan time.Time, func()) { return tickCh, func() {} }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	tickCh <- time.Time{} // heartbeat -> unknown_instance -> re-register
	select {
	case <-reregistered:
	case <-time.After(2 * time.Second):
		t.Fatal("client did not re-register after unknown_instance")
	}
	cancel()
	<-done
}

// fakeAMQPChannel captures published messages for AMQPPublisher transport tests.
type fakeAMQPChannel struct {
	mu        sync.Mutex
	published []amqp.Publishing
	pubErr    error
}

func (f *fakeAMQPChannel) PublishWithContext(_ context.Context, _, _ string, _, _ bool, msg amqp.Publishing) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.pubErr != nil {
		return f.pubErr
	}
	f.published = append(f.published, msg)
	return nil
}

func (f *fakeAMQPChannel) Close() error { return nil }

func (f *fakeAMQPChannel) last() amqp.Publishing {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.published[len(f.published)-1]
}

func TestAMQPPublisherMatchesCorrelationAndSkipsStale(t *testing.T) {
	replies := make(chan amqp.Delivery, 2)
	fch := &fakeAMQPChannel{}
	p := &AMQPPublisher{ch: fch, replies: replies}

	req := protocol.Envelope{
		MessageID:     "req-1",
		CorrelationID: "req-1",
		Type:          TypeRegister,
		Version:       protocol.EnvelopeVersionV1,
		Source:        protocol.Source{Service: "channel", Instance: "http-1"},
		Payload:       json.RawMessage(`{}`),
	}
	want, err := protocol.NewReply(req, protocol.Source{Service: "core", Instance: "core-1"},
		registerReply{Instance: "http-1", Registered: true})
	if err != nil {
		t.Fatal(err)
	}
	wantBody, _ := json.Marshal(want)

	// A stale reply for a different correlation id, then the real one.
	replies <- amqp.Delivery{CorrelationId: "other", Body: []byte(`{"status":"ok"}`)}
	replies <- amqp.Delivery{CorrelationId: "req-1", Body: wantBody}

	got, err := p.Call(context.Background(), req)
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if got.CorrelationID != "req-1" || got.Status != protocol.StatusOK {
		t.Errorf("reply = %+v, want correlation req-1 status ok", got)
	}

	pub := fch.last()
	if pub.ReplyTo != directReplyTo {
		t.Errorf("reply_to = %q, want %q", pub.ReplyTo, directReplyTo)
	}
	if pub.CorrelationId != "req-1" || pub.Type != TypeRegister {
		t.Errorf("publishing props = {corr:%q type:%q}", pub.CorrelationId, pub.Type)
	}
}

func TestAMQPPublisherHonorsContextCancel(t *testing.T) {
	p := &AMQPPublisher{ch: &fakeAMQPChannel{}, replies: make(chan amqp.Delivery)}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := p.Call(ctx, protocol.Envelope{CorrelationID: "x"})
	if err == nil {
		t.Fatal("expected context error, got nil")
	}
}

func TestAMQPPublisherPublishError(t *testing.T) {
	p := &AMQPPublisher{ch: &fakeAMQPChannel{pubErr: fmt.Errorf("broker down")}, replies: make(chan amqp.Delivery)}
	if _, err := p.Call(context.Background(), protocol.Envelope{CorrelationID: "x"}); err == nil {
		t.Fatal("expected publish error, got nil")
	}
}

func TestAMQPPublisherReplyChannelClosed(t *testing.T) {
	replies := make(chan amqp.Delivery)
	close(replies)
	p := &AMQPPublisher{ch: &fakeAMQPChannel{}, replies: replies}
	if _, err := p.Call(context.Background(), protocol.Envelope{CorrelationID: "x"}); err == nil {
		t.Fatal("expected error when reply channel closed, got nil")
	}
}

func TestAMQPPublisherCloseNilSafe(t *testing.T) {
	if err := (&AMQPPublisher{}).Close(); err != nil {
		t.Fatalf("Close on zero publisher: %v", err)
	}
	if err := (&AMQPPublisher{ch: &fakeAMQPChannel{}}).Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestIdentityValidate(t *testing.T) {
	base := testIdentity()
	cases := map[string]func(*Identity){
		"missing module_type": func(id *Identity) { id.ModuleType = "" },
		"missing module_name": func(id *Identity) { id.ModuleName = "" },
		"missing instance":    func(id *Identity) { id.Instance = "" },
		"missing rpc_queue":   func(id *Identity) { id.RPCQueue = "" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			id := base
			mutate(&id)
			if err := id.validate(); err == nil {
				t.Errorf("%s: expected validation error", name)
			}
		})
	}
	if err := base.validate(); err != nil {
		t.Errorf("valid identity rejected: %v", err)
	}
}

func TestWithDeregisterTimeout(t *testing.T) {
	c := NewClient(&fakePublisher{}, testIdentity(), WithDeregisterTimeout(2*time.Second))
	if c.deregisterTimeout != 2*time.Second {
		t.Errorf("deregisterTimeout = %v, want 2s", c.deregisterTimeout)
	}
	// Non-positive is ignored.
	c2 := NewClient(&fakePublisher{}, testIdentity(), WithDeregisterTimeout(0))
	if c2.deregisterTimeout != defaultDeregisterTimeout {
		t.Errorf("deregisterTimeout = %v, want default", c2.deregisterTimeout)
	}
}
