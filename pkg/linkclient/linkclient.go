// Package linkclient keeps a channel instance's control link to core open
// (docs: contracts/channel-core-connections/): one WebSocket over which core
// asks the channel to reach bind minions. Core sends a call (deliver this
// sealed message to a target), the channel's Handler makes it, and the link
// answers each request by its id, in any order. The link reopens whenever it
// drops, backing off; requests waiting on a link that dropped are lost, and
// core treats them as failed calls.
package linkclient

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/logoscore/logos-golang-module-core/pkg/coretls"
	"github.com/logoscore/logos-golang-protocol/protocol"
)

const (
	// PingEvery is how often the channel pings core's end; a ping without
	// its pong within pingWait drops the link.
	PingEvery = 30 * time.Second
	pingWait  = 30 * time.Second
	// DefaultMaxCalls bounds the calls in progress at once.
	DefaultMaxCalls = 64
	// DefaultMinBackoff and DefaultMaxBackoff bound the wait before the
	// link opens again: it starts at the first and doubles up to the
	// second, and starts over once a link stayed up a minute.
	DefaultMinBackoff = time.Second
	DefaultMaxBackoff = 30 * time.Second
	stableAfter       = time.Minute
	// writeWait bounds writing one answer.
	writeWait = 30 * time.Second
	// DefaultOpenDeadline bounds an open core gave no deadline (the
	// contract's 30 seconds).
	DefaultOpenDeadline = 30 * time.Second
)

// Call is core's request to deliver a sealed message to a target.
type Call struct {
	// Request is core's id for it; the answer carries it back.
	Request uint64
	// Values are the build connection's built-in values and the target's,
	// by the keys the channel declared for the kind.
	Values map[string]any
	// ID is the id the message is sent under (a build id or a session id).
	ID [16]byte
	// Data is the sealed message.
	Data []byte
	// Deadline is how long core waits, 0 when it does not say: the
	// handler keeps to the shorter of it and its own timeout. The context
	// a handler gets ends at it.
	Deadline time.Duration
	// Keys are ID's keys, for a channel that acts as the minion (an
	// agentless kind): it opens Data and seals its answer with them. Nil
	// for any other kind: the channel relays a frame it cannot read.
	Keys *protocol.LinkKeys
	// Credentials are the credentials Values name by id (credential
	// fields), by id: protocol.FieldCredentials reads a field's.
	Credentials map[string]protocol.Credential
	// Acting, on a call to an agentless kind, is the task whose identity is
	// in effect for the session the call goes under (a become): its id,
	// command and args, credentials resolved. A channel acting as the
	// minion keeps nothing between calls, so it takes it up again from
	// here; nil when the session runs as its process.
	Acting *protocol.LinkActing
}

// Answer is what a call reached: exactly one of a minion message (ID and
// Data, unpacked from the channel's transport format), a Failure, or
// neither, when something answered that is not a minion message (Status
// and Excerpt: an HTTP 404, a WAF's page).
type Answer struct {
	ID      *[16]byte
	Data    []byte
	Status  int
	Failure *protocol.LinkFailure
	Excerpt []byte
	// Peer is the IP address the channel connected to, when it knows it
	// without a lookup of its own (dialed directly, or the target an IP
	// address): the minion's external address. Not sent with a failure.
	Peer string
	// Logins are how the credentials the channel tried fared, in the order
	// it tried them (a login refused, the one taken): core marks the one
	// that worked, and notes the refused ones on the target. With a
	// failure or without one.
	Logins []protocol.LinkLogin
}

// Fail is an Answer saying the call reached nothing.
func Fail(kind int, format string, args ...any) Answer {
	msg := fmt.Sprintf(format, args...)
	if len(msg) > protocol.MaxLinkFailureMsg {
		msg = strings.ToValidUTF8(msg[:protocol.MaxLinkFailureMsg], "")
	}
	return Answer{Failure: &protocol.LinkFailure{Kind: kind, Msg: msg}}
}

// Handler makes a call and says what came back. It runs in its own
// goroutine, several at once.
type Handler func(ctx context.Context, c Call) Answer

// Open is core's request to open a bind-persistent connection to a target.
type Open struct {
	// Request is core's id for it: the connection socket the channel opens
	// for the connection names it (connclient.Options.OpenRequest), and the
	// answer carries it back.
	Request uint64
	// Values are the build connection's built-in values and the target's.
	Values map[string]any
	// Deadline is how long core waits, 0 when it does not say; the
	// context an Opener gets ends at it.
	Deadline time.Duration
	// Under is the id the minion speaks under on the connection: its
	// target's open session's id, else its build's (protocol.LinkFrame
	// Under). Zero when core says none. A channel acting as the minion
	// (agentless) speaks under it first; one whose minion knows its own
	// ids leaves it.
	Under [16]byte
	// Keys are Under's keys, for a channel that acts as the minion (an
	// agentless kind); nil for any other kind.
	Keys *protocol.LinkKeys
	// Credentials are the credentials Values name by id, as a Call's.
	Credentials map[string]protocol.Credential
	// Acting is the session's acting task, as a Call's: what the
	// connection takes up again as it opens.
	Acting *protocol.LinkActing
}

// Opener dials a target and, once the minion's connection is up, opens its
// connection socket to core naming the request, and keeps relaying until
// either side closes. It answers when the socket is up (an Answer with
// nothing in it) or why it could not (a Fail). The connection lives on
// after the answer: an Opener starts what relays it and returns.
type Opener func(ctx context.Context, o Open) Answer

// Options are how a channel keeps its link.
type Options struct {
	// BaseURL is core's base URL (the sync call's): http(s)://host[:port].
	BaseURL string
	// Instance is the channel's registered instance id.
	Instance string
	// Handler makes the calls; required.
	Handler Handler
	// Opener opens bind-persistent connections; nil answers every open
	// with a failure (the channel carries no bind-persistent kind).
	Opener Opener
	// HTTPClient makes the handshake; when nil, module-core's
	// coretls.Client: core's TLS trust from the environment.
	HTTPClient *http.Client
	// MaxCalls bounds the calls in progress at once (DefaultMaxCalls when
	// zero); one more is answered with a failure at once.
	MaxCalls int
	// MinBackoff and MaxBackoff bound the wait between attempts to open
	// the link; the defaults when zero.
	MinBackoff, MaxBackoff time.Duration
	// Logger logs the link's ups and downs; slog.Default when nil.
	Logger *slog.Logger
	// OnState is told whenever the link opens (true) or drops (false); nil
	// for nobody.
	OnState func(up bool)
}

// ErrRefused is a handshake core answered with a status: not registered
// yet (403), a bad header (400).
var ErrRefused = errors.New("core refused the control link")

// Run keeps the link open until ctx ends, reopening it whenever it drops.
// Calls in progress when ctx ends are cancelled.
func Run(ctx context.Context, o Options) error {
	if o.Handler == nil {
		return errors.New("linkclient: no handler")
	}
	u, err := linkURL(o.BaseURL)
	if err != nil {
		return err
	}
	if o.MaxCalls <= 0 {
		o.MaxCalls = DefaultMaxCalls
	}
	if o.MinBackoff <= 0 {
		o.MinBackoff = DefaultMinBackoff
	}
	if o.MaxBackoff < o.MinBackoff {
		o.MaxBackoff = max(DefaultMaxBackoff, o.MinBackoff)
	}
	log := o.Logger
	if log == nil {
		log = slog.Default()
	}
	wait := o.MinBackoff
	for {
		opened := time.Now()
		err := runOnce(ctx, u, o, log)
		if ctx.Err() != nil {
			return nil
		}
		if time.Since(opened) > stableAfter {
			wait = o.MinBackoff
		}
		level := slog.LevelWarn
		if errors.Is(err, ErrRefused) {
			// Expected until the instance is registered.
			level = slog.LevelDebug
		}
		log.Log(ctx, level, "control link down, opening it again", "err", err, "in", wait.String())
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
		wait = min(wait*2, o.MaxBackoff)
	}
}

// runOnce opens the link and serves it until it drops.
func runOnce(ctx context.Context, u string, o Options, log *slog.Logger) error {
	h := http.Header{}
	h.Set(protocol.HeaderChannelInstance, o.Instance)
	hc := o.HTTPClient
	if hc == nil {
		// The process's client for core: its TLS trust from the
		// environment (module-core's coretls).
		var err error
		if hc, err = coretls.Client(); err != nil {
			return err
		}
	}
	ws, resp, err := websocket.Dial(ctx, u, &websocket.DialOptions{HTTPClient: hc, HTTPHeader: h})
	if resp != nil && resp.Body != nil {
		// The handshake's body says nothing the status does not.
		_ = resp.Body.Close()
	}
	if err != nil {
		if resp != nil && resp.StatusCode != http.StatusSwitchingProtocols {
			return fmt.Errorf("%w: %d", ErrRefused, resp.StatusCode)
		}
		return fmt.Errorf("control link: %w", err)
	}
	ws.SetReadLimit(protocol.MaxLinkFrameSize + 64)
	log.Info("control link up")
	if o.OnState != nil {
		o.OnState(true)
		defer o.OnState(false)
	}

	lctx, cancel := context.WithCancel(ctx)
	var calls sync.WaitGroup
	defer func() {
		cancel()
		calls.Wait()
		_ = ws.Close(websocket.StatusGoingAway, "")
	}()

	var writeMu sync.Mutex
	write := func(f protocol.LinkFrame) error {
		b, err := protocol.EncodeLinkFrame(f)
		if err != nil {
			return err
		}
		writeMu.Lock()
		defer writeMu.Unlock()
		wctx, wcancel := context.WithTimeout(lctx, writeWait)
		defer wcancel()
		return ws.Write(wctx, websocket.MessageBinary, b)
	}
	pingErr := make(chan error, 1)
	go func() {
		t := time.NewTicker(PingEvery)
		defer t.Stop()
		for {
			select {
			case <-lctx.Done():
				return
			case <-t.C:
				pctx, pcancel := context.WithTimeout(lctx, pingWait)
				err := ws.Ping(pctx)
				pcancel()
				if err != nil {
					pingErr <- fmt.Errorf("ping: %w", err)
					cancel()
					return
				}
			}
		}
	}()

	slots := make(chan struct{}, o.MaxCalls)
	for {
		mt, b, err := ws.Read(lctx)
		if err != nil {
			select {
			case perr := <-pingErr:
				return perr
			default:
			}
			return fmt.Errorf("control link: %w", err)
		}
		if mt != websocket.MessageBinary {
			return errors.New("control link: a text frame from core")
		}
		f, err := protocol.DecodeLinkFrame(b)
		if err != nil {
			// A frame that does not read cannot be answered by its id.
			return err
		}
		switch f.Type {
		case protocol.LinkCall:
		case protocol.LinkOpen:
			if o.Opener == nil {
				_ = write(protocol.LinkFrame{Type: protocol.LinkAnswer, Request: f.Request,
					Failure: &protocol.LinkFailure{Kind: protocol.LinkFailOther, Msg: "this channel does not open bind-persistent connections"}})
				continue
			}
		default:
			return fmt.Errorf("control link: a frame of type %d from core", f.Type)
		}
		select {
		case slots <- struct{}{}:
		default:
			if err := write(answerFrame(f.Request, Fail(protocol.LinkFailOther, "the channel has %d calls in progress", o.MaxCalls))); err != nil {
				return err
			}
			continue
		}
		calls.Add(1)
		go func(f protocol.LinkFrame) {
			defer func() { <-slots; calls.Done() }()
			deadline := time.Duration(f.Deadline) * time.Millisecond
			cctx := lctx
			if deadline > 0 {
				var ccancel context.CancelFunc
				cctx, ccancel = context.WithTimeout(lctx, deadline)
				defer ccancel()
			}
			var a Answer
			if f.Type == protocol.LinkOpen {
				// The connection outlives the link: the Opener's context is
				// the open's deadline only.
				wait := deadline
				if wait <= 0 {
					wait = DefaultOpenDeadline
				}
				octx, ocancel := context.WithTimeout(context.WithoutCancel(lctx), wait)
				op := Open{Request: f.Request, Values: f.Values, Deadline: deadline, Keys: f.Keys, Credentials: f.Credentials,
					Acting: f.Acting}
				copy(op.Under[:], f.Under)
				a = o.Opener(octx, op)
				ocancel()
			} else {
				c := Call{Request: f.Request, Values: f.Values, Data: f.Data, Deadline: deadline, Keys: f.Keys, Credentials: f.Credentials,
					Acting: f.Acting}
				copy(c.ID[:], f.ID)
				a = o.Handler(cctx, c)
			}
			if err := write(answerFrame(f.Request, a)); err != nil && lctx.Err() == nil {
				log.Warn("control link: answer not sent", "request", f.Request, "err", err)
			}
		}(f)
	}
}

// answerFrame is a's frame for request; an answer that would not encode
// (a handler's mistake) goes as a failure.
func answerFrame(request uint64, a Answer) protocol.LinkFrame {
	f := protocol.LinkFrame{Type: protocol.LinkAnswer, Request: request, Status: a.Status, Failure: a.Failure, Peer: a.Peer,
		Logins: a.Logins}
	switch {
	case a.Failure != nil:
		f.Status, f.Peer = 0, ""
	case a.ID != nil:
		f.ID, f.Data = a.ID[:], a.Data
	default:
		f.Excerpt = protocol.Excerpt(a.Excerpt)
		if len(f.Excerpt) == 0 {
			f.Excerpt = nil
		}
	}
	if err := f.Validate(); err != nil {
		return protocol.LinkFrame{Type: protocol.LinkAnswer, Request: request,
			Failure: &protocol.LinkFailure{Kind: protocol.LinkFailOther, Msg: "the channel's answer: " + err.Error()}}
	}
	if len(f.Logins) == 0 {
		f.Logins = nil
	}
	return f
}

// linkURL is core's control link endpoint under base, as a WebSocket URL.
func linkURL(base string) (string, error) {
	u, err := url.Parse(strings.TrimRight(base, "/"))
	if err != nil || u.Host == "" {
		return "", fmt.Errorf("core base URL %q", base)
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	default:
		return "", fmt.Errorf("core base URL %q: http or https", base)
	}
	u.Path = strings.TrimRight(u.Path, "/") + protocol.LinkPath
	return u.String(), nil
}
