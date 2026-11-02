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

// Options are how a channel keeps its link.
type Options struct {
	// BaseURL is core's base URL (the sync call's): http(s)://host[:port].
	BaseURL string
	// Instance is the channel's registered instance id.
	Instance string
	// Handler makes the calls; required.
	Handler Handler
	// HTTPClient makes the handshake; http.DefaultClient when nil.
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
	ws, resp, err := websocket.Dial(ctx, u, &websocket.DialOptions{HTTPClient: o.HTTPClient, HTTPHeader: h})
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
			_ = write(protocol.LinkFrame{Type: protocol.LinkAnswer, Request: f.Request,
				Failure: &protocol.LinkFailure{Kind: protocol.LinkFailOther, Msg: "this channel does not open bind-persistent connections"}})
			continue
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
			c := Call{Request: f.Request, Values: f.Values, Data: f.Data, Deadline: time.Duration(f.Deadline) * time.Millisecond}
			copy(c.ID[:], f.ID)
			cctx := lctx
			if c.Deadline > 0 {
				var ccancel context.CancelFunc
				cctx, ccancel = context.WithTimeout(lctx, c.Deadline)
				defer ccancel()
			}
			a := o.Handler(cctx, c)
			if err := write(answerFrame(f.Request, a)); err != nil && lctx.Err() == nil {
				log.Warn("control link: answer not sent", "request", f.Request, "err", err)
			}
		}(f)
	}
}

// answerFrame is a's frame for request; an answer that would not encode
// (a handler's mistake) goes as a failure.
func answerFrame(request uint64, a Answer) protocol.LinkFrame {
	f := protocol.LinkFrame{Type: protocol.LinkAnswer, Request: request, Status: a.Status, Failure: a.Failure}
	switch {
	case a.Failure != nil:
		f.Status = 0
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
