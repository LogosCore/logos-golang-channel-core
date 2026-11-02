// Package connclient opens connection sockets to core (docs:
// contracts/channel-core-connections/): one WebSocket for each persistent
// minion connection a channel holds, over which the connection's messages go
// both ways, core sending when it has something.
package connclient

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/logoscore/logos-golang-protocol/protocol"
)

const (
	// PingEvery is how often the channel pings core's end.
	PingEvery = 30 * time.Second
	// pingWait bounds a ping's pong.
	pingWait = 30 * time.Second
	// DefaultMaxFrame caps a frame core sends: a message and a frame's
	// overhead.
	DefaultMaxFrame int64 = 16<<20 + 64
)

// Options are how a channel opens a connection socket.
type Options struct {
	// BaseURL is core's base URL (the sync call's): http(s)://host[:port].
	BaseURL string
	// Instance is the channel's registered instance id.
	Instance string
	// Kind is the connection's kind: protocol.KindReversePersistent, or
	// protocol.KindBindPersistent for one the channel dialed.
	Kind string
	// Peer is the minion's address as the channel saw it; "" for none.
	Peer string
	// OpenRequest is the control link's open this socket answers (a
	// bind-persistent connection's: the request id core gave it).
	OpenRequest uint64
	// HTTPClient makes the handshake; http.DefaultClient when nil.
	HTTPClient *http.Client
	// MaxFrame caps a frame read from core; DefaultMaxFrame when zero.
	MaxFrame int64
}

// Conn is an open connection socket.
type Conn struct {
	ws     *websocket.Conn
	cancel context.CancelFunc
	once   sync.Once
	done   chan struct{}
	err    error
}

// ErrRefused is a handshake core answered with a status: not registered
// (403), a bad header (400), core's registry unavailable (503).
var ErrRefused = errors.New("core refused the connection socket")

// Dial opens a connection socket to core and keeps pinging it until it
// closes.
func Dial(ctx context.Context, o Options) (*Conn, error) {
	u, err := socketURL(o.BaseURL)
	if err != nil {
		return nil, err
	}
	h := http.Header{}
	h.Set(protocol.HeaderChannelInstance, o.Instance)
	h.Set(protocol.HeaderConnectionKind, o.Kind)
	if o.Peer != "" {
		h.Set(protocol.HeaderPeerAddress, o.Peer)
	}
	if o.OpenRequest != 0 {
		h.Set(protocol.HeaderOpenRequest, strconv.FormatUint(o.OpenRequest, 10))
	}
	ws, resp, err := websocket.Dial(ctx, u, &websocket.DialOptions{HTTPClient: o.HTTPClient, HTTPHeader: h})
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	if err != nil {
		if resp != nil && resp.StatusCode != http.StatusSwitchingProtocols {
			return nil, fmt.Errorf("%w: %d", ErrRefused, resp.StatusCode)
		}
		return nil, fmt.Errorf("connection socket: %w", err)
	}
	max := o.MaxFrame
	if max <= 0 {
		max = DefaultMaxFrame
	}
	ws.SetReadLimit(max)
	pctx, cancel := context.WithCancel(context.Background())
	c := &Conn{ws: ws, cancel: cancel, done: make(chan struct{})}
	go c.ping(pctx)
	return c, nil
}

// socketURL is core's connection endpoint under base, as a WebSocket URL.
func socketURL(base string) (string, error) {
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
	u.Path = strings.TrimRight(u.Path, "/") + protocol.ConnectionPath
	return u.String(), nil
}

// ping pings core every PingEvery; a ping without its pong closes the
// socket. Pongs are read by whoever reads frames (Receive).
func (c *Conn) ping(ctx context.Context) {
	t := time.NewTicker(PingEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			pctx, cancel := context.WithTimeout(ctx, pingWait)
			err := c.ws.Ping(pctx)
			cancel()
			if err != nil {
				c.closeWith(fmt.Errorf("ping: %w", err))
				return
			}
		}
	}
}

// Send writes a frame.
func (c *Conn) Send(ctx context.Context, f protocol.ConnFrame) error {
	b, err := protocol.EncodeConnFrame(f)
	if err != nil {
		return err
	}
	return c.ws.Write(ctx, websocket.MessageBinary, b)
}

// Receive reads the next frame core sent: a reply, or a message core
// starts.
func (c *Conn) Receive(ctx context.Context) (protocol.ConnFrame, error) {
	mt, b, err := c.ws.Read(ctx)
	if err != nil {
		c.closeWith(err)
		return protocol.ConnFrame{}, err
	}
	if mt != websocket.MessageBinary {
		err := errors.New("connection socket: a text frame from core")
		c.closeWith(err)
		return protocol.ConnFrame{}, err
	}
	return protocol.DecodeConnFrame(b)
}

// Done is closed once the socket is.
func (c *Conn) Done() <-chan struct{} { return c.done }

// Err is why the socket closed, once Done is.
func (c *Conn) Err() error {
	select {
	case <-c.done:
		return c.err
	default:
		return nil
	}
}

// Close closes the socket: the minion's connection goes with it.
func (c *Conn) Close() error {
	c.closeWith(nil)
	return nil
}

func (c *Conn) closeWith(err error) {
	c.once.Do(func() {
		c.err = err
		c.cancel()
		_ = c.ws.Close(websocket.StatusNormalClosure, "")
		close(c.done)
	})
}
