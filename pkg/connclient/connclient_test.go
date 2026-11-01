package connclient

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/logoscore/logos-golang-protocol/protocol"
)

// A fake core: it checks the handshake's headers, answers each frame with
// "re:" before its message, and pushes one frame of its own first.
func fakeCore(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != protocol.ConnectionPath || r.Header.Get(protocol.HeaderChannelInstance) != "http-1" ||
			r.Header.Get(protocol.HeaderConnectionKind) != protocol.KindReversePersistent ||
			r.Header.Get(protocol.HeaderPeerAddress) != "203.0.113.7" {
			http.Error(w, "bad", http.StatusBadRequest)
			return
		}
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = ws.CloseNow() }()
		ctx := r.Context()
		push, _ := protocol.EncodeConnFrame(protocol.ConnFrame{ID: bytes.Repeat([]byte{7}, 16), Data: []byte("push")})
		if err := ws.Write(ctx, websocket.MessageBinary, push); err != nil {
			return
		}
		for {
			_, b, err := ws.Read(ctx)
			if err != nil {
				return
			}
			f, err := protocol.DecodeConnFrame(b)
			if err != nil {
				return
			}
			reply, _ := protocol.EncodeConnFrame(protocol.ConnFrame{ID: f.ID, Data: append([]byte("re:"), f.Data...)})
			if err := ws.Write(ctx, websocket.MessageBinary, reply); err != nil {
				return
			}
		}
	}))
}

func TestConnection(t *testing.T) {
	core := fakeCore(t)
	defer core.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := Dial(ctx, Options{BaseURL: core.URL + "/", Instance: "http-1", Kind: protocol.KindReversePersistent, Peer: "203.0.113.7"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()
	// What core sends first, unasked.
	if f, err := c.Receive(ctx); err != nil || string(f.Data) != "push" {
		t.Fatalf("push: %+v %v", f, err)
	}
	id := bytes.Repeat([]byte{1}, 16)
	if err := c.Send(ctx, protocol.ConnFrame{ID: id, Data: []byte("hello")}); err != nil {
		t.Fatal(err)
	}
	if f, err := c.Receive(ctx); err != nil || string(f.Data) != "re:hello" || !bytes.Equal(f.ID, id) {
		t.Fatalf("reply: %+v %v", f, err)
	}
	_ = c.Close()
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("not done after Close")
	}
}

func TestRefused(t *testing.T) {
	core := fakeCore(t)
	defer core.Close()
	_, err := Dial(context.Background(), Options{BaseURL: core.URL, Instance: "other", Kind: protocol.KindReversePersistent})
	if !errors.Is(err, ErrRefused) {
		t.Fatalf("an unknown instance: %v", err)
	}
	for _, base := range []string{"", "ftp://core", "core:8002"} {
		if _, err := Dial(context.Background(), Options{BaseURL: base}); err == nil {
			t.Errorf("base %q dialed", base)
		}
	}
}
