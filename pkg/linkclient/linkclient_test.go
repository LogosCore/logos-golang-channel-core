package linkclient

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/logoscore/logos-golang-protocol/protocol"
)

// A fake core: it refuses the first handshake (not registered yet), then
// on each link sends the frames of round, reads as many answers and closes
// the link, so the client opens another.
func fakeCore(t *testing.T, round []protocol.LinkFrame, answers chan<- protocol.LinkFrame, links *atomic.Int32) *httptest.Server {
	t.Helper()
	var refused atomic.Bool
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != protocol.LinkPath || r.Header.Get(protocol.HeaderChannelInstance) != "http-1" {
			http.Error(w, "bad", http.StatusBadRequest)
			return
		}
		if !refused.Swap(true) {
			http.Error(w, "not registered", http.StatusForbidden)
			return
		}
		ws, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = ws.CloseNow() }()
		links.Add(1)
		ctx := r.Context()
		for _, f := range round {
			b, err := protocol.EncodeLinkFrame(f)
			if err != nil {
				t.Errorf("encode: %v", err)
				return
			}
			if err := ws.Write(ctx, websocket.MessageBinary, b); err != nil {
				return
			}
		}
		for range round {
			_, b, err := ws.Read(ctx)
			if err != nil {
				return
			}
			f, err := protocol.DecodeLinkFrame(b)
			if err != nil {
				t.Errorf("answer: %v", err)
				return
			}
			answers <- f
		}
		_ = ws.Close(websocket.StatusNormalClosure, "")
	}))
}

// testKeys are an agentless call's keys.
var testKeys = &protocol.LinkKeys{Suite: protocol.CryptoAES256CBCHMACSHA256,
	Material: protocol.AES256CBCHMACSHA256Material{EncKey: bytes.Repeat([]byte{1}, 32), MACKey: bytes.Repeat([]byte{2}, 32)}}

func TestLink(t *testing.T) {
	id := bytes.Repeat([]byte{9}, 16)
	round := []protocol.LinkFrame{
		{Type: protocol.LinkCall, Request: 1, Values: map[string]any{"url": "http://page/ok"}, ID: id, Data: []byte("x"), Keys: testKeys},
		{Type: protocol.LinkCall, Request: 2, Values: map[string]any{"url": "http://page/404"}, ID: id, Data: []byte("x")},
		{Type: protocol.LinkCall, Request: 3, Values: map[string]any{"url": "http://page/slow"}, ID: id, Data: []byte("x"), Deadline: 50},
		{Type: protocol.LinkOpen, Request: 4, Values: map[string]any{"host": "h"}},
	}
	answers := make(chan protocol.LinkFrame, 16)
	var links atomic.Int32
	core := fakeCore(t, round, answers, &links)
	defer core.Close()

	handler := func(ctx context.Context, c Call) Answer {
		switch c.Values["url"] {
		case "http://page/ok":
			if c.Keys == nil || !bytes.Equal(c.Keys.Material.EncKey, testKeys.Material.EncKey) {
				t.Errorf("a call's keys: %+v", c.Keys)
			}
			out := c.ID
			return Answer{ID: &out, Data: append([]byte("re:"), c.Data...), Status: 200}
		case "http://page/404":
			if c.Keys != nil {
				t.Error("keys on a call that carried none")
			}
			return Answer{Status: 404, Excerpt: []byte("not found")}
		default:
			<-ctx.Done()
			return Fail(protocol.LinkFailTimeout, "no answer in %s", c.Deadline)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var ups atomic.Int32
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Options{BaseURL: core.URL, Instance: "http-1", Handler: handler,
			MinBackoff: 10 * time.Millisecond, MaxBackoff: 20 * time.Millisecond,
			OnState: func(up bool) {
				if up {
					ups.Add(1)
				}
			}})
	}()

	// Two links: the client opens another after core closes the first.
	for n := 0; n < 2; n++ {
		got := map[uint64]protocol.LinkFrame{}
		for range round {
			select {
			case f := <-answers:
				got[f.Request] = f
			case <-ctx.Done():
				t.Fatalf("link %d: answers %v", n, got)
			}
		}
		if a := got[1]; string(a.Data) != "re:x" || !bytes.Equal(a.ID, id) || a.Status != 200 {
			t.Errorf("call answered %+v", a)
		}
		if a := got[2]; a.Status != 404 || string(a.Excerpt) != "not found" || a.ID != nil {
			t.Errorf("not the minion answered %+v", a)
		}
		if a := got[3]; a.Failure == nil || a.Failure.Kind != protocol.LinkFailTimeout {
			t.Errorf("timeout answered %+v", a)
		}
		if a := got[4]; a.Failure == nil || a.Failure.Kind != protocol.LinkFailOther {
			t.Errorf("open answered %+v", a)
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if links.Load() < 2 || ups.Load() < 2 {
		t.Fatalf("links %d, ups %d", links.Load(), ups.Load())
	}
}

func TestAnswerFrame(t *testing.T) {
	id := [16]byte{1}
	// A handler's answer that does not hold together goes as a failure.
	f := answerFrame(5, Answer{ID: &id})
	if f.Failure == nil || f.Request != 5 {
		t.Fatalf("an id without data: %+v", f)
	}
	// A failure drops a status the handler set.
	f = answerFrame(6, Answer{Status: 500, Failure: &protocol.LinkFailure{Kind: protocol.LinkFailTLS}})
	if f.Status != 0 || f.Failure.Kind != protocol.LinkFailTLS {
		t.Fatalf("failure: %+v", f)
	}
	// The peer goes with what answered, not with a failure.
	f = answerFrame(7, Answer{Status: 404, Excerpt: []byte("nope"), Peer: "203.0.113.7"})
	if f.Peer != "203.0.113.7" || f.Failure != nil {
		t.Fatalf("peer: %+v", f)
	}
	f = answerFrame(8, Answer{Failure: &protocol.LinkFailure{Kind: protocol.LinkFailTimeout}, Peer: "203.0.113.7"})
	if f.Peer != "" || f.Failure == nil {
		t.Fatalf("a failure's peer: %+v", f)
	}
	if long := Fail(protocol.LinkFailOther, "%s", string(bytes.Repeat([]byte("é"), 400))); len(long.Failure.Msg) > protocol.MaxLinkFailureMsg {
		t.Fatalf("message of %d bytes", len(long.Failure.Msg))
	}
}

func TestRunRefusesBadOptions(t *testing.T) {
	if err := Run(context.Background(), Options{BaseURL: "http://core"}); err == nil {
		t.Error("no handler")
	}
	h := func(context.Context, Call) Answer { return Answer{} }
	for _, base := range []string{"", "ftp://core", "core:8002"} {
		if err := Run(context.Background(), Options{BaseURL: base, Handler: h}); err == nil {
			t.Errorf("base %q", base)
		}
	}
}

// An open goes to the channel's Opener, with its values, request and the id
// the minion speaks under; its answer (the socket up, or why not) goes back
// by the request id.
func TestOpen(t *testing.T) {
	under := [16]byte{5, 5, 5}
	round := []protocol.LinkFrame{
		{Type: protocol.LinkOpen, Request: 7, Values: map[string]any{"host": "10.0.0.5"}, Deadline: 2000, Under: under[:], Keys: testKeys},
		{Type: protocol.LinkOpen, Request: 8, Values: map[string]any{"host": "nowhere"}},
	}
	answers := make(chan protocol.LinkFrame, 4)
	var links atomic.Int32
	core := fakeCore(t, round, answers, &links)
	defer core.Close()
	opener := func(ctx context.Context, o Open) Answer {
		if _, ok := ctx.Deadline(); !ok {
			t.Error("an open without a deadline")
		}
		if o.Values["host"] == "nowhere" {
			return Fail(protocol.LinkFailUnreachable, "no such host")
		}
		if o.Request != 7 || o.Deadline != 2*time.Second || o.Under != under || o.Keys == nil {
			t.Errorf("open %+v", o)
		}
		return Answer{}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() {
		_ = Run(ctx, Options{BaseURL: core.URL, Instance: "http-1", Handler: func(context.Context, Call) Answer { return Answer{} },
			Opener: opener, MinBackoff: 10 * time.Millisecond, MaxBackoff: 20 * time.Millisecond})
	}()
	got := map[uint64]protocol.LinkFrame{}
	for range round {
		select {
		case f := <-answers:
			got[f.Request] = f
		case <-ctx.Done():
			t.Fatalf("answers %v", got)
		}
	}
	if a := got[7]; a.Failure != nil || a.ID != nil {
		t.Errorf("an open that worked answered %+v", a)
	}
	if a := got[8]; a.Failure == nil || a.Failure.Kind != protocol.LinkFailUnreachable {
		t.Errorf("an open that failed answered %+v", a)
	}
}

// A call's and an open's credentials reach the channel by id, as the values
// name them, and the logins the channel answers with reach core.
func TestCredentialsAndLogins(t *testing.T) {
	id := bytes.Repeat([]byte{9}, 16)
	creds := map[string]protocol.Credential{
		"k1": {ID: "k1", Type: protocol.CredentialTypeKey, Username: "root", Key: "KEY"},
		"p1": {ID: "p1", Type: protocol.CredentialTypePlaintext, Username: "admin", Password: "pw"},
	}
	values := map[string]any{"host": "h", "credentials": []any{"k1", "p1"}}
	round := []protocol.LinkFrame{
		{Type: protocol.LinkCall, Request: 1, Values: values, ID: id, Data: []byte("x"), Keys: testKeys, Credentials: creds},
		{Type: protocol.LinkOpen, Request: 2, Values: values, Under: id, Keys: testKeys, Credentials: creds},
	}
	answers := make(chan protocol.LinkFrame, 8)
	var links atomic.Int32
	core := fakeCore(t, round, answers, &links)
	defer core.Close()

	field := protocol.FieldSpec{Key: "credentials", Type: protocol.FieldCredentialList}
	logins := func(given map[string]protocol.Credential, v any) []protocol.LinkLogin {
		list, err := protocol.FieldCredentials(field, v, given)
		if err != nil || len(list) != 2 || list[0].Key != "KEY" || list[1].Password != "pw" {
			t.Errorf("credentials: %+v %v", list, err)
			return nil
		}
		// The key refused, the password taken.
		return []protocol.LinkLogin{{Credential: list[0].ID}, {Credential: list[1].ID, Accepted: true}}
	}
	handler := func(_ context.Context, c Call) Answer {
		out := c.ID
		return Answer{ID: &out, Data: []byte("re"), Status: 200, Logins: logins(c.Credentials, c.Values["credentials"])}
	}
	opener := func(_ context.Context, o Open) Answer {
		a := Fail(protocol.LinkFailLogin, "both refused")
		a.Logins = []protocol.LinkLogin{{Credential: "k1"}, {Credential: "p1"}}
		if l := logins(o.Credentials, o.Values["credentials"]); len(l) != 2 {
			return Fail(protocol.LinkFailOther, "no credentials")
		}
		return a
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- Run(ctx, Options{BaseURL: core.URL, Instance: "http-1", Handler: handler, Opener: opener,
			MinBackoff: 10 * time.Millisecond, MaxBackoff: 20 * time.Millisecond})
	}()
	got := map[uint64]protocol.LinkFrame{}
	for range round {
		select {
		case f := <-answers:
			got[f.Request] = f
		case <-ctx.Done():
			t.Fatalf("answers %v", got)
		}
	}
	if a := got[1]; len(a.Logins) != 2 || a.Logins[0].Accepted || !a.Logins[1].Accepted || a.Logins[1].Credential != "p1" {
		t.Errorf("the call's logins: %+v", a.Logins)
	}
	if a := got[2]; a.Failure == nil || a.Failure.Kind != protocol.LinkFailLogin || len(a.Logins) != 2 {
		t.Errorf("the open's logins: %+v", a)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
