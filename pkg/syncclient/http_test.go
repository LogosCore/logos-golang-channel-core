package syncclient

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	coreerrors "github.com/logoscore/logos-golang-channel-core/v3/pkg/errors"
	protocol "github.com/logoscore/logos-golang-protocol/protocol"
)

const buildID = "5b0c4f6e-8a1d-4c2b-9e7f-3d2a1b0c9e8f"

func inbound() protocol.InboundMinionMessage {
	return protocol.InboundMinionMessage{
		MessageID:       protocol.NewID(),
		BuildID:         buildID,
		ChannelInstance: "http-main",
		Data:            []byte{0x00, 0xff, 0x80, 0x0a},
	}
}

// core is a fake sync endpoint: it records the request and answers with reply.
func core(t *testing.T, reply func(w http.ResponseWriter, in protocol.InboundMinionMessage)) (*HTTPClient, *protocol.InboundMinionMessage) {
	t.Helper()
	var got protocol.InboundMinionMessage
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != protocol.SyncPath || r.Header.Get("Content-Type") != protocol.SyncContentType {
			t.Errorf("request %s %q", r.URL.Path, r.Header.Get("Content-Type"))
		}
		got = protocol.InboundFromHeaders(r.Header)
		got.Data, _ = io.ReadAll(r.Body)
		reply(w, got)
	}))
	t.Cleanup(srv.Close)
	return NewHTTPClient(srv.URL, srv.Client()), &got
}

func answer(status int, minion string, data []byte) func(http.ResponseWriter, protocol.InboundMinionMessage) {
	return func(w http.ResponseWriter, _ protocol.InboundMinionMessage) {
		protocol.OutboundMinionMessage{MessageID: protocol.NewID(), BuildID: minion, Data: data}.SetHeaders(w.Header())
		w.WriteHeader(status)
		_, _ = w.Write(data)
	}
}

func TestSyncSendsRawBytes(t *testing.T) {
	reply := []byte{0xfe, 0x00, 0x81}
	c, got := core(t, answer(http.StatusOK, buildID, reply))
	in := inbound()
	out, err := c.Sync(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Data, in.Data) || got.BuildID != buildID || got.ChannelInstance != "http-main" || got.MessageID != in.MessageID {
		t.Fatalf("core got %+v", got)
	}
	if !bytes.Equal(out.Data, reply) || out.BuildID != buildID {
		t.Fatalf("reply %+v", out)
	}
}

func TestSyncNothingForTheMinion(t *testing.T) {
	c, _ := core(t, answer(http.StatusNoContent, buildID, nil))
	out, err := c.Sync(context.Background(), inbound())
	if err != nil || len(out.Data) != 0 {
		t.Fatalf("204: %+v, %v", out, err)
	}
}

func TestSyncRefusals(t *testing.T) {
	for name, c := range map[string]struct {
		reply func(http.ResponseWriter, protocol.InboundMinionMessage)
		max   int64
		code  string
		want  string
	}{
		"core error": {reply: func(w http.ResponseWriter, _ protocol.InboundMinionMessage) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error":"channel not registered"}`))
		}, code: coreerrors.CodeSyncRejected, want: "channel not registered"},
		"another build": {reply: answer(http.StatusOK, "0f9a2c1e-6f1b-4a43-9d4e-2b9b3c1d7e55", []byte{1}), code: coreerrors.CodeSyncRejected, want: "not " + buildID},
		"no build id":   {reply: answer(http.StatusNoContent, "", nil), code: coreerrors.CodeCanonicalInvalid},
		"too big":       {reply: answer(http.StatusOK, buildID, make([]byte, 10)), max: 9, code: coreerrors.CodeSyncRejected, want: "limit"},
		"content type": {reply: func(w http.ResponseWriter, _ protocol.InboundMinionMessage) {
			w.Header().Set(protocol.HeaderMessageID, protocol.NewID())
			w.Header().Set(protocol.HeaderBuildID, buildID)
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{}`))
		}, code: coreerrors.CodeSyncRejected, want: "content type"},
	} {
		cl, _ := core(t, c.reply)
		cl.MaxReplyBytes = c.max
		_, err := cl.Sync(context.Background(), inbound())
		if coreerrors.Code(err) != c.code || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: %v (code %s), want %s %q", name, err, coreerrors.Code(err), c.code, c.want)
		}
	}
}

func TestSyncRefusesAnInvalidMessage(t *testing.T) {
	c, _ := core(t, answer(http.StatusNoContent, buildID, nil))
	in := inbound()
	in.Data = nil
	if _, err := c.Sync(context.Background(), in); coreerrors.Code(err) != coreerrors.CodeCanonicalInvalid {
		t.Fatalf("empty body sent: %v", err)
	}
}
