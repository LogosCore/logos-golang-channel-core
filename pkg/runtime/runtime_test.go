package runtime

import (
	"bytes"
	"context"
	"encoding/base64"
	"testing"

	coreerrors "github.com/logoscore/logos-golang-channel-core/v3/pkg/errors"
	"github.com/logoscore/logos-golang-channel-core/v3/pkg/profile"
	"github.com/logoscore/logos-golang-channel-core/v3/pkg/transform"
	protocol "github.com/logoscore/logos-golang-protocol/protocol"
)

type testEnvelope struct {
	source string
	data   map[string]string
}

func (e *testEnvelope) SourceKey() string {
	return e.source
}

func (e *testEnvelope) GetField(location, key string) (string, bool) {
	value, ok := e.data[location+"."+key]
	return value, ok
}

func (e *testEnvelope) SetField(location, key, value string) {
	if e.data == nil {
		e.data = map[string]string{}
	}
	e.data[location+"."+key] = value
}

type testSyncClient struct {
	outbound protocol.OutboundMinionMessage
	err      error
	captured protocol.InboundMinionMessage
}

func (s *testSyncClient) Sync(_ context.Context, in protocol.InboundMinionMessage) (protocol.OutboundMinionMessage, error) {
	s.captured = in
	return s.outbound, s.err
}

const (
	minionIn  = "5b0c4f6e-8a1d-4c2b-9e7f-3d2a1b0c9e8f"
	minionOut = "0f9a2c1e-6f1b-4a43-9d4e-2b9b3c1d7e55"
	replyID   = "3c9d2b7e-5a41-4f08-b6e2-91d7c0a4e8f3"
)

func validOutbound() protocol.OutboundMinionMessage {
	return protocol.OutboundMinionMessage{MessageID: replyID, BuildID: minionOut, Data: []byte("blob-out")}
}

func TestRuntimeHandleSuccess(t *testing.T) {
	env := &testEnvelope{data: map[string]string{
		"mapping.id":             minionIn,
		"mapping.encrypted_data": "blob-in",
	}}
	sync := &testSyncClient{outbound: validOutbound()}
	r := New(sync)

	got, err := r.Handle(context.Background(), env, "telegram")
	if err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}
	if got.BuildID != minionOut {
		t.Fatalf("unexpected outbound id: %s", got.BuildID)
	}
	if sync.captured.ChannelInstance != "telegram" || sync.captured.BuildID != minionIn {
		t.Fatalf("unexpected inbound %+v", sync.captured)
	}
	if env.data["mapping.id"] != minionOut {
		t.Fatalf("expected envelope mapping.id to be updated")
	}
}

func TestRuntimeHandleMissingRequiredField(t *testing.T) {
	env := &testEnvelope{data: map[string]string{
		"mapping.encrypted_data": "blob-in",
	}}
	r := New(&testSyncClient{})

	_, err := r.Handle(context.Background(), env, "http")
	if err == nil {
		t.Fatal("expected error for missing mapping.id")
	}
	if code := coreerrors.Code(err); code != coreerrors.CodeInvalidInput {
		t.Fatalf("unexpected error code: %s", code)
	}
}

func TestRuntimeHandleInvalidOutbound(t *testing.T) {
	env := &testEnvelope{data: map[string]string{
		"mapping.id":             minionIn,
		"mapping.encrypted_data": "blob-in",
	}}
	sync := &testSyncClient{outbound: protocol.OutboundMinionMessage{
		MessageID: replyID,
		Data:      []byte("blob-out"),
		// missing build id
	}}
	r := New(sync)

	_, err := r.Handle(context.Background(), env, "http")
	if err == nil {
		t.Fatal("expected outbound validation error")
	}
	if code := coreerrors.Code(err); code != coreerrors.CodeCanonicalInvalid {
		t.Fatalf("unexpected error code: %s", code)
	}
}

// Core has nothing for the minion: its reply carries no data, and
// the channel relays it like any other, through the profile's transforms.
func TestRuntimeHandleEmptyReply(t *testing.T) {
	empty := validOutbound()
	empty.Data = nil

	env := &testEnvelope{data: map[string]string{
		"mapping.id":             minionIn,
		"mapping.encrypted_data": "blob-in",
	}}
	if _, err := New(&testSyncClient{outbound: empty}).Handle(context.Background(), env, "http"); err != nil {
		t.Fatalf("Handle refused an empty reply: %v", err)
	}
	if v, ok := env.data["mapping.encrypted_data"]; !ok || v != "" {
		t.Fatalf("mapping.encrypted_data = %q (set %v), want empty", v, ok)
	}

	env = &testEnvelope{data: map[string]string{"body.id": minionIn, "body.data": "blob-in"}}
	p := profile.Profile{
		ProfileID: 1,
		Enabled:   true,
		Action:    profile.Action{Type: "sync"},
		Mapping: profile.Mapping{
			ID:              profile.MapField{Target: profile.Target{Location: "body", Key: "id"}},
			EncryptedDataIn: profile.MapField{Target: profile.Target{Location: "body", Key: "data"}},
			EncryptedDataOut: profile.MapField{
				Target:    profile.Target{Location: "body", Key: "out"},
				Transform: []transform.Spec{{Type: "base64"}},
			},
		},
	}
	if _, err := New(&testSyncClient{outbound: empty}).HandleWithProfile(context.Background(), env, "http", p); err != nil {
		t.Fatalf("HandleWithProfile refused an empty reply: %v", err)
	}
	if v, ok := env.data["body.out"]; !ok || v != "" {
		t.Fatalf("body.out = %q (set %v), want empty", v, ok)
	}
}

func TestRuntimeHandleWithProfileSuccess(t *testing.T) {
	env := &testEnvelope{data: map[string]string{
		"body.msg_id":  minionIn,
		"body.payload": "blob-in",
		"body.profile": "100",
	}}
	p := profile.Profile{
		ProfileID: 100,
		Enabled:   true,
		Action:    profile.Action{Type: "sync"},
		Mapping: profile.Mapping{
			ProfileID:        &profile.MapField{Target: profile.Target{Location: "body", Key: "profile"}},
			ID:               profile.MapField{Target: profile.Target{Location: "body", Key: "msg_id"}},
			EncryptedDataIn:  profile.MapField{Target: profile.Target{Location: "body", Key: "payload"}},
			EncryptedDataOut: profile.MapField{Target: profile.Target{Location: "body", Key: "payload"}},
		},
	}
	sync := &testSyncClient{outbound: validOutbound()}
	r := New(sync)

	got, err := r.HandleWithProfile(context.Background(), env, "telegram", p)
	if err != nil {
		t.Fatalf("HandleWithProfile returned error: %v", err)
	}
	if got.BuildID != minionOut {
		t.Fatalf("unexpected outbound id: %s", got.BuildID)
	}
	if sync.captured.BuildID != minionIn {
		t.Fatalf("unexpected inbound id: %s", sync.captured.BuildID)
	}
	if string(sync.captured.Data) != "blob-in" {
		t.Fatalf("unexpected inbound data: %q", sync.captured.Data)
	}
	if env.data["body.payload"] != "blob-out" {
		t.Fatalf("expected envelope body.payload to be updated")
	}
}

func TestRuntimeHandleWithProfileTransforms(t *testing.T) {
	encoded := base64.StdEncoding.EncodeToString([]byte("blob-in"))
	env := &testEnvelope{data: map[string]string{
		"body.id":   minionIn,
		"body.data": encoded,
	}}
	p := profile.Profile{
		ProfileID: 1,
		Enabled:   true,
		Action:    profile.Action{Type: "sync"},
		Mapping: profile.Mapping{
			ID: profile.MapField{Target: profile.Target{Location: "body", Key: "id"}},
			EncryptedDataIn: profile.MapField{
				Target:    profile.Target{Location: "body", Key: "data"},
				Transform: []transform.Spec{{Type: "base64"}},
			},
			EncryptedDataOut: profile.MapField{
				Target:    profile.Target{Location: "body", Key: "data"},
				Transform: []transform.Spec{{Type: "base64"}},
			},
		},
	}
	sync := &testSyncClient{outbound: validOutbound()}
	r := New(sync)

	_, err := r.HandleWithProfile(context.Background(), env, "http", p)
	if err != nil {
		t.Fatalf("HandleWithProfile returned error: %v", err)
	}
	if string(sync.captured.Data) != "blob-in" {
		t.Fatalf("expected decoded inbound data 'blob-in', got %q", sync.captured.Data)
	}
	expectedOut := base64.StdEncoding.EncodeToString([]byte("blob-out"))
	if env.data["body.data"] != expectedOut {
		t.Fatalf("expected base64-encoded outbound, got %q", env.data["body.data"])
	}
}

func TestRuntimeHandleWithProfileCompositeInDelimiter(t *testing.T) {
	env := &testEnvelope{data: map[string]string{
		"body.combined": minionIn + "||blob-in",
	}}
	p := profile.Profile{
		ProfileID: 1,
		Enabled:   true,
		Action:    profile.Action{Type: "sync"},
		Mapping: profile.Mapping{
			CompositeIn: &profile.CompositeField{
				Target:    profile.Target{Location: "body", Key: "combined"},
				Separator: profile.Separator{Type: "delimiter", Value: "||"},
			},
			EncryptedDataOut: profile.MapField{Target: profile.Target{Location: "body", Key: "out"}},
		},
	}
	sync := &testSyncClient{outbound: validOutbound()}
	r := New(sync)

	_, err := r.HandleWithProfile(context.Background(), env, "http", p)
	if err != nil {
		t.Fatalf("HandleWithProfile returned error: %v", err)
	}
	if sync.captured.BuildID != minionIn {
		t.Fatalf("expected id %s, got %q", minionIn, sync.captured.BuildID)
	}
	if string(sync.captured.Data) != "blob-in" {
		t.Fatalf("expected data 'blob-in', got %q", sync.captured.Data)
	}
}

func TestRuntimeHandleWithProfileCompositeInLengthPrefix(t *testing.T) {
	env := &testEnvelope{data: map[string]string{
		"body.combined": minionIn + "rest-of-payload",
	}}
	p := profile.Profile{
		ProfileID: 1,
		Enabled:   true,
		Action:    profile.Action{Type: "sync"},
		Mapping: profile.Mapping{
			CompositeIn: &profile.CompositeField{
				Target:    profile.Target{Location: "body", Key: "combined"},
				Separator: profile.Separator{Type: "length_prefix", IDLength: len(minionIn)},
			},
			EncryptedDataOut: profile.MapField{Target: profile.Target{Location: "body", Key: "out"}},
		},
	}
	sync := &testSyncClient{outbound: validOutbound()}
	r := New(sync)

	_, err := r.HandleWithProfile(context.Background(), env, "http", p)
	if err != nil {
		t.Fatalf("HandleWithProfile returned error: %v", err)
	}
	if sync.captured.BuildID != minionIn {
		t.Fatalf("expected id %s, got %q", minionIn, sync.captured.BuildID)
	}
	if string(sync.captured.Data) != "rest-of-payload" {
		t.Fatalf("expected data 'rest-of-payload', got %q", sync.captured.Data)
	}
}

func TestRuntimeHandleWithProfileCustomAction(t *testing.T) {
	env := &testEnvelope{data: map[string]string{
		"body.id":   minionIn,
		"body.data": "blob-in",
	}}
	p := profile.Profile{
		ProfileID: 1,
		Enabled:   true,
		Action:    profile.Action{Type: "redirect", Params: map[string]any{"url": "https://example.com"}},
		Mapping: profile.Mapping{
			ID:               profile.MapField{Target: profile.Target{Location: "body", Key: "id"}},
			EncryptedDataIn:  profile.MapField{Target: profile.Target{Location: "body", Key: "data"}},
			EncryptedDataOut: profile.MapField{Target: profile.Target{Location: "body", Key: "data"}},
		},
	}
	r := New(&testSyncClient{})
	r.RegisterAction("redirect", func(_ context.Context, params map[string]any, inbound protocol.InboundMinionMessage, _ TransportEnvelope) (protocol.OutboundMinionMessage, error) {
		return protocol.OutboundMinionMessage{MessageID: replyID, BuildID: inbound.BuildID, Data: []byte("redirected")}, nil
	})

	got, err := r.HandleWithProfile(context.Background(), env, "http", p)
	if err != nil {
		t.Fatalf("HandleWithProfile returned error: %v", err)
	}
	if string(got.Data) != "redirected" {
		t.Fatalf("expected 'redirected', got %q", got.Data)
	}
}

func TestRuntimeHandleWithProfileUnknownAction(t *testing.T) {
	env := &testEnvelope{data: map[string]string{
		"body.id":   minionIn,
		"body.data": "blob-in",
	}}
	p := profile.Profile{
		ProfileID: 1,
		Enabled:   true,
		Action:    profile.Action{Type: "unknown_action"},
		Mapping: profile.Mapping{
			ID:               profile.MapField{Target: profile.Target{Location: "body", Key: "id"}},
			EncryptedDataIn:  profile.MapField{Target: profile.Target{Location: "body", Key: "data"}},
			EncryptedDataOut: profile.MapField{Target: profile.Target{Location: "body", Key: "data"}},
		},
	}
	r := New(&testSyncClient{})

	_, err := r.HandleWithProfile(context.Background(), env, "http", p)
	if err == nil {
		t.Fatal("expected error for unknown action")
	}
	if code := coreerrors.Code(err); code != coreerrors.CodeNotImplemented {
		t.Fatalf("expected CodeNotImplemented, got %s", code)
	}
}

func TestRuntimeHandleWithProfileInvalidProfile(t *testing.T) {
	env := &testEnvelope{data: map[string]string{
		"body.id":   minionIn,
		"body.data": "blob-in",
	}}
	invalid := profile.Profile{
		ProfileID: 0, // invalid
		Mapping: profile.Mapping{
			ID:               profile.MapField{Target: profile.Target{Location: "body", Key: "id"}},
			EncryptedDataIn:  profile.MapField{Target: profile.Target{Location: "body", Key: "data"}},
			EncryptedDataOut: profile.MapField{Target: profile.Target{Location: "body", Key: "data"}},
		},
	}
	r := New(&testSyncClient{})

	_, err := r.HandleWithProfile(context.Background(), env, "http", invalid)
	if err == nil {
		t.Fatal("expected profile validation error")
	}
	if code := coreerrors.Code(err); code != coreerrors.CodeProfileInvalid {
		t.Fatalf("unexpected error code: %s", code)
	}
}

// The ciphertext is bytes, not text: a profile that decodes base64 hands core
// the raw bytes, and core's reply goes back through the transforms unchanged.
func TestRuntimeHandleWithProfileBinary(t *testing.T) {
	raw := []byte{0x00, 0xff, 0xfe, 0x80, 0x20, 0x0a, 0xc3}
	reply := validOutbound()
	reply.Data = []byte{0x80, 0x81, 0x00, 0xff}
	env := &testEnvelope{data: map[string]string{
		"body.id":   minionIn,
		"body.data": base64.StdEncoding.EncodeToString(raw),
	}}
	p := profile.Profile{
		ProfileID: 1,
		Enabled:   true,
		Action:    profile.Action{Type: "sync"},
		Mapping: profile.Mapping{
			ID:               profile.MapField{Target: profile.Target{Location: "body", Key: "id"}},
			EncryptedDataIn:  profile.MapField{Target: profile.Target{Location: "body", Key: "data"}, Transform: []transform.Spec{{Type: "base64"}}},
			EncryptedDataOut: profile.MapField{Target: profile.Target{Location: "body", Key: "data"}, Transform: []transform.Spec{{Type: "base64"}}},
		},
	}
	sync := &testSyncClient{outbound: reply}
	if _, err := New(sync).HandleWithProfile(context.Background(), env, "http", p); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(sync.captured.Data, raw) {
		t.Fatalf("core got %x, want %x", sync.captured.Data, raw)
	}
	if env.data["body.data"] != base64.StdEncoding.EncodeToString(reply.Data) {
		t.Fatalf("minion gets %q", env.data["body.data"])
	}
}
