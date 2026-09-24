package runtime

import (
	"context"
	"encoding/base64"
	"testing"

	coreerrors "github.com/logoscore/logos-golang-channel-core/pkg/errors"
	"github.com/logoscore/logos-golang-channel-core/pkg/profile"
	"github.com/logoscore/logos-golang-channel-core/pkg/transform"
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

func validOutbound() protocol.OutboundMinionMessage {
	return protocol.OutboundMinionMessage{
		MessageID:     "m-2",
		Type:          protocol.TypeOutboundMinionMessage,
		Version:       protocol.VersionV1,
		Timestamp:     "2026-03-10T15:00:00Z",
		ID:            "msg-2",
		EncryptedData: "blob-out",
	}
}

func TestRuntimeHandleSuccess(t *testing.T) {
	env := &testEnvelope{data: map[string]string{
		"mapping.id":             "msg-1",
		"mapping.encrypted_data": "blob-in",
	}}
	sync := &testSyncClient{outbound: validOutbound()}
	r := New(sync)

	got, err := r.Handle(context.Background(), env, "telegram")
	if err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}
	if got.ID != "msg-2" {
		t.Fatalf("unexpected outbound id: %s", got.ID)
	}
	if sync.captured.Type != protocol.TypeInboundMinionMessage {
		t.Fatalf("unexpected inbound type: %s", sync.captured.Type)
	}
	if env.data["mapping.id"] != "msg-2" {
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
		"mapping.id":             "msg-1",
		"mapping.encrypted_data": "blob-in",
	}}
	sync := &testSyncClient{outbound: protocol.OutboundMinionMessage{
		MessageID: "m-3",
		Type:      protocol.TypeOutboundMinionMessage,
		Version:   protocol.VersionV1,
		Timestamp: "2026-03-10T15:00:00Z",
		ID:        "msg-2",
		// missing encrypted_data
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

func TestRuntimeHandleWithProfileSuccess(t *testing.T) {
	env := &testEnvelope{data: map[string]string{
		"body.msg_id":  "msg-1",
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
	if got.ID != "msg-2" {
		t.Fatalf("unexpected outbound id: %s", got.ID)
	}
	if sync.captured.ID != "msg-1" {
		t.Fatalf("unexpected inbound id: %s", sync.captured.ID)
	}
	if sync.captured.EncryptedData != "blob-in" {
		t.Fatalf("unexpected inbound encrypted data: %s", sync.captured.EncryptedData)
	}
	if env.data["body.payload"] != "blob-out" {
		t.Fatalf("expected envelope body.payload to be updated")
	}
}

func TestRuntimeHandleWithProfileTransforms(t *testing.T) {
	encoded := base64.StdEncoding.EncodeToString([]byte("blob-in"))
	env := &testEnvelope{data: map[string]string{
		"body.id":   "msg-1",
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
	if sync.captured.EncryptedData != "blob-in" {
		t.Fatalf("expected decoded inbound encrypted data 'blob-in', got %q", sync.captured.EncryptedData)
	}
	expectedOut := base64.StdEncoding.EncodeToString([]byte("blob-out"))
	if env.data["body.data"] != expectedOut {
		t.Fatalf("expected base64-encoded outbound, got %q", env.data["body.data"])
	}
}

func TestRuntimeHandleWithProfileCompositeInDelimiter(t *testing.T) {
	env := &testEnvelope{data: map[string]string{
		"body.combined": "msg-1||blob-in",
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
	if sync.captured.ID != "msg-1" {
		t.Fatalf("expected id 'msg-1', got %q", sync.captured.ID)
	}
	if sync.captured.EncryptedData != "blob-in" {
		t.Fatalf("expected encrypted data 'blob-in', got %q", sync.captured.EncryptedData)
	}
}

func TestRuntimeHandleWithProfileCompositeInLengthPrefix(t *testing.T) {
	env := &testEnvelope{data: map[string]string{
		"body.combined": "ABCD1234rest-of-payload",
	}}
	p := profile.Profile{
		ProfileID: 1,
		Enabled:   true,
		Action:    profile.Action{Type: "sync"},
		Mapping: profile.Mapping{
			CompositeIn: &profile.CompositeField{
				Target:    profile.Target{Location: "body", Key: "combined"},
				Separator: profile.Separator{Type: "length_prefix", IDLength: 8},
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
	if sync.captured.ID != "ABCD1234" {
		t.Fatalf("expected id 'ABCD1234', got %q", sync.captured.ID)
	}
	if sync.captured.EncryptedData != "rest-of-payload" {
		t.Fatalf("expected encrypted data 'rest-of-payload', got %q", sync.captured.EncryptedData)
	}
}

func TestRuntimeHandleWithProfileCustomAction(t *testing.T) {
	env := &testEnvelope{data: map[string]string{
		"body.id":   "msg-1",
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
		return protocol.OutboundMinionMessage{
			MessageID:     "m-redirect",
			Type:          protocol.TypeOutboundMinionMessage,
			Version:       protocol.VersionV1,
			Timestamp:     "2026-03-10T15:00:00Z",
			ID:            inbound.ID,
			EncryptedData: "redirected",
		}, nil
	})

	got, err := r.HandleWithProfile(context.Background(), env, "http", p)
	if err != nil {
		t.Fatalf("HandleWithProfile returned error: %v", err)
	}
	if got.EncryptedData != "redirected" {
		t.Fatalf("expected 'redirected', got %q", got.EncryptedData)
	}
}

func TestRuntimeHandleWithProfileUnknownAction(t *testing.T) {
	env := &testEnvelope{data: map[string]string{
		"body.id":   "msg-1",
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
		"body.id":   "msg-1",
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
