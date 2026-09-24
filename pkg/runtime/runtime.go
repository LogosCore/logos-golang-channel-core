package runtime

import (
	"context"
	"fmt"
	"strings"
	"time"

	coreerrors "github.com/logoscore/logos-golang-channel-core/pkg/errors"
	"github.com/logoscore/logos-golang-channel-core/pkg/noise"
	"github.com/logoscore/logos-golang-channel-core/pkg/profile"
	"github.com/logoscore/logos-golang-channel-core/pkg/transform"
	protocol "github.com/logoscore/logos-golang-protocol/protocol"
)

// Runtime orchestrates canonical message handling for channel adapters.
type Runtime struct {
	SyncClient     SyncClient
	ActionHandlers map[string]ActionHandlerFunc
}

func New(syncClient SyncClient) *Runtime {
	r := &Runtime{
		SyncClient:     syncClient,
		ActionHandlers: map[string]ActionHandlerFunc{},
	}
	r.ActionHandlers["sync"] = func(ctx context.Context, _ map[string]any, inbound protocol.InboundAgentMessage, _ TransportEnvelope) (protocol.OutboundAgentMessage, error) {
		return syncClient.Sync(ctx, inbound)
	}
	return r
}

// RegisterAction registers a custom action handler for a given action type.
func (r *Runtime) RegisterAction(name string, handler ActionHandlerFunc) {
	r.ActionHandlers[name] = handler
}

// Handle converts envelope fields into canonical inbound message, validates it,
// syncs with Logos, validates outbound canonical message, and writes mapped fields back.
func (r *Runtime) Handle(ctx context.Context, envelope TransportEnvelope, channelID string) (protocol.OutboundAgentMessage, error) {
	if r == nil || r.SyncClient == nil {
		return protocol.OutboundAgentMessage{}, coreerrors.New(coreerrors.CodeInvalidInput, "runtime and sync client are required")
	}
	if envelope == nil {
		return protocol.OutboundAgentMessage{}, coreerrors.New(coreerrors.CodeInvalidInput, "transport envelope is required")
	}
	if strings.TrimSpace(channelID) == "" {
		return protocol.OutboundAgentMessage{}, coreerrors.New(coreerrors.CodeInvalidInput, "channelID is required")
	}

	inbound, err := inboundFromEnvelope(envelope, channelID)
	if err != nil {
		return protocol.OutboundAgentMessage{}, err
	}
	if err := protocol.ValidateInbound(inbound); err != nil {
		return protocol.OutboundAgentMessage{}, coreerrors.Wrap(coreerrors.CodeCanonicalInvalid, "invalid inbound canonical message", err)
	}

	outbound, err := r.SyncClient.Sync(ctx, inbound)
	if err != nil {
		return protocol.OutboundAgentMessage{}, err
	}
	if err := protocol.ValidateOutbound(outbound); err != nil {
		return protocol.OutboundAgentMessage{}, coreerrors.Wrap(coreerrors.CodeCanonicalInvalid, "invalid outbound canonical message", err)
	}

	envelope.SetField("mapping", "id", outbound.ID)
	envelope.SetField("mapping", "encrypted_data", outbound.EncryptedData)

	return outbound, nil
}

// HandleWithProfile is the profile-aware runtime entrypoint.
func (r *Runtime) HandleWithProfile(ctx context.Context, envelope TransportEnvelope, channelID string, p profile.Profile) (protocol.OutboundAgentMessage, error) {
	if r == nil {
		return protocol.OutboundAgentMessage{}, coreerrors.New(coreerrors.CodeInvalidInput, "runtime is required")
	}
	if envelope == nil {
		return protocol.OutboundAgentMessage{}, coreerrors.New(coreerrors.CodeInvalidInput, "transport envelope is required")
	}
	if strings.TrimSpace(channelID) == "" {
		return protocol.OutboundAgentMessage{}, coreerrors.New(coreerrors.CodeInvalidInput, "channelID is required")
	}
	if err := profile.Validate(p); err != nil {
		return protocol.OutboundAgentMessage{}, coreerrors.Wrap(coreerrors.CodeProfileInvalid, "invalid profile", err)
	}

	var id, encryptedData string
	var err error

	if p.Mapping.CompositeIn != nil {
		id, encryptedData, err = extractComposite(envelope, *p.Mapping.CompositeIn)
	} else {
		id, err = extractField(envelope, p.Mapping.ID)
		if err == nil {
			encryptedData, err = extractField(envelope, p.Mapping.EncryptedDataIn)
		}
	}
	if err != nil {
		return protocol.OutboundAgentMessage{}, err
	}

	inbound := protocol.InboundAgentMessage{
		MessageID: fmt.Sprintf("%s-%d", channelID, time.Now().UnixNano()),
		Type:      protocol.TypeInboundAgentMessage,
		Version:   protocol.VersionV1,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Source: protocol.SourceInfo{
			Module:         "channel-core",
			ModuleInstance: channelID,
			Transport:      "channel",
			Tenant:         "default",
		},
		ID:            id,
		EncryptedData: encryptedData,
	}

	if err := protocol.ValidateInbound(inbound); err != nil {
		return protocol.OutboundAgentMessage{}, coreerrors.Wrap(coreerrors.CodeCanonicalInvalid, "invalid inbound canonical message", err)
	}

	handler, ok := r.ActionHandlers[p.Action.Type]
	if !ok {
		return protocol.OutboundAgentMessage{}, coreerrors.New(coreerrors.CodeNotImplemented, "unsupported action type: "+p.Action.Type)
	}
	outbound, err := handler(ctx, p.Action.Params, inbound, envelope)
	if err != nil {
		return protocol.OutboundAgentMessage{}, err
	}
	if err := protocol.ValidateOutbound(outbound); err != nil {
		return protocol.OutboundAgentMessage{}, coreerrors.Wrap(coreerrors.CodeCanonicalInvalid, "invalid outbound canonical message", err)
	}

	outEncrypted, err := transform.ApplyEncode(outbound.EncryptedData, p.Mapping.EncryptedDataOut.Transform)
	if err != nil {
		return protocol.OutboundAgentMessage{}, coreerrors.Wrap(coreerrors.CodeInvalidInput, "transform encode failed for encrypted_data_out", err)
	}
	envelope.SetField(p.Mapping.EncryptedDataOut.Target.Location, p.Mapping.EncryptedDataOut.Target.Key, outEncrypted)

	if p.Noise != nil && len(p.Noise.Outbound) > 0 {
		kvs, err := noise.GenerateAll(p.Noise.Outbound)
		if err != nil {
			return protocol.OutboundAgentMessage{}, coreerrors.Wrap(coreerrors.CodeInternal, "outbound noise generation failed", err)
		}
		for _, kv := range kvs {
			envelope.SetField(kv.Location, kv.Key, kv.Value)
		}
	}

	return outbound, nil
}

// extractField extracts a value from the envelope and applies inbound decode transforms.
func extractField(envelope TransportEnvelope, f profile.MapField) (string, error) {
	loc := strings.TrimSpace(f.Target.Location)
	key := strings.TrimSpace(f.Target.Key)
	raw, ok := envelope.GetField(loc, key)
	if !ok || strings.TrimSpace(raw) == "" {
		return "", coreerrors.New(coreerrors.CodeInvalidInput, "missing required field: "+loc+"."+key)
	}
	decoded, err := transform.ApplyDecode(raw, f.Transform)
	if err != nil {
		return "", coreerrors.Wrap(coreerrors.CodeInvalidInput, "transform decode failed for "+loc+"."+key, err)
	}
	return decoded, nil
}

// extractComposite extracts a composite value and splits it into id + encrypted_data.
func extractComposite(envelope TransportEnvelope, cf profile.CompositeField) (string, string, error) {
	loc := strings.TrimSpace(cf.Target.Location)
	key := strings.TrimSpace(cf.Target.Key)
	raw, ok := envelope.GetField(loc, key)
	if !ok || strings.TrimSpace(raw) == "" {
		return "", "", coreerrors.New(coreerrors.CodeInvalidInput, "missing required field: "+loc+"."+key)
	}
	decoded, err := transform.ApplyDecode(raw, cf.Transform)
	if err != nil {
		return "", "", coreerrors.Wrap(coreerrors.CodeInvalidInput, "transform decode failed for composite_in", err)
	}

	switch cf.Separator.Type {
	case "length_prefix":
		if len(decoded) < cf.Separator.IDLength {
			return "", "", coreerrors.New(coreerrors.CodeInvalidInput, "composite_in value too short for length_prefix separator")
		}
		return decoded[:cf.Separator.IDLength], decoded[cf.Separator.IDLength:], nil
	case "delimiter":
		parts := strings.SplitN(decoded, cf.Separator.Value, 2)
		if len(parts) != 2 {
			return "", "", coreerrors.New(coreerrors.CodeInvalidInput, "composite_in value missing delimiter")
		}
		return parts[0], parts[1], nil
	default:
		return "", "", coreerrors.New(coreerrors.CodeProfileInvalid, "unsupported separator type: "+cf.Separator.Type)
	}
}

func inboundFromEnvelope(envelope TransportEnvelope, channelID string) (protocol.InboundAgentMessage, error) {
	id, err := requiredEnvelopeField(envelope, "mapping", "id")
	if err != nil {
		return protocol.InboundAgentMessage{}, err
	}
	encryptedData, err := requiredEnvelopeField(envelope, "mapping", "encrypted_data")
	if err != nil {
		return protocol.InboundAgentMessage{}, err
	}

	return protocol.InboundAgentMessage{
		MessageID: fmt.Sprintf("%s-%d", channelID, time.Now().UnixNano()),
		Type:      protocol.TypeInboundAgentMessage,
		Version:   protocol.VersionV1,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Source: protocol.SourceInfo{
			Module:         "channel-core",
			ModuleInstance: channelID,
			Transport:      "channel",
			Tenant:         "default",
		},
		ID:            id,
		EncryptedData: encryptedData,
	}, nil
}

func requiredEnvelopeField(envelope TransportEnvelope, location, key string) (string, error) {
	value, ok := envelope.GetField(location, key)
	if !ok || strings.TrimSpace(value) == "" {
		return "", coreerrors.New(coreerrors.CodeInvalidInput, "missing required field: "+location+"."+key)
	}
	return strings.TrimSpace(value), nil
}
