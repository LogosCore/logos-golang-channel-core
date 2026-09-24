package runtime

import (
	"context"
	"fmt"
	"strings"
	"time"

	coreerrors "github.com/logoscore/logos-golang-channel-core/pkg/errors"
	"github.com/logoscore/logos-golang-channel-core/pkg/profile"
	protocol "github.com/logoscore/logos-golang-protocol/protocol"
)

// Runtime orchestrates canonical message handling for channel adapters.
type Runtime struct {
	SyncClient SyncClient
}

func New(syncClient SyncClient) *Runtime {
	return &Runtime{SyncClient: syncClient}
}

// Handle converts envelope fields into canonical inbound message, validates it,
// syncs with Logos, validates outbound canonical message, and writes mapped fields back.
func (r *Runtime) Handle(ctx context.Context, envelope TransportEnvelope, channelID string) (protocol.OutboundAgentMessage, error) {
	if r == nil || r.SyncClient == nil {
		return protocol.OutboundAgentMessage{}, coreerrors.New(coreerrors.CodeInvalidInput, "runtime sync client is required")
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
	if r == nil || r.SyncClient == nil {
		return protocol.OutboundAgentMessage{}, coreerrors.New(coreerrors.CodeInvalidInput, "runtime sync client is required")
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

	id, err := requiredEnvelopeField(envelope, "mapping", p.Mapping.ID)
	if err != nil {
		return protocol.OutboundAgentMessage{}, err
	}
	encryptedData, err := requiredEnvelopeField(envelope, "mapping", p.Mapping.EncryptedData)
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

	outbound, err := r.SyncClient.Sync(ctx, inbound)
	if err != nil {
		return protocol.OutboundAgentMessage{}, err
	}
	if err := protocol.ValidateOutbound(outbound); err != nil {
		return protocol.OutboundAgentMessage{}, coreerrors.Wrap(coreerrors.CodeCanonicalInvalid, "invalid outbound canonical message", err)
	}

	envelope.SetField("mapping", p.Mapping.ID, outbound.ID)
	envelope.SetField("mapping", p.Mapping.EncryptedData, outbound.EncryptedData)

	return outbound, nil
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
