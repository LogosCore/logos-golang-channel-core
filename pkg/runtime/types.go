package runtime

import (
	"context"

	"github.com/logoscore/logos-golang-channel-core/pkg/profile"
	protocol "github.com/logoscore/logos-golang-protocol/protocol"
)

// TransportEnvelope is adapted by channel implementations.
type TransportEnvelope interface {
	SourceKey() string
	GetField(location, key string) (string, bool)
	SetField(location, key, value string)
}

// SyncClient sends canonical inbound messages and receives canonical outbound messages.
type SyncClient interface {
	Sync(ctx context.Context, in protocol.InboundMinionMessage) (protocol.OutboundMinionMessage, error)
}

// ActionHandlerFunc handles a matched profile action after inbound extraction.
type ActionHandlerFunc func(ctx context.Context, params map[string]any, inbound protocol.InboundMinionMessage, envelope TransportEnvelope) (protocol.OutboundMinionMessage, error)

// ProfileStore provides persistence for channel profiles.
type ProfileStore interface {
	List(ctx context.Context, channelID string) ([]profile.Profile, error)
	Get(ctx context.Context, channelID string, profileID int32) (profile.Profile, error)
	Put(ctx context.Context, channelID string, p profile.Profile) error
	Delete(ctx context.Context, channelID string, profileID int32) error
}
