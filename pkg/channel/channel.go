// Package channel runs a channel module's control-plane session with core:
// module-core's module.Run with module_type "channel", plus what a channel
// declares at registration, its minion connection schema.
package channel

import (
	"context"
	"fmt"

	"github.com/logoscore/logos-golang-module-core/pkg/lifecycle"
	"github.com/logoscore/logos-golang-module-core/pkg/module"
	"github.com/logoscore/logos-golang-protocol/protocol"
)

// ModuleType is the module_type every channel registers with.
const ModuleType = protocol.ServiceChannel

// RPCQueue is a channel instance's conventional rpc_queue,
// logos.channel.rpc.<instance>.
func RPCQueue(instance string) string { return module.RPCQueue(ModuleType, instance) }

// Config describes one channel instance's control-plane session.
type Config struct {
	// Config is the module session. Identity.ModuleType defaults to
	// ModuleType; any other value is refused.
	module.Config
	// Connection, when set, is the minion connection schema declared at
	// registration under "connection": what minions are given to reach this
	// channel. Run validates it (protocol.ValidateConnectionSchema).
	Connection *protocol.ConnectionSchema
}

// Run keeps the channel registered until ctx is cancelled, then deregisters
// and returns nil. It returns an error only for an invalid Config. See
// module.Run.
func Run(ctx context.Context, cfg Config) error {
	switch cfg.Identity.ModuleType {
	case "":
		cfg.Identity.ModuleType = ModuleType
	case ModuleType:
	default:
		return fmt.Errorf("channel: module_type must be %q, got %q", ModuleType, cfg.Identity.ModuleType)
	}
	if cfg.Connection != nil {
		if err := protocol.ValidateConnectionSchema(*cfg.Connection); err != nil {
			return fmt.Errorf("channel: invalid connection schema: %w", err)
		}
		cfg.Options = append([]lifecycle.Option{WithConnection(*cfg.Connection)}, cfg.Options...)
	}
	return module.Run(ctx, cfg.Config)
}

// WithConnection declares s at registration under "connection", for callers
// driving lifecycle.Client themselves. Run does this from Config.Connection
// after validating s; core refuses a register whose schema is invalid.
func WithConnection(s protocol.ConnectionSchema) lifecycle.Option {
	return lifecycle.WithRegisterField("connection", s)
}
