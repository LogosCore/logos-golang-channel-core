# Changelog

## Unreleased

- **Module lifecycle client** (`pkg/lifecycle`): module-side client for the
  registration/liveness control-plane contract (`module.register` /
  `module.heartbeat` / `module.deregister`). Provides `Client` with
  `Register`/`Heartbeat`/`Deregister` plus a `Run` loop (register → heartbeat on
  the core-provided interval → graceful deregister on shutdown, with
  re-register on `unknown_instance`/`config_changed`). Transport-agnostic via the
  `Publisher` interface, with a RabbitMQ `AMQPPublisher` (publishes to
  `logos.core.rpc`, reads replies over direct reply-to). Implements
  contracts/module-lifecycle.md and ADR-0003.
- **BREAKING: adopt protocol v0.2.x.** Bumped `logos-golang-protocol` to
  `v0.2.1`. Canonical message types renamed `InboundAgentMessage` →
  `InboundMinionMessage` and `OutboundAgentMessage` → `OutboundMinionMessage`
  (agent → minion terminology); `OutboundMinionMessage` no longer carries a
  `source`. This changes the `runtime.SyncClient` / `ActionHandlerFunc`
  signatures. Lifecycle uses the new control-plane envelope helpers
  (`Envelope`, `ReplyEnvelope`, `NewReply`, `NewErrorReply`, `NewULID`, ...).

## v0.3.0

- Switched profile parsing to `yaml.v3` for robust YAML handling.
- Added profile set validation rules:
  - exactly one enabled `default_fallback`
  - baseline overlap detection for enabled `mapping.profile_id`
- Added tests for profile set validation.

## v0.2.0

- Added matcher resolution contract:
  - `Resolve(...)` + `MatchSource`
  - typed errors for ambiguous/not-found outcomes
- Added profile-aware runtime entrypoint: `HandleWithProfile(...)`.
- Added matcher/runtime tests for new profile-aware flow.

## v0.1.0

- Initial channel-core scaffold:
  - runtime interfaces and sync path
  - profile model/parsing/validation baseline
  - matcher/syncclient/mgmtrpc skeletons
