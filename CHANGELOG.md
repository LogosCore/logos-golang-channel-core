# Changelog

## Unreleased

- **Module management** (`pkg/mgmt`): `Server` built from a
  `protocol.ManagementManifest` (validated at construction) that serves
  `mgmt.config.apply` (stale revisions ignored, absent keys revert to the
  property default, invalid values rejected per key while keeping the previous
  value, changed restart-mode values reported as restart-required),
  `mgmt.action.invoke` (param validation, `timeout_seconds` bound, default 30s,
  dedupe on `invocation_id`) and `mgmt.state.get` (secrets redacted).
  `AMQPConsumer` serves it on the module's `rpc_queue` (exclusive, auto-delete,
  bound to `logos.channel.rpc` with key = instance).
- **Lifecycle** `WithManagement(s)`: `module.register` declares `management`
  and `applied_revision` and applies `config.management` from every register
  ack; `module.heartbeat` sends `applied_revision` and merges status values into
  `metrics`. `BootstrapConfig.Management` holds the apply result. `Run` now
  adopts a changed heartbeat interval on re-register.
- **Module session** (`pkg/module`): `Run(ctx, Config)` keeps an instance
  registered across broker outages — connects, serves `Config.Management` on
  the rpc_queue before registering, heartbeats, deregisters on shutdown, and
  reconnects with exponential backoff (1s → 30s). `RPCQueue(instance)` gives
  the conventional `logos.channel.rpc.<instance>`.
- **Live log streaming** (`pkg/logstream`): `Hub` with a ring buffer
  (backlog), leased streams (`mgmt.logs.start` renews, `mgmt.logs.stop`,
  lease expiry), per-stream level filter and rate limit (drops counted),
  batches published to `logos.module.logs`. `Hub.Handler` wraps a
  `slog.Handler`. `mgmt.Server.ServeLogs` dispatches the RPCs and declares the
  `logs` capability; `module.Config.Logs` wires it per broker session.
- **BREAKING (lifecycle):** removed `StatusDraining`. Heartbeat health is only
  `healthy` / `degraded`; module-specific states such as an admin pause belong
  in the module's management status values.
- Deprecated `pkg/amqprpc` and `pkg/mgmtrpc` in favor of `pkg/mgmt`.

## v2.0.0

- **BREAKING: module path is now `github.com/logoscore/logos-golang-channel-core/v2`.**
  Update imports from `.../logos-golang-channel-core/pkg/...` to
  `.../logos-golang-channel-core/v2/pkg/...`.
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
