# logos-golang-channel-core — Foundation Plan (v1)

## Goal

Provide a ready-to-use Golang foundation so community contributors can quickly build custom channel modules with minimal boilerplate.

The package must:
- keep channel modules plaintext-blind
- standardize canonical channel<->Logos message handling
- support transposition profiles (YAML)
- provide production-grade runtime primitives (timeouts, retries, metrics, logging)

---

## Scope boundaries

### In scope for `logos-golang-channel-core` v1
- Channel runtime pipeline (ingest -> profile resolve -> canonicalize -> sync -> re-transpose)
- HTTP sync client for Logos endpoint (`POST /api/channel/sync`)
- Transposition profile engine (parse, validate, select, apply)
- YAML profile storage + cache
- RabbitMQ RPC management surface for profile operations
- Observability hooks (logs, metrics, trace IDs)
- Error model and retry policy

### Out of scope for v1
- Minion cryptography internals (decrypt/encrypt payload semantics)
- UI components
- Full multi-tenant control plane implementation

---

## Proposed package ecosystem

1. `logos-golang-protocol` (separate module)
   - canonical message types (`inbound.agent_message`, `outbound.agent_message`)
   - schema versioning + validation helpers
   - shared error/status codes

2. `logos-golang-channel-core` (this module)
   - runtime orchestration for channel modules
   - transposition profile management and matching
   - sync client + management RPC server

3. `logos-golang-channel-adapters` (future)
   - optional transport adapters (HTTP, Telegram, GitHub, etc.)

---

## Internal architecture for channel-core

### Packages
- `pkg/runtime`
  - channel runtime orchestrator
  - request lifecycle and policy orchestration
- `pkg/syncclient`
  - Logos HTTP sync client
  - retries, backoff, timeout budget
- `pkg/profile`
  - profile model, YAML parser, schema/semantic validator
  - overlap detection
- `pkg/matcher`
  - profile selection engine
  - hint-first (`profile_id`), cache-first, brute-force fallback
- `pkg/transform`
  - reversible transforms (base64/base64url, aliases, wrappers)
- `pkg/store`
  - persistent profile store abstraction + YAML implementation
- `pkg/mgmtrpc`
  - RabbitMQ RPC server for profile CRUD/activate/validate/stats
- `pkg/telemetry`
  - logging/metrics/tracing interfaces + default adapters
- `pkg/errors`
  - typed errors and machine-readable codes

---

## Core interfaces (developer UX)

```go
type TransportEnvelope interface {
    SourceKey() string // e.g. ip/chat/account fingerprint for affinity cache
    GetField(location, key string) (string, bool)
    SetField(location, key, value string)
}

type SyncClient interface {
    Sync(ctx context.Context, in protocol.InboundAgentMessage) (protocol.OutboundAgentMessage, error)
}

type ProfileStore interface {
    List(ctx context.Context, channelID string) ([]Profile, error)
    Get(ctx context.Context, channelID, profileID string) (Profile, error)
    Put(ctx context.Context, channelID string, p Profile) error
    Delete(ctx context.Context, channelID, profileID string) error
}
```

Goal: transport implementers only adapt envelope I/O and call runtime handler.

---

## Transposition profile strategy

### Required profile fields
- `profile_id`
- `channel_type`
- `enabled`
- `default_fallback` (exactly one enabled per channel)
- `priority`
- `match` pre-filters
- `mapping.profile_id` (optional hint)
- `mapping.id`
- `mapping.encrypted_data`

### Inbound selection order
1. Resolve profile via transport `profile_id` hint
2. Source affinity cache hit (e.g. IP/chat/account -> profile)
3. Match-prefiltered candidates by priority/frequency
4. Default fallback profile last

### Conflict prevention
On create/update:
- reject profile ID collisions
- reject ambiguous overlap among enabled profiles
- enforce single enabled fallback profile

---

## Performance model

- In-memory profile index per channel
- Source affinity cache with TTL
- Per-profile success counters to reorder matching
- Max profile attempts guardrail per request

---

## Reliability model

- Per-request timeout budget
- Retry with bounded exponential backoff for sync client
- Idempotency support via `message_id`
- Deterministic typed errors:
  - `ERR_PROFILE_NOT_FOUND`
  - `ERR_PROFILE_AMBIGUOUS`
  - `ERR_PROFILE_INVALID`
  - `ERR_SYNC_TIMEOUT`
  - `ERR_SYNC_REJECTED`

---

## Security model

- Channel never decrypts `encrypted_data`
- Profile engine must not inspect payload plaintext
- Management RPC must be authenticated/authorized
- Profile changes are auditable (who/when/what)

---

## Testing strategy

### Unit
- profile schema + semantic validation
- overlap detection
- matcher ordering and fallback
- transform roundtrip tests

### Integration
- fake transport -> runtime -> fake Logos sync server
- RabbitMQ mgmt RPC CRUD flows
- profile persistence reload tests

### Load
- many profiles and high request rate
- cache hit/miss behavior

---

## Delivery phases

### Phase 1 — Contracts and skeleton
- initialize Go module
- define interfaces and package boundaries
- integrate `logos-golang-protocol` dependency stub

### Phase 2 — Profile engine
- YAML schema
- parser + validator
- overlap detection

### Phase 3 — Runtime
- inbound resolve + canonical mapping
- sync client integration
- outbound re-transposition

### Phase 4 — Management RPC
- RabbitMQ RPC endpoints for profile operations
- validation and audit events

### Phase 5 — Hardening
- telemetry
- benchmarks
- examples and contributor docs

---

## Open decisions

1. Keep profile persistence inside channel module or shared DB driver package?
2. Should management RPC be request/reply only, or also emit async profile-change events?
3. Canonical protocol versioning policy (`v1`, `v1.1`, compatibility window)?
4. Minimal transform set for v1 vs plugin transform registry?
