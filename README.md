# logos-golang-channel-core

Golang foundation SDK for building Logos channel modules.

## v0 Architecture Summary

The repository now includes a compileable v0 scaffold with the primary package boundaries:

- `pkg/runtime`: channel runtime orchestrator and core interfaces
- `pkg/profile`: profile model, YAML parsing helpers, semantic validation
- `pkg/matcher`: hint-first profile selection with deterministic fallback resolution
- `pkg/transform`: reusable transform pipeline (`base64`, `base64url`)
- `pkg/resolver`: first-class location resolver helper (`body/header/query/cookie` refs)
- `pkg/cache`: source affinity cache utility (TTL)
- `pkg/syncclient`: HTTP sync client for Logos (`POST /api/channel/sync`)
- `pkg/mgmtrpc`: management RPC handlers for profile CRUD/activate/validate
- `pkg/errors`: typed error codes and helpers

Core runtime interfaces:

- `TransportEnvelope`: `GetField`, `SetField`, `SourceKey`
- `SyncClient`: canonical sync contract
- `ProfileStore`: list/get/put/delete profile persistence contract

Runtime pipeline entrypoint:

- `Handle(ctx, envelope, channelID) (protocol.OutboundAgentMessage, error)`
- `HandleWithProfile(ctx, envelope, channelID, profile) (protocol.OutboundAgentMessage, error)`
- Performs basic canonical inbound/outbound validation around sync execution.

Matcher resolution:

- `Resolve(ctx, hintProfileID, candidates) (Resolution, error)`
- Returns match metadata (`hint` or `fallback`) and typed errors for ambiguous/not-found outcomes.

## Quick Start

### 1) Test the scaffold

```bash
go test ./...
```

### 2) Wire your channel adapter envelope

Implement `runtime.TransportEnvelope` in your transport layer and populate:

- `mapping.id`
- `mapping.encrypted_data`
- optional `mapping.profile_id`

Then call:

```go
rt := runtime.New(mySyncClient)
out, err := rt.Handle(ctx, envelope, channelID)
```

### 3) Parse and validate profiles

```go
p, err := profile.ParseYAML(data)
if err != nil {
    // invalid YAML or semantic profile error
}
```

## Notes

- Depends on released protocol module: `github.com/logoscore/logos-golang-protocol@v0.1.0`.
- See `docs/FOUNDATION-PLAN.md` for broader roadmap details.
