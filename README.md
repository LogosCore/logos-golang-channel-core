# logos-golang-channel-core

Golang foundation SDK for building Logos channel modules.

The control plane every module type shares lives in
[`logos-golang-module-core`](https://github.com/logoscore/logos-golang-module-core):
`lifecycle`, `module`, `mgmt`, `logstream` and `logging`. This SDK builds on
it with what is specific to channels.

## Packages

- `pkg/channel`: `Run(ctx, Config)` runs the channel's control-plane session (module-core's `module.Run` with module_type `channel`) and declares its minion connection schema (`Config.Connection`)
- `pkg/runtime`: channel runtime orchestrator and core interfaces
- `pkg/profile`: profile model, YAML parsing helpers, semantic validation
- `pkg/matcher`: hint-first profile selection with deterministic fallback resolution
- `pkg/transform`: reusable transform pipeline (`base64`, `base64url`)
- `pkg/resolver`: first-class location resolver helper (`body/header/query/cookie` refs)
- `pkg/cache`: source affinity cache utility (TTL)
- `pkg/syncclient`: HTTP sync client for Logos (`POST /api/channel/sync`)
- `pkg/errors`: channel error codes on module-core's typed error

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

- Depends on `github.com/logoscore/logos-golang-module-core` and `github.com/logoscore/logos-golang-protocol`.
- See `docs/FOUNDATION-PLAN.md` for broader roadmap details.
