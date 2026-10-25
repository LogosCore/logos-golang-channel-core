# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Go SDK (`github.com/logoscore/logos-golang-channel-core/v2`) for building Logos channel modules. Channel modules bridge arbitrary transports (HTTP, Telegram, etc.) to the Logos server while remaining plaintext-blind — they never decrypt agent payloads.

Depends on `github.com/logoscore/logos-golang-protocol` for canonical message types and validation.

## Build & Test

```bash
go build ./...        # build all packages
go test ./...         # run all tests
go test ./pkg/runtime # run tests for a single package
go test -run TestHandleSuccess ./pkg/runtime  # run a single test
go vet ./...          # static analysis
```

No Makefile or task runner — use standard Go tooling. Go version: 1.25.7.

### Documentation Submodule

Upstream docs and specs live in a git submodule at `docs/logos-docs` (from `https://github.com/logoscore/logos-docs`). Fetch/update it when you need reference documentation:

```bash
git submodule update --init --recursive  # first clone or after a fresh checkout
git submodule update --remote docs/logos-docs  # pull latest docs
```

## Architecture

### Core Pipeline

The runtime orchestrates a single request through:

1. **Extract** — read fields from a `TransportEnvelope` (transport-specific adapter)
2. **Canonicalize** — build a `protocol.InboundMinionMessage`, validate it
3. **Sync** — send canonical message to Logos via `SyncClient.Sync()`
4. **Write back** — map outbound canonical fields back onto the envelope

Two entrypoints: `runtime.Handle()` (basic) and `runtime.HandleWithProfile()` (profile-aware with field mapping and transforms).

### Key Interfaces (pkg/runtime/types.go)

- **`TransportEnvelope`** — channel adapters implement this (`GetField`, `SetField`, `SourceKey`). Each transport (HTTP handler, bot webhook, etc.) wraps its native request in this interface.
- **`SyncClient`** — abstraction over Logos communication. `pkg/syncclient.HTTPClient` is the concrete implementation (POST to `/api/channel/sync`).
- **`ProfileStore`** — persistence for transposition profiles. Callers provide their own implementation (in-memory, database, etc.).

### Transposition Profiles

Profiles define how canonical fields map to/from transport-specific locations. A channel can have multiple profiles; exactly one enabled profile must be the default fallback.

- **`pkg/profile`** — YAML parsing (`ParseYAML`, `ParseYAMLProfiles`), model types, validation (single-profile via `Validate`, cross-profile via `ValidateSet`)
- **`pkg/matcher`** — resolves which profile to use: hint-first (explicit profile ID from transport), then brute-force via `EnabledOrdered` (priority desc, then profile_id asc)
- **`pkg/transform`** — reversible transform pipeline on mapped fields (base64, base64url, prefix, suffix, replace, url_encode/url_decode). `ApplyEncode` runs forward; `ApplyDecode` runs in reverse order for roundtrip correctness.
- **`pkg/resolver`** — extracts values from transport input by `"location:key"` refs (body, header, query, cookie)
- **`pkg/cache`** — thread-safe TTL cache mapping source keys to profile IDs (source affinity)

### Management & Errors

- **`pkg/mgmt`** — module management: `Server` built from a `protocol.ManagementManifest` handles `mgmt.config.apply` / `mgmt.action.invoke` / `mgmt.state.get` (`Handle` for any transport); `AMQPConsumer` serves it on the module's `rpc_queue`. Start the consumer before registering.
- **`pkg/lifecycle`** — module register/heartbeat/deregister client. `WithManagement(s)` declares the manifest, applies `config.management` from every register ack, and reports applied revision + status in heartbeats.
- **`pkg/module`** — `Run` supervises a module's whole control-plane session: connects, serves the `mgmt.Server` (and `Config.Logs`, which declares the `logs` capability) on the rpc_queue (default `RPCQueue(instance)`), registers, heartbeats, deregisters on shutdown, and reconnects with backoff on any failure. Plumbing only — never adds actions or properties of its own; module-specific behavior goes in the module's manifest.
- **`pkg/logstream`** — live log tail; records keep the module's attr structure (slog groups stay nested objects, as in its JSON output) and are never reshaped. `Hub` keeps a ring buffer of recent records and, while core holds a leased stream (`mgmt.logs.start`, renewed by repeating it; `mgmt.logs.stop`), publishes batches to `logos.module.logs` keyed by stream id. Feed it with `Hub.Handler(next)` (slog; debug is built only while a stream asks for it). Rate-limited per stream; drops are counted, never block logging. Nothing is persisted.
- **`pkg/logging`** — `New(format, w, level, hub)` builds the module logger (`json` default, or `text`; teed into the `logstream.Hub`); `Headers(h)` logs every HTTP header as a `headers` group (lowercased names, unredacted).
- **`pkg/mgmtrpc`** / **`pkg/amqprpc`** — deprecated profile CRUD server and its ad-hoc AMQP transport; use `pkg/mgmt`.
- **`pkg/errors`** — typed error codes (`Error` struct with `Code`, `Message`, wrapped `Err`). Use `coreerrors.New()` / `coreerrors.Wrap()` for consistency. Extract codes with `coreerrors.Code(err)`.

## Conventions

- All packages live under `pkg/`. Import paths follow `github.com/logoscore/logos-golang-channel-core/v2/pkg/<name>`.
- The errors package is imported as `coreerrors` throughout the codebase to avoid collision with the stdlib `errors`.
- Profile validation is two-phase: schema completeness (`Validate`) then set-level constraints (`ValidateSet`).
- Combined field mapping (`CombinedIn`/`CombinedOut`) is an alternative to separate `EncryptedDataIn`/`EncryptedDataOut` fields — profiles use one or the other, not both.
