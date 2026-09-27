# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Go SDK (`github.com/logoscore/logos-golang-channel-core`) for building Logos channel modules. Channel modules bridge arbitrary transports (HTTP, Telegram, etc.) to the Logos server while remaining plaintext-blind — they never decrypt agent payloads.

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

- **`pkg/mgmtrpc`** — profile CRUD server. All mutations call `ValidateAllProfiles()` to maintain set invariants.
- **`pkg/errors`** — typed error codes (`Error` struct with `Code`, `Message`, wrapped `Err`). Use `coreerrors.New()` / `coreerrors.Wrap()` for consistency. Extract codes with `coreerrors.Code(err)`.

## Conventions

- All packages live under `pkg/`. Import paths follow `github.com/logoscore/logos-golang-channel-core/pkg/<name>`.
- The errors package is imported as `coreerrors` throughout the codebase to avoid collision with the stdlib `errors`.
- Profile validation is two-phase: schema completeness (`Validate`) then set-level constraints (`ValidateSet`).
- Combined field mapping (`CombinedIn`/`CombinedOut`) is an alternative to separate `EncryptedDataIn`/`EncryptedDataOut` fields — profiles use one or the other, not both.
