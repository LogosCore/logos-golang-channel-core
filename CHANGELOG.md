# Changelog

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
