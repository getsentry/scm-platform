# Changelog
## 3.0.0

### Breaking Changes 🛠

#### Git

- Raise `GitGrantKeyError` for a missing or unusable git grant key, so callers can tell their own misconfiguration apart from a bad grant by @vaind in [#171](https://github.com/getsentry/scm-platform/pull/171)
  - It's a `ValueError`, not an `SCMCodedError`. A public key that isn't a string used to raise `TypeError`.
  - `verify_grant([], token)` raises it. 2.x refused the grant with `GitInvalidGrant` (a 403 from `GitServer`).
- Add `require_private_key` to check a signing key at startup, and `generate_private_key` to make one for tests by @vaind in [#171](https://github.com/getsentry/scm-platform/pull/171)
- Answer a remote that isn't https, a refused remote host and an untrusted TLS certificate with a 500 instead of a 502, since no retry can fix them by @vaind in [#171](https://github.com/getsentry/scm-platform/pull/171)
- Document that `get_token` may raise a coded error, which `GitServer` answers by its code, e.g. `ResourceServiceUnavailable` as a 503 by @vaind in [#171](https://github.com/getsentry/scm-platform/pull/171)
- Upgrade notes for earlier 3.0.0 changes by @vaind in [#171](https://github.com/getsentry/scm-platform/pull/171)
  - `GitServer`'s `get_token` must return an `scm.Secret`, or every relayed request fails with `TypeError` ([#167](https://github.com/getsentry/scm-platform/pull/167)).
  - Once Sentry maps `RestrictedIPAddress` to `GitRemoteRefused` with `GitRelay(map_error=...)`, its refusal of internal addresses changes from a 503 to a 500 ([#168](https://github.com/getsentry/scm-platform/pull/168)).
- Sign git grants with Ed25519 by @vaind in [#170](https://github.com/getsentry/scm-platform/pull/170)

### New Features ✨

- (git) Let callers map adapter errors to coded errors, like a refused host by @vaind in [#168](https://github.com/getsentry/scm-platform/pull/168)

### Bug Fixes 🐛

- (git) Keep the provider token out of error reports by @vaind in [#167](https://github.com/getsentry/scm-platform/pull/167)
- (rpc) Keep request signatures out of error reports by @vaind in [#169](https://github.com/getsentry/scm-platform/pull/169)

### Documentation 📚

- (release) Describe the auto-versioned release flow by @vaind in [#166](https://github.com/getsentry/scm-platform/pull/166)

## 2.0.0

### Breaking Changes 🛠

- Keep signing secrets and git grants out of error reports by @vaind in [#162](https://github.com/getsentry/scm-platform/pull/162)

### New Features ✨

#### Git

- Add bin/git-server to check the relay against real providers by @vaind in [#161](https://github.com/getsentry/scm-platform/pull/161)
- Add git grants and a streaming git relay by @vaind in [#156](https://github.com/getsentry/scm-platform/pull/156)

#### Other

- (scm) Ask providers for a repository's git remote by @vaind in [#158](https://github.com/getsentry/scm-platform/pull/158)

### Bug Fixes 🐛

- (gitlab) Build web links from the repository path by @vaind in [#157](https://github.com/getsentry/scm-platform/pull/157)

### Internal Changes 🔧

#### Release

- Prepare releases with craft's own action by @vaind in [#165](https://github.com/getsentry/scm-platform/pull/165)
- Enable craft's auto changelog by @vaind in [#163](https://github.com/getsentry/scm-platform/pull/163)
- Let craft pick the version bump by @vaind in [#164](https://github.com/getsentry/scm-platform/pull/164)

