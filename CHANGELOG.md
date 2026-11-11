# Changelog
## 4.0.0

### Breaking Changes 🛠

#### Git

- Make a git grant a list of fetch and push rules for repositories, branches and tags, and keep pushes to their rules' branches and tags. `verify_grant` refuses version 1 grants. by @vaind and @claude in [#188](https://github.com/getsentry/scm-platform/pull/188)
  - Sign `Grant(rules=[...])` in place of `Grant(operation=..., repositories=[...])`. A fetch grant becomes a `FetchRule(repository=..., any_object=True)` for each repository.
  - `GRANT_VERSION` and `scm.git.relay.authorize_request` are gone. `GitServer` is the only way to check a request against a grant.
- 🤖 Generated with [Claude Code](https://claude.com/claude-code) by @vaind and @claude in [#188](https://github.com/getsentry/scm-platform/pull/188)

### New Features ✨

#### Bitbucket

- Add commit reads by @wedamija in [#180](https://github.com/getsentry/scm-platform/pull/180)
- Add file reads by @wedamija in [#179](https://github.com/getsentry/scm-platform/pull/179)
- Add the git remote by @wedamija in [#178](https://github.com/getsentry/scm-platform/pull/178)
- Add a Bitbucket Cloud provider by @wedamija in [#176](https://github.com/getsentry/scm-platform/pull/176)

### Internal Changes 🔧

- (release) Add pre bump and version override by @BYK with @sentry-junior in [#187](https://github.com/getsentry/scm-platform/pull/187)
- (rpc) Let providers send raw request bodies by @wedamija in [#177](https://github.com/getsentry/scm-platform/pull/177)

## 3.1.1

### Bug Fixes 🐛

- (github) Percent-encode branch names and refs in URL paths by @joseph-sentry in [#175](https://github.com/getsentry/scm-platform/pull/175)

## 3.1.0

### New Features ✨

#### Git

- Add `key_id` to get a git grant key's `kid` from its public key, so callers needn't parse the grant's JWT header by @vaind in [#173](https://github.com/getsentry/scm-platform/pull/173)
- 🤖 Generated with [Claude Code](https://claude.com/claude-code) by @vaind in [#173](https://github.com/getsentry/scm-platform/pull/173)

### Bug Fixes 🐛

- (git) Report a provider stream breaking mid-response as a coded error by @vaind in [#172](https://github.com/getsentry/scm-platform/pull/172)

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

