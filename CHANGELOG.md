# Changelog
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

