## Introduction

The SCM (Source Code Management) platform is a vendor-agnostic abstraction layer for interacting with source code management service-providers such as GitHub, GitLab, and Bitbucket. It decouples Sentry's product features from service-provider-specific APIs by presenting a single, declarative interface for both reading and writing SCM resources and for reacting to SCM webhook events.

### Goals

1. **Service-provider independence.** Product code should never import a service-provider's client or parse a service-provider's response format directly. All interactions should flow through a common interface. Adding new service-providers should not require changes to existing implementations.
2. **Declarative usage.** Callers should describe _what_ they want (e.g. "create a pull request") not _how_ to accomplish it. Initialization, authentication, rate limiting, and response mapping are handled internally.
3. **Centrally enforced access controls.** Access controls must be strictly and consistently enforced across all SCM service-providers to prevent unprivileged access to sensitive customer data. The security model should be implemented once and applied universally.
4. **Observable.** Every outbound action and every inbound webhook listener automatically records success/failure metrics, emits traces, and reports errors and logs to Sentry. The health of the SCM platform should always be knowable.
5. **Extensible.** The SCM platform should be maximally and trivially extensible. As core infrastructure it should mutate as business needs change and not ossify a particular implementation.

### Features

The platform exposes four subsystems:

- **Actions** — outbound SCM operations initiated by Sentry code. The `SourceCodeManager` class provides 70+ methods covering comments, reactions, pull requests, branches, git objects, reviews, and check runs. With more actions planned to be added as we port more use cases.
- **Actions RPC** - outbound SCM commands exposed over the network. The `SourceCodeManager` is fully available over the network. This enables usage of the SCM for services outside the monolith.
- **Git relay** - `git` fetches and pushes for services that hold no provider credentials. Seer signs a grant naming the organization, operation and repositories with `scm.git.grant.sign_grant`, and `scm.git.server.GitServer` checks each git smart-HTTP request against it before relaying the request to the provider with a token, streaming both bodies.
- **Event Stream** — inbound webhook processing. SCM service-providers push events which are deserialized into typed, provider-neutral dataclasses (`CheckRunEvent`, `CommentEvent`, `PullRequestEvent`) which are then dispatched to registered listener functions.

### Why

Ad-hoc usage of a SCM service-provider's API client tightly couples your application code to that provider. Ad-hoc management of access controls increases Sentry's security vulnerability surface area (specifically IDOR vulnerabilities). And ad hoc use of API clients can lead to resource exhaustion, starving critical product features of quota without consideration.

You should not need to care about any of these things. These problems should be solved once and managed for you. It should be impossible for you to perform an action which violates a security boundary. It should be impossible for your usage of the SCM to break another feature's SLO. The less you have to think about, the more you can focus on solving the business case.

The SCM solves all the problems you don't want to care about.

### Getting Started

We have extensive documentation both inline in the Sentry codebase and on the [Sentry developer documentation](https://develop.sentry.dev/backend/source-code-management-platform/) portal. If you're interested in expanding your SCM usage or in enabling new service-providers for a limited amount of effort take a took at the SCM platform.

# Releasing a New Version

1. On the `getsentry/scm-platform` repository page click the **Actions** tab.
2. On the left-hand side click the **release** workflow.
3. Click **Run workflow** and run it with the default bump type, `auto`. [Craft](https://github.com/getsentry/craft) picks the version from the titles of the PRs merged since the last release tag: `type!:` (e.g. `fix!:`) makes a major release, `feat:` a minor one, and any other conventional-commit type a patch. Choose `major`, `minor`, or `patch` only to override it.
4. The workflow creates a `release/<version>` branch with the version bump and the new `CHANGELOG.md` section, and opens an issue on `getsentry/publish` titled `publish: getsentry/scm-platform@<version>`. Check that the version and changelog in the issue are what you expect.
5. On the `getsentry/publish` issue set the **accepted** label. Once CI passes on the release branch, Craft publishes to PyPI and the internal `getsentry/pypi` index, creates the GitHub release with the same changelog, and merges the release branch into `main`.
6. A pull request adding the version to `getsentry/pypi` is opened and merged automatically. Once it's merged and that repository's build finishes, the new version is available for use.

Because the changelog and version come from PR titles, give every PR a [conventional-commit](https://www.conventionalcommits.org/) title, and mark breaking changes with `!`.

To retract a release request, comment `#retract` on a line by itself on the `getsentry/publish` issue, close the issue, and delete the `release/<version>` branch before running the workflow again.