import re
from collections.abc import Callable
from typing import ClassVar, Literal, Protocol

import msgspec

from scm.secret import Secret
from scm.types import BranchName, Referrer, Repository, RepositoryId

type GitOperation = Literal["fetch", "push"]
"""What a grant or rule lets its holder do. ``scm.git.relay`` maps each to its git routes."""

GRANT_VERSION: Literal[1] = 1
"""The version of ``Grant``, which ``GrantV2`` replaces."""

GRANT_HEADER = "X-Scm-Git-Grant"
"""The request header that carries a signed grant to the git endpoint."""

GRANT_MAX_LENGTH = 4096
"""The longest signed grant, in characters. The grant travels in a request header, and sandbox-router's
``PUT /grant/{sandbox_id}`` refuses a longer one, so a change here must change the router too. A grant grows with
its rules: a grant with one is under 500 characters, and the claims are base64-encoded, so a fetch rule for any
object of a repository named as ``(provider, path)`` adds about 4/3 of its path's length plus 87, e.g. 120 for a
25-character path, and each branch a rule lists adds about 4/3 of its name's length plus 4. The limit is a
conservative pick rather than a measured ceiling: header limits further along the path are typically around
8 KiB."""


class FetchRule(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    forbid_unknown_fields=True,
    omit_defaults=True,
    tag="fetch",
    tag_field="operation",
):
    """Lets the holder fetch from ``repository``: any of its objects, or only what its listed ``branches`` reach.

    A rule sets exactly one of the two, so a rule that leaves its branches out is refused rather than read as one
    that reads everything. ``any_object`` reaches every object the provider serves, including objects no branch
    reaches and, on GitHub, objects of the repository's forks, so give it only to code that runs no agent, such as a
    checkout before the agent starts.
    """

    operation: ClassVar[GitOperation] = "fetch"
    repository: RepositoryId
    branches: list[BranchName] | None = None
    """Branch names as ``git branch`` takes them, without ``refs/heads/``."""
    any_object: bool = False

    def __post_init__(self) -> None:
        if self.branches is None and not self.any_object:
            raise ValueError("A fetch rule must list its branches or set any_object.")
        if self.branches is not None and self.any_object:
            raise ValueError("A fetch rule must not both list branches and set any_object.")
        if self.branches is not None:
            _check_branches(self.branches)


class PushRule(
    msgspec.Struct,
    frozen=True,
    kw_only=True,
    forbid_unknown_fields=True,
    tag="push",
    tag_field="operation",
):
    """Lets the holder update the listed ``branches`` of ``repository``, and no other ref: no tag, and no deletion.

    A push may move a listed branch to any commit, so a force push passes. Don't list the repository's default
    branch. The git endpoint doesn't know which branch that is, so keeping it out is the signer's job, with the
    provider's branch protection behind it.
    """

    operation: ClassVar[GitOperation] = "push"
    repository: RepositoryId
    branches: list[BranchName]
    """Branch names as ``git branch`` takes them, without ``refs/heads/``."""

    def __post_init__(self) -> None:
        _check_branches(self.branches)


type Rule = FetchRule | PushRule


class Grant(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Permission to run one git operation against a fixed set of an organization's repositories.

    Deprecated: sign a ``GrantV2``, which can also keep a fetch or a push to branches. Verifiers still accept this
    version, so a signer can switch once they can read both.

    A push grant lets its holder update any ref of its repositories, including tags and deletions, since GitServer
    relays its pushes unread. Each repository is a ``RepositoryId`` under the grant's organization, as in the RPC
    endpoint: Sentry's repository id or a ``(provider, external id)`` pair.

    A grant with a field this version does not know is refused rather than read without it. If a later version
    adds a field that restricts a grant, a verifier that ignored the field would allow more than the grant does.
    """

    organization_id: int
    referrer: Referrer
    """Who the grant is for, e.g. a coding job or Seer itself, as ``referrer`` is elsewhere in this library.
    Sentry tags the git endpoint's metrics with it. Seer sets it, because git requests carry nothing else that
    tells callers apart."""
    operation: GitOperation
    repositories: list[RepositoryId]
    expires_at: int
    """Unix time in seconds. Every grant expires."""
    version: Literal[1] = GRANT_VERSION
    """The format version. A verifier refuses a version it does not know."""


class GrantV2(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Permission to run git operations on an organization's repositories, as a list of rules.

    Seer signs a grant for each sandbox, and for its own git requests, and Sentry's git endpoint verifies it on
    every request. A sandbox holds one grant at a time, so a grant that lets a pod both fetch and push holds a rule
    for each. A request runs only if a rule for its repository and operation allows it, and a grant holds at most one
    rule per repository and operation.

    Each rule names its repository as a ``RepositoryId`` under the grant's organization, as in the RPC endpoint:
    Sentry's repository id or a ``(provider, external id)`` pair. Several repositories leave room for submodules and
    git-based dependencies, but only from repositories Sentry has connected for the organization: naming any other
    repository, such as a public dependency, would need a new kind of rule.

    A grant or rule with a field this version does not know is refused rather than read without it, because a
    verifier that ignored a field restricting the grant would allow more than the grant does. So a new field needs
    only the verifiers to know it before the signer uses it, and a new version only when a field changes meaning.
    """

    organization_id: int
    referrer: Referrer
    """Who the grant is for, e.g. a coding job or Seer itself, as ``referrer`` is elsewhere in this library.
    Sentry tags the git endpoint's metrics with it. Seer sets it, because git requests carry nothing else that
    tells callers apart."""
    rules: list[Rule]
    expires_at: int
    """Unix time in seconds. Every grant expires."""
    version: Literal[2] = 2
    """The format version. A verifier refuses a version it does not know, so one that knows only ``Grant`` refuses
    this one rather than reading its rules as broader than they are."""

    def __post_init__(self) -> None:
        if not self.rules:
            raise ValueError("A git grant must hold at least one rule.")
        seen: set[tuple[GitOperation, RepositoryId]] = set()
        for rule in self.rules:
            key = (rule.operation, rule.repository)
            if key in seen:
                raise ValueError(f"A git grant holds more than one {rule.operation} rule for {rule.repository!r}.")
            seen.add(key)

    def rule_for(self, repository: RepositoryId, operation: GitOperation) -> Rule | None:
        """Return the rule for ``operation`` on ``repository``, named as the rule names it, or None if there is none."""
        return next(
            (rule for rule in self.rules if rule.repository == repository and rule.operation == operation), None
        )


# What git check-ref-format refuses in a ref name: control characters, space, ~ ^ : ? * [ \, "..", "@{" and "//".
_NOT_IN_A_REF = re.compile(r"[\x00-\x20\x7f~^:?*\[\\]|\.\.|@\{|//")


def _check_branches(branches: list[BranchName]) -> None:
    if not branches:
        raise ValueError("A rule's branch list must not be empty.")
    for name in branches:
        # git check-ref-format's rules for refs/heads/<name>, with what git branch refuses on top (a leading "-" and
        # "HEAD"). A name starting with refs/ is a valid branch, but it's far more likely a full ref name by mistake.
        if (
            not name
            or name in ("@", "HEAD")
            or name.startswith(("-", "/", "refs/"))
            or name.endswith(("/", "."))
            or _NOT_IN_A_REF.search(name)
            or any(part.startswith(".") or part.endswith(".lock") for part in name.split("/"))
        ):
            raise ValueError(
                f"{name!r} is not a branch name. Name a branch as git branch takes it, without refs/heads/."
            )


type GetToken = Callable[[Repository, GitOperation], Secret | None]
"""Return a provider token that can run ``operation`` on ``repository``, or None if there is none.

Wrap the token in a ``Secret`` where you read it. GitServer and the relay hold it in their frames' local variables,
which an error report sends.

It may raise an ``SCMCodedError`` to say why there is no token. GitServer answers it as it answers its own
refusals, with the status for its code, or a 500 for a code it has none for. A ``ResourceServiceUnavailable`` for a
failure that may pass becomes a 503, which the client retries. Any other exception reaches GitServer's caller."""


class Readable(Protocol):
    """A request body the relay pulls from as it sends upstream."""

    def read(self, size: int = -1, /) -> bytes: ...
