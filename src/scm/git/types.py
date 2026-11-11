import re
from collections.abc import Callable
from typing import ClassVar, Literal, Protocol

import msgspec

from scm.secret import Secret
from scm.types import BranchName, Referrer, Repository, RepositoryId

type GitOperation = Literal["fetch", "push"]
"""What a rule lets its holder do. ``scm.git.relay`` maps each to its git routes."""

GRANT_HEADER = "X-Scm-Git-Grant"
"""The request header that carries a signed grant to the git endpoint."""

GRANT_MAX_LENGTH = 4096
"""The longest signed grant, in characters. The grant travels in a request header, and sandbox-router's
``PUT /grant/{sandbox_id}`` refuses a longer one, so a change here must change the router too. A grant grows with
its rules: a grant with one is under 500 characters, and the claims are base64-encoded, so a fetch rule for any
object of a repository named as ``(provider, path)`` adds about 4/3 of its path's length plus 87, e.g. 120 for a
25-character path, and each branch or tag a rule lists adds about 4/3 of its name's length plus 4. The limit is a
conservative pick rather than a measured ceiling: header limits further along the path are typically around
8 KiB."""


class _GrantV2RefRule(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True, omit_defaults=True):
    """What fetch and push rules share: the repository, and the branches and tags of it the rule covers.

    Each kind of ref is set by a list of names or by patterns, and not both. A pattern covers only names of its own
    kind, so a pattern too broad for branches still can't reach a tag or any other ref.
    """

    repository: RepositoryId
    branches: list[BranchName] | None = None
    """Branch names as ``git branch`` takes them, without ``refs/heads/``."""
    branch_patterns: list[str] | None = None
    """Python regular expressions, each matched against a branch's whole name without ``refs/heads/``, such as
    ``seer/.+``. They run with ``re.ASCII``, so ``\\d`` and ``\\w`` don't match lookalikes such as fullwidth digits. The
    pushing client chooses the names they are matched against, so a pattern must not be one that backtracks
    catastrophically."""
    tags: list[str] | None = None
    """Tag names as ``git tag`` takes them, without ``refs/tags/``."""
    tag_patterns: list[str] | None = None
    """Like ``branch_patterns``, each matched against a tag's whole name without ``refs/tags/``."""

    def __post_init__(self) -> None:
        for names_field, names, patterns_field, patterns, kind in (
            ("branches", self.branches, "branch_patterns", self.branch_patterns, "branch"),
            ("tags", self.tags, "tag_patterns", self.tag_patterns, "tag"),
        ):
            if names is not None and patterns is not None:
                raise ValueError(f"A rule must not set both {names_field} and {patterns_field}.")
            if names is not None:
                _check_names(names_field, names, kind)
            if patterns is not None:
                _check_patterns(patterns_field, patterns)

    @property
    def refs(self) -> list[str]:
        """The full names of the refs the rule lists, such as ``refs/heads/main``. Its patterns cover more."""
        return [f"refs/heads/{name}" for name in self.branches or ()] + [
            f"refs/tags/{name}" for name in self.tags or ()
        ]

    def covers(self, ref: str) -> bool:
        """Return whether the rule covers ``ref``, a full ref name such as ``refs/heads/main``."""
        if ref in self.refs:
            return True
        if ref.startswith("refs/heads/"):
            name, patterns = ref.removeprefix("refs/heads/"), self.branch_patterns
        elif ref.startswith("refs/tags/"):
            name, patterns = ref.removeprefix("refs/tags/"), self.tag_patterns
        else:
            return False
        return any(re.fullmatch(pattern, name, re.ASCII) for pattern in patterns or ())

    def _names_refs(self) -> bool:
        return any(field is not None for field in (self.branches, self.branch_patterns, self.tags, self.tag_patterns))


class GrantV2FetchRule(_GrantV2RefRule, frozen=True, tag="fetch", tag_field="operation"):
    """Lets the holder fetch from ``repository``: any of its objects, or only what the branches and tags it covers
    reach.

    A rule sets ``any_object`` or names refs, and not both, so a rule that names no refs is refused rather than read
    as one that reads everything. ``any_object`` reaches every object the provider serves, including objects no ref
    reaches and, on GitHub, objects of the repository's forks, so give it only to code that runs no agent, such as a
    checkout before the agent starts.
    """

    operation: ClassVar[GitOperation] = "fetch"
    any_object: bool = False

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.any_object and self._names_refs():
            raise ValueError(
                "A fetch rule that sets any_object must not set branches, branch_patterns, tags or tag_patterns."
            )
        if not self.any_object and not self._names_refs():
            raise ValueError("A fetch rule must set branches, branch_patterns, tags, tag_patterns or any_object.")

    def covers(self, ref: str) -> bool:
        return self.any_object or super().covers(ref)


class GrantV2PushRule(_GrantV2RefRule, frozen=True, tag="push", tag_field="operation"):
    """Lets the holder create or update the branches and tags of ``repository`` the rule covers, and no other ref,
    and delete none.

    With ``create_only`` it allows only creating a covered branch or tag that doesn't exist yet, so a pattern can let
    the holder start new branches or tag new releases without moving any that exist. Without it, a push may move a
    covered branch or tag to any commit, so a force push passes.

    Don't cover the repository's default branch, by name or by pattern. The git endpoint doesn't know which branch
    that is, so keeping it out is the signer's job, with the provider's branch protection behind it. Keep tag patterns
    to a release shape such as ``v[0-9]+\\.[0-9]+\\.[0-9]+``: git resolves a short name to a tag before a branch, so a
    tag named ``main`` redirects whoever fetches or resolves ``main``, and a new tag often starts a release pipeline.
    """

    operation: ClassVar[GitOperation] = "push"
    create_only: bool = False
    """Allow only creating a covered branch or tag. git sends a new ref with an old object id of all zeros, and the
    provider refuses such an update if the ref already exists."""

    def __post_init__(self) -> None:
        super().__post_init__()
        if not self._names_refs():
            raise ValueError("A push rule must set branches, branch_patterns, tags or tag_patterns.")


class GrantV2(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Permission to run git operations on an organization's repositories, as a list of rules.

    Seer signs a grant for each sandbox, and for its own git requests, and Sentry's git endpoint verifies it on
    every request. A sandbox holds one grant at a time, so a grant that lets a pod both fetch and push holds a rule
    for each. A request runs only if a rule for its repository and operation allows it. Several rules may name the
    same repository and operation, and a request runs if any of them allows it, so their order doesn't matter: a
    ``create_only`` rule with a pattern can sit next to a rule listing a branch the holder created and may update.

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
    rules: list[GrantV2FetchRule | GrantV2PushRule]
    expires_at: int
    """Unix time in seconds. Every grant expires."""
    version: Literal[2] = 2
    """The format version. A verifier refuses a version it does not know, so one that knows only version 1 refuses
    this one rather than reading its rules as broader than they are."""

    def __post_init__(self) -> None:
        if not self.rules:
            raise ValueError("A git grant must hold at least one rule.")

    def rules_for(self, repository: RepositoryId, operation: GitOperation) -> list[GrantV2FetchRule | GrantV2PushRule]:
        """Return the rules for ``operation`` on ``repository``, named as the rules name it."""
        return [rule for rule in self.rules if rule.repository == repository and rule.operation == operation]


# What git check-ref-format refuses in a ref name: control characters, space, ~ ^ : ? * [ \, "..", "@{" and "//".
_NOT_IN_A_REF = re.compile(r"[\x00-\x20\x7f~^:?*\[\\]|\.\.|@\{|//")


def _check_patterns(field: str, patterns: list[str]) -> None:
    if not patterns:
        raise ValueError(f"A rule's {field} must not be empty.")
    for pattern in patterns:
        if not pattern:
            raise ValueError(f"A pattern in a rule's {field} must not be empty.")
        try:
            re.compile(pattern, re.ASCII)
        except (re.error, ValueError) as e:
            raise ValueError(f"{pattern!r} is not a regular expression: {e}") from None


def _check_names(field: str, names: list[str], kind: str) -> None:
    if not names:
        raise ValueError(f"A rule's {field} must not be empty.")
    for name in names:
        if not _is_short_name(name):
            prefix = "refs/heads/" if kind == "branch" else "refs/tags/"
            raise ValueError(f"{name!r} is not a {kind} name. Name a {kind} as git {kind} takes it, without {prefix}.")


def _is_short_name(name: str) -> bool:
    """Return whether ``name`` is a branch or tag name, without its refs/heads/ or refs/tags/."""
    # What git branch and git tag refuse on top of git check-ref-format's rules: a leading "-" and "HEAD". A name
    # starting with refs/ is a valid one, but it's far more likely a full ref name by mistake.
    return _follows_ref_format(name) and name != "HEAD" and not name.startswith(("-", "refs/"))


def _follows_ref_format(name: str) -> bool:
    """Return whether git check-ref-format takes ``name``, a ref name or the part of one below refs/heads/."""
    return not (
        not name
        or name == "@"
        or name.startswith("/")
        or name.endswith(("/", "."))
        or _NOT_IN_A_REF.search(name)
        or any(part.startswith(".") or part.endswith(".lock") for part in name.split("/"))
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
