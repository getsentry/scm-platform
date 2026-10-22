from collections.abc import Callable
from typing import Literal, Protocol

import msgspec

from scm.types import Referrer, Repository, RepositoryId

type GitOperation = Literal["fetch", "push"]
"""What a grant lets its holder do. ``scm.git.relay`` maps each to its git routes."""

GRANT_VERSION: Literal[1] = 1

GRANT_HEADER = "X-Scm-Git-Grant"
"""The request header that carries a signed grant to the git endpoint."""


class Grant(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Permission to run one git operation against a fixed set of an organization's repositories.

    Seer signs a grant per coding job and Sentry's git endpoint verifies it on every request. Each repository is
    a ``RepositoryId`` under the grant's organization, as in the RPC endpoint: Sentry's repository id or a
    ``(provider, external id)`` pair. A fetch grant holds one repository today. The list leaves room for
    submodules and git-based dependencies, but only from repositories Sentry has connected for the organization:
    naming any other repository, such as a public dependency, would need a new kind of entry and a new version.

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


type GetToken = Callable[[Repository, GitOperation], str | None]
"""Return a provider token that can run ``operation`` on ``repository``, or None if there is none."""


class Readable(Protocol):
    """A request body the relay pulls from as it sends upstream."""

    def read(self, size: int = -1, /) -> bytes: ...
