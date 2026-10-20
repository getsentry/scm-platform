import dataclasses
from collections.abc import Callable
from typing import Literal, Protocol

import msgspec

from scm.types import Repository, RepositoryId

type GitOperation = Literal["fetch", "push"]
"""What a grant lets its holder do: fetch reaches git-upload-pack, push reaches git-receive-pack."""

GRANT_VERSION: Literal[1] = 1

GRANT_HEADER = "X-Scm-Git-Grant"
"""The request header that carries a signed grant to the git endpoint."""


class Grant(msgspec.Struct, frozen=True, kw_only=True, forbid_unknown_fields=True):
    """Permission to run one git operation against a fixed set of an organization's repositories.

    Seer signs a grant per coding job and Sentry's git endpoint verifies it on every request. Each repository
    is named the way the RPC endpoint names one, as a ``RepositoryId`` under the grant's organization. A fetch
    grant holds one repository today; the list leaves room for submodules and git-based dependencies without a
    new format.

    Unknown fields are refused rather than ignored, so a field a newer signer adds to narrow a grant can never
    be dropped by an older verifier that would then allow more than the signer meant.
    """

    organization_id: int
    operation: GitOperation
    repositories: tuple[RepositoryId, ...]
    expires_at: int
    """Unix time in seconds. Every grant expires."""
    version: Literal[1] = GRANT_VERSION


@dataclasses.dataclass(frozen=True)
class GitRemote:
    """Where a repository's git smart-HTTP endpoint lives and how a token authenticates against it."""

    url: str
    """The repository's clone URL, e.g. ``https://github.com/getsentry/sentry.git``."""
    username: str
    """The basic-auth username the provider pairs with a token, e.g. ``x-access-token`` on GitHub."""


type GetToken = Callable[[Repository, GitOperation], str | None]
"""Return a provider token that can run ``operation`` on ``repository``, or None if there is none.

The relay never builds credentials itself, so Sentry's integration token getters today and a central token
mint later plug in without changing it. The operation lets a mint scope the token, e.g. read-only for a fetch.
"""


class Readable(Protocol):
    """A request body the relay pulls from as it sends upstream, e.g. a WSGI input or a Django request."""

    def read(self, size: int = -1, /) -> bytes: ...
