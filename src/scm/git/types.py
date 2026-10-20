import dataclasses
from collections.abc import Callable
from typing import Literal, Protocol

import msgspec

from scm.types import Repository, RepositoryId

type GitOperation = Literal["fetch", "push"]
"""What a grant lets its holder do. ``scm.git.operations`` maps each to its git routes."""

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
    operation: GitOperation
    repositories: tuple[RepositoryId, ...]
    expires_at: int
    """Unix time in seconds. Every grant expires."""
    version: Literal[1] = GRANT_VERSION
    """The format version. A verifier refuses a version it does not know."""


@dataclasses.dataclass(frozen=True)
class GitRemote:
    """Where a repository's git smart-HTTP endpoint lives and how a token authenticates against it."""

    url: str
    """The repository's clone URL, e.g. ``https://github.com/getsentry/sentry.git``."""
    username: str
    """The basic-auth username the provider pairs with a token, e.g. ``x-access-token`` on GitHub."""


type GetToken = Callable[[Repository, GitOperation], str | None]
"""Return a provider token that can run ``operation`` on ``repository``, or None if there is none.

The git module never builds credentials itself: Sentry's integration clients supply them today, and a central
token mint can later without a change here. The operation lets the supplier scope the token, e.g. read-only for a
fetch.
"""


class Readable(Protocol):
    """A request body the relay pulls from as it sends upstream.

    It must yield the body already decoded from its transfer encoding, ending where the body ends. A Django
    request under WSGI reads a chunked body as empty, and git sends a push over about 1 MiB chunked, so a view
    needs to pass a stream that has the whole body.
    """

    def read(self, size: int = -1, /) -> bytes: ...
