from collections.abc import Callable
from typing import Literal, Protocol

import msgspec

from scm.secret import Secret
from scm.types import Referrer, Repository, RepositoryId

type GitOperation = Literal["fetch", "push"]
"""What a grant lets its holder do. ``scm.git.relay`` maps each to its git routes."""

GRANT_VERSION: Literal[1] = 1

GRANT_HEADER = "X-Scm-Git-Grant"
"""The request header that carries a signed grant to the git endpoint."""

GRANT_MAX_LENGTH = 4096
"""The longest signed grant, in characters. The grant travels in a request header, and sandbox-router's
``PUT /grant/{sandbox_id}`` refuses a longer one, so a change here must change the router too. A grant grows with
its repositories: a grant for one is under 400 characters, and the claims are base64-encoded, so each repository
named as ``(provider, path)`` adds about 4/3 of its path's length plus 20, e.g. 52 for a 25-character path. The
limit is a conservative pick rather than a measured ceiling: header limits further along the path are typically
around 8 KiB."""


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


type GetToken = Callable[[Repository, GitOperation], Secret | None]
"""Return a provider token that can run ``operation`` on ``repository``, or None if there is none.

Wrap the token in a ``Secret`` where you read it. GitServer and the relay hold it in their frames' local variables,
which an error report sends."""


class Readable(Protocol):
    """A request body the relay pulls from as it sends upstream."""

    def read(self, size: int = -1, /) -> bytes: ...
