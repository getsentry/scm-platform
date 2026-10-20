from collections.abc import Callable, Mapping

from requests.structures import CaseInsensitiveDict

from scm.errors import ProviderNotFound, ResourceBadGateway, SCMCodedError
from scm.git.grant import verify_grant
from scm.git.operations import check_request
from scm.git.relay import GitRelay
from scm.git.remote import git_remote
from scm.git.types import GRANT_HEADER, GetToken, Readable
from scm.helpers import resolve_repository
from scm.rpc.errors import SPECIAL_STATUS_MAP
from scm.rpc.types import StreamResponse
from scm.types import Repository, RepositoryId


class GitServer:
    """Serves a git endpoint: checks each request against its grant, then relays it with a provider token.

    The order of the checks matters. The grant is verified, and the request's route and repository checked
    against it, before the repository is looked up or a token fetched, so a request the grant does not allow
    never reaches the database, the token supplier or the provider. Build one per process: its relay pools
    upstream connections. Give it a relay whose adapter refuses internal addresses; see ``GitRelay``.

    Refusals and provider failures come back as plain-text responses. Any other exception, such as an adapter
    refusing a host or the token supplier failing, reaches the caller unchanged, so the caller can answer it the
    way it answers the same error elsewhere.
    """

    def __init__(
        self,
        secrets: list[str],
        fetch_repository: Callable[[int, RepositoryId], Repository | None],
        get_token: GetToken,
        record_count: Callable[[str, int, dict[str, str]], None],
        relay: GitRelay,
    ) -> None:
        self.secrets = secrets
        self.fetch_repository = fetch_repository
        self.get_token = get_token
        self.record_count = record_count
        self.relay = relay

    def handle(
        self,
        *,
        method: str,
        repository_id: RepositoryId,
        route: str,
        query: str,
        headers: Mapping[str, str],
        body: Readable | None,
    ) -> StreamResponse:
        """Answer one git request.

        ``route`` is the path below ``<repo>.git/`` and ``query`` the raw query string. ``body`` is read only
        while the request is sent upstream, so pass the unread request stream.
        """
        try:
            grant = verify_grant(self.secrets, CaseInsensitiveDict(headers).get(GRANT_HEADER, ""))
            operation = check_request(grant, repository_id, method, route, query)
            repository = resolve_repository(
                grant.organization_id, repository_id, fetch_repository=self.fetch_repository
            )
            remote = git_remote(repository)
            token = self.get_token(repository, operation)
            if token is None:
                raise ProviderNotFound(detail="No git credentials are available for this repository.")
            response = self.relay.forward(
                remote, token, method=method, route=route, query=query, headers=headers, body=body
            )
            if response.status_code == 401:
                # The provider rejected the token Sentry supplied. Passed on, the 401 would make git ask the client
                # for credentials, which it must never have and which would not help.
                _close(response)
                raise ResourceBadGateway(detail="The provider rejected Sentry's credentials for this repository.")
        except SCMCodedError as e:
            self.record_count("sentry.scm.git.request.failed", 1, {"code": e.code})
            return _error_response(e)

        self.record_count(
            "sentry.scm.git.request.succeeded",
            1,
            {
                "operation": operation,
                "provider": repository["provider_name"],
                "status": str(response.status_code),
            },
        )
        return response


def _close(response: StreamResponse) -> None:
    # Closing the relay's stream before it is read to the end drops the upstream connection.
    close = getattr(response.content, "close", None)
    if close is not None:
        close()


def _error_response(error: SCMCodedError) -> StreamResponse:
    # On a 401 git asks the user for credentials, which a sandboxed client must never have, so refuse with a 403.
    status = SPECIAL_STATUS_MAP.get(error.code, 400)
    if status == 401:
        status = 403
    # When its first request fails, git shows a text/plain body to the user as "remote: <line>".
    content = f"{error.detail}\n".encode()
    return StreamResponse(
        status_code=status,
        headers={"Content-Type": "text/plain; charset=utf-8", "Content-Length": str(len(content))},
        content=iter([content]),
    )
