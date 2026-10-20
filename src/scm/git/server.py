from collections.abc import Callable, Mapping

from requests.structures import CaseInsensitiveDict

from scm.errors import ProviderNotFound, SCMCodedError
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
    upstream connections. Pass a relay whose adapter refuses internal addresses; see ``GitRelay``.
    """

    def __init__(
        self,
        secrets: list[str],
        fetch_repository: Callable[[int, RepositoryId], Repository | None],
        get_token: GetToken,
        record_count: Callable[[str, int, dict[str, str]], None],
        emit_error: Callable[[Exception], None],
        relay: GitRelay | None = None,
    ) -> None:
        self.secrets = secrets
        self.fetch_repository = fetch_repository
        self.get_token = get_token
        self.record_count = record_count
        self.emit_error = emit_error
        self.relay = relay or GitRelay()

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
        except SCMCodedError as e:
            self.record_count("sentry.scm.git.request.failed", 1, {"code": e.code})
            if e.code == "unhandled_exception":
                self.emit_error(e)
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
