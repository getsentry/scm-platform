from collections.abc import Callable, Iterator, Mapping

from requests.structures import CaseInsensitiveDict

from scm.errors import GitProviderNotSupported, ProviderNotFound, ResourceBadGateway, SCMCodedError
from scm.git.grant import verify_grant
from scm.git.operations import check_request
from scm.git.relay import GitRelay
from scm.git.types import GRANT_HEADER, GetToken, Readable
from scm.helpers import initialize_provider
from scm.rpc.errors import SPECIAL_STATUS_MAP
from scm.rpc.types import StreamResponse
from scm.types import GetGitRemoteProtocol, Provider, Repository, RepositoryId


class GitServer:
    """Serves a git endpoint: checks each request against its grant, then relays it with a provider token.

    The order of the checks matters. The grant is verified, and the request's route and repository checked
    against it, before the repository and its provider are looked up or a token fetched, so a request the grant
    does not allow never reaches the database, the token supplier or the provider. Give it a relay whose adapter
    refuses internal addresses; see ``GitRelay``.

    The repository's provider says where its git endpoint is, through ``GetGitRemoteProtocol``. A repository whose
    provider has no git remote, such as Cursor Origin's, is refused.

    Refusals and provider failures come back as plain-text responses. Any other exception, such as an adapter
    refusing a host or the token supplier failing, reaches the caller unchanged, so the caller can answer it the
    way it answers the same error elsewhere.

    A relayed request is counted when its response stream ends, not when its headers arrive, so a clone that
    fails halfway is counted as failed: ``stream_interrupted`` when the provider's stream breaks and
    ``client_closed_request`` when the client goes away first. The bytes sent each way are counted then too.
    """

    def __init__(
        self,
        secrets: list[str],
        fetch_repository: Callable[[int, RepositoryId], Repository | None],
        fetch_provider: Callable[[int, Repository], Provider | None],
        get_token: GetToken,
        record_count: Callable[[str, int, dict[str, str]], None],
        relay: GitRelay,
    ) -> None:
        # Fail when the server is built rather than on every request, because no grant could ever verify.
        if not secrets or not all(secrets):
            raise ValueError("GitServer needs at least one secret, and none may be empty.")
        self.secrets = secrets
        self.fetch_repository = fetch_repository
        self.fetch_provider = fetch_provider
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
        counted_body = _CountingReader(body) if body is not None else None
        try:
            grant = verify_grant(self.secrets, CaseInsensitiveDict(headers).get(GRANT_HEADER, ""))
            operation = check_request(grant, repository_id, method, route, query)
            provider = initialize_provider(
                grant.organization_id,
                repository_id,
                fetch_repository=self.fetch_repository,
                fetch_provider=self.fetch_provider,
            )
            repository = provider.repository
            if not isinstance(provider, GetGitRemoteProtocol):
                raise GitProviderNotSupported(detail=f"The {repository['provider_name']} provider has no git remote.")
            remote = provider.get_git_remote()
            token = self.get_token(repository, operation)
            if token is None:
                raise ProviderNotFound(detail="No git credentials are available for this repository.")
            response = self.relay.forward(
                remote, token, method=method, route=route, query=query, headers=headers, body=counted_body
            )
            if response.status_code == 401:
                # The provider rejected the token Sentry supplied. Passed on, the 401 would make git ask the client
                # for credentials, which it must never have and which would not help.
                _close_stream(response.content)
                raise ResourceBadGateway(detail="The provider rejected Sentry's credentials for this repository.")
        except SCMCodedError as e:
            self.record_count("sentry.scm.git.request.failed", 1, {"code": e.code})
            return _error_response(e)
        except Exception:
            self.record_count("sentry.scm.git.request.failed", 1, {"code": "unhandled_exception"})
            raise

        tags: dict[str, str] = {"operation": operation, "provider": repository["provider_name"]}

        def finish(failure: str | None, response_bytes: int) -> None:
            if failure is None:
                self.record_count("sentry.scm.git.request.succeeded", 1, tags | {"status": str(response.status_code)})
            else:
                self.record_count("sentry.scm.git.request.failed", 1, {"code": failure})
            if counted_body is not None and counted_body.bytes_read:
                self.record_count("sentry.scm.git.request_bytes", counted_body.bytes_read, tags)
            if response_bytes:
                self.record_count("sentry.scm.git.response_bytes", response_bytes, tags)

        return StreamResponse(
            status_code=response.status_code,
            headers=response.headers,
            content=_MeteredStream(response.content, finish),
        )


class _CountingReader:
    """The client's request body, counting the bytes the relay reads from it."""

    def __init__(self, body: Readable) -> None:
        self.body = body
        self.bytes_read = 0

    def read(self, size: int = -1, /) -> bytes:
        data = self.body.read(size)
        self.bytes_read += len(data)
        return data


class _MeteredStream:
    """The relay's response stream, reporting once how it ended and how many bytes it passed on."""

    def __init__(self, content: Iterator[bytes], finish: Callable[[str | None, int], None]) -> None:
        self.content = content
        self.finish = finish
        self.bytes_sent = 0
        self.finished = False

    def __iter__(self) -> "_MeteredStream":
        return self

    def __next__(self) -> bytes:
        try:
            chunk = next(self.content)
        except StopIteration:
            self._end(None)
            raise
        except Exception:
            self._end("stream_interrupted")
            raise
        self.bytes_sent += len(chunk)
        return chunk

    def close(self) -> None:
        # The server calls this after the last chunk too, when the stream has already ended.
        self._end("client_closed_request")
        _close_stream(self.content)

    def _end(self, failure: str | None) -> None:
        if not self.finished:
            self.finished = True
            self.finish(failure, self.bytes_sent)


def _close_stream(content: Iterator[bytes]) -> None:
    # Closing the relay's stream before it is read to the end drops the upstream connection.
    close = getattr(content, "close", None)
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
