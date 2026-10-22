from collections.abc import Callable, Mapping, Sequence
from urllib.parse import urlsplit

from scm.errors import ErrorCode, GitProviderNotSupported, GitTokenRejected, GitTokenUnavailable, SCMCodedError
from scm.git.grant import verify_grant
from scm.git.relay import GitRelay, ResponseBody, authorize_request
from scm.git.types import GRANT_HEADER, GetToken, Readable
from scm.helpers import initialize_provider
from scm.rpc.types import StreamResponse
from scm.secret import Secret, require_secrets
from scm.types import GetGitRemoteProtocol, Provider, Repository, RepositoryId

# The status of each refusal GitServer answers itself. None is a 401: on a 401 git asks the user for credentials,
# which a sandboxed client must never have and which would not help.
_STATUS: dict[ErrorCode, int] = {
    # The client's own request is malformed, or is not a git request this endpoint serves.
    "rpc_malformed_request_headers": 400,
    "git_route_not_supported": 404,
    # The grant does not allow the request.
    "git_invalid_grant": 403,
    "git_grant_expired": 403,
    "git_operation_not_granted": 403,
    "git_repository_not_granted": 403,
    # The grant allows it, but there is no repository to serve.
    "repository_not_found": 404,
    "repository_inactive": 404,
    "repository_organization_mismatch": 404,
    "provider_not_found": 404,
    "git_provider_not_supported": 404,
    "client_closed_request": 499,
    # Sentry's record of the repository is incomplete.
    "malformed_external_id": 500,
    "missing_repository_path": 500,
    # The provider cannot be reached safely, or failed.
    "git_remote_not_https": 502,
    "git_token_rejected": 502,
    "git_token_unavailable": 502,
    "resource_bad_gateway": 502,
    "resource_certificate_invalid": 502,
    "resource_service_unavailable": 503,
    "resource_gateway_timeout": 504,
}


class GitServer:
    """Serves a git endpoint: checks each request against its grant, then relays it with a provider token.

    The order of the checks matters. The grant is verified, and the request's route and repository checked
    against it, before the repository and its provider are looked up or a token fetched, so a request the grant
    does not allow never reaches the database, the token supplier or the provider. Give it a relay whose adapter
    refuses internal addresses; see ``GitRelay``.

    The repository's provider says where its git endpoint is, through ``GetGitRemoteProtocol``. A repository whose
    provider has no git remote is refused.

    Refusals and provider failures come back as plain-text responses, and each is passed to ``emit_error`` as
    well: the client only shows the message, so the server reports it rather than count on every client to. The
    message leaves out where the request was going, so ``emit_error`` also gets what the request had resolved
    when it failed, as tags: ``repository_id`` as requested, then ``organization_id`` and ``referrer`` once the
    grant is verified, ``operation``, ``provider`` and ``remote_host``. A tag is missing when the request failed
    before it was known. A client closing its request is not reported, since there is nothing to fix. Any other
    exception, such as an adapter refusing a host or the token supplier failing, reaches the caller unchanged, so
    the caller can answer it the way it answers the same error elsewhere.

    Every count is tagged with the grant's ``referrer`` once the grant is verified, so Sentry can tell a coding
    job's clone from Seer's own fetch or push.

    A relayed request is counted when its response stream ends, not when its headers arrive, so a clone that
    fails halfway is counted as failed: ``stream_interrupted`` when the provider's stream breaks and
    ``client_closed_request`` when the client goes away first. A response the provider sends with a status other
    than 2xx is passed on to the client but counted as failed too, as ``provider_error``. The bytes sent each way
    are counted then too, as ``request_size`` and ``response_size``.
    """

    def __init__(
        self,
        secrets: Sequence[Secret],
        fetch_repository: Callable[[int, RepositoryId], Repository | None],
        fetch_provider: Callable[[int, Repository], Provider | None],
        get_token: GetToken,
        record_count: Callable[[str, int, dict[str, str]], None],
        emit_error: Callable[[Exception, dict[str, str]], None],
        relay: GitRelay,
    ) -> None:
        # Fail when the server is built rather than on every request, because no grant could ever verify.
        if not secrets or not all(secrets):
            raise ValueError("GitServer needs at least one secret, and none may be empty.")
        self.secrets = require_secrets(secrets, parameter="secrets")
        self.fetch_repository = fetch_repository
        self.fetch_provider = fetch_provider
        self.get_token = get_token
        self.record_count = record_count
        self.emit_error = emit_error
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
        while the request is sent upstream, so pass the unread request stream. It must yield the body already
        decoded from its transfer encoding, ending where the body ends. A Django request under WSGI reads a chunked
        body as empty, and git sends a push over about 1 MiB chunked, so a Django view needs to pass a stream that
        has the whole body.
        """
        # Rebound without the grant, so it shows as [Filtered] in the frames of a reported error rather than in
        # ``headers``, here and in the relay, which never sends it upstream anyway.
        grant_token, headers = _take_grant(headers)
        counted_body = _CountingReader(body) if body is not None else None
        context = {"repository_id": str(repository_id)}
        referrer_tag: dict[str, str] = {}
        try:
            grant = verify_grant(self.secrets, grant_token.reveal())
            context["organization_id"] = str(grant.organization_id)
            referrer_tag["referrer"] = context["referrer"] = grant.referrer
            operation = authorize_request(grant, repository_id, method, route, query)
            context["operation"] = operation
            provider = initialize_provider(
                grant.organization_id,
                repository_id,
                fetch_repository=self.fetch_repository,
                fetch_provider=self.fetch_provider,
            )
            repository = provider.repository
            context["provider"] = repository["provider_name"]
            if not isinstance(provider, GetGitRemoteProtocol):
                raise GitProviderNotSupported(detail=f"The {repository['provider_name']} provider has no git remote.")
            remote = provider.get_git_remote()
            # The host and port only: a URL can carry credentials before the host.
            context["remote_host"] = urlsplit(remote["url"]).netloc.rpartition("@")[2]
            token = self.get_token(repository, operation)
            if token is None:
                raise GitTokenUnavailable(
                    detail=f"Sentry could not get a {repository['provider_name']} token for this repository."
                )
            response = self.relay.forward(
                remote, token, method=method, route=route, query=query, headers=headers, body=counted_body
            )
            if response.status_code == 401:
                # The provider rejected the token Sentry supplied. Passed on, the 401 would make git ask the client
                # for credentials, which it must never have and which would not help.
                response.content.close()
                raise GitTokenRejected(detail="The provider rejected Sentry's credentials for this repository.")
        except SCMCodedError as e:
            self.record_count("sentry.scm.git.request.failed", 1, {"code": e.code} | referrer_tag)
            if e.code != "client_closed_request":
                self.emit_error(e, context)
            return _error_response(e)
        except Exception:
            self.record_count("sentry.scm.git.request.failed", 1, {"code": "unhandled_exception"} | referrer_tag)
            raise

        tags: dict[str, str] = {"operation": operation, "provider": repository["provider_name"]} | referrer_tag

        def finish(failure: str | None, bytes_sent: int) -> None:
            if failure is None and not 200 <= response.status_code < 300:
                # The provider refused the request itself, e.g. a 404 for a repository the token cannot see. git
                # shows the client the provider's message, but did not get what it asked for.
                failure = "provider_error"
            if failure is None:
                self.record_count("sentry.scm.git.request.succeeded", 1, tags | {"status": str(response.status_code)})
            else:
                self.record_count(
                    "sentry.scm.git.request.failed", 1, tags | {"status": str(response.status_code), "code": failure}
                )
            if counted_body is not None and counted_body.bytes_read:
                self.record_count("sentry.scm.git.request_size", counted_body.bytes_read, tags)
            if bytes_sent:
                self.record_count("sentry.scm.git.response_size", bytes_sent, tags)

        return StreamResponse(
            status_code=response.status_code,
            headers=response.headers,
            content=_MeteredStream(response.content, finish),
        )


def _take_grant(headers: Mapping[str, str]) -> tuple[Secret, dict[str, str]]:
    """Split the grant, empty if there is none, from the other headers. Header names are case-insensitive."""
    grant = Secret("")
    rest: dict[str, str] = {}
    for name, value in headers.items():
        if name.lower() == GRANT_HEADER.lower():
            grant = Secret(value)
        else:
            rest[name] = value
    return grant, rest


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

    def __init__(self, content: ResponseBody, finish: Callable[[str | None, int], None]) -> None:
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
        # Closing the relay's stream before it is read to the end drops the upstream connection.
        self.content.close()

    def _end(self, failure: str | None) -> None:
        if not self.finished:
            self.finished = True
            self.finish(failure, self.bytes_sent)


def _error_response(error: SCMCodedError) -> StreamResponse:
    # A code missing from the map comes from a failure nobody planned for, which is the server's fault.
    status = _STATUS.get(error.code, 500)
    # When its first request fails, git shows a text/plain body to the user as "remote: <line>".
    content = f"{error.detail}\n".encode()
    return StreamResponse(
        status_code=status,
        headers={"Content-Type": "text/plain; charset=utf-8", "Content-Length": str(len(content))},
        content=iter([content]),
    )
