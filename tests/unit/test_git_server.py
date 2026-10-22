import io
import time
from collections.abc import Generator
from types import SimpleNamespace
from unittest.mock import MagicMock
from urllib.parse import urlsplit

import pytest

from scm.errors import ClientClosedRequest, RateLimitExceeded
from scm.git.grant import sign_grant
from scm.git.relay import GitRelay, RelayResponse, ResponseBody
from scm.git.server import _STATUS, GitServer
from scm.git.types import GRANT_HEADER, GitOperation, Readable
from scm.rpc.types import StreamResponse
from scm.types import GitRemote, Provider, Repository, RepositoryId
from tests.unit.git_builders import make_grant, make_provider, make_repository
from tests.unit.git_upstream import ClosingAdapter, basic_auth, running_upstream

SECRET = "test-secret-0000000000000000000000"
TOKEN = "ghs_secrettoken"
REFERRER = make_grant().referrer


def signed_grant(**overrides) -> str:
    return sign_grant(SECRET, make_grant(**overrides))


class Recorder:
    def __init__(self, repository: Repository | None = None, token: str | None = TOKEN) -> None:
        self.repository = make_repository() if repository is None else repository
        self.token = token
        self.repository_lookups: list[tuple[int, RepositoryId]] = []
        self.provider_lookups: list[tuple[int, Repository]] = []
        self.token_lookups: list[tuple[Repository, GitOperation]] = []
        self.counts: list[tuple[str, dict[str, str]]] = []
        self.errors: list[tuple[Exception, dict[str, str]]] = []
        self.bytes: dict[tuple[str, str], int] = {}

    def fetch_repository(self, organization_id: int, repository_id: RepositoryId) -> Repository | None:
        self.repository_lookups.append((organization_id, repository_id))
        return self.repository

    def fetch_provider(self, organization_id: int, repository: Repository) -> Provider | None:
        self.provider_lookups.append((organization_id, repository))
        return make_provider(organization_id, repository)

    def get_token(self, repository: Repository, operation: GitOperation) -> str | None:
        self.token_lookups.append((repository, operation))
        return self.token

    def record_count(self, name: str, value: int, tags: dict[str, str]) -> None:
        if name.endswith("_size"):
            key = (name, tags["operation"])
            self.bytes[key] = self.bytes.get(key, 0) + value
        else:
            self.counts.append((name, tags))

    def emit_error(self, error: Exception, context: dict[str, str]) -> None:
        self.errors.append((error, context))

    @property
    def error_codes(self) -> list[str]:
        return [getattr(error, "code", "") for error, _ in self.errors]


def chunks(*parts: bytes) -> Generator[bytes]:
    """A provider response body, which, like the relay's, can be closed."""
    yield from parts


def relay_returning(content: ResponseBody, status_code: int = 200) -> GitRelay:
    """A stand-in relay whose every forward returns ``content`` as the provider's response."""
    relay = MagicMock(spec=GitRelay)
    relay.forward.return_value = RelayResponse(status_code=status_code, headers={}, content=content)
    return relay


def make_server(recorder: Recorder, relay: GitRelay | None = None) -> GitServer:
    if relay is None:
        relay = relay_returning(chunks(b"refs"))
    return GitServer(
        secrets=[SECRET],
        fetch_repository=recorder.fetch_repository,
        fetch_provider=recorder.fetch_provider,
        get_token=recorder.get_token,
        record_count=recorder.record_count,
        emit_error=recorder.emit_error,
        relay=relay,
    )


def request(
    server: GitServer,
    grant: str | None,
    *,
    method: str = "GET",
    route: str = "info/refs",
    query: str = "service=git-upload-pack",
    repository_id: RepositoryId = 5,
    headers: dict[str, str] | None = None,
    body: Readable | None = None,
) -> StreamResponse:
    """Send a request to the server, an info/refs fetch unless told otherwise."""
    grant_headers = {GRANT_HEADER: grant} if grant is not None else {}
    return server.handle(
        method=method,
        repository_id=repository_id,
        route=route,
        query=query,
        headers=grant_headers | (headers or {}),
        body=body,
    )


def push(server: GitServer, grant: str) -> StreamResponse:
    return request(server, grant, method="POST", route="git-receive-pack", query="", body=io.BytesIO(b"0000"))


def assert_refused(response: StreamResponse, status: int, detail: str) -> None:
    assert response.status_code == status
    assert response.headers["Content-Type"] == "text/plain; charset=utf-8"
    body = b"".join(response.content).decode()
    assert detail in body
    assert int(response.headers["Content-Length"]) == len(body.encode())


def test_requires_a_relay():
    # A default relay would connect to internal addresses, so the caller has to choose its adapter.
    with pytest.raises(TypeError, match="relay"):
        GitServer(  # type: ignore[call-arg]
            secrets=[SECRET],
            fetch_repository=lambda organization_id, repository_id: None,
            fetch_provider=lambda organization_id, repository: None,
            get_token=lambda repository, operation: None,
            record_count=lambda name, value, tags: None,
            emit_error=lambda error, context: None,
        )


@pytest.mark.parametrize("secrets", [[], [SECRET, ""]])
def test_requires_non_empty_secrets(secrets):
    with pytest.raises(ValueError, match="secret"):
        GitServer(
            secrets=secrets,
            fetch_repository=lambda organization_id, repository_id: None,
            fetch_provider=lambda organization_id, repository: None,
            get_token=lambda repository, operation: None,
            record_count=lambda name, value, tags: None,
            emit_error=lambda error, context: None,
            relay=MagicMock(spec=GitRelay),
        )


class TestRefusedBeforeTheProvider:
    """Nothing the grant does not allow may look up a repository or a token."""

    @pytest.mark.parametrize(
        "grant,detail,code",
        [
            (None, "missing", "git_invalid_grant"),
            ("garbage", "malformed", "git_invalid_grant"),
            (
                sign_grant("other-secret-0000000000000000000000", make_grant()),
                "not signed with a known secret",
                "git_invalid_grant",
            ),
            (signed_grant(expires_at=int(time.time()) - 1), "expired", "git_grant_expired"),
        ],
    )
    def test_invalid_grant(self, grant, detail, code):
        recorder = Recorder()
        server = make_server(recorder)
        assert_refused(request(server, grant), 403, detail)
        assert recorder.repository_lookups == []
        assert recorder.provider_lookups == []
        assert recorder.token_lookups == []
        server.relay.forward.assert_not_called()  # type: ignore[attr-defined]
        assert recorder.counts == [("sentry.scm.git.request.failed", {"code": code})]
        assert recorder.error_codes == [code]
        # Only the requested repository is known before the grant is verified.
        assert recorder.errors[0][1] == {"repository_id": "5"}

    def test_route_that_is_not_git(self):
        recorder = Recorder()
        response = request(
            make_server(recorder), signed_grant(), method="POST", route="info/lfs/objects/batch", query=""
        )
        assert_refused(response, 404, "not a git fetch or push route")
        assert recorder.repository_lookups == []
        assert recorder.counts == [
            ("sentry.scm.git.request.failed", {"referrer": REFERRER, "code": "git_route_not_supported"})
        ]

    def test_operation_the_grant_does_not_allow(self):
        recorder = Recorder()
        assert_refused(push(make_server(recorder), signed_grant()), 403, "allows fetch, not push")
        assert recorder.repository_lookups == []
        assert recorder.token_lookups == []
        assert recorder.counts == [
            ("sentry.scm.git.request.failed", {"referrer": REFERRER, "code": "git_operation_not_granted"})
        ]
        assert recorder.errors[0][1] == {"repository_id": "5", "organization_id": "1", "referrer": REFERRER}

    def test_repository_the_grant_does_not_list(self):
        recorder = Recorder()
        assert_refused(request(make_server(recorder), signed_grant(), repository_id=6), 403, "does not cover")
        assert recorder.repository_lookups == []
        assert recorder.counts == [
            ("sentry.scm.git.request.failed", {"referrer": REFERRER, "code": "git_repository_not_granted"})
        ]

    def test_grant_header_name_is_case_insensitive(self):
        response = request(make_server(Recorder()), None, headers={GRANT_HEADER.lower(): signed_grant()})
        assert response.status_code == 200


class TestRepository:
    def test_looked_up_under_the_grants_organization(self):
        recorder = Recorder()
        request(make_server(recorder), signed_grant(organization_id=1))
        assert recorder.repository_lookups == [(1, 5)]
        assert recorder.provider_lookups == [(1, make_repository())]

    def test_provider_without_a_git_remote(self):
        recorder = Recorder()
        recorder.fetch_provider = lambda organization_id, repository: SimpleNamespace(  # type: ignore[method-assign]
            organization_id=organization_id, repository=repository
        )
        assert_refused(request(make_server(recorder), signed_grant()), 404, "github provider has no git remote")
        assert recorder.token_lookups == []

    def test_remote_comes_from_the_provider(self):
        repository = make_repository(provider_name="gitlab", external_id="gitlab.example.com:7", path="group/project")
        recorder = Recorder(repository=repository)
        server = make_server(recorder)
        request(server, signed_grant())
        remote = server.relay.forward.call_args.args[0]  # type: ignore[attr-defined]
        assert remote == GitRemote(url="https://gitlab.example.com/group/project.git", basic_auth_username="oauth2")

    @pytest.mark.parametrize(
        "repository,status,detail",
        [
            (None, 404, "could not be found"),
            (make_repository(organization_id=2), 404, "did not belong"),
            (make_repository(is_active=False), 404, "inactive"),
            (make_repository(provider_name="bitbucket"), 404, "unsupported integration provider"),
            (make_repository(provider_name="gitlab", external_id="gitlab.com:7"), 500, "has no path"),
        ],
    )
    def test_refused_before_the_token(self, repository, status, detail):
        recorder = Recorder()
        recorder.repository = repository
        assert_refused(request(make_server(recorder), signed_grant()), status, detail)
        assert recorder.token_lookups == []


class TestRelay:
    def test_no_token(self):
        recorder = Recorder(token=None)
        response = request(make_server(recorder), signed_grant())
        assert_refused(response, 502, "could not get a github token")
        assert recorder.counts == [
            ("sentry.scm.git.request.failed", {"referrer": REFERRER, "code": "git_token_unavailable"})
        ]

    def test_token_asked_for_the_grants_operation(self):
        recorder = Recorder()
        push(make_server(recorder), signed_grant(operation="push"))
        assert recorder.token_lookups == [(make_repository(), "push")]

    def test_forwards_with_remote_and_token(self):
        recorder = Recorder()
        server = make_server(recorder)
        grant = signed_grant()
        response = request(server, grant, headers={"Git-Protocol": "version=2"})
        assert b"".join(response.content) == b"refs"
        server.relay.forward.assert_called_once_with(  # type: ignore[attr-defined]
            GitRemote(url="https://github.com/getsentry/sentry.git", basic_auth_username="x-access-token"),
            TOKEN,
            method="GET",
            route="info/refs",
            query="service=git-upload-pack",
            headers={GRANT_HEADER: grant, "Git-Protocol": "version=2"},
            body=None,
        )
        assert recorder.counts == [
            (
                "sentry.scm.git.request.succeeded",
                {"referrer": REFERRER, "operation": "fetch", "provider": "github", "status": "200"},
            )
        ]

    def test_counted_when_the_response_ends_not_when_it_starts(self):
        recorder = Recorder()
        response = request(make_server(recorder), signed_grant())
        assert recorder.counts == []
        assert b"".join(response.content) == b"refs"
        response.content.close()  # type: ignore[attr-defined]
        assert recorder.counts == [
            (
                "sentry.scm.git.request.succeeded",
                {"referrer": REFERRER, "operation": "fetch", "provider": "github", "status": "200"},
            )
        ]
        assert recorder.bytes == {("sentry.scm.git.response_size", "fetch"): 4}

    def test_provider_stream_breaking_is_a_failure(self):
        def broken_stream():
            yield b"half a pack"
            raise ConnectionError("provider went away")

        recorder = Recorder()
        content = request(make_server(recorder, relay_returning(broken_stream())), signed_grant()).content
        assert next(content) == b"half a pack"
        with pytest.raises(ConnectionError):
            next(content)
        assert recorder.counts == [
            (
                "sentry.scm.git.request.failed",
                {
                    "referrer": REFERRER,
                    "operation": "fetch",
                    "provider": "github",
                    "status": "200",
                    "code": "stream_interrupted",
                },
            )
        ]
        assert recorder.bytes == {("sentry.scm.git.response_size", "fetch"): 11}

    def test_client_leaving_early_is_a_failure_and_closes_the_provider_stream(self):
        closed = []

        def provider_stream():
            try:
                yield b"first"
                yield b"second"
            finally:
                closed.append(True)

        recorder = Recorder()
        content = request(make_server(recorder, relay_returning(provider_stream())), signed_grant()).content
        assert next(content) == b"first"
        content.close()  # type: ignore[attr-defined]
        assert closed == [True]
        assert recorder.counts == [
            (
                "sentry.scm.git.request.failed",
                {
                    "referrer": REFERRER,
                    "operation": "fetch",
                    "provider": "github",
                    "status": "200",
                    "code": "client_closed_request",
                },
            )
        ]

    def test_upstream_unavailable(self):
        recorder = Recorder(
            repository=make_repository(provider_name="github_enterprise", web_base_url="https://127.0.0.1:1")
        )
        response = request(make_server(recorder, GitRelay(timeout=(1, 1))), signed_grant())
        assert_refused(response, 503, "temporarily unavailable")
        assert "127.0.0.1" not in b"".join(response.content).decode()
        assert TOKEN not in b"".join(response.content).decode()

    def test_reported_remote_host_leaves_out_credentials_in_the_url(self):
        recorder = Recorder(
            repository=make_repository(provider_name="github_enterprise", web_base_url="https://user:pw@127.0.0.1:1")
        )
        request(make_server(recorder, GitRelay(timeout=(1, 1))), signed_grant())
        assert recorder.error_codes == ["resource_service_unavailable"]
        assert recorder.errors[0][1]["remote_host"] == "127.0.0.1:1"

    def test_plain_http_remote(self):
        # A self-managed instance served over http; the token must not cross the network in the clear.
        recorder = Recorder(
            repository=make_repository(provider_name="github_enterprise", web_base_url="http://127.0.0.1:1")
        )
        response = request(make_server(recorder, GitRelay()), signed_grant())
        assert_refused(response, 502, "not an https URL")
        assert recorder.counts == [
            ("sentry.scm.git.request.failed", {"referrer": REFERRER, "code": "git_remote_not_https"})
        ]

    def test_provider_rejecting_the_token(self):
        # Passed on, a 401 would make git ask the sandboxed client for credentials.
        adapters: list[ClosingAdapter] = []

        def adapter() -> ClosingAdapter:
            adapters.append(ClosingAdapter())
            return adapters[-1]

        with running_upstream() as upstream:
            upstream.handler = lambda h: h.respond(401, b"Bad credentials")
            recorder = Recorder(
                repository=make_repository(provider_name="github_enterprise", web_base_url=upstream.url)
            )
            relay = GitRelay(adapter=adapter, ca_bundle=upstream.ca_bundle)
            response = request(make_server(recorder, relay), signed_grant())
        assert_refused(response, 502, "rejected Sentry's credentials")
        # The provider's unread response is closed with its connection.
        assert [a.closed for a in adapters] == [True]
        assert recorder.counts == [
            ("sentry.scm.git.request.failed", {"referrer": REFERRER, "code": "git_token_rejected"})
        ]
        assert recorder.error_codes == ["git_token_rejected"]
        assert recorder.errors[0][1] == {
            "repository_id": "5",
            "organization_id": "1",
            "referrer": REFERRER,
            "operation": "fetch",
            "provider": "github_enterprise",
            "remote_host": urlsplit(upstream.url).netloc,
        }

    @pytest.mark.parametrize("status", [301, 403, 404, 500])
    def test_provider_refusing_the_request_is_passed_on_and_counted_as_failed(self, status):
        # e.g. GitHub's 404 for a repository the installation cannot see; git shows the client its message.
        recorder = Recorder()
        response = request(
            make_server(recorder, relay_returning(chunks(b"Repository not found."), status)), signed_grant()
        )
        assert response.status_code == status
        assert b"".join(response.content) == b"Repository not found."
        response.content.close()  # type: ignore[attr-defined]
        assert recorder.counts == [
            (
                "sentry.scm.git.request.failed",
                {
                    "referrer": REFERRER,
                    "operation": "fetch",
                    "provider": "github",
                    "status": str(status),
                    "code": "provider_error",
                },
            )
        ]

    def test_client_closing_its_request_is_counted_but_not_reported(self):
        relay = MagicMock(spec=GitRelay)
        relay.forward.side_effect = ClientClosedRequest()
        recorder = Recorder()
        assert_refused(request(make_server(recorder, relay), signed_grant()), 499, "")
        assert recorder.counts == [
            ("sentry.scm.git.request.failed", {"referrer": REFERRER, "code": "client_closed_request"})
        ]
        assert recorder.errors == []

    def test_unexpected_error_code_is_a_server_error(self):
        relay = MagicMock(spec=GitRelay)
        relay.forward.side_effect = RateLimitExceeded()
        assert_refused(request(make_server(Recorder(), relay), signed_grant()), 500, "")

    def test_no_refusal_is_a_401(self):
        # On a 401 git asks the sandboxed client for credentials.
        assert 401 not in _STATUS.values()

    def test_other_errors_reach_the_caller(self):
        # Sentry's view answers these the way it answers them elsewhere, e.g. a refused host as ApiHostError.
        class HostRefused(Exception):
            pass

        relay = MagicMock(spec=GitRelay)
        relay.forward.side_effect = HostRefused()
        recorder = Recorder()
        with pytest.raises(HostRefused):
            request(make_server(recorder, relay), signed_grant())
        assert recorder.counts == [
            ("sentry.scm.git.request.failed", {"referrer": REFERRER, "code": "unhandled_exception"})
        ]
        # The caller reports it.
        assert recorder.errors == []

    def test_counts_bytes_each_way_end_to_end(self):
        with running_upstream() as upstream:
            recorder = Recorder(
                repository=make_repository(provider_name="github_enterprise", web_base_url=upstream.url)
            )
            relay = GitRelay(ca_bundle=upstream.ca_bundle)
            response = request(
                make_server(recorder, relay),
                signed_grant(operation="push"),
                method="POST",
                route="git-receive-pack",
                query="",
                headers={"Content-Length": "8"},
                body=io.BytesIO(b"00000000"),
            )
            assert b"".join(response.content) == b"00000000"  # The fake upstream echoes the body.
        assert recorder.bytes == {
            ("sentry.scm.git.request_size", "push"): 8,
            ("sentry.scm.git.response_size", "push"): 8,
        }

    def test_relays_end_to_end(self):
        with running_upstream() as upstream:
            recorder = Recorder(
                repository=make_repository(provider_name="github_enterprise", web_base_url=upstream.url)
            )
            relay = GitRelay(ca_bundle=upstream.ca_bundle)
            response = request(make_server(recorder, relay), signed_grant())
            assert response.status_code == 200
            b"".join(response.content)
        [received] = upstream.requests
        assert received.path == "/getsentry/sentry.git/info/refs?service=git-upload-pack"
        assert received.headers["Authorization"] == basic_auth("x-access-token", TOKEN)
        assert GRANT_HEADER not in received.headers
