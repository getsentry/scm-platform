import io
import time
from collections.abc import Iterator
from contextlib import closing
from unittest.mock import MagicMock

import pytest

from scm.git.grant import sign_grant
from scm.git.relay import GitRelay
from scm.git.server import GitServer
from scm.git.types import GRANT_HEADER, GitOperation, GitRemote, Readable
from scm.rpc.types import StreamResponse
from scm.types import Repository, RepositoryId
from tests.unit.git_builders import make_grant, make_repository
from tests.unit.git_upstream import basic_auth, running_upstream

SECRET = "test-secret-0000000000000000000000"
TOKEN = "ghs_secrettoken"


def signed_grant(**overrides) -> str:
    return sign_grant(SECRET, make_grant(**overrides))


class Recorder:
    def __init__(self, repository: Repository | None = None, token: str | None = TOKEN) -> None:
        self.repository = make_repository() if repository is None else repository
        self.token = token
        self.repository_lookups: list[tuple[int, RepositoryId]] = []
        self.token_lookups: list[tuple[Repository, GitOperation]] = []
        self.counts: list[tuple[str, dict[str, str]]] = []
        self.bytes: dict[tuple[str, str], int] = {}

    def fetch_repository(self, organization_id: int, repository_id: RepositoryId) -> Repository | None:
        self.repository_lookups.append((organization_id, repository_id))
        return self.repository

    def get_token(self, repository: Repository, operation: GitOperation) -> str | None:
        self.token_lookups.append((repository, operation))
        return self.token

    def record_count(self, name: str, value: int, tags: dict[str, str]) -> None:
        if name.endswith("_bytes"):
            key = (name, tags["operation"])
            self.bytes[key] = self.bytes.get(key, 0) + value
        else:
            self.counts.append((name, tags))


def relay_returning(content: Iterator[bytes], status_code: int = 200) -> GitRelay:
    """A stand-in relay whose every forward returns ``content`` as the provider's response."""
    relay = MagicMock(spec=GitRelay)
    relay.forward.return_value = StreamResponse(status_code=status_code, headers={}, content=content)
    return relay


def make_server(recorder: Recorder, relay: GitRelay | None = None) -> GitServer:
    if relay is None:
        relay = relay_returning(iter([b"refs"]))
    return GitServer(
        secrets=[SECRET],
        fetch_repository=recorder.fetch_repository,
        get_token=recorder.get_token,
        record_count=recorder.record_count,
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
            get_token=lambda repository, operation: None,
            record_count=lambda name, value, tags: None,
        )


@pytest.mark.parametrize("secrets", [[], [SECRET, ""]])
def test_requires_non_empty_secrets(secrets):
    with pytest.raises(ValueError, match="secret"):
        GitServer(
            secrets=secrets,
            fetch_repository=lambda organization_id, repository_id: None,
            get_token=lambda repository, operation: None,
            record_count=lambda name, value, tags: None,
            relay=MagicMock(spec=GitRelay),
        )


class TestRefusedBeforeTheProvider:
    """Nothing the grant does not allow may look up a repository or a token."""

    @pytest.mark.parametrize(
        "grant,detail",
        [
            (None, "missing"),
            ("garbage", "malformed"),
            (sign_grant("other-secret-0000000000000000000000", make_grant()), "not signed with a known secret"),
            (signed_grant(expires_at=int(time.time()) - 1), "expired"),
        ],
    )
    def test_invalid_grant(self, grant, detail):
        recorder = Recorder()
        server = make_server(recorder)
        assert_refused(request(server, grant), 403, detail)
        assert recorder.repository_lookups == []
        assert recorder.token_lookups == []
        server.relay.forward.assert_not_called()  # type: ignore[attr-defined]
        assert recorder.counts == [("sentry.scm.git.request.failed", {"code": "git_invalid_grant"})]

    def test_operation_the_grant_does_not_allow(self):
        recorder = Recorder()
        assert_refused(push(make_server(recorder), signed_grant()), 403, "allows fetch, not push")
        assert recorder.repository_lookups == []
        assert recorder.token_lookups == []

    def test_repository_the_grant_does_not_list(self):
        recorder = Recorder()
        assert_refused(request(make_server(recorder), signed_grant(), repository_id=6), 403, "does not cover")
        assert recorder.repository_lookups == []

    def test_grant_header_name_is_case_insensitive(self):
        response = request(make_server(Recorder()), None, headers={GRANT_HEADER.lower(): signed_grant()})
        assert response.status_code == 200


class TestRepository:
    def test_looked_up_under_the_grants_organization(self):
        recorder = Recorder()
        request(make_server(recorder), signed_grant(organization_id=1))
        assert recorder.repository_lookups == [(1, 5)]

    @pytest.mark.parametrize(
        "repository,status,detail",
        [
            (None, 404, "could not be found"),
            (make_repository(organization_id=2), 404, "did not belong"),
            (make_repository(is_active=False), 404, "inactive"),
            (make_repository(provider_name="bitbucket"), 400, "no git remote"),
        ],
    )
    def test_refused_before_the_token(self, repository, status, detail):
        recorder = Recorder()
        recorder.repository = repository
        assert_refused(request(make_server(recorder), signed_grant()), status, detail)
        assert recorder.token_lookups == []


class TestRelay:
    def test_no_token(self):
        assert_refused(request(make_server(Recorder(token=None)), signed_grant()), 404, "No git credentials")

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
            GitRemote(url="https://github.com/getsentry/sentry.git", username="x-access-token"),
            TOKEN,
            method="GET",
            route="info/refs",
            query="service=git-upload-pack",
            headers={GRANT_HEADER: grant, "Git-Protocol": "version=2"},
            body=None,
        )
        assert recorder.counts == [
            ("sentry.scm.git.request.succeeded", {"operation": "fetch", "provider": "github", "status": "200"})
        ]

    def test_counted_when_the_response_ends_not_when_it_starts(self):
        recorder = Recorder()
        response = request(make_server(recorder), signed_grant())
        assert recorder.counts == []
        assert b"".join(response.content) == b"refs"
        response.content.close()  # type: ignore[attr-defined]
        assert recorder.counts == [
            ("sentry.scm.git.request.succeeded", {"operation": "fetch", "provider": "github", "status": "200"})
        ]
        assert recorder.bytes == {("sentry.scm.git.response_bytes", "fetch"): 4}

    def test_provider_stream_breaking_is_a_failure(self):
        def broken_stream():
            yield b"half a pack"
            raise ConnectionError("provider went away")

        recorder = Recorder()
        content = request(make_server(recorder, relay_returning(broken_stream())), signed_grant()).content
        assert next(content) == b"half a pack"
        with pytest.raises(ConnectionError):
            next(content)
        assert recorder.counts == [("sentry.scm.git.request.failed", {"code": "stream_interrupted"})]
        assert recorder.bytes == {("sentry.scm.git.response_bytes", "fetch"): 11}

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
        assert recorder.counts == [("sentry.scm.git.request.failed", {"code": "client_closed_request"})]

    def test_upstream_unavailable(self):
        recorder = Recorder(
            repository=make_repository(provider_name="github_enterprise", web_base_url="https://127.0.0.1:1")
        )
        with closing(GitRelay(timeout=(1, 1))) as relay:
            response = request(make_server(recorder, relay), signed_grant())
        assert_refused(response, 503, "Could not connect")
        assert TOKEN not in b"".join(response.content).decode()

    def test_provider_rejecting_the_token_is_a_gateway_error(self):
        # Passed on, a 401 would make git ask the sandboxed client for credentials.
        closed = []

        def provider_body():
            try:
                yield b"Bad credentials"
            finally:
                closed.append(True)

        content = provider_body()
        next(content)  # Started, as a relay's stream is once its first chunk is read.
        recorder = Recorder()
        response = request(make_server(recorder, relay_returning(content, status_code=401)), signed_grant())
        assert_refused(response, 502, "rejected Sentry's credentials")
        assert closed == [True]
        assert recorder.counts == [("sentry.scm.git.request.failed", {"code": "resource_bad_gateway"})]

    def test_other_errors_reach_the_caller(self):
        # Sentry's view answers these the way it answers them elsewhere, e.g. a refused host as ApiHostError.
        class HostRefused(Exception):
            pass

        relay = MagicMock(spec=GitRelay)
        relay.forward.side_effect = HostRefused()
        recorder = Recorder()
        with pytest.raises(HostRefused):
            request(make_server(recorder, relay), signed_grant())
        assert recorder.counts == [("sentry.scm.git.request.failed", {"code": "unhandled_exception"})]

    def test_counts_bytes_each_way_end_to_end(self):
        with running_upstream() as upstream:
            recorder = Recorder(
                repository=make_repository(provider_name="github_enterprise", web_base_url=upstream.url)
            )
            with closing(GitRelay(ca_bundle=upstream.ca_bundle)) as relay:
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
            ("sentry.scm.git.request_bytes", "push"): 8,
            ("sentry.scm.git.response_bytes", "push"): 8,
        }

    def test_relays_end_to_end(self):
        with running_upstream() as upstream:
            recorder = Recorder(
                repository=make_repository(provider_name="github_enterprise", web_base_url=upstream.url)
            )
            with closing(GitRelay(ca_bundle=upstream.ca_bundle)) as relay:
                response = request(make_server(recorder, relay), signed_grant())
                assert response.status_code == 200
                b"".join(response.content)
        [received] = upstream.requests
        assert received.path == "/getsentry/sentry.git/info/refs?service=git-upload-pack"
        assert received.headers["Authorization"] == basic_auth("x-access-token", TOKEN)
        assert GRANT_HEADER not in received.headers
