import io
import time
from unittest.mock import MagicMock

import pytest

from scm.git.grant import sign_grant
from scm.git.relay import GitRelay
from scm.git.server import GitServer
from scm.git.types import GRANT_HEADER, GitOperation, GitRemote, Grant
from scm.rpc.types import StreamResponse
from scm.types import Repository, RepositoryId
from tests.unit.git_upstream import basic_auth, running_upstream

SECRET = "test-secret-0000000000000000000000"
TOKEN = "ghs_secrettoken"


def make_repository(**overrides) -> Repository:
    defaults: Repository = {
        "id": 5,
        "external_id": "123",
        "integration_id": 1,
        "is_active": True,
        "name": "getsentry/sentry",
        "organization_id": 1,
        "provider_name": "github",
        "web_base_url": None,
        "installation_id": None,
    }
    return {**defaults, **overrides}  # type: ignore[typeddict-item]


def make_grant(**overrides) -> str:
    fields = {
        "organization_id": 1,
        "operation": "fetch",
        "repositories": (5,),
        "expires_at": int(time.time()) + 600,
    }
    fields.update(overrides)
    return sign_grant(SECRET, Grant(**fields))  # type: ignore[arg-type]


class Recorder:
    def __init__(self, repository: Repository | None = None, token: str | None = TOKEN) -> None:
        self.repository = make_repository() if repository is None else repository
        self.token = token
        self.repository_lookups: list[tuple[int, RepositoryId]] = []
        self.token_lookups: list[tuple[Repository, GitOperation]] = []
        self.counts: list[tuple[str, dict[str, str]]] = []
        self.errors: list[Exception] = []

    def fetch_repository(self, organization_id: int, repository_id: RepositoryId) -> Repository | None:
        self.repository_lookups.append((organization_id, repository_id))
        return self.repository

    def get_token(self, repository: Repository, operation: GitOperation) -> str | None:
        self.token_lookups.append((repository, operation))
        return self.token

    def record_count(self, name: str, value: int, tags: dict[str, str]) -> None:
        self.counts.append((name, tags))


def make_server(recorder: Recorder, relay: GitRelay | None = None) -> GitServer:
    if relay is None:
        relay = MagicMock(spec=GitRelay)
        relay.forward.return_value = StreamResponse(status_code=200, headers={}, content=iter([b"refs"]))
    return GitServer(
        secrets=[SECRET],
        fetch_repository=recorder.fetch_repository,
        get_token=recorder.get_token,
        record_count=recorder.record_count,
        emit_error=recorder.errors.append,
        relay=relay,
    )


def fetch_refs(server: GitServer, grant: str | None, repository_id: RepositoryId = 5) -> StreamResponse:
    return server.handle(
        method="GET",
        repository_id=repository_id,
        route="info/refs",
        query="service=git-upload-pack",
        headers={GRANT_HEADER: grant} if grant is not None else {},
        body=None,
    )


def assert_refused(response: StreamResponse, status: int, detail: str) -> None:
    assert response.status_code == status
    assert response.headers["Content-Type"] == "text/plain; charset=utf-8"
    body = b"".join(response.content).decode()
    assert detail in body
    assert int(response.headers["Content-Length"]) == len(body.encode())


class TestRefusedBeforeTheProvider:
    """Nothing the grant does not allow may look up a repository or a token."""

    @pytest.mark.parametrize(
        "grant,detail",
        [
            (None, "missing"),
            ("garbage", "malformed"),
            (
                sign_grant(
                    "other-secret-0000000000000000000000",
                    Grant(organization_id=1, operation="fetch", repositories=(5,), expires_at=2**40),
                ),
                "not signed with a known secret",
            ),
            (make_grant(expires_at=int(time.time()) - 1), "expired"),
        ],
    )
    def test_invalid_grant(self, grant, detail):
        recorder = Recorder()
        server = make_server(recorder)
        assert_refused(fetch_refs(server, grant), 403, detail)
        assert recorder.repository_lookups == []
        assert recorder.token_lookups == []
        server.relay.forward.assert_not_called()  # type: ignore[attr-defined]
        assert recorder.counts == [("sentry.scm.git.request.failed", {"code": "git_invalid_grant"})]

    def test_operation_the_grant_does_not_allow(self):
        recorder = Recorder()
        server = make_server(recorder)
        response = server.handle(
            method="POST",
            repository_id=5,
            route="git-receive-pack",
            query="",
            headers={GRANT_HEADER: make_grant()},
            body=io.BytesIO(b"0000"),
        )
        assert_refused(response, 403, "allows fetch, not push")
        assert recorder.repository_lookups == []
        assert recorder.token_lookups == []

    def test_repository_the_grant_does_not_list(self):
        recorder = Recorder()
        assert_refused(fetch_refs(make_server(recorder), make_grant(), repository_id=6), 403, "does not cover")
        assert recorder.repository_lookups == []

    def test_grant_header_name_is_case_insensitive(self):
        recorder = Recorder()
        server = make_server(recorder)
        response = server.handle(
            method="GET",
            repository_id=5,
            route="info/refs",
            query="service=git-upload-pack",
            headers={GRANT_HEADER.lower(): make_grant()},
            body=None,
        )
        assert response.status_code == 200


class TestRepository:
    def test_looked_up_under_the_grants_organization(self):
        recorder = Recorder()
        fetch_refs(make_server(recorder), make_grant(organization_id=1))
        assert recorder.repository_lookups == [(1, 5)]

    def test_not_found(self):
        recorder = Recorder()
        recorder.repository = None  # type: ignore[assignment]
        assert_refused(fetch_refs(make_server(recorder), make_grant()), 404, "could not be found")
        assert recorder.token_lookups == []

    def test_other_organization(self):
        recorder = Recorder(repository=make_repository(organization_id=2))
        assert_refused(fetch_refs(make_server(recorder), make_grant()), 404, "did not belong")
        assert recorder.token_lookups == []

    def test_inactive(self):
        recorder = Recorder(repository=make_repository(is_active=False))
        assert_refused(fetch_refs(make_server(recorder), make_grant()), 404, "inactive")
        assert recorder.token_lookups == []

    def test_provider_without_a_git_remote(self):
        recorder = Recorder(repository=make_repository(provider_name="bitbucket"))
        assert_refused(fetch_refs(make_server(recorder), make_grant()), 400, "no git remote")
        assert recorder.token_lookups == []


class TestRelay:
    def test_no_token(self):
        recorder = Recorder(token=None)
        assert_refused(fetch_refs(make_server(recorder), make_grant()), 404, "No git credentials")

    def test_token_asked_for_the_grants_operation(self):
        recorder = Recorder()
        server = make_server(recorder)
        server.handle(
            method="POST",
            repository_id=5,
            route="git-receive-pack",
            query="",
            headers={GRANT_HEADER: make_grant(operation="push")},
            body=io.BytesIO(b"0000"),
        )
        assert recorder.token_lookups == [(make_repository(), "push")]

    def test_forwards_with_remote_and_token(self):
        recorder = Recorder()
        server = make_server(recorder)
        headers = {GRANT_HEADER: make_grant(), "Git-Protocol": "version=2"}
        response = server.handle(
            method="GET",
            repository_id=5,
            route="info/refs",
            query="service=git-upload-pack",
            headers=headers,
            body=None,
        )
        assert b"".join(response.content) == b"refs"
        server.relay.forward.assert_called_once_with(  # type: ignore[attr-defined]
            GitRemote(url="https://github.com/getsentry/sentry.git", username="x-access-token"),
            TOKEN,
            method="GET",
            route="info/refs",
            query="service=git-upload-pack",
            headers=headers,
            body=None,
        )
        assert recorder.counts == [
            ("sentry.scm.git.request.succeeded", {"operation": "fetch", "provider": "github", "status": "200"})
        ]

    def test_upstream_unavailable(self):
        recorder = Recorder(
            repository=make_repository(provider_name="github_enterprise", web_base_url="https://127.0.0.1:1")
        )
        relay = GitRelay(timeout=(1, 1))
        try:
            response = fetch_refs(make_server(recorder, relay), make_grant())
        finally:
            relay.close()
        assert_refused(response, 503, "Could not connect")
        assert TOKEN not in b"".join(response.content).decode()
        assert recorder.errors == []

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
        relay = MagicMock(spec=GitRelay)
        relay.forward.return_value = StreamResponse(status_code=401, headers={}, content=content)
        recorder = Recorder()
        response = fetch_refs(make_server(recorder, relay), make_grant())
        assert_refused(response, 502, "rejected Sentry's credentials")
        assert closed == [True]
        assert recorder.counts == [("sentry.scm.git.request.failed", {"code": "resource_bad_gateway"})]

    def test_relays_end_to_end(self, monkeypatch):
        with running_upstream() as upstream:
            monkeypatch.setenv("REQUESTS_CA_BUNDLE", upstream.ca_bundle)
            recorder = Recorder(
                repository=make_repository(provider_name="github_enterprise", web_base_url=upstream.url)
            )
            relay = GitRelay()
            try:
                response = fetch_refs(make_server(recorder, relay), make_grant())
                assert response.status_code == 200
                b"".join(response.content)
            finally:
                relay.close()
        [request] = upstream.requests
        assert request.path == "/getsentry/sentry.git/info/refs?service=git-upload-pack"
        assert request.headers["Authorization"] == basic_auth("x-access-token", TOKEN)
        assert GRANT_HEADER not in request.headers
