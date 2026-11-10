import contextlib
import io
import ssl
import time
from collections.abc import Generator
from types import SimpleNamespace
from unittest.mock import MagicMock
from urllib.parse import urlsplit

import pytest

from scm.errors import (
    ClientClosedRequest,
    GitGrantKeyError,
    GitRemoteRefused,
    RateLimitExceeded,
    ResourceGatewayTimeout,
    ResourceServiceUnavailable,
)
from scm.git.grant import sign_grant
from scm.git.relay import GitRelay, RelayResponse, ResponseBody
from scm.git.server import _STATUS, GitServer
from scm.git.types import GRANT_HEADER, GitOperation, GrantV2FetchRule, GrantV2PushRule, Readable
from scm.rpc.types import StreamResponse
from scm.secret import Secret
from scm.types import GitRemote, Provider, Repository, RepositoryId
from tests.unit.error_reports import assert_not_in_frames, assert_not_reported, reported_local
from tests.unit.git_builders import (
    NEW_ID,
    OLD_ID,
    make_grant,
    make_key_pair,
    make_provider,
    make_push_body,
    make_repository,
    make_v1_grant,
    pkt,
)
from tests.unit.git_upstream import (
    ClosingAdapter,
    HostRefused,
    RefusingAdapter,
    UpstreamHandler,
    basic_auth,
    running_upstream,
)

KEY = make_key_pair()
TOKEN = Secret("ghs_secrettoken")
REFERRER = make_grant().referrer
# A constant, not a test's local, since the test's own frame is in the traceback.
PARTIAL_PACK = b"PACK-partial-repository-contents" * 64


def signed_grant(**overrides) -> str:
    return sign_grant(KEY.private, make_grant(**overrides))


class Recorder:
    def __init__(
        self, repository: Repository | None = None, token: Secret | None = TOKEN, token_error: Exception | None = None
    ) -> None:
        self.repository = make_repository() if repository is None else repository
        self.token = token
        self.token_error = token_error
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

    def get_token(self, repository: Repository, operation: GitOperation) -> Secret | None:
        self.token_lookups.append((repository, operation))
        if self.token_error is not None:
            raise self.token_error
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
        public_keys=[KEY.public],
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
            public_keys=[KEY.public],
            fetch_repository=lambda organization_id, repository_id: None,
            fetch_provider=lambda organization_id, repository: None,
            get_token=lambda repository, operation: None,
            record_count=lambda name, value, tags: None,
            emit_error=lambda error, context: None,
        )


@pytest.mark.parametrize(
    "public_keys,message",
    [
        ([], "no git grant public keys"),
        ([KEY.public, "not a key"], r"^public_keys\[1\] is not a public key in PEM"),
        ([KEY.private.reveal()], r"^public_keys\[0\] is a private key\."),
        ([KEY.private], r"^public_keys\[0\] must be a PEM string, not Secret\."),
    ],
    ids=["empty", "not-a-key", "private-key", "secret"],
)
def test_requires_public_keys(public_keys, message):
    # Checked when the server is built, since no grant could verify against them. Not a coded error, so a caller
    # that answers those as refusals does not blame the client for the server's keys.
    with pytest.raises(GitGrantKeyError, match=message):
        GitServer(
            public_keys=public_keys,
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
            (sign_grant(make_key_pair().private, make_grant()), "not signed with a known key", "git_invalid_grant"),
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
        assert_refused(push(make_server(recorder), signed_grant()), 403, "allows fetch on this repository, not push")
        assert recorder.repository_lookups == []
        assert recorder.token_lookups == []
        assert recorder.counts == [
            ("sentry.scm.git.request.failed", {"referrer": REFERRER, "code": "git_operation_not_granted"})
        ]
        assert recorder.errors[0][1] == {"repository_id": "5", "organization_id": "1", "referrer": REFERRER}

    def test_fetch_rule_that_lists_branches(self):
        recorder = Recorder()
        grant = signed_grant(rules=[GrantV2FetchRule(repository=5, branches=["main"])])
        assert_refused(request(make_server(recorder), grant), 403, "cannot keep a fetch to a rule's branches")
        assert recorder.repository_lookups == []
        assert recorder.token_lookups == []

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

    def test_token_asked_for_the_requests_operation(self):
        recorder = Recorder()
        request(make_server(recorder), signed_grant())
        assert recorder.token_lookups == [(make_repository(), "fetch")]

    def test_forwards_with_remote_and_token(self):
        recorder = Recorder()
        server = make_server(recorder)
        response = request(server, signed_grant(), headers={"Git-Protocol": "version=2"})
        assert b"".join(response.content) == b"refs"
        server.relay.forward.assert_called_once_with(  # type: ignore[attr-defined]
            GitRemote(url="https://github.com/getsentry/sentry.git", basic_auth_username="x-access-token"),
            TOKEN,
            method="GET",
            route="info/refs",
            query="service=git-upload-pack",
            # Without the grant, which the relay has no use for and would otherwise hold in its frame.
            headers={"Git-Protocol": "version=2"},
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
        # Not coded, so the caller reports it.
        assert recorder.errors == []

    def test_provider_stream_breaking_with_a_coded_error_is_reported_and_raised(self):
        error = ResourceGatewayTimeout()

        def broken_stream():
            yield b"half a pack"
            raise error

        recorder = Recorder()
        content = request(make_server(recorder, relay_returning(broken_stream())), signed_grant()).content
        assert next(content) == b"half a pack"
        assert recorder.errors == []
        # Raised, so the WSGI server drops the connection.
        with pytest.raises(ResourceGatewayTimeout) as exc_info:
            next(content)
        assert exc_info.value is error
        assert recorder.errors == [
            (
                error,
                {
                    "repository_id": "5",
                    "organization_id": "1",
                    "referrer": REFERRER,
                    "operation": "fetch",
                    "provider": "github",
                    "remote_host": "github.com",
                },
            )
        ]
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
        # Reported once, however the caller goes on to close the stream.
        content.close()  # type: ignore[attr-defined]
        assert len(recorder.errors) == 1
        assert len(recorder.counts) == 1

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
        assert TOKEN.reveal() not in b"".join(response.content).decode()

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
        # Retrying cannot change the remote's scheme, so this is the server's error rather than a 502.
        assert_refused(response, 500, "not an https URL")
        assert recorder.counts == [
            ("sentry.scm.git.request.failed", {"referrer": REFERRER, "code": "git_remote_not_https"})
        ]

    def test_adapter_refusing_the_host(self):
        # e.g. Sentry's BlacklistAdapter refusing a GitHub Enterprise host that resolves to an internal address.
        recorder = Recorder(
            repository=make_repository(provider_name="github_enterprise", web_base_url="https://127.0.0.1:1")
        )
        relay = GitRelay(
            adapter=RefusingAdapter,
            map_error=lambda error: GitRemoteRefused() if isinstance(error, HostRefused) else None,
        )
        response = request(make_server(recorder, relay), signed_grant())
        assert_refused(response, 500, "at an address Sentry does not connect to")
        assert recorder.counts == [
            ("sentry.scm.git.request.failed", {"referrer": REFERRER, "code": "git_remote_refused"})
        ]
        assert recorder.error_codes == ["git_remote_refused"]

    def test_untrusted_certificate(self):
        # A self-managed instance whose certificate Sentry does not trust. Retrying cannot fix it.
        with running_upstream() as upstream:
            recorder = Recorder(
                repository=make_repository(provider_name="github_enterprise", web_base_url=upstream.url)
            )
            response = request(make_server(recorder, GitRelay()), signed_grant())
        assert_refused(response, 500, "TLS certificate")
        assert recorder.counts == [
            ("sentry.scm.git.request.failed", {"referrer": REFERRER, "code": "resource_certificate_invalid"})
        ]
        assert upstream.requests == []

    def test_token_supplier_raising_a_coded_error(self):
        # e.g. Sentry failing to reach the control silo for a token: the supplier says the failure may pass, and
        # GitServer answers with a 503, which the client retries.
        recorder = Recorder(token_error=ResourceServiceUnavailable())
        server = make_server(recorder)
        assert_refused(request(server, signed_grant()), 503, "temporarily unavailable")
        server.relay.forward.assert_not_called()  # type: ignore[attr-defined]
        assert recorder.counts == [
            ("sentry.scm.git.request.failed", {"referrer": REFERRER, "code": "resource_service_unavailable"})
        ]
        assert recorder.error_codes == ["resource_service_unavailable"]
        assert recorder.errors[0][1] == {
            "repository_id": "5",
            "organization_id": "1",
            "referrer": REFERRER,
            "operation": "fetch",
            "provider": "github",
            "remote_host": "github.com",
        }

    def test_token_supplier_raising_another_error(self):
        recorder = Recorder(token_error=ConnectionError("control silo unreachable"))
        with pytest.raises(ConnectionError):
            request(make_server(recorder), signed_grant())
        assert recorder.counts == [
            ("sentry.scm.git.request.failed", {"referrer": REFERRER, "code": "unhandled_exception"})
        ]
        assert recorder.errors == []

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
        # Sentry's view answers these the way it answers them elsewhere, e.g. an adapter's error that the relay's
        # map_error leaves alone.
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
                signed_grant(),
                method="POST",
                route="git-upload-pack",
                query="",
                headers={"Content-Length": "8"},
                body=io.BytesIO(b"00000000"),
            )
            assert b"".join(response.content) == b"00000000"  # The fake upstream echoes the body.
        assert recorder.bytes == {
            ("sentry.scm.git.request_size", "fetch"): 8,
            ("sentry.scm.git.response_size", "fetch"): 8,
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
        assert received.headers["Authorization"] == basic_auth("x-access-token", TOKEN.reveal())
        assert GRANT_HEADER not in received.headers

    def test_provider_cutting_off_the_body_end_to_end(self):
        def handler(h: UpstreamHandler) -> None:
            h.start_chunked(200)
            h.write_chunk(b"first")
            h.wfile.write(b"%x\r\n" % (2 * len(PARTIAL_PACK)) + PARTIAL_PACK)
            h.wfile.flush()
            # See test_git_relay's test_provider_cutting_off_the_body.
            with contextlib.suppress(ssl.SSLError):
                h.connection.unwrap()  # type: ignore[attr-defined]
            h.close_connection = True

        with running_upstream() as upstream:
            upstream.handler = handler
            recorder = Recorder(
                repository=make_repository(provider_name="github_enterprise", web_base_url=upstream.url)
            )
            content = request(make_server(recorder, GitRelay(ca_bundle=upstream.ca_bundle)), signed_grant()).content
            assert next(content) == b"first"
            with pytest.raises(ResourceServiceUnavailable) as exc_info:
                next(content)
        assert exc_info.value.__cause__ is None
        assert exc_info.value.__suppress_context__
        assert_not_reported(exc_info.value, PARTIAL_PACK[:32])
        [(error, context)] = recorder.errors
        assert error is exc_info.value
        assert set(context) == {"repository_id", "organization_id", "referrer", "operation", "provider", "remote_host"}
        assert [tags["code"] for _, tags in recorder.counts] == ["stream_interrupted"]


class TestKeptOutOfErrorReports:
    """What GitServer passes to emit_error or raises is reported to Sentry, with every frame's local variables."""

    @pytest.mark.parametrize("header", [GRANT_HEADER, GRANT_HEADER.lower()])
    def test_relay_failure_leaves_out_the_grant(self, header):
        recorder = Recorder(
            repository=make_repository(provider_name="github_enterprise", web_base_url="https://127.0.0.1:1")
        )
        grant = signed_grant()
        request(make_server(recorder, GitRelay(timeout=(1, 1))), None, headers={header: grant})
        [(error, _)] = recorder.errors
        # Failed after the grant check, in the relay.
        assert error.code != "git_invalid_grant"  # type: ignore[attr-defined]
        assert_not_in_frames(error, grant)

    def test_relay_failure_leaves_out_the_provider_token(self):
        recorder = Recorder(
            repository=make_repository(provider_name="github_enterprise", web_base_url="https://127.0.0.1:1")
        )
        request(make_server(recorder, GitRelay(timeout=(1, 1))), signed_grant())
        [(error, _)] = recorder.errors
        assert error.code == "resource_service_unavailable"  # type: ignore[attr-defined]
        assert_not_in_frames(error, TOKEN.reveal())
        # Not because the frames dropped it: both hold it, masked.
        assert reported_local(error, "handle", "token") == "[Filtered]"
        assert reported_local(error, "forward", "token") == "[Filtered]"

    def test_rejected_token_is_left_out(self):
        recorder = Recorder()
        request(make_server(recorder, relay_returning(chunks(b"Bad credentials"), 401)), signed_grant())
        [(error, _)] = recorder.errors
        assert error.code == "git_token_rejected"  # type: ignore[attr-defined]
        assert_not_in_frames(error, TOKEN.reveal())
        assert reported_local(error, "handle", "token") == "[Filtered]"

    def test_token_that_is_not_a_secret_is_refused_and_left_out(self):
        # A plain string from code the type checker does not see, which the caller would report with its frames.
        recorder = Recorder(token=TOKEN.reveal())  # type: ignore[arg-type]
        server = make_server(recorder)
        with pytest.raises(TypeError, match=r"^get_token must return an scm\.Secret, not str\.") as exc_info:
            request(server, signed_grant())
        assert TOKEN.reveal() not in str(exc_info.value)
        assert_not_in_frames(exc_info.value, TOKEN.reveal())
        server.relay.forward.assert_not_called()  # type: ignore[attr-defined]
        assert recorder.counts == [
            ("sentry.scm.git.request.failed", {"referrer": REFERRER, "code": "unhandled_exception"})
        ]


PUSH_GRANT = sign_grant(KEY.private, make_grant(rules=[GrantV2PushRule(repository=5, branches=["seer/fix"])]))


def push_body(ref: str, capabilities: str = "report-status side-band-64k") -> bytes:
    return make_push_body((OLD_ID, NEW_ID, ref), capabilities=capabilities)


class Unreliable(io.BytesIO):
    def read(self, size: int | None = -1, /) -> bytes:
        raise ConnectionResetError()


class TestPushRules:
    """A push under a push rule reaches the provider only if the rule allows every ref it updates."""

    def send(self, server: GitServer, body: Readable, headers: dict[str, str] | None = None) -> StreamResponse:
        return request(
            server, PUSH_GRANT, method="POST", route="git-receive-pack", query="", headers=headers, body=body
        )

    @pytest.mark.parametrize("sized", [True, False], ids=["content-length", "chunked"])
    def test_listed_branch_end_to_end(self, sized):
        body = push_body("refs/heads/seer/fix")
        with running_upstream() as upstream:
            recorder = Recorder(
                repository=make_repository(provider_name="github_enterprise", web_base_url=upstream.url)
            )
            response = self.send(
                make_server(recorder, GitRelay(ca_bundle=upstream.ca_bundle)),
                io.BytesIO(body),
                headers={"Content-Length": str(len(body))} if sized else None,
            )
            # The fake upstream echoes the body, so the provider got the updates the server read and the pack after.
            assert b"".join(response.content) == body
        assert recorder.token_lookups == [(recorder.repository, "push")]
        assert recorder.bytes[("sentry.scm.git.request_size", "push")] == len(body)

    def test_probe(self):
        recorder = Recorder()
        server = make_server(recorder)
        assert self.send(server, io.BytesIO(b"0000")).status_code == 200
        assert server.relay.forward.call_args.kwargs["body"].read() == b"0000"  # type: ignore[attr-defined]

    def test_info_refs(self):
        recorder = Recorder()
        response = request(make_server(recorder), PUSH_GRANT, query="service=git-receive-pack")
        assert b"".join(response.content) == b"refs"

    def test_unlisted_branch_gets_a_report(self):
        recorder = Recorder()
        server = make_server(recorder)
        body = io.BytesIO(push_body("refs/heads/main"))
        response = self.send(server, body)
        assert response.status_code == 200
        assert response.headers["Content-Type"] == "application/x-git-receive-pack-result"
        report = b"".join(response.content)
        assert int(response.headers["Content-Length"]) == len(report)
        assert report == (
            pkt(
                b"\1"
                + pkt(b"unpack ok\n")
                + pkt(b"ng refs/heads/main the git grant does not cover this branch\n")
                + b"0000"
            )
            + b"0000"
        )
        # The pack is read to the end, so git gets to read the report rather than a broken pipe.
        assert body.read() == b""
        assert recorder.repository_lookups == []
        assert recorder.token_lookups == []
        server.relay.forward.assert_not_called()  # type: ignore[attr-defined]
        assert recorder.counts == [
            ("sentry.scm.git.request.failed", {"referrer": REFERRER, "code": "git_operation_not_granted"})
        ]
        [(error, context)] = recorder.errors
        assert error.detail == (  # type: ignore[attr-defined]
            "The git grant does not allow this push: refs/heads/main (the git grant does not cover this branch)."
        )
        assert context == {"repository_id": "5", "organization_id": "1", "referrer": REFERRER, "operation": "push"}

    @pytest.mark.parametrize(
        "ref,old_id,new_id,reason",
        [
            ("refs/tags/v1", OLD_ID, NEW_ID, "does not cover this tag"),
            ("refs/notes/commits", OLD_ID, NEW_ID, "allows pushing branches and tags only"),
            ("refs/heads/seer/fix", OLD_ID, "0" * 40, "does not allow deleting a branch"),
        ],
    )
    def test_other_refused_updates(self, ref, old_id, new_id, reason):
        body = make_push_body((old_id, new_id, ref), capabilities="report-status")
        assert reason.encode() in b"".join(self.send(make_server(Recorder()), io.BytesIO(body)).content)

    def test_no_body(self):
        # Refused rather than relayed unchecked.
        recorder = Recorder()
        server = make_server(recorder)
        response = request(server, PUSH_GRANT, method="POST", route="git-receive-pack", query="", body=None)
        assert_refused(response, 400, "The push has no body.")
        assert recorder.repository_lookups == []
        server.relay.forward.assert_not_called()  # type: ignore[attr-defined]

    def test_signed_push_on_a_side_band(self):
        body = pkt(b"push-cert\0report-status side-band-64k") + pkt(b"certificate version 0.1\n") + b"0000"
        response = self.send(make_server(Recorder()), io.BytesIO(body))
        assert response.status_code == 200
        assert b"".join(response.content) == pkt(b"\3The git endpoint does not relay signed pushes.\n") + b"0000"

    def test_create_only_pattern_and_listed_branch(self):
        grant = sign_grant(
            KEY.private,
            make_grant(
                rules=[
                    GrantV2PushRule(repository=5, branch_patterns=["seer/.+"], create_only=True),
                    GrantV2PushRule(repository=5, branches=["seer/fix"]),
                ]
            ),
        )
        server = make_server(Recorder())
        for body in (
            make_push_body(("0" * 40, NEW_ID, "refs/heads/seer/next")),
            make_push_body((OLD_ID, NEW_ID, "refs/heads/seer/fix")),
        ):
            response = request(server, grant, method="POST", route="git-receive-pack", query="", body=io.BytesIO(body))
            assert response.status_code == 200
        assert server.relay.forward.call_count == 2  # type: ignore[attr-defined]
        refused = make_push_body((OLD_ID, NEW_ID, "refs/heads/seer/next"), capabilities="report-status")
        response = request(server, grant, method="POST", route="git-receive-pack", query="", body=io.BytesIO(refused))
        assert b"the git grant allows only creating this branch" in b"".join(response.content)

    def test_client_that_asked_for_no_report(self):
        recorder = Recorder()
        body = io.BytesIO(push_body("refs/heads/main", capabilities="side-band-64k"))
        assert_refused(self.send(make_server(recorder), body), 403, "does not allow this push: refs/heads/main")
        assert body.read() == b""

    def test_malformed(self):
        recorder = Recorder()
        assert_refused(self.send(make_server(recorder), io.BytesIO(b"garbage")), 400, "not pkt-lines")
        assert recorder.repository_lookups == []
        assert recorder.counts == [
            ("sentry.scm.git.request.failed", {"referrer": REFERRER, "code": "git_malformed_request_body"})
        ]

    def test_compressed(self):
        response = self.send(
            make_server(Recorder()), io.BytesIO(push_body("refs/heads/seer/fix")), headers={"Content-Encoding": "gzip"}
        )
        assert_refused(response, 400, "content-encoding header holds a value git doesn't send")

    def test_client_closes_while_sending_its_updates(self):
        recorder = Recorder()
        response = self.send(make_server(recorder), Unreliable())
        assert response.status_code == 499
        assert recorder.errors == []

    def test_version_1_push_grant_relays_unread(self):
        recorder = Recorder()
        server = make_server(recorder)
        grant = sign_grant(KEY.private, make_v1_grant(operation="push"))
        body = io.BytesIO(b"not a push the server could read")
        request(server, grant, method="POST", route="git-receive-pack", query="", body=body)
        assert body.tell() == 0
        assert server.relay.forward.call_args.kwargs["body"].read() == b"not a push the server could read"  # type: ignore[attr-defined]
