import gzip
import io
import socket
import threading
import time
from collections.abc import Callable, Iterator
from unittest.mock import MagicMock

import pytest
import requests
import urllib3
from requests.adapters import HTTPAdapter

from scm.errors import (
    GitOperationNotGranted,
    GitRemoteNotHttps,
    GitRemoteRefused,
    GitRepositoryNotGranted,
    GitRouteNotSupported,
    ResourceBadGateway,
    ResourceCertificateInvalid,
    ResourceGatewayTimeout,
    ResourceServiceUnavailable,
    SCMCodedError,
)
from scm.git.relay import GitRelay, _UpstreamStream, authorize_request
from scm.types import GitRemote
from tests.unit.git_builders import make_grant
from tests.unit.git_upstream import (
    ClosingAdapter,
    HostRefused,
    RefusingAdapter,
    UpstreamHandler,
    basic_auth,
    ca_bundle,
    running_upstream,
)

TOKEN = "ghs_secrettoken"
# Long enough that a relay that buffers fails the test instead of hanging it.
WAIT = 5


@pytest.fixture
def upstream():
    with running_upstream() as upstream:
        yield upstream


@pytest.fixture
def relay():
    return GitRelay(ca_bundle=ca_bundle())


def remote_for(upstream) -> GitRemote:
    return GitRemote(url=f"{upstream.url}/getsentry/sentry.git", basic_auth_username="x-access-token")


def fetch_refs(relay: GitRelay, upstream=None, headers: dict[str, str] | None = None, *, remote=None):
    """Fetch the refs advertisement from the fake upstream, or from ``remote`` when given."""
    return relay.forward(
        remote or remote_for(upstream),
        TOKEN,
        method="GET",
        route="info/refs",
        query="service=git-upload-pack",
        headers=headers or {},
        body=None,
    )


def upload_pack(relay: GitRelay, upstream, body, headers: dict[str, str] | None = None):
    return relay.forward(
        remote_for(upstream),
        TOKEN,
        method="POST",
        route="git-upload-pack",
        query="",
        headers={"Content-Type": "application/x-git-upload-pack-request", **(headers or {})},
        body=body,
    )


class TestRequest:
    def test_url_and_auth(self, relay, upstream):
        b"".join(fetch_refs(relay, upstream).content)
        [request] = upstream.requests
        assert request.method == "GET"
        assert request.path == "/getsentry/sentry.git/info/refs?service=git-upload-pack"
        assert request.headers["Authorization"] == basic_auth("x-access-token", TOKEN)

    def test_git_headers_pass_through(self, relay, upstream):
        body = gzip.compress(b"0000")
        response = upload_pack(
            relay,
            upstream,
            io.BytesIO(body),
            headers={
                "Git-Protocol": "version=2",
                "Content-Encoding": "gzip",
                "Accept": "application/x-git-upload-pack-result",
                "Accept-Encoding": "deflate, gzip",
                "User-Agent": "git/2.47.0",
                "Content-Length": str(len(body)),
            },
        )
        b"".join(response.content)
        [request] = upstream.requests
        assert request.headers["Git-Protocol"] == "version=2"
        assert request.headers["Content-Encoding"] == "gzip"
        assert request.headers["Content-Type"] == "application/x-git-upload-pack-request"
        assert request.headers["Accept"] == "application/x-git-upload-pack-result"
        assert request.headers["Accept-Encoding"] == "deflate, gzip"
        assert request.headers["User-Agent"] == "git/2.47.0"
        # The body is passed on as it came, still compressed.
        assert request.body == body

    def test_other_headers_stay_behind(self, relay, upstream):
        b"".join(
            fetch_refs(
                relay,
                upstream,
                headers={
                    "Authorization": "Bearer router-credential",
                    "X-Scm-Git-Grant": "grant",
                    "Cookie": "session=1",
                    "X-Forwarded-For": "10.0.0.1",
                    "Host": "sentry.io",
                },
            ).content
        )
        [request] = upstream.requests
        assert request.headers["Authorization"] == basic_auth("x-access-token", TOKEN)
        assert request.headers["Host"] == upstream.url.removeprefix("https://")
        for name in ("X-Scm-Git-Grant", "Cookie", "X-Forwarded-For"):
            assert name not in request.headers

    def test_header_names_are_case_insensitive(self, relay, upstream):
        b"".join(fetch_refs(relay, upstream, headers={"git-protocol": "version=2", "accept-encoding": "gzip"}).content)
        [request] = upstream.requests
        assert request.headers["Git-Protocol"] == "version=2"
        assert request.headers["Accept-Encoding"] == "gzip"

    def test_asks_for_an_unencoded_response_unless_the_client_says_otherwise(self, relay, upstream):
        b"".join(fetch_refs(relay, upstream).content)
        assert upstream.requests[0].headers["Accept-Encoding"] == "identity"

    def test_refuses_plain_http(self, relay, upstream):
        remote = GitRemote(
            url=upstream.url.replace("https://", "http://") + "/repo.git", basic_auth_username="x-access-token"
        )
        with pytest.raises(GitRemoteNotHttps, match="not an https URL"):
            fetch_refs(relay, remote=remote)
        assert upstream.requests == []

    def test_refuses_routes_that_are_not_git(self, relay, upstream):
        with pytest.raises(GitRouteNotSupported):
            relay.forward(
                remote_for(upstream),
                TOKEN,
                method="POST",
                route="info/lfs/objects/batch",
                query="",
                headers={},
                body=None,
            )
        assert upstream.requests == []

    def test_get_sends_no_body(self, relay, upstream):
        # A view passes its request stream for every method; a GET's must not go upstream as an empty chunked body.
        body = io.BytesIO(b"")
        b"".join(
            relay.forward(
                remote_for(upstream),
                TOKEN,
                method="GET",
                route="info/refs",
                query="service=git-upload-pack",
                headers={},
                body=body,
            ).content
        )
        [request] = upstream.requests
        assert "Transfer-Encoding" not in request.headers
        assert "Content-Length" not in request.headers

    def test_known_length_body_is_sent_with_its_length(self, relay, upstream):
        b"".join(upload_pack(relay, upstream, io.BytesIO(b"0000"), headers={"Content-Length": "4"}).content)
        [request] = upstream.requests
        assert request.headers["Content-Length"] == "4"
        assert "Transfer-Encoding" not in request.headers
        assert request.body == b"0000"

    def test_unknown_length_body_is_sent_chunked(self, relay, upstream):
        b"".join(upload_pack(relay, upstream, io.BytesIO(b"0000")).content)
        [request] = upstream.requests
        assert request.headers["Transfer-Encoding"] == "chunked"
        assert request.body == b"0000"

    def test_empty_body_is_sent_as_one(self, relay, upstream):
        b"".join(upload_pack(relay, upstream, io.BytesIO(b""), headers={"Content-Length": "0"}).content)
        [request] = upstream.requests
        assert request.headers["Content-Length"] == "0"
        assert "Transfer-Encoding" not in request.headers
        assert request.body == b""

    def test_reads_no_further_than_the_declared_length(self, relay, upstream):
        body = io.BytesIO(b"0000trailing")
        b"".join(upload_pack(relay, upstream, body, headers={"Content-Length": "4"}).content)
        assert upstream.requests[0].body == b"0000"
        assert body.read() == b"trailing"

    def test_body_shorter_than_its_length(self, relay, upstream):
        with pytest.raises(SCMCodedError) as exc_info:
            upload_pack(relay, upstream, io.BytesIO(b"00"), headers={"Content-Length": "4"})
        assert exc_info.value.code == "client_closed_request"

    @pytest.mark.parametrize("known_length", [True, False])
    @pytest.mark.parametrize("error", [ConnectionResetError, BrokenPipeError])
    def test_client_resetting_mid_body_is_reported_at_once(self, upstream, known_length, error):
        # urllib3 ignores ECONNRESET and EPIPE while sending a body, taking them for the provider closing the
        # connection, and then waits for a response to the cut-off request until its read timeout.
        upstream.handler = lambda h: (b"".join(h.body_chunks()), h.respond(200, b"ok"))

        class ResettingClient:
            def __init__(self) -> None:
                self.reads = 0

            def read(self, size: int = -1, /) -> bytes:
                self.reads += 1
                if self.reads > 1:
                    raise error()
                return b"first"

        headers = {"Content-Length": "100"} if known_length else {}
        relay = GitRelay(ca_bundle=ca_bundle(), timeout=(1, 2))
        with pytest.raises(SCMCodedError) as exc_info:
            upload_pack(relay, upstream, ResettingClient(), headers=headers)
        assert exc_info.value.code == "client_closed_request"

    @pytest.mark.parametrize("content_length", ["abc", "-1", "1.5", "²", ""])
    def test_malformed_content_length(self, relay, upstream, content_length):
        with pytest.raises(SCMCodedError) as exc_info:
            upload_pack(relay, upstream, io.BytesIO(b"0000"), headers={"Content-Length": content_length})
        assert exc_info.value.code == "rpc_malformed_request_headers"
        assert upstream.requests == []


class TestResponse:
    def test_status_body_and_headers(self, relay, upstream):
        def handler(h: UpstreamHandler) -> None:
            h.record(b"")
            h.respond(
                200,
                b"001e# service=git-upload-pack\n0000",
                {
                    "Content-Type": "application/x-git-upload-pack-advertisement",
                    "Cache-Control": "no-cache",
                    "Set-Cookie": "_gh_sess=abc",
                    "X-GitHub-Request-Id": "ABC",
                    "WWW-Authenticate": 'Basic realm="GitHub"',
                },
            )

        upstream.handler = handler
        response = fetch_refs(relay, upstream)
        assert response.status_code == 200
        assert b"".join(response.content) == b"001e# service=git-upload-pack\n0000"
        assert response.headers == {
            "Content-Type": "application/x-git-upload-pack-advertisement",
            "Cache-Control": "no-cache",
            "Content-Length": "34",
        }

    def test_error_status_passes_through(self, relay, upstream):
        upstream.handler = lambda h: h.respond(404, b"Repository not found.", {"Content-Type": "text/plain"})
        response = fetch_refs(relay, upstream)
        assert response.status_code == 404
        assert b"".join(response.content) == b"Repository not found."

    def test_encoded_body_passes_through_undecoded(self, relay, upstream):
        compressed = gzip.compress(b"0008NAK\n" * 100)
        upstream.handler = lambda h: h.respond(200, compressed, {"Content-Encoding": "gzip"})
        response = fetch_refs(relay, upstream, headers={"Accept-Encoding": "gzip"})
        assert response.headers["Content-Encoding"] == "gzip"
        assert response.headers["Content-Length"] == str(len(compressed))
        assert b"".join(response.content) == compressed

    def test_redirect_is_not_followed(self, relay, upstream):
        def handler(h: UpstreamHandler) -> None:
            h.record(b"")
            h.respond(301, b"", {"Location": "https://elsewhere.example.com/repo.git/info/refs"})

        upstream.handler = handler
        response = fetch_refs(relay, upstream)
        assert response.status_code == 301
        assert "Location" not in response.headers
        b"".join(response.content)
        assert len(upstream.requests) == 1

    def test_redirect_body_streams(self, relay, upstream):
        # requests reads a redirect's whole body into memory to prepare the next request, even when it will not
        # follow it.
        body = b"x" * 1024 * 1024
        upstream.handler = lambda h: h.respond(302, body, {"Location": "https://elsewhere.example.com/"})
        response = fetch_refs(relay, upstream)
        assert response.content.upstream._content is False  # type: ignore[attr-defined]
        assert b"".join(response.content) == body


class TestStreaming:
    def test_response_streams_before_the_upstream_finishes(self, relay, upstream):
        client_got_first_chunk = threading.Event()
        upstream_waited = []

        def handler(h: UpstreamHandler) -> None:
            h.record(b"".join(h.body_chunks()))
            h.start_chunked(200, {"Content-Type": "application/x-git-upload-pack-result"})
            h.write_chunk(b"first")
            upstream_waited.append(client_got_first_chunk.wait(WAIT))
            h.write_chunk(b"second")
            h.write_chunk(b"")

        upstream.handler = handler
        content = upload_pack(relay, upstream, io.BytesIO(b"0000"), headers={"Content-Length": "4"}).content
        assert next(content) == b"first"
        client_got_first_chunk.set()
        assert b"".join(content) == b"second"
        assert upstream_waited == [True]

    @pytest.mark.parametrize("known_length", [True, False])
    def test_request_streams_before_the_client_finishes(self, relay, upstream, known_length):
        upstream_got_first_chunk = threading.Event()
        received: list[bytes] = []

        def handler(h: UpstreamHandler) -> None:
            chunks = h.body_chunks()
            received.append(next(chunks))
            upstream_got_first_chunk.set()
            received.extend(chunks)
            h.respond(200, b"ok")

        class SlowClient:
            """Holds the rest of its body back until the upstream has the first part."""

            def __init__(self) -> None:
                self.parts: Iterator[bytes] = iter([b"first", b"second"])
                self.waited: list[bool] = []

            def read(self, size: int = -1, /) -> bytes:
                part = next(self.parts, b"")
                if part == b"second":
                    self.waited.append(upstream_got_first_chunk.wait(WAIT))
                return part

        upstream.handler = handler
        client = SlowClient()
        headers = {"Content-Length": str(len(b"firstsecond"))} if known_length else {}
        response = upload_pack(relay, upstream, client, headers=headers)
        assert b"".join(response.content) == b"ok"
        assert client.waited == [True]
        assert b"".join(received) == b"firstsecond"


class TestConnections:
    def test_opens_a_connection_per_request(self, relay, upstream):
        b"".join(fetch_refs(relay, upstream).content)
        b"".join(upload_pack(relay, upstream, io.BytesIO(b"0000"), headers={"Content-Length": "4"}).content)
        b"".join(upload_pack(relay, upstream, io.BytesIO(b"0000")).content)
        assert len(upstream.requests) == 3
        assert upstream.connections == 3

    def test_closes_the_connection_when_the_response_ends(self, upstream):
        adapters: list[ClosingAdapter] = []

        def adapter() -> ClosingAdapter:
            adapters.append(ClosingAdapter())
            return adapters[-1]

        response = fetch_refs(GitRelay(adapter=adapter, ca_bundle=ca_bundle()), upstream)
        assert [a.closed for a in adapters] == [False]
        b"".join(response.content)
        assert [a.closed for a in adapters] == [True]

    def test_closes_the_connection_when_an_unread_response_is_closed(self, upstream):
        # As GitServer does with a 401 it replaces.
        adapters: list[ClosingAdapter] = []

        def adapter() -> ClosingAdapter:
            adapters.append(ClosingAdapter())
            return adapters[-1]

        upstream.handler = lambda h: h.respond(401, b"Bad credentials")
        response = fetch_refs(GitRelay(adapter=adapter, ca_bundle=ca_bundle()), upstream)
        response.content.close()  # type: ignore[attr-defined]
        assert [a.closed for a in adapters] == [True]

    def test_closes_the_connection_when_the_request_fails(self):
        adapters: list[ClosingAdapter] = []

        def adapter() -> ClosingAdapter:
            adapters.append(ClosingAdapter())
            return adapters[-1]

        remote = GitRemote(url="https://127.0.0.1:1/getsentry/sentry.git", basic_auth_username="x-access-token")
        with pytest.raises(ResourceServiceUnavailable):
            fetch_refs(GitRelay(adapter=adapter, timeout=(1, 1)), remote=remote)
        assert [a.closed for a in adapters] == [True]

    def test_does_not_carry_cookies_between_requests(self, relay, upstream):
        def handler(h: UpstreamHandler) -> None:
            h.record(b"")
            h.respond(200, b"", {"Set-Cookie": "_gh_sess=abc; Path=/"})

        upstream.handler = handler
        b"".join(fetch_refs(relay, upstream).content)
        b"".join(fetch_refs(relay, upstream).content)
        assert [r.headers.get("Cookie") for r in upstream.requests] == [None, None]


class TestAdapter:
    def test_uses_the_adapter_it_is_given(self, upstream):
        # Sentry passes an adapter that refuses internal addresses; the relay must send through it.
        with pytest.raises(HostRefused, match="internal address"):
            fetch_refs(GitRelay(adapter=RefusingAdapter, ca_bundle=ca_bundle()), upstream)
        assert upstream.connections == 0


def refusal_mapper(seen: list[Exception]) -> Callable[[Exception], SCMCodedError | None]:
    """A map_error that records each error it gets and maps HostRefused, the way Sentry maps its refusal."""

    def map_error(error: Exception) -> SCMCodedError | None:
        seen.append(error)
        return GitRemoteRefused() if isinstance(error, HostRefused) else None

    return map_error


class TestMapError:
    def test_mapped_error_is_raised_in_place_of_the_original(self, upstream):
        seen: list[Exception] = []
        relay = GitRelay(adapter=RefusingAdapter, map_error=refusal_mapper(seen), ca_bundle=ca_bundle())
        with pytest.raises(GitRemoteRefused) as exc_info:
            fetch_refs(relay, upstream)
        assert [type(error) for error in seen] == [HostRefused]
        # The refusal passes through urllib3's frames, which hold the Authorization header.
        assert_token_not_in(exc_info.value)
        # The refusal names the address; the client has no use for it.
        assert "127.0.0.1" not in exc_info.value.detail

    @pytest.mark.parametrize("map_error", [None, lambda error: None], ids=["no_mapper", "unmapped"])
    def test_unmapped_error_reaches_the_caller_unchanged(self, upstream, map_error):
        with pytest.raises(HostRefused, match="internal address"):
            fetch_refs(GitRelay(adapter=RefusingAdapter, map_error=map_error, ca_bundle=ca_bundle()), upstream)

    @pytest.mark.parametrize(
        ("make_error", "expected"),
        [
            pytest.param(
                lambda request: requests.ConnectionError("refused", request=request),
                ResourceServiceUnavailable,
                id="requests",
            ),
            pytest.param(
                lambda request: urllib3.exceptions.HTTPError("bad response"), ResourceBadGateway, id="urllib3"
            ),
        ],
    )
    def test_requests_and_urllib3_errors_never_reach_the_mapper(self, upstream, make_error, expected):
        # A requests exception holds the request and its Authorization header.
        class FailingAdapter(HTTPAdapter):
            def send(self, request, *args, **kwargs):
                raise make_error(request)

        seen: list[Exception] = []
        relay = GitRelay(adapter=FailingAdapter, map_error=refusal_mapper(seen), ca_bundle=ca_bundle())
        with pytest.raises(expected):
            fetch_refs(relay, upstream)
        assert seen == []

    def test_the_relays_own_errors_never_reach_the_mapper(self, upstream):
        seen: list[Exception] = []
        relay = GitRelay(map_error=refusal_mapper(seen), ca_bundle=ca_bundle())
        with pytest.raises(SCMCodedError) as exc_info:
            upload_pack(relay, upstream, io.BytesIO(b"00"), headers={"Content-Length": "4"})
        assert exc_info.value.code == "client_closed_request"
        assert seen == []


class TestEnvironment:
    # Behind a proxy, an adapter that refuses internal addresses would check the proxy's address rather than the
    # provider's, so the relay takes nothing from the environment.
    @pytest.mark.parametrize("variable", ["HTTPS_PROXY", "https_proxy", "ALL_PROXY"])
    def test_ignores_proxy_settings(self, relay, upstream, monkeypatch, variable):
        monkeypatch.setenv(variable, "http://127.0.0.1:1")
        b"".join(fetch_refs(relay, upstream).content)
        assert len(upstream.requests) == 1

    def test_ignores_ca_settings(self, upstream, monkeypatch):
        monkeypatch.setenv("REQUESTS_CA_BUNDLE", upstream.ca_bundle)
        with pytest.raises(ResourceCertificateInvalid):
            fetch_refs(GitRelay(), upstream)


class TestUpstreamFailures:
    def test_connection_refused(self, relay):
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        remote = GitRemote(url=f"https://127.0.0.1:{port}/getsentry/sentry.git", basic_auth_username="x-access-token")

        with pytest.raises(ResourceServiceUnavailable) as exc_info:
            fetch_refs(relay, remote=remote)
        assert_token_not_in(exc_info.value)
        # The client has no use for the provider's address.
        assert "127.0.0.1" not in exc_info.value.detail

    def test_untrusted_certificate(self, upstream):
        with pytest.raises(ResourceCertificateInvalid, match="TLS certificate") as exc_info:
            fetch_refs(GitRelay(), upstream)
        assert_token_not_in(exc_info.value)
        assert upstream.requests == []

    def test_read_timeout(self, upstream):
        release = threading.Event()

        def handler(h: UpstreamHandler) -> None:
            release.wait(WAIT)
            h.respond(200, b"")

        upstream.handler = handler
        try:
            with pytest.raises(ResourceGatewayTimeout) as exc_info:
                fetch_refs(GitRelay(ca_bundle=ca_bundle(), timeout=(1, 0.1)), upstream)
            assert_token_not_in(exc_info.value)
        finally:
            release.set()

    @pytest.mark.parametrize(
        "make_error",
        [
            pytest.param(
                lambda request: requests.exceptions.InvalidHeader("bad header", request=request), id="requests"
            ),
            # HTTPAdapter.send re-raises the urllib3 errors it does not translate.
            pytest.param(lambda request: urllib3.exceptions.HTTPError("bad response"), id="urllib3"),
        ],
    )
    def test_other_request_failures(self, upstream, make_error):
        class FailingAdapter(HTTPAdapter):
            def send(self, request, *args, **kwargs):
                raise make_error(request)

        with pytest.raises(ResourceBadGateway) as exc_info:
            fetch_refs(GitRelay(adapter=FailingAdapter, ca_bundle=ca_bundle()), upstream)
        assert_token_not_in(exc_info.value)


@pytest.mark.parametrize(
    ("basic_auth_username", "token"),
    [("x-access-token", "ghs_tok\u20acn"), ("x-acc\u00e9ss-token", TOKEN)],
    ids=["token", "basic_auth_username"],
)
def test_refuses_credentials_that_are_not_ascii(upstream, basic_auth_username, token):
    remote = GitRemote(url=f"{upstream.url}/getsentry/sentry.git", basic_auth_username=basic_auth_username)
    with pytest.raises(ValueError, match="not ASCII") as exc_info:
        GitRelay(ca_bundle=ca_bundle()).forward(
            remote,
            token,
            method="GET",
            route="info/refs",
            query="service=git-upload-pack",
            headers={},
            body=None,
        )
    assert token not in str(exc_info.value)
    assert exc_info.value.__context__ is None
    assert not upstream.requests


def assert_token_not_in(error: Exception) -> None:
    # The requests exception holds the prepared request and its Authorization header, so it must not be chained.
    assert error.__cause__ is None
    assert error.__suppress_context__
    assert TOKEN not in str(error)
    assert basic_auth("x-access-token", TOKEN) not in repr(error)


def upstream_stream(*chunks: bytes | Exception) -> tuple[_UpstreamStream, MagicMock, MagicMock]:
    """An _UpstreamStream over a response whose body yields ``chunks``, raising any that is an exception."""

    def stream(chunk_size, decode_content):
        for chunk in chunks:
            if isinstance(chunk, Exception):
                raise chunk
            yield chunk

    session, response = MagicMock(spec=requests.Session), MagicMock(spec=requests.Response)
    response.raw = MagicMock(stream=stream)
    return _UpstreamStream(session, response, 1024), session, response


class TestUpstreamStream:
    def test_skips_empty_chunks(self):
        # An empty chunk carries nothing, so it is not passed on.
        stream, _, _ = upstream_stream(b"a", b"", b"", b"b")
        assert list(stream) == [b"a", b"b"]

    def test_closes_the_response_and_session_when_the_body_ends(self):
        stream, session, response = upstream_stream(b"a")
        assert list(stream) == [b"a"]
        response.close.assert_called_once()
        session.close.assert_called_once()

    def test_closes_the_response_and_session_when_the_provider_stream_breaks(self):
        stream, session, response = upstream_stream(b"a", urllib3.exceptions.ProtocolError("connection broken"))
        assert next(stream) == b"a"
        response.close.assert_not_called()
        with pytest.raises(urllib3.exceptions.ProtocolError):
            next(stream)
        response.close.assert_called_once()
        session.close.assert_called_once()

    def test_close_before_reading(self):
        # A response GitServer replaces, such as a 401, is closed without being read.
        stream, session, response = upstream_stream(b"a")
        stream.close()
        response.close.assert_called_once()
        session.close.assert_called_once()


class TestClose:
    def test_closing_an_unread_response_drops_the_upstream_connection(self, relay, upstream):
        upstream_saw_close = threading.Event()

        def handler(h: UpstreamHandler) -> None:
            h.start_chunked(200)
            try:
                for _ in range(1000):
                    h.write_chunk(b"x" * 64 * 1024)
                    time.sleep(0.005)
            except OSError:
                h.close_connection = True
                upstream_saw_close.set()

        upstream.handler = handler
        response = fetch_refs(relay, upstream)
        response.content.close()  # type: ignore[attr-defined]
        assert upstream_saw_close.wait(WAIT)


FETCH_ROUTES = [("GET", "info/refs", "service=git-upload-pack"), ("POST", "git-upload-pack", "")]
PUSH_ROUTES = [("GET", "info/refs", "service=git-receive-pack"), ("POST", "git-receive-pack", "")]


class TestAuthorizeRequest:
    @pytest.mark.parametrize(
        "operation,route",
        [("fetch", route) for route in FETCH_ROUTES] + [("push", route) for route in PUSH_ROUTES],
    )
    def test_grant_reaches_its_operations_routes(self, operation, route):
        assert authorize_request(make_grant(operation=operation), 5, *route) == operation

    @pytest.mark.parametrize(
        "operation,route",
        [("fetch", route) for route in PUSH_ROUTES] + [("push", route) for route in FETCH_ROUTES],
    )
    def test_grant_cannot_reach_the_other_operations_routes(self, operation, route):
        other = "push" if operation == "fetch" else "fetch"
        with pytest.raises(GitOperationNotGranted, match=f"allows {operation}, not {other}"):
            authorize_request(make_grant(operation=operation), 5, *route)

    @pytest.mark.parametrize(
        "method,route,query",
        [
            ("GET", "info/refs", ""),
            ("GET", "info/refs", "service=git-upload-pack&x=1"),
            ("GET", "info/refs", "service=git-upload-archive"),
            ("POST", "info/refs", "service=git-upload-pack"),
            ("GET", "git-upload-pack", ""),
            ("POST", "git-upload-pack", "service=git-upload-pack"),
            ("post", "git-upload-pack", ""),
            ("POST", "git-upload-archive", ""),
            ("POST", "info/lfs/objects/batch", ""),
            ("GET", "HEAD", ""),
            ("GET", "objects/info/packs", ""),
            ("GET", "../info/refs", "service=git-upload-pack"),
            ("POST", "/git-upload-pack", ""),
        ],
    )
    def test_routes_that_are_not_git(self, method, route, query):
        for operation in ("fetch", "push"):
            with pytest.raises(GitRouteNotSupported, match="not a git fetch or push route"):
                authorize_request(make_grant(operation=operation), 5, method, route, query)

    def test_repository_not_in_grant(self):
        with pytest.raises(GitRepositoryNotGranted, match="does not cover this repository"):
            authorize_request(make_grant(), 6, "POST", "git-upload-pack", "")

    def test_repository_named_another_way_is_not_in_grant(self):
        # The grant and the request name repositories the same way; matching a database id to a provider id
        # would mean resolving every repository the grant lists.
        grant = make_grant(repositories=[("github", "123")])
        with pytest.raises(GitRepositoryNotGranted):
            authorize_request(grant, 5, "POST", "git-upload-pack", "")
        assert authorize_request(grant, ("github", "123"), "POST", "git-upload-pack", "") == "fetch"

    def test_any_listed_repository(self):
        grant = make_grant(repositories=[5, 6, ("gitlab", "gitlab.com:7")])
        assert authorize_request(grant, 6, "POST", "git-upload-pack", "") == "fetch"
        assert authorize_request(grant, ("gitlab", "gitlab.com:7"), "POST", "git-upload-pack", "") == "fetch"
