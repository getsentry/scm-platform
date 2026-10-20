import gzip
import io
import socket
import threading
import time
from collections.abc import Iterator

import pytest

from scm.errors import GitRouteNotAllowed, ResourceGatewayTimeout, ResourceServiceUnavailable, SCMCodedError
from scm.git.relay import GitRelay
from scm.git.types import GitRemote
from tests.unit.git_upstream import UpstreamHandler, basic_auth, running_upstream

TOKEN = "ghs_secrettoken"
# Long enough that a relay that buffers fails the test instead of hanging it.
WAIT = 5


@pytest.fixture
def upstream():
    with running_upstream() as upstream:
        yield upstream


@pytest.fixture
def relay():
    relay = GitRelay()
    yield relay
    relay.close()


def remote_for(upstream) -> GitRemote:
    return GitRemote(url=f"{upstream.url}/getsentry/sentry.git", username="x-access-token")


def fetch_refs(relay: GitRelay, upstream, headers: dict[str, str] | None = None):
    return relay.forward(
        remote_for(upstream),
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
        assert request.headers["Host"] == upstream.url.removeprefix("http://")
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

    def test_refuses_routes_that_are_not_git(self, relay, upstream):
        with pytest.raises(GitRouteNotAllowed):
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

    def test_reads_no_further_than_the_declared_length(self, relay, upstream):
        body = io.BytesIO(b"0000trailing")
        b"".join(upload_pack(relay, upstream, body, headers={"Content-Length": "4"}).content)
        assert upstream.requests[0].body == b"0000"
        assert body.read() == b"trailing"

    def test_body_shorter_than_its_length(self, relay, upstream):
        with pytest.raises(SCMCodedError) as exc_info:
            upload_pack(relay, upstream, io.BytesIO(b"00"), headers={"Content-Length": "4"})
        assert exc_info.value.code == "client_closed_request"

    @pytest.mark.parametrize("content_length", ["abc", "-1", "1.5"])
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
    def test_reuses_the_upstream_connection(self, relay, upstream):
        b"".join(fetch_refs(relay, upstream).content)
        b"".join(upload_pack(relay, upstream, io.BytesIO(b"0000"), headers={"Content-Length": "4"}).content)
        b"".join(upload_pack(relay, upstream, io.BytesIO(b"0000")).content)
        assert len(upstream.requests) == 3
        assert upstream.connections == 1

    def test_does_not_carry_cookies_between_requests(self, relay, upstream):
        def handler(h: UpstreamHandler) -> None:
            h.record(b"")
            h.respond(200, b"", {"Set-Cookie": "_gh_sess=abc; Path=/"})

        upstream.handler = handler
        b"".join(fetch_refs(relay, upstream).content)
        b"".join(fetch_refs(relay, upstream).content)
        assert [r.headers.get("Cookie") for r in upstream.requests] == [None, None]


class TestUpstreamFailures:
    def test_connection_refused(self, relay):
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            port = s.getsockname()[1]
        remote = GitRemote(url=f"http://127.0.0.1:{port}/getsentry/sentry.git", username="x-access-token")

        with pytest.raises(ResourceServiceUnavailable) as exc_info:
            relay.forward(
                remote, TOKEN, method="GET", route="info/refs", query="service=git-upload-pack", headers={}, body=None
            )
        assert_token_not_in(exc_info.value)

    def test_read_timeout(self, upstream):
        release = threading.Event()

        def handler(h: UpstreamHandler) -> None:
            release.wait(WAIT)
            try:
                h.respond(200, b"")
            except OSError:  # The relay stopped waiting and closed the connection.
                h.close_connection = True

        upstream.handler = handler
        relay = GitRelay(timeout=(1, 0.1))
        try:
            with pytest.raises(ResourceGatewayTimeout) as exc_info:
                fetch_refs(relay, upstream)
            assert_token_not_in(exc_info.value)
        finally:
            release.set()
            relay.close()


def assert_token_not_in(error: Exception) -> None:
    # The requests exception holds the prepared request and its Authorization header, so it must not be chained.
    assert error.__cause__ is None
    assert error.__suppress_context__
    assert TOKEN not in str(error)
    assert basic_auth("x-access-token", TOKEN) not in repr(error)


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
