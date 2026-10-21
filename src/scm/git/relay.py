from collections.abc import Callable, Iterator, Mapping
from urllib.parse import urlsplit

import requests
import urllib3
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth
from requests.structures import CaseInsensitiveDict

from scm.errors import (
    ClientClosedRequest,
    GitRemoteNotHttps,
    ResourceBadGateway,
    ResourceCertificateInvalid,
    ResourceGatewayTimeout,
    ResourceServiceUnavailable,
    RpcMalformedRequestHeaders,
)
from scm.git.operations import require_route
from scm.git.types import Readable
from scm.rpc.types import StreamResponse
from scm.types import GitRemote

# The most bytes the relay reads at once, from the client's body or from the provider's response, so also the
# most it holds per request in each direction. Smaller chunks cost more Python per byte of a multi-gigabyte pack;
# larger ones hold more memory per request. 64 KiB is what RpcServer streams with and what the prototype ran; no
# other size was measured.
DEFAULT_CHUNK_SIZE = 64 * 1024
# (connect, read) in seconds. The read timeout bounds a silence, not a transfer: it is the longest the provider may
# go without sending a byte, so a clone that streams for minutes never hits it. 300s leaves room for a provider that
# assembles a large pack before it sends anything. Both values come from the prototype.
DEFAULT_TIMEOUT = (10.0, 300.0)

# The headers git sends that the provider needs. Accept and Content-Type name the git service, Git-Protocol
# negotiates protocol v2, Content-Encoding marks a gzipped request body (git gzips large fetch requests),
# Accept-Encoding says what the client can decode, and User-Agent names the git client. Everything else stays
# behind, including the caller's own credentials and the grant.
_REQUEST_HEADERS = frozenset(
    {"accept", "accept-encoding", "content-encoding", "content-type", "git-protocol", "user-agent"}
)
# Response bodies are passed on without decoding, so Content-Encoding and Content-Length still describe the bytes
# the client receives. Location is dropped because following a redirect would take the client to the provider
# directly, past the relay.
_RESPONSE_HEADERS = frozenset(
    {"cache-control", "content-encoding", "content-length", "content-type", "expires", "pragma"}
)


class GitRelay:
    """Forwards git smart-HTTP requests to a provider with a token, streaming the body in both directions.

    Nothing buffers a request or a response, and neither body is parsed.

    Each request opens its own connection and closes it when the response ends, so nothing one organization's
    request leaves behind, such as a cookie or an unread response, can reach another's. A clone's three requests
    each pay a TCP and TLS handshake. Over the prototype's slow local link that added hundreds of milliseconds per
    clone; from Sentry's servers it should be small.

    Only https remotes are accepted, because every request carries a provider token.

    ``adapter`` builds the adapter that sends each upstream request. Pass one that refuses internal addresses: a
    GitHub Enterprise or self-managed GitLab host is whatever the customer configured, and the relay connects
    wherever it resolves. Sentry's ``BlacklistAdapter`` does this for its API clients, and the class itself can be
    passed. An error the adapter raises for a refused host reaches the caller unchanged, unless it is a requests or
    urllib3 exception, which the relay replaces like any other so that it cannot carry the token. The adapter must
    not retry, because a retry cannot resend a body that has already been streamed. Without one, the relay uses a plain
    ``HTTPAdapter``, which checks nothing.

    The relay takes no settings from the environment. Through a proxy from ``HTTPS_PROXY``, the adapter would
    check the proxy's address rather than the provider's. ``ca_bundle`` names a CA file to trust instead of the
    system's, in place of ``REQUESTS_CA_BUNDLE``.
    """

    def __init__(
        self,
        *,
        adapter: Callable[[], HTTPAdapter] = HTTPAdapter,
        ca_bundle: str | None = None,
        timeout: tuple[float, float] = DEFAULT_TIMEOUT,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> None:
        self.adapter = adapter
        self.ca_bundle = ca_bundle
        self.timeout = timeout
        self.chunk_size = chunk_size

    def forward(
        self,
        remote: GitRemote,
        token: str,
        *,
        method: str,
        route: str,
        query: str,
        headers: Mapping[str, str],
        body: Readable | None,
    ) -> StreamResponse:
        """Send one request to ``remote`` and return the provider's response as it arrives.

        ``route`` is the path below ``<repo>.git/``. Check the request against a grant before calling this.
        """
        if not remote["url"].startswith("https://"):
            # The token would cross the network in the clear, e.g. to a self-managed instance served over http.
            raise GitRemoteNotHttps(detail=f"Refusing to send a token to {remote['url']}, which is not https.")
        # Callers check the request against a grant first; this only makes sure the token never goes to anything
        # but a git route.
        require_route(method, route, query)
        if not f"{remote['username']}:{token}".isascii():
            # requests would fail to encode the credentials with a UnicodeEncodeError that holds them. No provider
            # issues such a token, so the caller has a bug; the message leaves the token out.
            raise ValueError(f"The credentials for {remote['url']} contain characters that are not ASCII.")

        client_headers = CaseInsensitiveDict(headers)
        upstream_headers = CaseInsensitiveDict(
            {name: value for name, value in client_headers.items() if name.lower() in _REQUEST_HEADERS}
        )
        # Otherwise requests asks for gzip, deflate and more, and since the response is passed on undecoded the
        # client could get an encoding it cannot read.
        upstream_headers.setdefault("Accept-Encoding", "identity")

        # Of the git routes only the POSTs carry a body. A caller may pass its request stream for a GET too, and sent
        # on, it would go upstream as an empty chunked body.
        data = _request_body(body, _content_length(client_headers), self.chunk_size) if method == "POST" else None
        url = f"{remote['url']}/{route}?{query}" if query else f"{remote['url']}/{route}"
        session = self._session()
        upstream = None
        try:
            upstream = session.request(
                method,
                url,
                headers=upstream_headers,
                data=data,
                auth=HTTPBasicAuth(remote["username"], token),
                stream=True,
                # Following a redirect would send the request somewhere other than the repository's remote.
                allow_redirects=False,
                timeout=self.timeout,
            )
        # "from None" drops requests' exception, whose request holds the Authorization header.
        except requests.Timeout:
            raise ResourceGatewayTimeout(detail=f"{remote['url']} did not respond in time.") from None
        # SSLError is a kind of ConnectionError, so catch it before that one, or a bad certificate would be
        # reported as an unreachable host.
        except requests.exceptions.SSLError:
            host = urlsplit(remote["url"]).netloc
            raise ResourceCertificateInvalid(detail=f"The TLS certificate of {host} could not be verified.") from None
        except requests.ConnectionError:
            raise ResourceServiceUnavailable(detail=f"Could not connect to {remote['url']}.") from None
        # HTTPAdapter re-raises the urllib3 errors it does not translate, which are not RequestExceptions.
        except (requests.RequestException, urllib3.exceptions.HTTPError):
            # Any other failure of the request, e.g. a malformed response. A requests exception, like those above,
            # holds the request.
            raise ResourceBadGateway(detail=f"The request to {remote['url']} failed.") from None
        finally:
            # Once a response arrives, _stream closes the session when the response ends.
            if upstream is None:
                session.close()

        return StreamResponse(
            status_code=upstream.status_code,
            headers={name: value for name, value in upstream.headers.items() if name.lower() in _RESPONSE_HEADERS},
            content=_UpstreamStream(session, upstream, self.chunk_size),
        )

    def _session(self) -> requests.Session:
        session = _NoRedirectSession()
        # Without this, requests reads proxies, CA files and .netrc credentials from the environment.
        session.trust_env = False
        session.verify = self.ca_bundle or True
        session.mount("https://", self.adapter())
        return session


class _NoRedirectSession(requests.Session):
    """A session that never looks for a redirect.

    Even with redirects off, requests prepares the next request of a redirect response for ``Response.next``, and
    reads the whole response body into memory first, however large the provider makes it.
    """

    def get_redirect_target(self, resp: requests.Response) -> None:
        return None


class _UpstreamStream:
    """The provider's response body, which closes the response and its session when it ends or is closed.

    A generator holding them in a ``with`` would not do: closing a generator that never started does not run its
    body, so a response nobody read, such as a 401 that GitServer replaces, would keep its connection open.
    """

    def __init__(self, session: requests.Session, upstream: requests.Response, chunk_size: int) -> None:
        self.session = session
        self.upstream = upstream
        self.chunks = upstream.raw.stream(chunk_size, decode_content=False)

    def __iter__(self) -> "_UpstreamStream":
        return self

    def __next__(self) -> bytes:
        try:
            while not (chunk := next(self.chunks)):
                pass
        except BaseException:
            # The end of the body, or the provider's stream breaking.
            self.close()
            raise
        return chunk

    def close(self) -> None:
        # Closing the response first drops its connection if the body was not read to the end; closing the session
        # then closes a connection the finished response returned to it.
        self.upstream.close()
        self.session.close()


def _content_length(headers: CaseInsensitiveDict[str]) -> int | None:
    raw = headers.get("Content-Length")
    if raw is None:
        return None
    # isdigit alone also accepts digits such as "²", which int() refuses, and "٣", which it reads as 3.
    if not (raw.isascii() and raw.isdigit()):
        raise RpcMalformedRequestHeaders(detail="Could not parse Content-Length header")
    return int(raw)


def _request_body(
    body: Readable | None, content_length: int | None, chunk_size: int
) -> "Iterator[bytes] | _SizedBody | None":
    if body is None or content_length == 0:
        return None
    if content_length is None:
        # requests sends a generator with chunked transfer encoding, the way git sends a large push.
        return _read_until_eof(body, chunk_size)
    return _SizedBody(body, content_length, chunk_size)


def _read_until_eof(body: Readable, chunk_size: int) -> Iterator[bytes]:
    while chunk := _read(body, chunk_size):
        yield chunk


def _read(body: Readable, size: int) -> bytes:
    try:
        return body.read(size)
    except OSError:
        # urllib3 ignores ECONNRESET and EPIPE while it sends a body, taking them for the provider closing the
        # connection, and would then wait out the read timeout for a response to the cut-off request. Raised as
        # anything other than an OSError, the client's reset ends the request at once.
        raise ClientClosedRequest(detail="The client's connection failed while it sent its body.") from None


class _SizedBody:
    """A body of known length, read as it is sent.

    requests sends Content-Length for anything with a ``__len__`` and streams anything iterable, so this passes
    the client's length on without reading the body first. Reading stops at the declared length.
    """

    def __init__(self, body: Readable, length: int, chunk_size: int) -> None:
        self.body = body
        self.length = length
        self.chunk_size = chunk_size

    def __len__(self) -> int:
        return self.length

    def __iter__(self) -> Iterator[bytes]:
        remaining = self.length
        while remaining > 0:
            chunk = _read(self.body, min(self.chunk_size, remaining))
            if not chunk:
                raise ClientClosedRequest(detail="The client closed the request before sending its whole body.")
            remaining -= len(chunk)
            yield chunk
