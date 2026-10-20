from collections.abc import Iterator, Mapping
from http.cookiejar import DefaultCookiePolicy
from urllib.parse import urlsplit

import requests
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth
from requests.structures import CaseInsensitiveDict

from scm.errors import (
    ClientClosedRequest,
    ResourceCertificateInvalid,
    ResourceGatewayTimeout,
    ResourceServiceUnavailable,
    RpcMalformedRequestHeaders,
)
from scm.git.operations import require_route
from scm.git.types import GitRemote, Readable
from scm.rpc.types import StreamResponse

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

    Nothing buffers a request or a response, and neither body is parsed. Build one per process and reuse it: it
    keeps a pool of keep-alive connections per provider host, because a clone makes three requests and opening a
    new connection for each added hundreds of milliseconds per clone in the prototype.

    Only https remotes are accepted, because every request carries a provider token.

    ``adapter`` sends every upstream request and holds the connection pools. Pass one that refuses internal
    addresses: a GitHub Enterprise or self-managed GitLab host is whatever the customer configured, and the relay
    connects wherever it resolves. Sentry's ``BlacklistAdapter`` does this for its API clients. Errors the adapter
    raises for a refused host reach the caller unchanged. The adapter must not retry, because a retry cannot
    resend a body that has already been streamed. Without one, the relay pools connections with a plain adapter
    that checks nothing.
    """

    def __init__(
        self,
        *,
        adapter: HTTPAdapter | None = None,
        timeout: tuple[float, float] = DEFAULT_TIMEOUT,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> None:
        self.timeout = timeout
        self.chunk_size = chunk_size
        self.session = requests.Session()
        # Every organization's requests share this session, so a cookie a provider sets on one request must not
        # be sent with the next.
        self.session.cookies.set_policy(DefaultCookiePolicy(allowed_domains=[]))
        # The default keeps pools for up to 32 provider hosts and up to 64 idle connections per host. These are
        # guesses for one Sentry worker's concurrent clones, not measurements.
        self.session.mount("https://", adapter or HTTPAdapter(pool_connections=32, pool_maxsize=64, max_retries=0))

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
        if not remote.url.startswith("https://"):
            # git_remote only builds https URLs, so this is a caller's bug rather than a request to refuse.
            raise ValueError(f"Refusing to send a token to {remote.url}, which is not https.")
        # Callers check the request against a grant first; this only makes sure the token never goes to anything
        # but a git route.
        require_route(method, route, query)

        client_headers = CaseInsensitiveDict(headers)
        upstream_headers = CaseInsensitiveDict(
            {name: value for name, value in client_headers.items() if name.lower() in _REQUEST_HEADERS}
        )
        # Otherwise requests asks for gzip, deflate and more, and since the response is passed on undecoded the
        # client could get an encoding it cannot read.
        upstream_headers.setdefault("Accept-Encoding", "identity")

        url = f"{remote.url}/{route}?{query}" if query else f"{remote.url}/{route}"
        try:
            upstream = self.session.request(
                method,
                url,
                headers=upstream_headers,
                # Of the git routes only the POSTs carry a body. A caller may pass its request stream for a GET too,
                # and sent on, it would go upstream as an empty chunked body.
                data=_request_body(body, _content_length(client_headers), self.chunk_size)
                if method == "POST"
                else None,
                # Passing auth also stops requests from replacing it with credentials from a .netrc file.
                auth=HTTPBasicAuth(remote.username, token),
                stream=True,
                # Following a redirect would send the request somewhere other than the repository's remote.
                allow_redirects=False,
                timeout=self.timeout,
            )
        # "from None" drops requests' exception, whose request holds the Authorization header.
        except requests.Timeout:
            raise ResourceGatewayTimeout(detail=f"{remote.url} did not respond in time.") from None
        # SSLError is a kind of ConnectionError, so catch it before that one, or a bad certificate would be
        # reported as an unreachable host.
        except requests.exceptions.SSLError:
            host = urlsplit(remote.url).netloc
            raise ResourceCertificateInvalid(detail=f"The TLS certificate of {host} could not be verified.") from None
        except requests.ConnectionError:
            raise ResourceServiceUnavailable(detail=f"Could not connect to {remote.url}.") from None

        return StreamResponse(
            status_code=upstream.status_code,
            headers={name: value for name, value in upstream.headers.items() if name.lower() in _RESPONSE_HEADERS},
            content=self._stream(upstream),
        )

    def close(self) -> None:
        self.session.close()

    def _stream(self, upstream: requests.Response) -> Iterator[bytes]:
        # Once the response is read to the end its connection goes back to the pool. Closing it before then,
        # because the client went away, drops the connection instead, since it is mid-response.
        with upstream:
            yield from filter(bool, upstream.raw.stream(self.chunk_size, decode_content=False))


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
