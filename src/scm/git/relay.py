from collections.abc import Iterator, Mapping
from http.cookiejar import DefaultCookiePolicy

import requests
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth
from requests.structures import CaseInsensitiveDict

from scm.errors import (
    ClientClosedRequest,
    GitRouteNotAllowed,
    ResourceGatewayTimeout,
    ResourceServiceUnavailable,
    RpcMalformedRequestHeaders,
)
from scm.git.operations import route_operation
from scm.git.types import GitRemote, Readable
from scm.rpc.types import StreamResponse

DEFAULT_CHUNK_SIZE = 64 * 1024
# A large clone streams its pack for minutes, but the provider sends something well within the read timeout.
DEFAULT_TIMEOUT = (10.0, 300.0)

# What git sends that the provider needs: protocol v2 is negotiated with Git-Protocol, and git gzips large fetch
# requests. Everything else stays behind, including the caller's own credentials and the grant.
_REQUEST_HEADERS = frozenset(
    {"accept", "accept-encoding", "content-encoding", "content-type", "git-protocol", "user-agent"}
)
# Bodies pass through still encoded, so Content-Encoding and Content-Length describe what the client receives.
# Location is left out because a redirect would send the client to the provider directly.
_RESPONSE_HEADERS = frozenset(
    {"cache-control", "content-encoding", "content-length", "content-type", "expires", "pragma"}
)


class GitRelay:
    """Forwards git smart-HTTP requests to a provider with a token, streaming the body in both directions.

    Nothing buffers a request or a response, and neither body is parsed. Build one per process and reuse it: it
    keeps a pool of keep-alive connections per provider host, because a clone makes three requests and a new
    connection for each cost 0.4-0.8s per clone in the prototype.
    """

    def __init__(
        self,
        *,
        max_hosts: int = 32,
        max_connections_per_host: int = 64,
        timeout: tuple[float, float] = DEFAULT_TIMEOUT,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> None:
        self.timeout = timeout
        self.chunk_size = chunk_size
        self.session = requests.Session()
        # The session is shared by every organization's requests, so it must not carry a provider's cookies from
        # one request to the next.
        self.session.cookies.set_policy(DefaultCookiePolicy(allowed_domains=[]))
        adapter = HTTPAdapter(pool_connections=max_hosts, pool_maxsize=max_connections_per_host, max_retries=0)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

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
        # The caller has checked the operation; this only keeps anything but a git route away from the token.
        if route_operation(method, route, query) is None:
            raise GitRouteNotAllowed(detail=f"{method} {route} is not a git fetch or push route.")

        client_headers = CaseInsensitiveDict(headers)
        upstream_headers = CaseInsensitiveDict(
            {name: value for name, value in client_headers.items() if name.lower() in _REQUEST_HEADERS}
        )
        # requests asks for gzip and more by default, which the client may not decode once the body is passed on.
        upstream_headers.setdefault("Accept-Encoding", "identity")

        url = f"{remote.url}/{route}?{query}" if query else f"{remote.url}/{route}"
        try:
            upstream = self.session.request(
                method,
                url,
                headers=upstream_headers,
                data=_request_body(body, _content_length(client_headers), self.chunk_size),
                # Passing auth also stops requests from replacing it with credentials from a .netrc file.
                auth=HTTPBasicAuth(remote.username, token),
                stream=True,
                # A redirect would carry the token to wherever the provider points.
                allow_redirects=False,
                timeout=self.timeout,
            )
        # "from None" drops requests' exception, whose request holds the Authorization header.
        except requests.Timeout:
            raise ResourceGatewayTimeout(detail=f"{remote.url} did not respond in time.") from None
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
        # Closing the response once it is read to the end returns its connection to the pool; closing it early,
        # because the client went away, drops the connection instead.
        with upstream:
            yield from filter(bool, upstream.raw.stream(self.chunk_size, decode_content=False))


def _content_length(headers: CaseInsensitiveDict[str]) -> int | None:
    raw = headers.get("Content-Length")
    if raw is None:
        return None
    if not raw.isdigit():
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
    while chunk := body.read(chunk_size):
        yield chunk


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
            chunk = self.body.read(min(self.chunk_size, remaining))
            if not chunk:
                raise ClientClosedRequest(detail="The client closed the request before sending its whole body.")
            remaining -= len(chunk)
            yield chunk
