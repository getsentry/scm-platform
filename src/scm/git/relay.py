from collections.abc import Callable, Iterator, Mapping
from typing import Protocol

import requests
import urllib3
from requests.adapters import HTTPAdapter
from requests.auth import HTTPBasicAuth
from requests.structures import CaseInsensitiveDict

from scm.errors import (
    ClientClosedRequest,
    GitOperationNotGranted,
    GitRemoteNotHttps,
    GitRepositoryNotGranted,
    GitRouteNotSupported,
    ResourceBadGateway,
    ResourceCertificateInvalid,
    ResourceGatewayTimeout,
    ResourceServiceUnavailable,
    RpcMalformedRequestHeaders,
    SCMCodedError,
)
from scm.git.types import GitOperation, Grant, Readable
from scm.rpc.types import StreamResponse
from scm.types import GitRemote, RepositoryId

# The most bytes the relay reads at once, from the client's body or from the provider's response, so also the
# most it holds per request in each direction. Smaller chunks cost more Python per byte of a multi-gigabyte pack;
# larger ones hold more memory per request. 64 KiB is what RpcServer streams with and what the prototype ran; no
# other size was measured.
DEFAULT_CHUNK_SIZE = 64 * 1024
# (connect, read) in seconds. The read timeout bounds a silence, not a transfer: it is the longest the provider may
# go without sending a byte, so a clone that streams for minutes never hits it. 300s leaves room for a provider that
# assembles a large pack before it sends anything. Both values come from the prototype.
DEFAULT_TIMEOUT = (10.0, 300.0)

# The git smart-HTTP routes and the operation each belongs to, keyed by (method, path below <repo>.git/, query
# string). git names its two services from the server's side: in a fetch the server uploads a pack to the client
# (git-upload-pack), and in a push it receives a pack from the client (git-receive-pack). Each service has a GET
# that lists the refs and a POST that does the transfer.
#
# The query string is compared whole instead of parsed. git sends exactly these strings, so a request with extra
# or repeated parameters is refused rather than passed on to a provider that might read them differently.
_ROUTES: dict[tuple[str, str, str], GitOperation] = {
    ("GET", "info/refs", "service=git-upload-pack"): "fetch",
    ("POST", "git-upload-pack", ""): "fetch",
    ("GET", "info/refs", "service=git-receive-pack"): "push",
    ("POST", "git-receive-pack", ""): "push",
}

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


def authorize_request(grant: Grant, repository_id: RepositoryId, method: str, route: str, query: str) -> GitOperation:
    """Return the git operation a request belongs to, refusing the request unless the grant allows it.

    A fetch grant reaches only upload-pack and a push grant only receive-pack. The repository must be listed in
    the grant in the same form the request uses: a database id does not match the same repository's
    ``(provider, external id)`` pair.
    """
    operation = _ROUTES.get((method, route, query))
    if operation is None:
        raise GitRouteNotSupported(detail=f"{method} {route} is not a git fetch or push route.")
    if operation != grant.operation:
        raise GitOperationNotGranted(detail=f"The git grant allows {grant.operation}, not {operation}.")
    if repository_id not in grant.repositories:
        raise GitRepositoryNotGranted(detail="The git grant does not cover this repository.")
    return operation


class ResponseBody(Iterator[bytes], Protocol):
    """The provider's response body. Closing it before it ends drops the upstream connection."""

    def close(self) -> None: ...


class RelayResponse(StreamResponse):
    """The provider's response, whose body GitServer closes when it no longer needs it."""

    content: ResponseBody


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
    passed. The adapter must not retry, because a retry cannot resend a body that has already been streamed. Without
    one, the relay uses a plain ``HTTPAdapter``, which checks nothing.

    The relay replaces a requests or urllib3 exception from the upstream request with a coded error, so that it
    cannot carry the token. ``map_error`` gets any other exception, such as the adapter's own for a refused host,
    and returns either a coded error, which the relay raises in its place without the original, or None to let the
    original reach the caller unchanged. GitServer answers a coded error like its other refusals. For an adapter
    that refuses a host with ``HostRefused``::

        GitRelay(adapter=..., map_error=lambda e: GitRemoteRefused() if isinstance(e, HostRefused) else None)

    The relay takes no settings from the environment. Through a proxy from ``HTTPS_PROXY``, the adapter would
    check the proxy's address rather than the provider's. ``ca_bundle`` names a CA file to trust instead of the
    system's, in place of ``REQUESTS_CA_BUNDLE``.
    """

    def __init__(
        self,
        *,
        adapter: Callable[[], HTTPAdapter] = HTTPAdapter,
        map_error: Callable[[Exception], SCMCodedError | None] | None = None,
        ca_bundle: str | None = None,
        timeout: tuple[float, float] = DEFAULT_TIMEOUT,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> None:
        self.adapter = adapter
        self.map_error = map_error
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
    ) -> RelayResponse:
        """Send one request to ``remote`` and return the provider's response as it arrives.

        ``route`` is the path below ``<repo>.git/``. The relay enforces what keeps the token safe: an https remote
        and one of the four git routes. It knows nothing of grants, so whether the request may run at all is the
        caller's to decide; GitServer calls ``authorize_request`` before it looks up the repository or a token.

        Errors carry no URL, because the client has no use for the provider's address.
        """
        if not remote["url"].startswith("https://"):
            # The token would cross the network in the clear, e.g. to a self-managed instance served over http.
            raise GitRemoteNotHttps()
        # The token never goes to anything but a git route, whatever the caller checked.
        if (method, route, query) not in _ROUTES:
            raise GitRouteNotSupported(detail=f"{method} {route} is not a git fetch or push route.")
        if not f"{remote['basic_auth_username']}:{token}".isascii():
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
        data: Iterator[bytes] | _SizedBody | None = None
        if method == "POST" and body is not None:
            raw_length = client_headers.get("Content-Length")
            if raw_length is None:
                # requests sends a generator with chunked transfer encoding, the way git sends a large push.
                data = _read_until_eof(body, self.chunk_size)
            else:
                try:
                    length = int(raw_length)
                except ValueError:
                    raise RpcMalformedRequestHeaders(detail="Could not parse Content-Length header") from None
                if length < 0:
                    raise RpcMalformedRequestHeaders(detail="Could not parse Content-Length header")
                if length > 0:
                    data = _SizedBody(body, length, self.chunk_size)

        url = f"{remote['url']}/{route}"
        if query:
            url += f"?{query}"
        session = self._session()
        upstream = None
        try:
            upstream = session.request(
                method,
                url,
                headers=upstream_headers,
                data=data,
                auth=HTTPBasicAuth(remote["basic_auth_username"], token),
                stream=True,
                # Following a redirect would send the request somewhere other than the repository's remote.
                allow_redirects=False,
                timeout=self.timeout,
            )
        # "from None" drops requests' exception, whose request holds the Authorization header.
        except requests.Timeout:
            raise ResourceGatewayTimeout() from None
        # SSLError is a kind of ConnectionError, so catch it before that one, or a bad certificate would be
        # reported as an unreachable host.
        except requests.exceptions.SSLError:
            raise ResourceCertificateInvalid() from None
        except requests.ConnectionError:
            raise ResourceServiceUnavailable() from None
        # HTTPAdapter re-raises the urllib3 errors it does not translate, which are not RequestExceptions.
        except (requests.RequestException, urllib3.exceptions.HTTPError):
            # Any other failure of the request, e.g. a malformed response. A requests exception, like those above,
            # holds the request.
            raise ResourceBadGateway() from None
        # The relay's own, such as the client's connection failing while its body is sent.
        except SCMCodedError:
            raise
        except Exception as error:
            mapped = self.map_error(error) if self.map_error is not None else None
            if mapped is None:
                raise
            # "from None" drops the original. Raised as the adapter connects, e.g. for a refused host, its traceback
            # runs through urllib3's frames, which hold the Authorization header.
            raise mapped from None
        finally:
            # Once a response arrives, _stream closes the session when the response ends.
            if upstream is None:
                session.close()

        return RelayResponse(
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
        except Exception:
            # The end of the body (StopIteration), or the provider's stream breaking.
            self.close()
            raise
        return chunk

    def close(self) -> None:
        # Closing the response first drops its connection if the body was not read to the end; closing the session
        # then closes a connection the finished response returned to it.
        self.upstream.close()
        self.session.close()


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
