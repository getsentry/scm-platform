import time
from collections.abc import Callable
from typing import Any, Protocol

import msgspec
import requests
from requests.exceptions import ChunkedEncodingError
from requests.exceptions import ConnectionError as RequestsConnectionError
from urllib3.exceptions import ProtocolError

from scm.errors import RpcErrorsCouldNotBeDeserialized, RpcInvalidGrant, SCMCodedError, TruncatedResponse
from scm.providers.github.provider import GitHubProvider
from scm.providers.gitlab.provider import GitLabProvider
from scm.rpc.helpers import deserialize_repository, sign_get, sign_post
from scm.rpc.types import ActionAttributes, ActionRequest, ErrorResponse
from scm.types import ApiClient, CredentialsSet, Provider, Repository, RepositoryId

SCM_API_URL = "{base_url}/api/0/internal/scm-rpc/"

# The proxy streams the upstream body after the status line is already on the wire, so a
# mid-stream disconnect arrives as a transport-level abort rather than an HTTP error status.
_RETRIABLE_TRANSPORT_ERRORS: tuple[type[Exception], ...] = (
    ChunkedEncodingError,
    RequestsConnectionError,
    ProtocolError,
)
# Only methods without side effects can be safely re-sent after a partial response.
_IDEMPOTENT_METHODS = frozenset({"GET", "HEAD"})
_DEFAULT_MAX_TRANSPORT_RETRIES = 2
_TRANSPORT_RETRY_BACKOFF_SECONDS = 0.25


class Session(Protocol):
    def get(self, url: str, headers: dict[str, str]) -> requests.Response: ...
    def post(self, url: str, data: bytes, headers: dict[str, str]) -> requests.Response: ...


class RequestsSession:
    def get(self, url: str, headers: dict[str, str]) -> requests.Response:
        return requests.get(url, headers=headers)

    def post(self, url: str, data: bytes, headers: dict[str, str]) -> requests.Response:
        return requests.post(url, data=data, headers=headers, allow_redirects=False)


class NoOpRateLimiter:
    """
    Provider instances will attempt to enforce rate-limits. We provide a no-op class which always succeeds. Rate-limits
    are managed server-side. Client's are not required, and are encouraged not to, enforce their own rate-limits.
    """

    def is_rate_limited(self, referrer: str) -> bool:
        return False

    def update_rate_limit_meta(self, capacity: int, consumed: int, next_window_start: int) -> None:
        return None


def fetch_repository(
    url: str,
    signing_secret: str,
    organization_id: int,
    repository_id: RepositoryId,
    session: Callable[[], Session] = lambda: RequestsSession(),
) -> Repository:
    """Fetch repositorty metadata."""
    response = session().get(
        url,
        headers={
            "Authorization": f"rpcsignature {sign_get(signing_secret, organization_id, repository_id)}",
            "X-Organization-Id": str(organization_id),
            "X-Repository-Id": msgspec.json.encode(repository_id).decode("utf-8"),
        },
    )

    if response.status_code == 200:
        return deserialize_repository(response.content)

    try:
        resp = msgspec.json.decode(response.content, type=ErrorResponse)
    except msgspec.DecodeError as e:
        raise RpcErrorsCouldNotBeDeserialized() from e

    exceptions = [SCMCodedError.from_code(error.code, detail=error.detail) for error in resp.errors]

    if len(exceptions) == 1:
        raise exceptions[0]
    else:
        raise ExceptionGroup("Several errors occurred while processing the request.", exceptions)


def fetch_provider(client: ApiClient, organization_id: int, repository: Repository) -> Provider | None:
    """
    Return a provider instance.

    The RPC client's version of the provider swaps direct contact with the service-provider for a proxy API client which
    makes requests to SCM Platform's RPC server. The RPC server will initialize its own SourceCodeManager and process
    the request.
    """
    if repository["provider_name"] == "github":
        return GitHubProvider(client, organization_id, repository, rate_limiter=NoOpRateLimiter())
    elif repository["provider_name"] == "github_enterprise":
        web_base_url = repository["web_base_url"]
        if not web_base_url:
            raise RpcInvalidGrant(
                detail="web_base_url is required for github_enterprise repositories",
            )

        return GitHubProvider(
            client,
            organization_id,
            repository,
            rate_limiter=NoOpRateLimiter(),
            web_base_url=web_base_url,
        )
    elif repository["provider_name"] == "gitlab":
        return GitLabProvider(client, organization_id, repository)
    else:
        return None


class RpcApiClient(ApiClient):
    """
    RPC API Client.

    The RPC API client can be injected into any provider class. It redirects requests to a Sentry-hosted proxy URL which
    makes requests on behalf of the requesting service. It streams the raw API response data from the service-provider
    to the connected client enabling low-latency request handling.

    Sentry will not parse the response into the standardized format. It is on the consuming client to parse. Because the
    API client slots into the pre-defined providers these providers can handle the parsing in a deterministic way
    regardless of request origin (i.e. within Sentry or without).

    Access controls are handling on the Sentry-side with required scoping metadata specified in the request's headers.
    """

    def __init__(
        self,
        full_url: str,
        signing_secret: str,
        organization_id: int,
        referrer: str,
        repository_id: RepositoryId,
        session: Callable[[], Session] = lambda: RequestsSession(),
        max_transport_retries: int = _DEFAULT_MAX_TRANSPORT_RETRIES,
    ) -> None:
        self.full_url = full_url
        self.signing_secret = signing_secret
        self.organization_id = organization_id
        self.referrer = referrer
        self.repository_id = repository_id
        self.session = session()
        self.max_transport_retries = max_transport_retries

    def request(
        self,
        method: str,
        path: str,
        headers: dict[str, str] | None = None,
        data: dict[str, Any] | None = None,
        params: dict[str, str] | None = None,
        allow_redirects: bool | None = None,
        stream: bool = True,
        raw_response: bool = True,
        credentials_set: CredentialsSet = "installation",
        timeout: float | tuple[float, float] | None = None,
    ) -> requests.Response:
        body = msgspec.json.encode(
            ActionRequest(
                type="action",
                data=ActionAttributes(
                    method=method,
                    path=path,
                    headers=headers,
                    data=data,
                    params=params,
                    allow_redirects=allow_redirects,
                    stream=stream,
                    timeout=timeout,
                ),
            )
        )

        request_headers = {
            "Authorization": f"rpcsignature {sign_post(self.signing_secret, body)}",
            "Content-Type": "application/json",
            "X-Organization-Id": str(self.organization_id),
            "X-Referrer": self.referrer,
            "X-Repository-Id": msgspec.json.encode(self.repository_id).decode("utf-8"),
            "X-Credentials-Set": credentials_set,
        }

        # The non-streaming session reads the body during ``post``, so a body that is cut off
        # mid-stream raises here. Retry idempotent reads with backoff; surface a typed, retriable
        # ``TruncatedResponse`` once the budget is exhausted (or immediately for unsafe methods)
        # so callers see what happened instead of an opaque ``UnhandledException``.
        attempt = 0
        while True:
            try:
                return self.session.post(url=self.full_url, data=body, headers=request_headers)
            except _RETRIABLE_TRANSPORT_ERRORS as exc:
                if method.upper() in _IDEMPOTENT_METHODS and attempt < self.max_transport_retries:
                    time.sleep(_TRANSPORT_RETRY_BACKOFF_SECONDS * (2**attempt))
                    attempt += 1
                    continue
                raise TruncatedResponse(detail=f"{type(exc).__name__}: {exc}") from exc
