import time
from collections.abc import Callable
from typing import Any, Protocol

import msgspec
import requests
from requests.exceptions import ConnectionError as RequestsConnectionError

from scm.errors import (
    ResourceServiceUnavailable,
    RpcErrorsCouldNotBeDeserialized,
    RpcInvalidGrant,
    SCMCodedError,
)
from scm.providers.github.provider import GitHubProvider
from scm.providers.gitlab.provider import GitLabProvider
from scm.rpc.helpers import deserialize_repository, sign_get, sign_post
from scm.rpc.types import ActionAttributes, ActionRequest, ErrorResponse
from scm.types import ApiClient, CredentialsSet, Provider, Repository, RepositoryId

SCM_API_URL = "{base_url}/api/0/internal/scm-rpc/"

# A transient blip between us and the proxy surfaces one of two ways: the connection drops before
# a response is framed (a transport-level ``ConnectionError``), or the gateway returns a 503/504.
# Both are safe to re-send for reads that have no side effects.
_RETRIABLE_TRANSPORT_ERRORS: tuple[type[Exception], ...] = (RequestsConnectionError,)
_RETRIABLE_STATUS_CODES = frozenset({503, 504})
# Only methods without side effects can be safely re-sent: a write that errored at the transport
# layer may still have landed upstream, so re-sending it could double-apply it.
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
        record_count: Callable[[str, int, dict[str, str]], None] = lambda name, value, tags: None,
    ) -> None:
        self.full_url = full_url
        self.signing_secret = signing_secret
        self.organization_id = organization_id
        self.referrer = referrer
        self.repository_id = repository_id
        self.session = session()
        self.max_transport_retries = max_transport_retries
        self.record_count = record_count

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

        # Retry transient proxy failures for idempotent reads with exponential backoff. Two flavors:
        # a transport-level ``ConnectionError`` (no response framed) and a 503/504 from the gateway.
        # Metrics make the retries visible: a counter per retry, one on recovery, one on exhaustion.
        idempotent = method.upper() in _IDEMPOTENT_METHODS
        attempt = 0
        while True:
            try:
                response = self.session.post(url=self.full_url, data=body, headers=request_headers)
            except _RETRIABLE_TRANSPORT_ERRORS as exc:
                # A write that died at the transport layer may still have landed upstream, so only
                # reads are safe to re-send; everything else surfaces the raw error untouched.
                if not idempotent:
                    raise
                if attempt < self.max_transport_retries:
                    self._retry("connection_error", method, attempt)
                    attempt += 1
                    continue
                self.record_count(
                    "sentry.scm.rpc.client.transport_retry_exhausted",
                    1,
                    {"method": method, "reason": "connection_error"},
                )
                # Classify the exhausted read as a typed, retriable error instead of an opaque
                # ConnectionError so callers can branch on ``exc.allow_retry``.
                raise ResourceServiceUnavailable(detail=f"{type(exc).__name__}: {exc}") from exc

            if idempotent and response.status_code in _RETRIABLE_STATUS_CODES:
                if attempt < self.max_transport_retries:
                    self._retry(f"status_{response.status_code}", method, attempt)
                    attempt += 1
                    continue
                self.record_count(
                    "sentry.scm.rpc.client.transport_retry_exhausted",
                    1,
                    {"method": method, "reason": f"status_{response.status_code}"},
                )
                # Hand the still-failing response back; the provider maps the status to a coded error.
                return response

            if attempt:
                self.record_count("sentry.scm.rpc.client.transport_retry_recovered", 1, {"method": method})
            return response

    def _retry(self, reason: str, method: str, attempt: int) -> None:
        """Record a retry and back off before the next attempt."""
        self.record_count("sentry.scm.rpc.client.transport_retry", 1, {"method": method, "reason": reason})
        time.sleep(_TRANSPORT_RETRY_BACKOFF_SECONDS * (2**attempt))
