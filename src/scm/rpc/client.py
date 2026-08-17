import time
from collections.abc import Callable, Collection
from typing import Any, NotRequired, Protocol, TypedDict

import msgspec
import requests
from requests.exceptions import ConnectionError as RequestsConnectionError

from scm.errors import (
    ResourceServiceUnavailable,
    RpcErrorsCouldNotBeDeserialized,
    RpcInvalidGrant,
    SCMCodedError,
)
from scm.providers.bitbucket.provider import BitbucketProvider
from scm.providers.github.provider import GitHubProvider
from scm.providers.gitlab.provider import GitLabProvider
from scm.rpc.helpers import deserialize_repository, sign_get, sign_post
from scm.rpc.types import ActionAttributes, ActionRequest, ErrorResponse
from scm.types import ApiClient, CredentialsSet, Provider, Repository, RepositoryId

SCM_API_URL = "{base_url}/api/0/internal/scm-rpc/"

# A transient blip between us and the proxy surfaces one of two ways: the connection drops before
# a response is framed (a transport-level ``ConnectionError``), or the gateway returns a bad status
# (commonly 503/504). Both are safe to re-send for reads that have no side effects. Which statuses
# count as retriable is left to the caller's ``RetryConfig``; the default is to retry nothing.
_RETRIABLE_TRANSPORT_ERRORS: tuple[type[Exception], ...] = (RequestsConnectionError,)
# Only methods without side effects can be safely re-sent: a write that errored at the transport
# layer may still have landed upstream, so re-sending it could double-apply it.
_IDEMPOTENT_METHODS = frozenset({"GET", "HEAD"})


class RetryConfig(TypedDict):
    """A complete transport-retry policy for the RPC client.

    The retry-budget keys (``max_retries``, ``backoff_seconds``, ``status_codes``) are required so no
    surprising latency budget is ever silently applied. Retries always remain restricted to
    idempotent methods (GET/HEAD) — that is a fixed safety invariant, because a write that failed
    mid-flight may already have landed upstream.

    - ``status_codes`` selects which gateway HTTP responses are re-sent (pass a list or set, e.g.
      ``{503, 504}``; an empty collection retries no statuses).
    - ``retry_connection_errors`` (optional, defaults to ``False``) enables catching a
      transport-level ``ConnectionError`` (no response framed) on idempotent reads. The read is
      re-sent up to ``max_retries`` times; whether or not any re-send happens (e.g. ``max_retries``
      is 0), the failure is surfaced as the typed, RPC-serializable ``ResourceServiceUnavailable``
      rather than an opaque ``ConnectionError`` that would not survive the proxy boundary. When
      disabled, the raw error propagates untouched.

    Retries are opt-in: pass ``retry=None`` (the default) and the library never re-sends a request
    nor intercepts a connection error.
    """

    max_retries: int
    backoff_seconds: float
    status_codes: Collection[int]
    retry_connection_errors: NotRequired[bool]


class Session(Protocol):
    def get(self, url: str, headers: dict[str, str]) -> requests.Response: ...
    def post(self, url: str, data: bytes, headers: dict[str, str]) -> requests.Response: ...


class RequestsSession:
    def get(self, url: str, headers: dict[str, str]) -> requests.Response:
        return requests.get(url, headers=headers)

    def post(self, url: str, data: bytes, headers: dict[str, str]) -> requests.Response:
        return requests.post(url, data=data, headers=headers, allow_redirects=False)


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
        return GitHubProvider(client, organization_id, repository)
    elif repository["provider_name"] == "github_enterprise":
        web_base_url = repository["web_base_url"]
        if not web_base_url:
            raise RpcInvalidGrant(
                detail="web_base_url is required for github_enterprise repositories",
            )

        return GitHubProvider(client, organization_id, repository, web_base_url=web_base_url)
    elif repository["provider_name"] == "gitlab":
        return GitLabProvider(client, organization_id, repository)
    elif repository["provider_name"] == "bitbucket":
        return BitbucketProvider(client, organization_id, repository)
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
        retry: RetryConfig | None = None,
        record_count: Callable[[str, int, dict[str, str]], None] = lambda name, value, tags: None,
    ) -> None:
        self.full_url = full_url
        self.signing_secret = signing_secret
        self.organization_id = organization_id
        self.referrer = referrer
        self.repository_id = repository_id
        self.session = session()
        self.retry = retry
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

        # Retry transient proxy failures for idempotent reads with exponential backoff. Two flavors,
        # each opted into via ``RetryConfig``: a transport-level ``ConnectionError`` (no response
        # framed) and a configured gateway status. Metrics make retries visible: a counter per retry,
        # one on recovery, one on exhaustion. With ``retry`` unset, this runs once and intercepts
        # nothing — a read is never re-sent and a connection error propagates raw.
        retry = self.retry
        max_retries = retry["max_retries"] if retry else 0
        status_codes = retry["status_codes"] if retry else frozenset()
        retry_connection_errors = retry.get("retry_connection_errors", False) if retry else False
        backoff_seconds = retry["backoff_seconds"] if retry else 0.0  # unused when max_retries == 0

        idempotent = method.upper() in _IDEMPOTENT_METHODS
        attempt = 0
        last_reason = ""  # trigger of the most recent retry; tags the recovery metric on success
        while True:
            try:
                response = self.session.post(url=self.full_url, data=body, headers=request_headers)
            except _RETRIABLE_TRANSPORT_ERRORS as exc:
                # Only intercept connection errors when the caller opted in, and only for reads: a
                # write that died at the transport layer may already have landed upstream, so it is
                # never safe to re-send. Anything else surfaces the raw error untouched.
                if not (retry_connection_errors and idempotent):
                    raise
                if attempt < max_retries:
                    last_reason = "connection_error"
                    self._retry(last_reason, method, attempt, backoff_seconds)
                    attempt += 1
                    continue
                # Only a re-send that ran out of attempts is "exhausted"; with retries off there was
                # nothing to exhaust, so stay quiet rather than mislabel a single failure. The reason
                # is the failure we are giving up on, which here is always the connection error.
                if attempt:
                    self._record_retry_outcome("exhausted", method, "connection_error")
                # Surface a typed, RPC-serializable error instead of an opaque ConnectionError that
                # would not survive the proxy boundary — independent of whether we retried.
                raise ResourceServiceUnavailable(detail=f"{type(exc).__name__}: {exc}") from exc

            if idempotent and response.status_code in status_codes:
                if attempt < max_retries:
                    last_reason = f"status_{response.status_code}"
                    self._retry(last_reason, method, attempt, backoff_seconds)
                    attempt += 1
                    continue
                # Reason is the status we are giving up on, taken from the final response rather than
                # ``last_reason`` so a mixed sequence (503 retried, then a final 504) reports the 504.
                if attempt:
                    self._record_retry_outcome("exhausted", method, f"status_{response.status_code}")
                # Hand the still-failing response back; the provider maps the status to a coded error.
                return response

            # Falling through means the response is no longer a retriable status (and was not a
            # connection error). If we had retried, the transient condition has cleared — that is a
            # "recovery" at the transport layer regardless of the final HTTP status: a 503 that clears
            # to a 200 or to a 404 both count, because the gateway stopped returning the retriable
            # status. ``reason`` is the trigger of the last retry; a mixed-reason sequence reports
            # only that final trigger.
            if attempt:
                self._record_retry_outcome("recovered", method, last_reason)
            return response

    def _retry(self, reason: str, method: str, attempt: int, backoff_seconds: float) -> None:
        """Record a retry attempt and back off before the next one."""
        self.record_count("sentry.scm.rpc.client.transport_retry", 1, {"method": method, "reason": reason})
        time.sleep(backoff_seconds * (2**attempt))

    def _record_retry_outcome(self, outcome: str, method: str, reason: str) -> None:
        """Emit a terminal retry counter (``recovered`` or ``exhausted``) with consistent tags.

        Centralizing the metric name and ``{method, reason}`` tag shape keeps the three retry
        counters from drifting apart as the loop evolves.
        """
        self.record_count(
            f"sentry.scm.rpc.client.transport_retry_{outcome}",
            1,
            {"method": method, "reason": reason},
        )
