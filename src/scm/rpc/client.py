from collections.abc import Callable
from typing import Any, Protocol

import msgspec
import requests

from scm.errors import SCMCodedError
from scm.providers.github.provider import GitHubProvider
from scm.providers.gitlab.provider import GitLabProvider
from scm.rpc.helpers import deserialize_repository, sign_get, sign_post
from scm.rpc.types import ActionAttributes, ActionRequest, ErrorResponse
from scm.types import ApiClient, CredentialsSet, Provider, Repository, RepositoryId

SCM_API_URL = "{base_url}/api/0/internal/scm-rpc/"


class Response(Protocol):
    @property
    def content(self) -> bytes: ...

    @property
    def status_code(self) -> int: ...

    def json(self, *args, **kwargs) -> Any: ...


class Session(Protocol):
    def get(self, url: str, headers: dict[str, str]) -> Response: ...
    def post(self, url: str, data: bytes, headers: dict[str, str]) -> Response: ...


class RequestsSession:
    def get(self, url: str, headers: dict[str, str]) -> Response:
        return requests.get(url, headers=headers)

    def post(self, url: str, data: bytes, headers: dict[str, str]) -> Response:
        return requests.post(url, data=data, headers=headers, allow_redirects=False)


class NoOpRateLimitProvider:
    """
    Provider instances will attempt to enforce rate-limits. We provide a no-op class which always succeeds. Rate-limits
    are managed server-side. Client's are not required, and are encouraged not to, enforce their own rate-limits.
    """

    def get_and_set_rate_limit(self, total_key: str, usage_key: str, expiration: int) -> tuple[int | None, int]:
        return None, 0

    def get_accounted_usage(self, keys: list[str]) -> int:
        return 0

    def set_key_values(self, kvs: dict[str, tuple[int, int | None]]) -> None:
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
        raise SCMCodedError(code="rpc_errors_could_not_be_deserialized") from e

    exceptions = [SCMCodedError(code=error.code) for error in resp.errors]

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
    if repository["provider_name"] in ("github", "github_enterprise"):
        return GitHubProvider(client, organization_id, repository, rate_limit_provider=NoOpRateLimitProvider())
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
    ) -> None:
        self.full_url = full_url
        self.signing_secret = signing_secret
        self.organization_id = organization_id
        self.referrer = referrer
        self.repository_id = repository_id
        self.session = session()

    def request(
        self,
        method: str,
        path: str,
        headers: dict[str, str] | None = None,
        data: dict[str, Any] | None = None,
        params: dict[str, str] | None = None,
        allow_redirects: bool | None = None,
        stream: bool | None = None,
        raw_response: bool = True,
        credentials_set: CredentialsSet = "installation",
    ) -> Response:
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
                ),
            )
        )

        response = self.session.post(
            url=self.full_url,
            data=body,
            headers={
                "Authorization": f"rpcsignature {sign_post(self.signing_secret, body)}",
                "Content-Type": "application/json",
                "X-Organization-Id": str(self.organization_id),
                "X-Referrer": self.referrer,
                "X-Repository-Id": msgspec.json.encode(self.repository_id).decode("utf-8"),
                "X-Credentials-Set": credentials_set,
            },
        )
        return response
