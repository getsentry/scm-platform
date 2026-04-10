import os
from collections.abc import Callable
from typing import Any

import msgspec
import requests

from scm.errors import SCMCodedError
from scm.manager import SourceCodeManager as ScmBase
from scm.providers.github.provider import GitHubProvider
from scm.providers.gitlab.provider import GitLabProvider
from scm.rpc.helpers import sign_get, sign_post
from scm.rpc.types import ErrorResponse, RepositoryResponse
from scm.types import ApiClient, Provider, Referrer, Repository, RepositoryId

SCM_API_URL = "{base_url}/api/0/internal/scm-rpc"


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
    base_url: str, signing_secret: str, organization_id: int, repository_id: RepositoryId
) -> Repository:
    """Fetch repositorty metadata."""
    url = SCM_API_URL.format(base_url=base_url)

    response = requests.get(
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


class SourceCodeManager(ScmBase):
    @classmethod
    def make_from_repository_id(  # type: ignore[override]
        cls,
        organization_id: int,
        repository_id: RepositoryId,
        *,
        referrer: Referrer = "shared",
        fetch_repository: Callable[[str, str, int, RepositoryId], Repository | None] = fetch_repository,
        fetch_provider: Callable[[ApiClient, int, Repository], Provider | None] = fetch_provider,
        fetch_base_url: Callable[[], str] = lambda: os.environ["SCM_RPC_BASE_URL"],
        fetch_signing_secret: Callable[[], str] = lambda: os.environ["SCM_RPC_SIGNING_SECRET"],
    ):
        base_url = fetch_base_url()
        signing_secret = fetch_signing_secret()

        # A specialized RpcApiClient is initialized. It will proxy the service-provider requests through Sentry. This
        # forces clients to obey Sentry's strict access control requirements.
        client = RpcApiClient(
            base_url=base_url,
            signing_secret=signing_secret,
            organization_id=organization_id,
            referrer=referrer,
            repository_id=repository_id,
        )

        return super().make_from_repository_id(
            organization_id,
            repository_id,
            referrer=referrer,
            fetch_repository=lambda oid, rid: fetch_repository(base_url, signing_secret, oid, rid),
            fetch_provider=lambda oid, repo: fetch_provider(client, oid, repo),
            record_count=lambda name, value, tags: None,
        )


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
        base_url: str,
        signing_secret: str,
        organization_id: int,
        referrer: str,
        repository_id: RepositoryId,
    ) -> None:
        self.base_url = base_url
        self.signing_secret = signing_secret
        self.organization_id = organization_id
        self.referrer = referrer
        self.repository_id = repository_id

        self.session = requests.Session()

    def _request(
        self,
        method: str,
        path: str,
        headers: dict[str, str] | None = None,
        data: dict[str, Any] | None = None,
        params: dict[str, str] | None = None,
        allow_redirects: bool | None = None,
        stream: bool | None = None,
        raw_response: bool = True,
    ) -> requests.Response:
        body = msgspec.json.encode(
            {
                "method": method,
                "path": path,
                "headers": headers,
                "data": data,
                "params": params,
                "allow_redirects": allow_redirects,
                "stream": True,
                "raw_response": True,
            }
        )

        response = self.session.post(
            SCM_API_URL.format(base_url=self.base_url),
            data=body,
            headers={
                "Authorization": f"rpcsignature {sign_post(self.signing_secret, body)}",
                "Content-Type": "application/json",
                "X-Organization-Id": str(self.organization_id),
                "X-Referrer": self.referrer,
                "X-Repository-Id": msgspec.json.encode(self.repository_id).decode("utf-8"),
            },
            stream=True,
        )
        return response


def deserialize_repository(content: bytes) -> Repository:
    try:
        repository = msgspec.json.decode(content, type=RepositoryResponse).data
    except msgspec.DecodeError as e:
        raise SCMCodedError(code="repository_could_not_be_deserialized") from e
    else:
        return {
            "external_id": repository.external_id,
            "integration_id": repository.integration_id,
            "is_active": repository.is_active,
            "name": repository.name,
            "organization_id": repository.organization_id,
            "provider_name": repository.provider_name,
        }
