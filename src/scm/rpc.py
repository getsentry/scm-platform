import hashlib
import hmac
from collections.abc import Callable, Mapping
from typing import Any

import msgspec
import requests

from scm.facade import Facade
from scm.providers.github.provider import GitHubProvider
from scm.providers.gitlab.provider import GitLabProvider
from scm.types import ApiClient, Provider, Referrer, Repository, RepositoryId

SCM_API_URL = "{base_url}/api/0/internal/scm-rpc"


class SourceCodeManager(Facade):
    def __new__(
        cls,
        provider: Provider,
        *,
        referrer: Referrer = "shared",
        organization_id: int,
        repository_id: RepositoryId,
    ) -> "SourceCodeManager":
        return cls.init_scoped_facade(provider)

    def __init__(
        self,
        provider: Provider,
        *,
        referrer: Referrer = "shared",
        organization_id: int,
        repository_id: RepositoryId,
    ) -> None:
        self.provider = provider
        self.referrer = referrer
        self.organization_id = organization_id
        self.repository_id = repository_id

    @classmethod
    def make_from_repository_id(
        cls,
        organization_id: int,
        repository_id: RepositoryId,
        *,
        base_url: str,
        referrer: Referrer = "shared",
        signing_secret: str,
        fetch_repository: Callable[[str, str, int, RepositoryId], Repository],
    ):
        # Look up the name of the service-provider for a given credential set. This allows us to initialize the facade
        # with a set of methods that match the capabilities of the service provider.
        repository = fetch_repository(base_url, signing_secret, organization_id, repository_id)

        # A specialized RpcApiClient is initialized. It will proxy the service-provider requests through Sentry. This
        # forces clients to obey Sentry's strict access control requirements.
        client = RpcApiClient(
            base_url=base_url,
            signing_secret=signing_secret,
            organization_id=organization_id,
            referrer=referrer,
            repository_id=repository_id,
        )

        # Given the repository metadata we initialize a provider instance. This provider thinks its making requests to
        # the service-provider but in reality its sending requests to Sentry.
        if repository["provider_name"] == "github":
            # OOF! Rate-limits need to be handled Sentry-side. How to configure for both...
            provider = GitHubProvider(client, organization_id, repository)
        elif repository["provider_name"] == "gitlab":
            provider = GitLabProvider(client, organization_id, repository)
        else:
            raise NotImplementedError("Not working yet. Sorry!")

        return cls(
            provider,
            referrer=referrer,
            organization_id=organization_id,
            repository_id=repository_id,
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
        headers: Mapping[str, str] | None,
        data: Mapping[str, Any] | None,
        params: Mapping[str, str] | None,
        allow_redirects: bool | None,
        raw_response: bool,
    ) -> requests.Response:
        body = msgspec.json.encode(
            {
                "method": method,
                "path": path,
                "headers": headers,
                "data": data,
                "params": params,
                "allow_redirects": allow_redirects,
            }
        )

        response = self.session.post(
            SCM_API_URL.format(base_url=self.base_url),
            data=body,
            headers={
                "Authorization": f"rpcsignature {sign_message(self.signing_secret, body)}",
                "Content-Type": "application/json",
                "X-Organization-ID": str(self.organization_id),
                "X-Referrer": self.referrer,
                "X-Repository-ID": msgspec.json.encode(self.repository_id).decode("utf-8"),
            },
        )
        return response


def sign_message(signing_secret: str, message: bytes) -> str:
    return f"rpc0:{hmac.new(signing_secret.encode('utf-8'), message, hashlib.sha256).hexdigest()}"
