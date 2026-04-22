from collections.abc import Callable

from scm.facade import Facade
from scm.helpers import initialize_provider
from scm.rpc.client import SCM_API_URL, RequestsSession, RpcApiClient
from scm.rpc.client import fetch_provider as fetch_proxy_provider
from scm.rpc.client import fetch_repository as fetch_proxy_repository
from scm.types import Provider, Referrer, Repository, RepositoryId


class SourceCodeManager(Facade):
    @classmethod
    def make_client(
        cls,
        organization_id: int,
        repository_id: RepositoryId,
        *,
        referrer: Referrer = "shared",
        fetch_repository: Callable[[int, RepositoryId], Repository | None],
        fetch_provider: Callable[[int, Repository], Provider | None],
        record_count: Callable[[str, int, dict[str, str]], None],
    ) -> "SourceCodeManager":
        return cls(
            initialize_provider(
                organization_id,
                repository_id,
                fetch_repository=fetch_repository,
                fetch_provider=fetch_provider,
            ),
            referrer=referrer,
            record_count=record_count,
        )

    @classmethod
    def make_proxy_client(
        cls,
        organization_id: int,
        repository_id: RepositoryId,
        *,
        referrer: Referrer = "shared",
        base_url: str,
        signing_secret: str,
        record_count: Callable[[str, int, dict[str, str]], None] = lambda name, value, tags: None,
    ):
        full_url = SCM_API_URL.format(base_url=base_url)

        # A specialized RpcApiClient is initialized. It will proxy the service-provider requests through Sentry. This
        # forces clients to obey Sentry's strict access control requirements.
        client = RpcApiClient(
            full_url=full_url,
            signing_secret=signing_secret,
            organization_id=organization_id,
            referrer=referrer,
            repository_id=repository_id,
            session=RequestsSession,
        )

        return cls.make_client(
            organization_id,
            repository_id,
            referrer=referrer,
            fetch_repository=lambda oid, rid: fetch_proxy_repository(
                full_url, signing_secret, oid, rid, RequestsSession
            ),
            fetch_provider=lambda oid, repo: fetch_proxy_provider(client, oid, repo),
            record_count=record_count,
        )


__all__ = ("SourceCodeManager",)
