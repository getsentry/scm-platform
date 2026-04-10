from collections.abc import Callable

from scm.facade import Facade
from scm.helpers import initialize_provider
from scm.types import Provider, Referrer, Repository, RepositoryId


class SourceCodeManager(Facade):
    @classmethod
    def make_from_repository_id(
        cls,
        organization_id: int,
        repository_id: RepositoryId,
        *,
        referrer: Referrer = "shared",
        fetch_repository: Callable[[int, RepositoryId], Repository | None],
        fetch_provider: Callable[[int, Repository], Provider | None],
        record_count: Callable[[str, int, dict[str, str]], None],
    ) -> "SourceCodeManager":
        provider = initialize_provider(
            organization_id,
            repository_id,
            fetch_repository=fetch_repository,
            fetch_provider=fetch_provider,
        )
        return cls(provider, referrer=referrer, record_count=record_count)
