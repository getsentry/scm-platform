from collections.abc import Callable, Iterator

from scm.errors import (
    ProviderNotFound,
    RateLimitExceeded,
    RepositoryInactive,
    RepositoryNotFound,
    RepositoryOrganizationMismatch,
    SCMError,
    UnhandledException,
)
from scm.types import PaginatedActionResult, PaginationParams, Provider, Referrer, Repository, RepositoryId


def iter_all_pages[T](
    action_fn: Callable[[PaginationParams], PaginatedActionResult[T]],
    per_page: int = 50,
    cursor: str = "1",
) -> Iterator[PaginatedActionResult[T]]:
    while True:
        result = action_fn({"per_page": per_page, "cursor": cursor})

        # If page is empty exit the loop.
        if isinstance(result["data"], list) and len(result["data"]) == 0:
            return None

        yield result

        # If the next-cursor value is empty exit the loop.
        next_cursor = result["meta"]["next_cursor"]
        if next_cursor is None:
            return None

        cursor = next_cursor


def initialize_provider(
    organization_id: int,
    repository_id: RepositoryId,
    *,
    fetch_repository: Callable[[int, RepositoryId], Repository | None],
    fetch_provider: Callable[[int, Repository], Provider | None],
) -> Provider:
    repository = fetch_repository(organization_id, repository_id)
    if not repository:
        raise RepositoryNotFound(organization_id=organization_id, repository_id=repository_id)
    if not repository["is_active"]:
        raise RepositoryInactive(organization_id=organization_id, repository_id=repository_id)
    if repository["organization_id"] != organization_id:
        raise RepositoryOrganizationMismatch(organization_id=organization_id, repository_id=repository_id)

    provider = fetch_provider(organization_id, repository)
    if provider is None:
        raise ProviderNotFound()

    return provider


def exec_provider_fn[T](
    provider: Provider,
    *,
    referrer: Referrer = "shared",
    provider_fn: Callable[[], T],
    record_count: Callable[[str, int, dict[str, str]], None],
) -> T:
    if provider.is_rate_limited(referrer):
        raise RateLimitExceeded()

    provider_name = provider.__class__.__name__

    try:
        result = provider_fn()
        record_count("sentry.scm.actions.success_by_provider", 1, {"provider": provider_name})
        record_count("sentry.scm.actions.success_by_referrer", 1, {"referrer": referrer})
        return result
    except SCMError:
        record_count("sentry.scm.actions.failed_by_provider", 1, {"provider": provider_name})
        record_count("sentry.scm.actions.failed_by_referrer", 1, {"referrer": referrer})
        raise
    except Exception as e:
        record_count("sentry.scm.actions.failed_by_provider", 1, {"provider": provider_name})
        record_count("sentry.scm.actions.failed_by_referrer", 1, {"referrer": referrer})
        # The cause is chained for server-side Sentry, but exception chains do not survive the RPC
        # boundary. Surface the type and message as `detail` so a client sees what actually failed
        # instead of an opaque "unhandled_exception".
        raise UnhandledException(detail=f"{type(e).__name__}: {e}") from e
