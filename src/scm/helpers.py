from collections.abc import Callable, Iterator

from scm.errors import SCMCodedError, SCMError
from scm.types import PaginatedActionResult, PaginationParams, Provider, Referrer, Repository, RepositoryId


def iter_all_pages[T](
    action_fn: Callable[[PaginationParams], PaginatedActionResult[T]],
    per_page: int = 50,
    cursor: str = "1",
    max_pages: int = 10,
    raise_if_max_pages_exceeded: bool = False,
) -> Iterator[PaginatedActionResult[T]]:
    """
    Iterate through all the pages until the iterator is exhausted.

    The `max_pages` parameter defaults to 10. Fetching more than 10 pages serially is a code smell that should be
    caught. You can not disable this feature. You can raise the max_pages limit to a very high number but you can not
    disable it.

    When `raise_if_max_pages_exceeded` is True, hitting the `max_pages` cap raises `SCMError` instead of stopping
    iteration silently.
    """
    page = 0
    while True:
        if max_pages and max_pages <= page:
            if raise_if_max_pages_exceeded:
                raise SCMError(f"iter_all_pages exceeded max_pages={max_pages}")
            return None

        result = action_fn({"per_page": per_page, "cursor": cursor})

        # If page is empty exit the loop.
        if len(result["data"]) == 0:
            return None

        yield result

        # If the next-cursor value is empty exit the loop.
        next_cursor = result["meta"]["next_cursor"]
        if next_cursor is None:
            return None

        cursor = next_cursor
        page += 1


def initialize_provider(
    organization_id: int,
    repository_id: RepositoryId,
    *,
    fetch_repository: Callable[[int, RepositoryId], Repository | None],
    fetch_provider: Callable[[int, Repository], Provider | None],
) -> Provider:
    repository = fetch_repository(organization_id, repository_id)
    if not repository:
        raise SCMCodedError(organization_id, repository_id, code="repository_not_found")
    if not repository["is_active"]:
        raise SCMCodedError(repository, code="repository_inactive")
    if repository["organization_id"] != organization_id:
        raise SCMCodedError(repository, code="repository_organization_mismatch")

    provider = fetch_provider(organization_id, repository)
    if provider is None:
        raise SCMCodedError(code="provider_not_found")

    return provider


def exec_provider_fn[T](
    provider: Provider,
    *,
    referrer: Referrer = "shared",
    provider_fn: Callable[[], T],
    record_count: Callable[[str, int, dict[str, str]], None],
) -> T:
    if provider.is_rate_limited(referrer):
        raise SCMCodedError(code="rate_limit_exceeded")

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
        raise SCMCodedError(code="unhandled_exception") from e
