from collections.abc import Callable

from scm.errors import SCMCodedError, SCMError, SCMUnhandledException
from scm.types import Provider, Referrer, Repository, RepositoryId


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
        raise SCMUnhandledException from e
