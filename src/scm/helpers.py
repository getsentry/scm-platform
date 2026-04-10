from collections.abc import Callable

from scm.errors import SCMCodedError, SCMError, SCMUnhandledException
from scm.types import Provider, Referrer


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
