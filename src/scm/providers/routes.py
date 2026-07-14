from dataclasses import dataclass
from typing import TypedDict


class FormattedRoute(TypedDict):
    """A concrete request path paired with the low-cardinality route template it came from.

    ``path`` is what is sent to the service-provider (e.g. ``/repos/getsentry/sentry/issues/5``).
    ``template`` is the stable, low-cardinality route it was built from (e.g. ``/repos/{repo}/issues/{issue_id}``),
    which — unlike the concrete path — is safe to use as a Sentry tag or metric dimension.
    """

    path: str
    template: str


@dataclass(frozen=True)
class Route:
    """A parameterized request path defined once and formatted at each call site.

    Calling the route interpolates ``params`` into the template and returns a :class:`FormattedRoute` that
    keeps the template alongside the concrete path. Because the template is *carried* rather than
    reverse-parsed from the final path, a route stays recoverable even when a parameter value contains
    slashes (e.g. a file path, branch name, or git ref) — the case that makes reverse-mapping ambiguous.
    """

    template: str

    def __call__(self, **params: object) -> FormattedRoute:
        return FormattedRoute(path=self.template.format(**params), template=self.template)
