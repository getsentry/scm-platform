from scm.errors import GitRouteNotAllowed
from scm.git.types import GitOperation, Grant
from scm.types import RepositoryId

# The git smart-HTTP routes, keyed by (method, path below <repo>.git/, query string). git sends these exact query
# strings, so an exact match leaves nothing for the provider to read differently than this table does.
_ROUTES: dict[tuple[str, str, str], GitOperation] = {
    ("GET", "info/refs", "service=git-upload-pack"): "fetch",
    ("POST", "git-upload-pack", ""): "fetch",
    ("GET", "info/refs", "service=git-receive-pack"): "push",
    ("POST", "git-receive-pack", ""): "push",
}


def route_operation(method: str, route: str, query: str) -> GitOperation | None:
    """Return the operation a git smart-HTTP request belongs to, or None if it is not one."""
    return _ROUTES.get((method, route, query))


def check_request(grant: Grant, repository_id: RepositoryId, method: str, route: str, query: str) -> GitOperation:
    """Refuse a request the grant does not reach, before anything is sent to the provider.

    A fetch grant reaches only upload-pack and a push grant only receive-pack, and the repository must be one the
    grant lists, named the same way.
    """
    operation = route_operation(method, route, query)
    if operation is None:
        raise GitRouteNotAllowed(detail=f"{method} {route} is not a git fetch or push route.")
    if operation != grant.operation:
        raise GitRouteNotAllowed(detail=f"The git grant allows {grant.operation}, not {operation}.")
    if repository_id not in grant.repositories:
        raise GitRouteNotAllowed(detail="The git grant does not cover this repository.")
    return operation
