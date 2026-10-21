from scm.errors import GitOperationNotGranted, GitRepositoryNotGranted, GitRouteNotSupported
from scm.git.types import GitOperation, Grant
from scm.types import RepositoryId

# The git smart-HTTP routes and the operation each belongs to, keyed by (method, path below <repo>.git/, query
# string). git names its two services from the server's side: in a fetch the server uploads a pack to the client
# (git-upload-pack), and in a push it receives a pack from the client (git-receive-pack). Each service has a GET
# that lists the refs and a POST that does the transfer.
#
# The query string is compared whole instead of parsed. git sends exactly these strings, so a request with extra
# or repeated parameters is refused rather than passed on to a provider that might read them differently.
_ROUTES: dict[tuple[str, str, str], GitOperation] = {
    ("GET", "info/refs", "service=git-upload-pack"): "fetch",
    ("POST", "git-upload-pack", ""): "fetch",
    ("GET", "info/refs", "service=git-receive-pack"): "push",
    ("POST", "git-receive-pack", ""): "push",
}


def route_operation(method: str, route: str, query: str) -> GitOperation | None:
    """Return the operation a git smart-HTTP request belongs to, or None if it is not one."""
    return _ROUTES.get((method, route, query))


def require_route(method: str, route: str, query: str) -> GitOperation:
    """Return the operation a git smart-HTTP request belongs to, refusing any other request."""
    operation = route_operation(method, route, query)
    if operation is None:
        raise GitRouteNotSupported(detail=f"{method} {route} is not a git fetch or push route.")
    return operation


def check_request(grant: Grant, repository_id: RepositoryId, method: str, route: str, query: str) -> GitOperation:
    """Refuse a request the grant does not reach, before anything is sent to the provider.

    A fetch grant reaches only upload-pack and a push grant only receive-pack. The repository must be listed in
    the grant in the same form the request uses: a database id does not match the same repository's
    ``(provider, external id)`` pair.
    """
    operation = require_route(method, route, query)
    if operation != grant.operation:
        raise GitOperationNotGranted(detail=f"The git grant allows {grant.operation}, not {operation}.")
    if repository_id not in grant.repositories:
        raise GitRepositoryNotGranted(detail="The git grant does not cover this repository.")
    return operation
