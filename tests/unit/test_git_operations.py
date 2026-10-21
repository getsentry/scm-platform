import pytest

from scm.errors import GitOperationNotGranted, GitRepositoryNotGranted, GitRouteNotSupported
from scm.git.operations import check_request, require_route, route_operation
from tests.unit.git_builders import make_grant

FETCH_ROUTES = [("GET", "info/refs", "service=git-upload-pack"), ("POST", "git-upload-pack", "")]
PUSH_ROUTES = [("GET", "info/refs", "service=git-receive-pack"), ("POST", "git-receive-pack", "")]


class TestRouteOperation:
    @pytest.mark.parametrize(
        "method,route,query,operation",
        [
            ("GET", "info/refs", "service=git-upload-pack", "fetch"),
            ("POST", "git-upload-pack", "", "fetch"),
            ("GET", "info/refs", "service=git-receive-pack", "push"),
            ("POST", "git-receive-pack", "", "push"),
        ],
    )
    def test_git_routes(self, method, route, query, operation):
        assert route_operation(method, route, query) == operation

    @pytest.mark.parametrize(
        "method,route,query",
        [
            ("GET", "info/refs", ""),
            ("GET", "info/refs", "service=git-upload-pack&x=1"),
            ("GET", "info/refs", "service=git-upload-archive"),
            ("POST", "info/refs", "service=git-upload-pack"),
            ("GET", "git-upload-pack", ""),
            ("POST", "git-upload-pack", "service=git-upload-pack"),
            ("post", "git-upload-pack", ""),
            ("POST", "git-upload-archive", ""),
            ("POST", "info/lfs/objects/batch", ""),
            ("GET", "HEAD", ""),
            ("GET", "objects/info/packs", ""),
            ("GET", "../info/refs", "service=git-upload-pack"),
            ("POST", "/git-upload-pack", ""),
        ],
    )
    def test_other_routes(self, method, route, query):
        assert route_operation(method, route, query) is None


class TestRequireRoute:
    def test_returns_the_operation(self):
        assert require_route("POST", "git-receive-pack", "") == "push"

    def test_refuses_other_routes(self):
        with pytest.raises(GitRouteNotSupported, match="not a git fetch or push route"):
            require_route("POST", "info/lfs/objects/batch", "")


class TestCheckRequest:
    @pytest.mark.parametrize(
        "operation,route",
        [("fetch", route) for route in FETCH_ROUTES] + [("push", route) for route in PUSH_ROUTES],
    )
    def test_grant_reaches_its_operations_routes(self, operation, route):
        assert check_request(make_grant(operation=operation), 5, *route) == operation

    @pytest.mark.parametrize(
        "operation,route",
        [("fetch", route) for route in PUSH_ROUTES] + [("push", route) for route in FETCH_ROUTES],
    )
    def test_grant_cannot_reach_the_other_operations_routes(self, operation, route):
        other = "push" if operation == "fetch" else "fetch"
        with pytest.raises(GitOperationNotGranted, match=f"allows {operation}, not {other}"):
            check_request(make_grant(operation=operation), 5, *route)

    def test_unknown_route(self):
        with pytest.raises(GitRouteNotSupported, match="not a git fetch or push route"):
            check_request(make_grant(), 5, "POST", "info/lfs/objects/batch", "")

    def test_repository_not_in_grant(self):
        with pytest.raises(GitRepositoryNotGranted, match="does not cover this repository"):
            check_request(make_grant(), 6, "POST", "git-upload-pack", "")

    def test_repository_named_another_way_is_not_in_grant(self):
        # The grant and the request name repositories the same way; matching a database id to a provider id
        # would mean resolving every repository the grant lists.
        grant = make_grant(repositories=(("github", "123"),))
        with pytest.raises(GitRepositoryNotGranted):
            check_request(grant, 5, "POST", "git-upload-pack", "")
        assert check_request(grant, ("github", "123"), "POST", "git-upload-pack", "") == "fetch"

    def test_any_listed_repository(self):
        grant = make_grant(repositories=(5, 6, ("gitlab", "gitlab.com:7")))
        assert check_request(grant, 6, "POST", "git-upload-pack", "") == "fetch"
        assert check_request(grant, ("gitlab", "gitlab.com:7"), "POST", "git-upload-pack", "") == "fetch"
