import pytest

from scm.errors import GitRouteNotAllowed
from scm.git.operations import check_request, route_operation
from scm.git.types import Grant


def make_grant(**overrides) -> Grant:
    fields = {"organization_id": 1, "operation": "fetch", "repositories": (5,), "expires_at": 0}
    fields.update(overrides)
    return Grant(**fields)  # type: ignore[arg-type]


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


class TestCheckRequest:
    def test_fetch_grant_reaches_fetch_routes(self):
        grant = make_grant()
        assert check_request(grant, 5, "GET", "info/refs", "service=git-upload-pack") == "fetch"
        assert check_request(grant, 5, "POST", "git-upload-pack", "") == "fetch"

    def test_push_grant_reaches_push_routes(self):
        grant = make_grant(operation="push")
        assert check_request(grant, 5, "GET", "info/refs", "service=git-receive-pack") == "push"
        assert check_request(grant, 5, "POST", "git-receive-pack", "") == "push"

    @pytest.mark.parametrize(
        "method,route,query",
        [("GET", "info/refs", "service=git-receive-pack"), ("POST", "git-receive-pack", "")],
    )
    def test_fetch_grant_cannot_push(self, method, route, query):
        with pytest.raises(GitRouteNotAllowed, match="allows fetch, not push"):
            check_request(make_grant(), 5, method, route, query)

    @pytest.mark.parametrize(
        "method,route,query",
        [("GET", "info/refs", "service=git-upload-pack"), ("POST", "git-upload-pack", "")],
    )
    def test_push_grant_cannot_fetch(self, method, route, query):
        with pytest.raises(GitRouteNotAllowed, match="allows push, not fetch"):
            check_request(make_grant(operation="push"), 5, method, route, query)

    def test_unknown_route(self):
        with pytest.raises(GitRouteNotAllowed, match="not a git fetch or push route"):
            check_request(make_grant(), 5, "POST", "info/lfs/objects/batch", "")

    def test_repository_not_in_grant(self):
        with pytest.raises(GitRouteNotAllowed, match="does not cover this repository"):
            check_request(make_grant(), 6, "POST", "git-upload-pack", "")

    def test_repository_named_another_way_is_not_in_grant(self):
        # The grant and the request name repositories the same way; matching a database id to a provider id
        # would mean resolving every repository the grant lists.
        grant = make_grant(repositories=(("github", "123"),))
        with pytest.raises(GitRouteNotAllowed):
            check_request(grant, 5, "POST", "git-upload-pack", "")
        assert check_request(grant, ("github", "123"), "POST", "git-upload-pack", "") == "fetch"

    def test_any_listed_repository(self):
        grant = make_grant(repositories=(5, 6, ("gitlab", "gitlab.com:7")))
        assert check_request(grant, 6, "POST", "git-upload-pack", "") == "fetch"
        assert check_request(grant, ("gitlab", "gitlab.com:7"), "POST", "git-upload-pack", "") == "fetch"
