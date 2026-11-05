import json
import pathlib
import unittest.mock
from typing import Any

import pytest
import requests

from scm.errors import ResourceBadRequest, ResourceNotFound
from scm.providers.bitbucket.provider import (
    BitbucketProvider,
    map_app_installation,
    map_paginated_action,
    map_repository,
    next_cursor,
    parse_scopes,
)
from scm.types import (
    CreateBranchProtocol,
    DeleteBranchProtocol,
    GetAppInstallationProtocol,
    GetBranchProtocol,
    GetGitRefProtocol,
    GetRepositoryProtocol,
    Repository,
    UpdateBranchProtocol,
)

FIXTURES = pathlib.Path(__file__).parent / "fixtures" / "bitbucket"
REPO = "test-workspace/test-repo"

SUPPORTED_PROTOCOLS = [
    CreateBranchProtocol,
    DeleteBranchProtocol,
    GetAppInstallationProtocol,
    GetBranchProtocol,
    GetGitRefProtocol,
    GetRepositoryProtocol,
]


def recorded(name: str) -> requests.Response:
    """Build a response from a fixture recorded against Bitbucket Cloud."""
    fixture = json.loads((FIXTURES / f"{name}.json").read_text())
    response = requests.Response()
    response.status_code = fixture["status"]
    response.headers.update(fixture["headers"])
    body = fixture["body"]
    response._content = (json.dumps(body) if not isinstance(body, str) else body).encode()
    response.request = requests.Request("GET", "https://api.bitbucket.org/2.0/x").prepare()
    return response


@pytest.fixture
def client() -> unittest.mock.MagicMock:
    return unittest.mock.MagicMock(_name="client")


@pytest.fixture
def provider(client: unittest.mock.MagicMock) -> BitbucketProvider:
    return BitbucketProvider(
        client=client,
        organization_id=1,
        repository=Repository(
            id=1,
            integration_id=1,
            name=REPO,
            organization_id=1,
            is_active=True,
            external_id="{00000000-0000-0000-0000-00000000000a}",
            provider_name="bitbucket",
            web_base_url=None,
            installation_id=None,
        ),
    )


def request_kwargs(client: unittest.mock.MagicMock) -> dict[str, Any]:
    return client.request.call_args.kwargs


@pytest.mark.parametrize("protocol", SUPPORTED_PROTOCOLS, ids=lambda p: p.__name__)
def test_implements_protocol(provider: BitbucketProvider, protocol: type) -> None:
    assert isinstance(provider, protocol)


def test_cannot_update_branch(provider: BitbucketProvider) -> None:
    assert not isinstance(provider, UpdateBranchProtocol)


class TestRequests:
    def test_error_status_raises_coded_error(
        self, provider: BitbucketProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = recorded("repository_not_found")
        with pytest.raises(ResourceNotFound):
            provider.get_repository()

    def test_first_page_sends_no_page(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("branches_page_1")
        provider.get("/2.0/x", pagination={"cursor": "1", "per_page": 2})
        assert request_kwargs(client)["params"] == {"pagelen": "2"}

    def test_later_page_sends_cursor(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("branches_page_1")
        provider.get("/2.0/x", pagination={"cursor": "2", "per_page": 50})
        assert request_kwargs(client)["params"] == {"pagelen": "50", "page": "2"}

    def test_page_size_is_capped(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("branches_page_1")
        provider.get("/2.0/x", pagination={"per_page": 500})
        assert request_kwargs(client)["params"] == {"pagelen": "100"}


class TestPagination:
    def test_next_cursor_from_recorded_page(self) -> None:
        result = map_paginated_action(recorded("branches_page_1"), lambda raw: raw["name"])
        assert len(result["data"]) == 2
        assert result["meta"]["next_cursor"] == "2"

    def test_opaque_page_token(self) -> None:
        raw = {
            "next": "https://api.bitbucket.org/2.0/repositories/test-workspace/test-repo/src/abc/?page=eyJsYXN0IjoiYSJ9"
        }
        assert next_cursor(raw) == "eyJsYXN0IjoiYSJ9"

    def test_last_page(self) -> None:
        assert next_cursor({"values": []}) is None


class TestGetRepository:
    def test_maps_repository(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("repository")

        result = provider.get_repository()

        assert request_kwargs(client)["path"] == f"/2.0/repositories/{REPO}"
        assert result["type"] == "bitbucket"
        assert result["data"] == {
            "full_name": REPO,
            "default_branch": "main",
            "clone_url": f"https://bitbucket.org/{REPO}.git",
            "private": True,
            "size": 2,
            "description": None,
            "topics": [],
        }

    def test_empty_repository_has_no_default_branch(self) -> None:
        raw = json.loads((FIXTURES / "repository.json").read_text())["body"]
        raw["mainbranch"] = None

        assert map_repository(raw)["default_branch"] == ""


class TestGetAppInstallation:
    def test_reads_scopes_from_recorded_header(
        self, provider: BitbucketProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = recorded("repository")

        result = provider.get_app_installation()

        assert result["data"] == {
            "has_read_access": True,
            "has_write_access": True,
            "has_check_run_write_access": True,
        }

    @pytest.mark.parametrize(
        ("header", "read", "write"),
        [
            ("", False, False),
            ("account, repository, pullrequest", True, False),
            ("repository:write, pullrequest", True, False),
            ("repository:write, pullrequest:write", True, True),
            ("read:repository:bitbucket, read:pullrequest:bitbucket", True, False),
            ("write:repository:bitbucket, write:pullrequest:bitbucket", True, True),
        ],
    )
    def test_scope_mapping(self, header: str, read: bool, write: bool) -> None:
        installation = map_app_installation(parse_scopes(header))
        assert installation["has_read_access"] is read
        assert installation["has_write_access"] is write


MAIN_SHA = "a682892aa0f4502ef7a242f75d72d0b41eaa27b9"
FEATURE_SHA = "1c0671c60997ab4bd70884bf9f3bd0c41d7043f1"


class TestBranches:
    def test_get_branch(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("branch")

        result = provider.get_branch("feature/test-branch")

        assert request_kwargs(client)["path"] == f"/2.0/repositories/{REPO}/refs/branches/feature/test-branch"
        assert result["data"] == {"ref": "feature/test-branch", "sha": FEATURE_SHA}

    def test_get_branch_encodes_name(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("branch")
        provider.get_branch("feature/#12-50%-fix")
        assert request_kwargs(client)["path"] == f"/2.0/repositories/{REPO}/refs/branches/feature/%2312-50%25-fix"

    def test_get_missing_branch(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("branch_not_found")
        with pytest.raises(ResourceNotFound):
            provider.get_branch("s5-missing")

    def test_create_branch(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("branch_created")

        result = provider.create_branch("s5-create", MAIN_SHA)

        kwargs = request_kwargs(client)
        assert kwargs["method"] == "POST"
        assert kwargs["path"] == f"/2.0/repositories/{REPO}/refs/branches"
        assert kwargs["data"] == {"name": "s5-create", "target": {"hash": MAIN_SHA}}
        assert result["data"] == {"ref": "s5-create", "sha": MAIN_SHA}

    def test_create_existing_branch(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("branch_exists")
        with pytest.raises(ResourceBadRequest):
            provider.create_branch("s5-create", MAIN_SHA)

    def test_delete_branch(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("branch_deleted")

        provider.delete_branch("feature/#12")

        kwargs = request_kwargs(client)
        assert kwargs["method"] == "DELETE"
        assert kwargs["path"] == f"/2.0/repositories/{REPO}/refs/branches/feature/%2312"


class TestGetGitRef:
    def test_branch_ref(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("branch")

        result = provider.get_git_ref("heads/feature/test-branch")

        assert request_kwargs(client)["path"] == f"/2.0/repositories/{REPO}/refs/branches/feature/test-branch"
        assert result["data"] == {"ref": "refs/heads/feature/test-branch", "sha": FEATURE_SHA}

    def test_tag_ref(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("tag")

        result = provider.get_git_ref("tags/s5-tag")

        assert request_kwargs(client)["path"] == f"/2.0/repositories/{REPO}/refs/tags/s5-tag"
        assert result["data"] == {"ref": "refs/tags/s5-tag", "sha": FEATURE_SHA}

    @pytest.mark.parametrize("ref", ["main", "heads/", "pull/1/head", "refs/heads/main"])
    def test_unsupported_ref(self, provider: BitbucketProvider, client: unittest.mock.MagicMock, ref: str) -> None:
        with pytest.raises(ResourceNotFound):
            provider.get_git_ref(ref)
        client.request.assert_not_called()
