import json
import pathlib
import unittest.mock
from typing import Any

import pytest
import requests

from scm.errors import ResourceNotFound
from scm.providers.bitbucket.provider import (
    BitbucketProvider,
    map_app_installation,
    map_paginated_action,
    map_repository,
    next_cursor,
    parse_scopes,
)
from scm.types import (
    GetAppInstallationProtocol,
    GetFullTreeProtocol,
    GetGitCommitProtocol,
    GetRepositoryProtocol,
    GetTreeProtocol,
    Repository,
)

FIXTURES = pathlib.Path(__file__).parent / "fixtures" / "bitbucket"
REPO = "test-workspace/test-repo"

COMMIT = "a682892aa0f4502ef7a242f75d72d0b41eaa27b9"

SUPPORTED_PROTOCOLS = [
    GetAppInstallationProtocol,
    GetFullTreeProtocol,
    GetGitCommitProtocol,
    GetRepositoryProtocol,
    GetTreeProtocol,
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


class TestGetGitCommit:
    def test_names_tree_by_commit(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("git_commit")

        result = provider.get_git_commit(COMMIT)

        assert request_kwargs(client)["path"] == f"/2.0/repositories/{REPO}/commit/{COMMIT}"
        assert result["data"] == {
            "sha": COMMIT,
            "tree": {"sha": COMMIT},
            "message": "Add pull request template\n",
        }


class TestGetTree:
    def test_recursive_page(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("src_recursive_page_1")

        result = provider.get_tree(COMMIT, pagination={"cursor": "1", "per_page": 5})

        assert request_kwargs(client)["path"] == f"/2.0/repositories/{REPO}/src/{COMMIT}/"
        assert request_kwargs(client)["params"]["max_depth"] == str(2**31 - 1)
        assert request_kwargs(client)["params"]["pagelen"] == "5"
        assert result["meta"]["next_cursor"] == "rXtr"
        assert result["data"]["truncated"] is True
        assert result["data"]["sha"] == COMMIT
        assert result["data"]["tree"][:3] == [
            {"path": "src", "mode": "040000", "type": "tree", "sha": f"{COMMIT}:src", "size": None},
            {"path": "tests", "mode": "040000", "type": "tree", "sha": f"{COMMIT}:tests", "size": None},
            {
                "path": "PULL_REQUEST_TEMPLATE.md",
                "mode": "100644",
                "type": "blob",
                "sha": f"{COMMIT}:PULL_REQUEST_TEMPLATE.md",
                "size": 25,
            },
        ]

    def test_not_recursive(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("src_special")

        provider.get_tree(COMMIT, recursive=False)

        assert "max_depth" not in request_kwargs(client)["params"]
        assert request_kwargs(client)["params"]["pagelen"] == "100"

    def test_subtree_paths_are_relative(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("src_subdirectory")

        result = provider.get_tree(f"{COMMIT}:src")

        assert request_kwargs(client)["path"] == f"/2.0/repositories/{REPO}/src/{COMMIT}/src/"
        assert result["data"]["truncated"] is False
        assert [(e["path"], e["sha"]) for e in result["data"]["tree"]] == [
            ("test_app", f"{COMMIT}:src/test_app"),
            ("test_app/__init__.py", f"{COMMIT}:src/test_app/__init__.py"),
            ("test_app/math.py", f"{COMMIT}:src/test_app/math.py"),
        ]

    def test_entry_modes(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("src_special")

        result = provider.get_tree("01a69636e47e2c25b7000087e2386c393dc46624:generated/special", recursive=False)

        assert [(e["path"], e["mode"], e["type"], e["size"]) for e in result["data"]["tree"]] == [
            ("sub", "160000", "commit", None),
            ("link", "120000", "blob", 6),
            ("run.sh", "100755", "blob", 18),
        ]

    def test_quotes_path(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("src_subdirectory")

        provider.get_tree(f"{COMMIT}:a dir")

        assert request_kwargs(client)["path"] == f"/2.0/repositories/{REPO}/src/{COMMIT}/a%20dir/"


class TestGetFullTree:
    def test_walks_every_page(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.side_effect = [recorded("src_recursive_page_1"), recorded("src_recursive_page_2")]

        result = provider.get_full_tree(COMMIT)

        pages = [call.kwargs["params"] for call in client.request.call_args_list]
        assert "page" not in pages[0]
        assert pages[1]["page"] == "rXtr"
        assert result["data"]["truncated"] is False
        assert [e["path"] for e in result["data"]["tree"]] == [
            "src",
            "tests",
            "PULL_REQUEST_TEMPLATE.md",
            "README.md",
            "src/test_app",
            "tests/test_math.py",
            "src/test_app/__init__.py",
            "src/test_app/math.py",
        ]
