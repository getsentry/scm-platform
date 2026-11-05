import base64
import json
import pathlib
import unittest.mock
from typing import Any

import pytest
import requests

from scm.errors import PathIsDirectory, PathIsNotDirectory, ReadmeNotFound, ResourceNotFound
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
    GetDirectoryContentsProtocol,
    GetFileContentProtocol,
    GetFileUrlProtocol,
    GetGitRemoteProtocol,
    GetPullRequestTemplateProtocol,
    GetReadmeProtocol,
    GetRepositoryProtocol,
    Repository,
)

FIXTURES = pathlib.Path(__file__).parent / "fixtures" / "bitbucket"
REPO = "test-workspace/test-repo"
SRC = f"/2.0/repositories/{REPO}/src"

SUPPORTED_PROTOCOLS = [
    GetAppInstallationProtocol,
    GetGitRemoteProtocol,
    GetDirectoryContentsProtocol,
    GetFileContentProtocol,
    GetFileUrlProtocol,
    GetPullRequestTemplateProtocol,
    GetReadmeProtocol,
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


def test_get_git_remote(provider: BitbucketProvider) -> None:
    assert provider.get_git_remote() == {
        "url": f"https://bitbucket.org/{REPO}.git",
        "basic_auth_username": "x-token-auth",
    }


def requested_paths(client: unittest.mock.MagicMock) -> list[str]:
    return [call.kwargs["path"] for call in client.request.call_args_list]


class TestGetFileContent:
    def test_maps_recorded_file(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("file_readme")

        result = provider.get_file_content("README.md", "main")

        assert request_kwargs(client)["path"] == f"{SRC}/main/README.md"
        assert result["data"] == {
            "path": "README.md",
            # The blob id git reports for this README.
            "sha": "836111e2b449e2fae5a91e40bd277d491d4a965f",
            "content": base64.b64encode(b"# test-repo\n\nA test repository.\n").decode(),
            "encoding": "base64",
            "size": 32,
            "type": "file",
        }

    def test_resolves_ref_with_slash(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.side_effect = [recorded("commits_slash_branch"), recorded("file_readme")]

        provider.get_file_content("README.md", "feature/test-branch")

        assert requested_paths(client) == [
            f"/2.0/repositories/{REPO}/commits/feature/test-branch",
            f"{SRC}/1c0671c60997ab4bd70884bf9f3bd0c41d7043f1/README.md",
        ]

    def test_directory_raises(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("directory_docs")
        with pytest.raises(PathIsDirectory):
            provider.get_file_content("docs", "s2-file-types")

    def test_missing_file_raises(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("file_not_found")
        with pytest.raises(ResourceNotFound):
            provider.get_file_content("nope.md", "main")


class TestGetDirectoryContents:
    def test_maps_recorded_pages(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("directory_page_1")

        first = provider.get_directory_contents("", "s2-file-types", pagination={"per_page": 5})

        assert request_kwargs(client)["path"] == f"{SRC}/s2-file-types/"
        assert request_kwargs(client)["params"] == {"pagelen": "5"}
        assert [(e["path"], e["type"]) for e in first["data"]] == [
            ("docs", "directory"),
            ("src", "directory"),
            ("tests", "directory"),
            ("vendor", "directory"),
            (".gitmodules", "file"),
        ]
        assert first["meta"]["next_cursor"] == "rXtr"

        client.request.return_value = recorded("directory_page_2")
        second = provider.get_directory_contents("", "s2-file-types", pagination={"cursor": "rXtr", "per_page": 5})

        assert request_kwargs(client)["params"] == {"pagelen": "5", "page": "rXtr"}
        assert second["data"][-1] == {
            "path": "readme-link",
            "sha": "",
            "content": "",
            "encoding": "",
            "size": 9,
            "type": "symlink",
        }
        assert second["meta"]["next_cursor"] is None

    def test_maps_submodule(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("directory_submodule")

        result = provider.get_directory_contents("vendor/", "s2-file-types")

        assert request_kwargs(client)["path"] == f"{SRC}/s2-file-types/vendor/"
        assert [(e["path"], e["type"]) for e in result["data"]] == [("vendor/lib", "submodule")]

    def test_defaults_to_head(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("directory_main_root")
        provider.get_directory_contents("src")
        assert request_kwargs(client)["path"] == f"{SRC}/HEAD/src/"

    def test_file_raises(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("file_readme")
        with pytest.raises(PathIsNotDirectory):
            provider.get_directory_contents("README.md", "main")


class TestGetReadme:
    def test_finds_readme_in_root(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.side_effect = [recorded("directory_main_root"), recorded("file_readme")]

        result = provider.get_readme("main")

        assert requested_paths(client) == [f"{SRC}/main/", f"{SRC}/main/README.md"]
        assert result["data"]["path"] == "README.md"

    def test_quotes_ref_once(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.side_effect = [recorded("directory_main_root"), recorded("file_readme")]

        provider.get_readme("v1@rc#2")

        assert requested_paths(client) == [f"{SRC}/v1%40rc%232/", f"{SRC}/v1%40rc%232/README.md"]

    def test_no_readme(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("directory_docs")
        with pytest.raises(ReadmeNotFound):
            provider.get_readme("main")


class TestGetPullRequestTemplate:
    def test_finds_root_and_directory_templates(
        self, provider: BitbucketProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.side_effect = [
            recorded("directory_not_found"),
            recorded("directory_page_1"),
            recorded("directory_page_2"),
            recorded("file_pull_request_template"),
            recorded("directory_docs"),
            recorded("directory_docs_templates"),
            recorded("file_bug_template"),
        ]

        templates = list(provider.get_pull_request_template("s2-file-types", pagination={"per_page": 5}))

        assert [t["data"]["path"] for t in templates] == [
            "PULL_REQUEST_TEMPLATE.md",
            "docs/pull_request_template/bug.md",
        ]
        assert requested_paths(client)[0] == f"{SRC}/s2-file-types/.github/"
        assert base64.b64decode(templates[1]["data"]["content"]) == b"## Bug fix\n\nWhat broke?\n"


class TestGetFileUrl:
    @pytest.mark.parametrize(
        ("start", "end", "suffix"),
        [
            (None, None, ""),
            (5, None, "#lines-5"),
            (None, 9, "#lines-9"),
            (5, 9, "#lines-5:9"),
        ],
    )
    def test_line_anchors(self, provider: BitbucketProvider, start: int | None, end: int | None, suffix: str) -> None:
        url = provider.get_file_url("src/test_app/math.py", "abc123", start, end)
        assert url == f"https://bitbucket.org/{REPO}/src/abc123/src/test_app/math.py{suffix}"

    def test_quotes_path(self, provider: BitbucketProvider) -> None:
        assert provider.get_file_url("docs/read me.md", "abc123").endswith("/src/abc123/docs/read%20me.md")
