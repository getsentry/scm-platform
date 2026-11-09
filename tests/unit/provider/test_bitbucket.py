import base64
import email
import email.policy
import json
import pathlib
import unittest.mock
from datetime import UTC, date, datetime
from typing import Any

import pytest
import requests

from scm.errors import (
    PathIsDirectory,
    PathIsNotDirectory,
    ReadmeNotFound,
    ResourceBadRequest,
    ResourceConflict,
    ResourceNotFound,
    StaleBranchHead,
)
from scm.providers.bitbucket.provider import (
    BitbucketProvider,
    decode_content,
    diff_text,
    map_app_installation,
    map_author,
    map_commit_author,
    map_paginated_action,
    map_pull_request_file,
    map_repository,
    map_tree_entry,
    next_cursor,
    parse_scopes,
    split_patches,
)
from scm.types import (
    ChmodCommitAction,
    CompareCommitsProtocol,
    CreateBranchProtocol,
    CreateCommitProtocol,
    DeleteBranchProtocol,
    DeleteCommitAction,
    GetAppInstallationProtocol,
    GetBranchProtocol,
    GetCommitChangesProtocol,
    GetCommitProtocol,
    GetCommitsByPathProtocol,
    GetCommitsProtocol,
    GetCommitsUrlProtocol,
    GetCommitUrlProtocol,
    GetDirectoryContentsProtocol,
    GetFileContentProtocol,
    GetFileUrlProtocol,
    GetFullTreeProtocol,
    GetGitCommitProtocol,
    GetGitRefProtocol,
    GetGitRemoteProtocol,
    GetPullRequestCommitsProtocol,
    GetPullRequestFilesProtocol,
    GetPullRequestProtocol,
    GetPullRequestsProtocol,
    GetPullRequestTemplateProtocol,
    GetPullRequestUrlProtocol,
    GetReadmeProtocol,
    GetRepositoryProtocol,
    GetTreeProtocol,
    MoveCommitAction,
    PullRequestState,
    Repository,
    UpdateBranchProtocol,
    WriteCommitAction,
)

FIXTURES = pathlib.Path(__file__).parent / "fixtures" / "bitbucket"
REPO = "test-workspace/test-repo"
SRC = f"/2.0/repositories/{REPO}/src"

SUPPORTED_PROTOCOLS = [
    CompareCommitsProtocol,
    GetAppInstallationProtocol,
    GetGitRemoteProtocol,
    GetDirectoryContentsProtocol,
    GetFileContentProtocol,
    GetFileUrlProtocol,
    GetPullRequestTemplateProtocol,
    GetReadmeProtocol,
    GetCommitChangesProtocol,
    GetCommitProtocol,
    GetCommitsByPathProtocol,
    GetCommitsProtocol,
    GetCommitsUrlProtocol,
    GetCommitUrlProtocol,
    GetRepositoryProtocol,
    GetPullRequestCommitsProtocol,
    GetPullRequestFilesProtocol,
    GetPullRequestProtocol,
    GetPullRequestsProtocol,
    GetPullRequestUrlProtocol,
    CreateCommitProtocol,
    CreateBranchProtocol,
    DeleteBranchProtocol,
    GetBranchProtocol,
    GetGitRefProtocol,
    GetFullTreeProtocol,
    GetGitCommitProtocol,
    GetTreeProtocol,
]
COMMIT_SHA = "e723f15089c22737bb51aee8db06e0eab91e4ba1"
HEAD_SHA = "e3ef83d1a831b7b26c838372f668b1627bbf9a1a"
BRANCH_SHA = "c495d0431cfae535ec9a05e4def54229d0eb2ad2"
TEST_AUTHOR = {"name": "Test User", "email": "test@example.com", "date": datetime(2026, 11, 5, 20, 16, 34, tzinfo=UTC)}


TEST_USER = {"id": "{00000000-0000-0000-0000-000000000001}", "username": "test-user"}

HEAD_HASH = "c495d0431cfae535ec9a05e4def54229d0eb2ad2"

BASE_HASH = "a682892aa0f4502ef7a242f75d72d0b41eaa27b9"
PARENT = "a682892aa0f4502ef7a242f75d72d0b41eaa27b9"

PNG = b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\xff\xfe"

PNG_BASE64 = base64.b64encode(PNG).decode()
MAIN_SHA = "a682892aa0f4502ef7a242f75d72d0b41eaa27b9"

FEATURE_SHA = "1c0671c60997ab4bd70884bf9f3bd0c41d7043f1"
COMMIT = "a682892aa0f4502ef7a242f75d72d0b41eaa27b9"


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

    @pytest.mark.parametrize("path", ["../../../other-repo/src/HEAD/README.md", "docs/./README.md"])
    def test_dot_segments_raise(self, provider: BitbucketProvider, client: unittest.mock.MagicMock, path: str) -> None:
        with pytest.raises(ResourceBadRequest):
            provider.get_file_content(path, "main")
        client.request.assert_not_called()

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


class TestCommitUrls:
    def test_commit_url(self, provider: BitbucketProvider) -> None:
        assert provider.get_commit_url("abc") == f"https://bitbucket.org/{REPO}/commits/abc"

    def test_file_history_url_ignores_dates(self, provider: BitbucketProvider) -> None:
        url = provider.get_commits_url(
            "abc", file_path="src/my file.py", since=date(2026, 1, 1), until=date(2026, 2, 1)
        )
        assert url == f"https://bitbucket.org/{REPO}/history-node/abc/src/my%20file.py"

    def test_commits_url_without_path(self, provider: BitbucketProvider) -> None:
        assert provider.get_commits_url("abc", since=date(2026, 1, 1)) == f"https://bitbucket.org/{REPO}/commits"


class TestGetCommit:
    def test_maps_commit_with_files(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.side_effect = [recorded("commit"), recorded("diffstat"), recorded("diff")]

        result = provider.get_commit(COMMIT_SHA)

        assert requested_paths(client) == [
            f"/2.0/repositories/{REPO}/commit/{COMMIT_SHA}",
            f"/2.0/repositories/{REPO}/diffstat/{COMMIT_SHA}",
            f"/2.0/repositories/{REPO}/diff/{COMMIT_SHA}",
        ]
        commit = result["data"]
        assert commit["id"] == COMMIT_SHA
        assert commit["message"] == "Rename, remove, add and change files\n"
        assert commit["author"] == TEST_AUTHOR
        assert (commit["additions"], commit["deletions"]) == (6, 5)
        assert [(f["filename"], f["status"]) for f in commit["files"] or []] == [
            ("docs_PULL_REQUEST_TEMPLATE.md", "renamed"),
            ("src/test_app/math.py", "modified"),
            ("src/test_app/ops.py", "added"),
            ("tests/test_math.py", "removed"),
        ]

    def test_partial_files_leave_totals_unset(
        self, provider: BitbucketProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.side_effect = [recorded("commit"), recorded("diffstat_page_1"), recorded("diff")]

        commit = provider.get_commit(COMMIT_SHA)["data"]

        assert len(commit["files"] or []) == 2
        assert (commit["additions"], commit["deletions"]) == (None, None)

    def test_missing_commit(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("commit_not_found")
        with pytest.raises(ResourceNotFound):
            provider.get_commit("deadbeef")


class TestGetCommitChanges:
    def test_maps_files_with_patches(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.side_effect = [recorded("diffstat"), recorded("diff")]

        result = provider.get_commit_changes(COMMIT_SHA)

        assert result["meta"]["next_cursor"] is None
        renamed, modified, added, removed = result["data"]
        assert renamed == {
            "filename": "docs_PULL_REQUEST_TEMPLATE.md",
            "status": "renamed",
            "patch": None,
            "additions": 0,
            "deletions": 0,
            "previous_filename": "PULL_REQUEST_TEMPLATE.md",
        }
        assert modified["patch"] == (
            "@@ -1,2 +1,6 @@\n def add(a, b):\n     return a + b\n+\n+\n+def multiply(a, b):\n+    return a * b\n"
        )
        assert (modified["additions"], modified["deletions"], modified["previous_filename"]) == (4, 0, None)
        assert added["patch"] == "@@ -0,0 +1,2 @@\n+def subtract(a, b):\n+    return a - b\n"
        assert (removed["filename"], removed["deletions"]) == ("tests/test_math.py", 5)
        assert (removed["patch"] or "").startswith("@@ -1,5 +0,0 @@\n-from test_app.math import add")

    def test_pages(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.side_effect = [recorded("diffstat_page_1"), recorded("diff")]

        result = provider.get_commit_changes(COMMIT_SHA, pagination={"cursor": "1", "per_page": 2})

        assert len(result["data"]) == 2
        assert result["meta"]["next_cursor"] == "2"
        assert client.request.call_args_list[0].kwargs["params"] == {"pagelen": "2"}

    def test_unknown_status(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        diffstat = recorded("diffstat")
        body = json.loads(diffstat.content)
        body["values"][1]["status"] = "merge conflict"
        diffstat._content = json.dumps(body).encode()
        client.request.side_effect = [diffstat, recorded("diff")]

        assert provider.get_commit_changes(COMMIT_SHA)["data"][1]["status"] == "unknown"


class TestGetCommits:
    def test_lists_commits_on_ref(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("commits_page_1")

        result = provider.get_commits("feature/x", pagination={"per_page": 2})

        assert request_kwargs(client)["path"] == f"/2.0/repositories/{REPO}/commits/feature/x"
        assert request_kwargs(client)["params"] == {"pagelen": "2"}
        assert [c["id"] for c in result["data"]] == [HEAD_SHA, COMMIT_SHA]
        assert result["data"][0]["message"] == "Update README\n"
        assert result["data"][0]["author"] == TEST_AUTHOR
        assert result["meta"]["next_cursor"] == "2"

    def test_defaults_to_main_branch(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("commits_page_1")
        provider.get_commits()
        assert request_kwargs(client)["path"] == f"/2.0/repositories/{REPO}/commits/HEAD"

    def test_date_filter_skips_pages_after_until(
        self, provider: BitbucketProvider, client: unittest.mock.MagicMock
    ) -> None:
        def page(date: str, next_url: str | None) -> requests.Response:
            response = requests.Response()
            response.status_code = 200
            commit = {"hash": date, "message": "m", "author": {"raw": "Test User <test@example.com>"}, "date": date}
            body: dict[str, Any] = {"values": [commit]}
            if next_url:
                body["next"] = next_url
            response._content = json.dumps(body).encode()
            return response

        client.request.side_effect = [
            page("2026-11-05T00:00:00+00:00", "https://api.bitbucket.org/2.0/commits?page=2"),
            page("2026-11-01T00:00:00+00:00", None),
        ]

        result = provider.get_commits(until=datetime(2026, 11, 2, tzinfo=UTC), pagination={"per_page": 1})

        assert [c["id"] for c in result["data"]] == ["2026-11-01T00:00:00+00:00"]
        assert client.request.call_args_list[1].kwargs["params"] == {"pagelen": "1", "page": "2"}

    def test_lists_commits_by_path(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("commits_by_path_page_1")

        result = provider.get_commits_by_path("src/test_app/math.py", ref="s4-commits", pagination={"per_page": 1})

        assert request_kwargs(client)["path"] == f"/2.0/repositories/{REPO}/commits/s4-commits"
        assert request_kwargs(client)["params"] == {"path": "src/test_app/math.py", "pagelen": "1"}
        assert [c["id"] for c in result["data"]] == [COMMIT_SHA]
        assert result["meta"]["next_cursor"] == "2"

    @pytest.mark.parametrize(
        ("kwargs", "ids", "next_cursor"),
        [
            ({"since": datetime(2026, 11, 3, tzinfo=UTC)}, [HEAD_SHA], "2"),
            ({"since": datetime(2026, 11, 1)}, [HEAD_SHA, COMMIT_SHA], "2"),
            ({"until": datetime(2026, 11, 3, tzinfo=UTC)}, [COMMIT_SHA], "2"),
            ({"since": datetime(2026, 11, 6, tzinfo=UTC)}, [], None),
        ],
    )
    def test_filters_by_date(
        self,
        provider: BitbucketProvider,
        client: unittest.mock.MagicMock,
        kwargs: dict[str, Any],
        ids: list[str],
        next_cursor: str | None,
    ) -> None:
        page = recorded("commits_page_1")
        body = json.loads(page.content)
        body["values"][1]["date"] = "2026-11-02T08:00:00+00:00"
        page._content = json.dumps(body).encode()
        client.request.return_value = page

        result = provider.get_commits_by_path("README.md", **kwargs)

        assert [c["id"] for c in result["data"]] == ids
        assert result["meta"]["next_cursor"] == next_cursor


class TestCompareCommits:
    def test_compares_against_merge_base(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.side_effect = [
            recorded("compare_commits"),
            recorded("compare_diff"),
            recorded("compare_diffstat"),
            recorded("compare_behind"),
        ]

        result = provider.compare_commits(BRANCH_SHA, HEAD_SHA)

        calls = client.request.call_args_list
        assert calls[0].kwargs["params"] == {"include": HEAD_SHA, "exclude": BRANCH_SHA, "pagelen": "100"}
        assert requested_paths(client)[1:3] == [
            f"/2.0/repositories/{REPO}/diff/{HEAD_SHA}..{BRANCH_SHA}",
            f"/2.0/repositories/{REPO}/diffstat/{HEAD_SHA}..{BRANCH_SHA}",
        ]
        assert calls[3].kwargs["params"] == {"include": BRANCH_SHA, "exclude": HEAD_SHA, "pagelen": "100"}
        comparison = result["data"]
        assert (comparison["ahead_by"], comparison["behind_by"]) == (2, 1)
        assert [c["id"] for c in comparison["commits"]] == [HEAD_SHA, COMMIT_SHA]
        assert [(f["filename"], f["status"]) for f in comparison["diff"]] == [
            ("README.md", "modified"),
            ("docs_PULL_REQUEST_TEMPLATE.md", "renamed"),
            ("src/test_app/math.py", "modified"),
            ("src/test_app/ops.py", "added"),
            ("tests/test_math.py", "removed"),
        ]
        assert all(f["patch"] for f in comparison["diff"] if f["status"] != "renamed")
        assert result["meta"]["next_cursor"] is None

    def test_paged_compare_skips_behind(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.side_effect = [
            recorded("compare_commits"),
            recorded("compare_diff"),
            recorded("compare_diffstat"),
        ]

        result = provider.compare_commits(BRANCH_SHA, HEAD_SHA, pagination={"per_page": 2})

        assert client.request.call_count == 3
        assert "behind_by" not in result["data"]

    def test_reads_every_changed_file(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.side_effect = [
            recorded("compare_commits"),
            recorded("compare_diff"),
            recorded("diffstat_page_1"),
            recorded("diffstat"),
        ]

        result = provider.compare_commits(BRANCH_SHA, HEAD_SHA, pagination={"per_page": 2})

        files = len(recorded("diffstat_page_1").json()["values"]) + len(recorded("diffstat").json()["values"])
        assert len(result["data"]["diff"]) == files

    def test_include_behind_with_pagination(self, provider: BitbucketProvider) -> None:
        with pytest.raises(ResourceBadRequest):
            provider.compare_commits(BRANCH_SHA, HEAD_SHA, pagination={"per_page": 1}, include_behind=True)


class TestCommitMapping:
    @pytest.mark.parametrize(
        ("raw", "name", "email"),
        [
            ({"raw": "Test User <test@example.com>"}, "Test User", "test@example.com"),
            ({"raw": "<test@example.com>"}, "", "test@example.com"),
            ({"raw": "Test User"}, "Test User", ""),
        ],
    )
    def test_author_without_linked_user(self, raw: dict[str, Any], name: str, email: str) -> None:
        author = map_commit_author(raw, "2026-11-05T20:16:34+00:00")
        assert (author["name"], author["email"]) == (name, email)

    def test_split_patches_reads_paths_containing_b_slash(self) -> None:
        diff = (
            "diff --git a/Appendix b/notes.md b/Appendix b/notes.md\n"
            "--- a/Appendix b/notes.md\t\n"
            "+++ b/Appendix b/notes.md\t\n"
            "@@ -1 +1 @@\n-x\n+y\n"
            "diff --git a/gone.py b/gone.py\n"
            "--- a/gone.py\n"
            "+++ /dev/null\n"
            "@@ -1 +0,0 @@\n-x\n"
        )
        assert split_patches(diff) == {
            "Appendix b/notes.md": "@@ -1 +1 @@\n-x\n+y\n",
            "gone.py": "@@ -1 +0,0 @@\n-x\n",
        }

    def test_split_patches_reads_quoted_paths(self) -> None:
        diff = (
            'diff --git "a/r\\303\\251sum\\303\\251.txt" "b/r\\303\\251sum\\303\\251.txt"\n'
            '--- "a/r\\303\\251sum\\303\\251.txt"\n'
            '+++ "b/r\\303\\251sum\\303\\251.txt"\n'
            "@@ -1 +1 @@\n-x\n+y\n"
            'diff --git "a/say \\"hi\\".txt" "b/say \\"hi\\".txt"\n'
            '--- "a/say \\"hi\\".txt"\n'
            "+++ /dev/null\n"
            "@@ -1 +0,0 @@\n-x\n"
        )
        assert split_patches(diff) == {
            "résumé.txt": "@@ -1 +1 @@\n-x\n+y\n",
            'say "hi".txt': "@@ -1 +0,0 @@\n-x\n",
        }

    def test_split_patches_skips_files_without_hunks(self) -> None:
        diff = (
            "diff --git a/old.md b/new.md\nsimilarity index 100%\nrename from old.md\nrename to new.md\n"
            "diff --git a/img.png b/img.png\nBinary files a/img.png and b/img.png differ\n"
            "diff --git a/a.py b/a.py\n--- a/a.py\n+++ b/a.py\n@@ -1 +1 @@\n-x\n+y\n"
        )
        assert split_patches(diff) == {"a.py": "@@ -1 +1 @@\n-x\n+y\n"}


def json_response(body: dict[str, Any], status: int = 200) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response._content = json.dumps(body).encode()
    response.request = requests.Request("GET", "https://api.bitbucket.org/2.0/x").prepare()
    return response


class TestGetPullRequest:
    def test_maps_open_pull_request(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.side_effect = [
            recorded("pull_request"),
            json_response({"hash": HEAD_HASH}),
            json_response({"hash": BASE_HASH}),
        ]

        result = provider.get_pull_request("1")

        assert [(c.kwargs["path"], c.kwargs["params"]) for c in client.request.call_args_list] == [
            (f"/2.0/repositories/{REPO}/pullrequests/1", {}),
            (f"/2.0/repositories/{REPO}/commit/c495d0431cfa", {"fields": "hash"}),
            (f"/2.0/repositories/{REPO}/commit/a682892aa0f4", {"fields": "hash"}),
        ]
        assert result["data"] == {
            "id": "1",
            "internal_id": "1",
            "title": "Test pull request",
            "body": "A test pull request.",
            "state": "open",
            "merged": False,
            "html_url": f"https://bitbucket.org/{REPO}/pull-requests/1",
            "head": {"ref": "test-branch-1", "sha": HEAD_HASH},
            "base": {"ref": "main", "sha": BASE_HASH},
            "author": TEST_USER,
        }

    def test_expands_head_hash_in_source_repository(
        self, provider: BitbucketProvider, client: unittest.mock.MagicMock
    ) -> None:
        response = recorded("pull_request")
        body = response.json()
        body["source"]["repository"]["full_name"] = "fork-workspace/test-repo"
        client.request.side_effect = [
            json_response(body),
            json_response({"hash": HEAD_HASH}),
            json_response({"hash": BASE_HASH}),
        ]

        provider.get_pull_request("1")

        assert (
            client.request.call_args_list[1].kwargs["path"]
            == "/2.0/repositories/fork-workspace/test-repo/commit/c495d0431cfa"
        )

    @pytest.mark.parametrize("status", [403, 404])
    def test_keeps_short_hash_it_cannot_expand(
        self, provider: BitbucketProvider, client: unittest.mock.MagicMock, status: int
    ) -> None:
        client.request.side_effect = [
            recorded("pull_request"),
            json_response({"type": "error"}, status=status),
            json_response({"hash": BASE_HASH}),
        ]

        pull_request = provider.get_pull_request("1")["data"]

        assert pull_request["head"]["sha"] == "c495d0431cfa"
        assert pull_request["base"]["sha"] == BASE_HASH

    def test_other_errors_expanding_a_hash_raise(
        self, provider: BitbucketProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.side_effect = [recorded("pull_request"), json_response({"type": "error"}, status=400)]
        with pytest.raises(ResourceBadRequest):
            provider.get_pull_request("1")

    def test_skips_expanding_a_missing_commit(
        self, provider: BitbucketProvider, client: unittest.mock.MagicMock
    ) -> None:
        body = recorded("pull_request").json()
        body["source"]["commit"] = None
        client.request.side_effect = [json_response(body), json_response({"hash": BASE_HASH})]

        pull_request = provider.get_pull_request("1")["data"]

        assert pull_request["head"]["sha"] is None
        assert pull_request["base"]["sha"] == BASE_HASH
        assert client.request.call_count == 2

    @pytest.mark.parametrize("repository", [{}, {"repository": None}], ids=["missing", "null"])
    def test_keeps_short_hash_without_a_repository(
        self, provider: BitbucketProvider, client: unittest.mock.MagicMock, repository: dict[str, Any]
    ) -> None:
        body = recorded("pull_request").json()
        del body["source"]["repository"]
        body["source"].update(repository)
        client.request.side_effect = [json_response(body), json_response({"hash": BASE_HASH})]

        pull_request = provider.get_pull_request("1")["data"]

        assert pull_request["head"]["sha"] == "c495d0431cfa"
        assert client.request.call_count == 2

    def test_merged_pull_request_is_closed(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.side_effect = [
            recorded("pull_request_merged"),
            json_response({"hash": "a69b43321a2390a5eeff8c4bb711348457a64396"}),
            json_response({"hash": BASE_HASH}),
        ]

        pull_request = provider.get_pull_request("2")["data"]

        assert pull_request["state"] == "closed"
        assert pull_request["merged"] is True
        assert pull_request["base"]["ref"] == "merge-base"

    def test_not_found(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("pull_request_not_found")
        with pytest.raises(ResourceNotFound):
            provider.get_pull_request("999")


@pytest.mark.parametrize(
    ("raw", "username"),
    [
        ({"nickname": "test-user", "display_name": "Test User"}, "test-user"),
        ({"type": "app_user", "display_name": "Test App"}, "Test App"),
        ({}, ""),
    ],
)
def test_map_author(raw: dict[str, Any], username: str) -> None:
    raw = {"uuid": "{00000000-0000-0000-0000-000000000002}", **raw}
    assert map_author(raw) == {"id": "{00000000-0000-0000-0000-000000000002}", "username": username}


class TestGetPullRequests:
    def test_maps_page(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("pull_requests_page_1")

        result = provider.get_pull_requests(state=None, pagination={"cursor": "5", "per_page": 2})

        assert request_kwargs(client)["params"]["pagelen"] == "2"
        assert request_kwargs(client)["params"]["page"] == "5"
        assert [(pr["id"], pr["state"], pr["merged"]) for pr in result["data"]] == [
            ("3", "closed", False),
            ("2", "closed", True),
        ]
        assert result["data"][0]["body"] is None
        assert result["meta"]["next_cursor"] == "2"

    @pytest.mark.parametrize(
        ("state", "query"),
        [
            ("open", 'state="OPEN"'),
            ("closed", '(state="MERGED" OR state="DECLINED" OR state="SUPERSEDED")'),
            (None, '(state="OPEN" OR state="MERGED" OR state="DECLINED" OR state="SUPERSEDED")'),
        ],
    )
    def test_state_filter(
        self,
        provider: BitbucketProvider,
        client: unittest.mock.MagicMock,
        state: PullRequestState | None,
        query: str,
    ) -> None:
        client.request.return_value = recorded("pull_requests_page_1")
        provider.get_pull_requests(state=state)
        assert request_kwargs(client)["params"] == {"q": query}

    @pytest.mark.parametrize("head", ["feature/x", "test-workspace:feature/x", "test-workspace:refs/heads/feature/x"])
    def test_head_filter(self, provider: BitbucketProvider, client: unittest.mock.MagicMock, head: str) -> None:
        client.request.return_value = recorded("pull_requests_page_1")
        provider.get_pull_requests(head=head)
        assert request_kwargs(client)["params"] == {"q": 'source.branch.name="feature/x" AND state="OPEN"'}

    def test_head_filter_escapes_quotes(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("pull_requests_page_1")
        provider.get_pull_requests(head='a"b')
        assert request_kwargs(client)["params"]["q"].startswith('source.branch.name="a\\"b" AND ')

    def test_head_filter_keeps_non_ascii(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("pull_requests_page_1")
        provider.get_pull_requests(head="feature/é")
        assert request_kwargs(client)["params"]["q"].startswith('source.branch.name="feature/é" AND ')


class TestGetPullRequestFiles:
    def test_maps_diffstat(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.side_effect = [recorded("pull_request_diffstat"), recorded("pull_request_merged_diff")]

        result = provider.get_pull_request_files("2")

        assert [(c.kwargs["path"], c.kwargs["allow_redirects"]) for c in client.request.call_args_list] == [
            (f"/2.0/repositories/{REPO}/pullrequests/2/diffstat", True),
            (f"/2.0/repositories/{REPO}/pullrequests/2/diff", True),
        ]
        assert result["data"] == [
            {
                "filename": "PULL_REQUEST_TEMPLATE.md",
                "status": "removed",
                "patch": "@@ -1,3 +0,0 @@\n-## Summary\n-\n-## Test plan\n",
                "changes": 3,
                "sha": "",
                "previous_filename": None,
            },
            {
                "filename": "README.md",
                "status": "modified",
                "patch": "@@ -1,3 +1,4 @@\n # test-repo\n \n A test repository.\n+More docs.\n",
                "changes": 1,
                "sha": "",
                "previous_filename": None,
            },
            {
                "filename": "src/test_app/greet.py",
                "status": "added",
                "patch": '@@ -0,0 +1,2 @@\n+def greet(name):\n+    return f"Hello, {name}"\n',
                "changes": 2,
                "sha": "",
                "previous_filename": None,
            },
            {
                "filename": "tests/test_arithmetic.py",
                "status": "renamed",
                "patch": None,
                "changes": 0,
                "sha": "",
                "previous_filename": "tests/test_math.py",
            },
        ]
        assert result["meta"]["next_cursor"] is None

    def test_passes_pagination_to_diffstat(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.side_effect = [recorded("pull_request_diffstat"), recorded("pull_request_merged_diff")]

        provider.get_pull_request_files("2", pagination={"cursor": "3", "per_page": 5})

        assert client.request.call_args_list[0].kwargs["params"] == {"pagelen": "5", "page": "3"}

    def test_conflict_status_is_unknown(self) -> None:
        raw = {"status": "merge conflict", "old": None, "new": {"path": "a.py"}, "lines_added": 1, "lines_removed": 0}
        assert map_pull_request_file(raw, {})["status"] == "unknown"


class TestGetPullRequestCommits:
    def test_maps_commits_oldest_first(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("pull_request_commits")

        result = provider.get_pull_request_commits("2")

        assert request_kwargs(client)["path"] == f"/2.0/repositories/{REPO}/pullrequests/2/commits"
        assert request_kwargs(client)["params"] == {"pagelen": "100"}
        assert client.request.call_count == 1
        assert result["meta"]["next_cursor"] is None
        assert result["data"] == [
            {
                "sha": "f1ee80b1c7c743738fb49d063f9a30a880343314",
                "message": "Add greet module\n",
                "author": {
                    "name": "Test User",
                    "email": "test@example.com",
                    "date": datetime(2026, 11, 5, 20, 16, 11, tzinfo=UTC),
                },
            },
            {
                "sha": "a69b43321a2390a5eeff8c4bb711348457a64396",
                "message": "Rename tests, drop template, update README\n",
                "author": {
                    "name": "Test User",
                    "email": "test@example.com",
                    "date": datetime(2026, 11, 5, 20, 16, 11, tzinfo=UTC),
                },
            },
        ]

    def test_reads_every_page_oldest_first(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        newest, oldest = recorded("pull_request_commits").json()["values"]
        next_url = f"https://api.bitbucket.org/2.0/repositories/{REPO}/pullrequests/2/commits?page=67Fg"
        client.request.side_effect = [
            json_response({"values": [newest], "next": next_url}),
            json_response({"values": [oldest]}),
        ]

        result = provider.get_pull_request_commits("2", pagination={"per_page": 1})

        assert client.request.call_args_list[1].kwargs["params"] == {"pagelen": "100", "page": "67Fg"}
        assert [commit["sha"] for commit in result["data"]] == [oldest["hash"], newest["hash"]]
        assert result["meta"]["next_cursor"] is None

    @staticmethod
    def with_merge_commit(merge_hash: str) -> requests.Response:
        body = recorded("pull_request_commits").json()
        merge_commit = {
            **body["values"][0],
            "hash": merge_hash,
            "message": "Merged in file-changes (pull request #2)\n",
            "parents": [{"hash": BASE_HASH}, {"hash": body["values"][0]["hash"]}],
        }
        body["values"].insert(0, merge_commit)
        return json_response(body)

    def test_drops_bitbucket_merge_commit(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.side_effect = [
            self.with_merge_commit("69c00816acfdb151b90d7dd925cfa79922f362e9"),
            recorded("pull_request_merged"),
        ]

        result = provider.get_pull_request_commits("2")

        assert client.request.call_args_list[1].kwargs["path"] == f"/2.0/repositories/{REPO}/pullrequests/2"
        assert [commit["sha"] for commit in result["data"]] == [
            "f1ee80b1c7c743738fb49d063f9a30a880343314",
            "a69b43321a2390a5eeff8c4bb711348457a64396",
        ]

    def test_keeps_other_merge_commits(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        merge_hash = "1111111111111111111111111111111111111111"
        client.request.side_effect = [self.with_merge_commit(merge_hash), recorded("pull_request_merged")]

        result = provider.get_pull_request_commits("2")

        assert result["data"][-1]["sha"] == merge_hash

    def test_keeps_merge_commit_on_open_pull_request(
        self, provider: BitbucketProvider, client: unittest.mock.MagicMock
    ) -> None:
        merge_hash = "2222222222222222222222222222222222222222"
        client.request.side_effect = [self.with_merge_commit(merge_hash), recorded("pull_request")]

        result = provider.get_pull_request_commits("1")

        assert result["data"][-1]["sha"] == merge_hash


def test_diff_text_decodes_utf8_without_charset() -> None:
    response = requests.Response()
    response._content = "+Ünïcode\n".encode()
    response.encoding = "ISO-8859-1"

    assert diff_text(response) == "+Ünïcode\n"


def test_get_pull_request_url(provider: BitbucketProvider) -> None:
    assert provider.get_pull_request_url("1") == f"https://bitbucket.org/{REPO}/pull-requests/1"


def sent_parts(client: unittest.mock.MagicMock) -> list[tuple[str, str | None, bytes]]:
    """The (name, filename, content) of each part of the multipart body sent to Bitbucket."""
    kwargs = request_kwargs(client)
    head = f"Content-Type: {kwargs['headers']['Content-Type']}\r\n\r\n".encode()
    message = email.message_from_bytes(head + kwargs["data"], policy=email.policy.HTTP)
    parts = []
    for part in message.walk():
        if part.is_multipart():
            continue
        name = part.get_param("name", header="content-disposition")
        payload = part.get_payload(decode=True)
        assert isinstance(name, str) and isinstance(payload, bytes)
        parts.append((name, part.get_filename(), payload))
    return parts


def sent_form(client: unittest.mock.MagicMock) -> list[tuple[str, bytes]]:
    return [(name, content) for name, _, content in sent_parts(client)]


def sent_filenames(client: unittest.mock.MagicMock) -> dict[str, str]:
    return {name: filename for name, filename, _ in sent_parts(client) if filename}


class TestCreateCommit:
    def test_commits_writes_and_deletes(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.side_effect = [recorded("commit_branch"), recorded("commit_created")]

        result = provider.create_commit(
            "test-branch",
            PARENT,
            "Add notes",
            [
                WriteCommitAction(action="create", filename="notes.txt", content="hello\n", encoding="utf-8"),
                WriteCommitAction(action="create", filename="logo.png", content=PNG_BASE64, encoding="base64"),
                DeleteCommitAction(filename="PULL_REQUEST_TEMPLATE.md"),
            ],
            author={"name": "Test User", "email": "test@example.com"},
        )

        branch_call, commit_call = client.request.call_args_list
        assert branch_call.kwargs["path"] == f"/2.0/repositories/{REPO}/refs/branches/test-branch"
        assert commit_call.kwargs["method"] == "POST"
        assert commit_call.kwargs["path"] == f"/2.0/repositories/{REPO}/src"
        assert commit_call.kwargs["headers"]["Content-Type"].startswith("multipart/form-data; boundary=")
        assert sent_form(client) == [
            ("message", b"Add notes"),
            ("branch", b"test-branch"),
            ("parents", PARENT.encode()),
            ("author", b"Test User <test@example.com>"),
            ("/notes.txt", b"hello\n"),
            ("/logo.png", PNG),
            ("files", b"PULL_REQUEST_TEMPLATE.md"),
        ]
        assert sent_filenames(client) == {"/notes.txt": "notes.txt", "/logo.png": "logo.png"}
        assert result["data"] == {
            "id": "25a878f7635147b9eef09e958ca34395bc67b705",
            "message": "Add notes",
            "author": {"name": "Test User", "email": "test@example.com", "date": None},
            "additions": None,
            "deletions": None,
        }

    def test_creates_branch(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.side_effect = [recorded("commit_branch_not_found"), recorded("commit_created")]

        result = provider.create_commit("test-branch", PARENT, "m", [], create_branch=True)

        assert result["data"]["id"] == "25a878f7635147b9eef09e958ca34395bc67b705"
        assert result["data"]["author"] is None
        assert "author" not in dict(sent_form(client))

    def test_create_branch_conflicts_with_existing_branch(
        self, provider: BitbucketProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = recorded("commit_branch")
        with pytest.raises(ResourceConflict):
            provider.create_commit("test-branch", PARENT, "m", [], create_branch=True)
        assert client.request.call_count == 1

    def test_missing_branch_is_not_created(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("commit_branch_not_found")
        with pytest.raises(ResourceNotFound):
            provider.create_commit("missing-branch", PARENT, "m", [])
        assert client.request.call_count == 1

    def test_moved_head_raises_stale_branch_head(
        self, provider: BitbucketProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.side_effect = [recorded("commit_branch"), recorded("commit_stale_parent")]
        with pytest.raises(StaleBranchHead):
            provider.create_commit("test-branch", PARENT, "m", [], expected_head_sha=PARENT)

    def test_other_bad_request_is_not_stale(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        rejected = recorded("commit_stale_parent")
        rejected._content = json.dumps({"type": "error", "error": {"message": "Invalid file path"}}).encode()
        client.request.side_effect = [recorded("commit_branch"), rejected]
        with pytest.raises(ResourceBadRequest):
            provider.create_commit("test-branch", PARENT, "m", [])

    def test_encodes_branch_name(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.side_effect = [recorded("commit_branch"), recorded("commit_created")]

        provider.create_commit("feature/#12", PARENT, "m", [])

        assert (
            client.request.call_args_list[0].kwargs["path"] == f"/2.0/repositories/{REPO}/refs/branches/feature/%2312"
        )

    def test_strips_leading_slash_from_filename(
        self, provider: BitbucketProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.side_effect = [recorded("commit_branch"), recorded("commit_created")]

        provider.create_commit(
            "test-branch",
            PARENT,
            "m",
            [WriteCommitAction(action="create", filename="/notes.txt", content="hello\n", encoding="utf-8")],
        )

        assert ("/notes.txt", b"hello\n") in sent_form(client)
        assert sent_filenames(client) == {"/notes.txt": "notes.txt"}

    def test_strips_leading_slash_from_removed_paths(
        self, provider: BitbucketProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.side_effect = [
            recorded("commit_branch"),
            recorded("src_meta"),
            recorded("src_file"),
            recorded("commit_created"),
        ]

        provider.create_commit(
            "test-branch",
            PARENT,
            "m",
            [
                DeleteCommitAction(filename="/old.txt"),
                MoveCommitAction(old_filename="/README.md", new_filename="docs/README.md"),
            ],
        )

        form = sent_form(client)
        assert ("files", b"old.txt") in form
        assert ("files", b"README.md") in form

    def test_moves_file(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.side_effect = [
            recorded("commit_branch"),
            recorded("src_meta"),
            recorded("src_file"),
            recorded("commit_created"),
        ]

        provider.create_commit(
            "test-branch", PARENT, "m", [MoveCommitAction(old_filename="README.md", new_filename="docs/README.md")]
        )

        meta_call = client.request.call_args_list[1]
        assert meta_call.kwargs["path"] == f"/2.0/repositories/{REPO}/src/{PARENT}/README.md"
        assert meta_call.kwargs["params"] == {"format": "meta"}
        form = sent_form(client)
        assert ("files", b"README.md") in form
        assert ("/docs/README.md", b"# test-repo\n\nA test repository.\n") in form

    def test_moves_binary_file(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        meta = recorded("src_meta")
        meta._content = json.dumps({**meta.json(), "attributes": ["binary"]}).encode()
        binary_file = recorded("src_file")
        binary_file._content = PNG
        client.request.side_effect = [recorded("commit_branch"), meta, binary_file, recorded("commit_created")]

        provider.create_commit(
            "test-branch", PARENT, "m", [MoveCommitAction(old_filename="logo.png", new_filename="img/logo.png")]
        )

        assert ("/img/logo.png", PNG) in sent_form(client)

    def test_refuses_to_move_executable(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.side_effect = [recorded("commit_branch"), recorded("src_meta_executable")]
        with pytest.raises(ResourceBadRequest):
            provider.create_commit(
                "test-branch", PARENT, "m", [MoveCommitAction(old_filename="deploy.sh", new_filename="run.sh")]
            )
        assert client.request.call_count == 2

    def test_refuses_to_move_directory(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        meta = recorded("src_meta")
        body = {key: value for key, value in meta.json().items() if key != "attributes"}
        meta._content = json.dumps({**body, "type": "commit_directory"}).encode()
        client.request.side_effect = [recorded("commit_branch"), meta]

        with pytest.raises(ResourceBadRequest, match="is a directory"):
            provider.create_commit(
                "test-branch", PARENT, "m", [MoveCommitAction(old_filename="docs", new_filename="d")]
            )

        assert client.request.call_count == 2

    @pytest.mark.parametrize(
        "action",
        [
            WriteCommitAction(action="create", filename="logo.png", content="not base64!", encoding="base64"),
            ChmodCommitAction(executable=True, filename="run.sh"),
            WriteCommitAction(action="create", filename='say "hi".txt', content="x", encoding="utf-8"),
            WriteCommitAction(action="create", filename="docs/../escape.txt", content="x", encoding="utf-8"),
            DeleteCommitAction(filename="./a.txt"),
        ],
        ids=["invalid_base64", "chmod", "quote", "dot_dot", "dot"],
    )
    def test_refuses_what_an_upload_cannot_write(
        self,
        provider: BitbucketProvider,
        client: unittest.mock.MagicMock,
        action: WriteCommitAction | ChmodCommitAction | DeleteCommitAction,
    ) -> None:
        client.request.return_value = recorded("commit_branch")
        with pytest.raises(ResourceBadRequest):
            provider.create_commit("test-branch", PARENT, "m", [action])
        assert client.request.call_count == 1

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"force": True},
            {"expected_head_sha": "0" * 40},
            {"expected_head_sha": PARENT, "create_branch": True},
        ],
        ids=["force", "lease_not_parent", "lease_with_create_branch"],
    )
    def test_rejects_unsupported_options(
        self, provider: BitbucketProvider, client: unittest.mock.MagicMock, kwargs: dict[str, Any]
    ) -> None:
        with pytest.raises(ResourceBadRequest):
            provider.create_commit("test-branch", PARENT, "m", [], **kwargs)
        client.request.assert_not_called()


def test_decode_content_ignores_line_wrapping() -> None:
    action = WriteCommitAction(action="create", filename="a.bin", content="aGVs\nbG8=\n", encoding="base64")
    assert decode_content(action) == b"hello"


def test_cannot_update_branch(provider: BitbucketProvider) -> None:
    assert not isinstance(provider, UpdateBranchProtocol)


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
            provider.get_branch("missing-branch")

    def test_create_branch(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("branch_created")

        result = provider.create_branch("new-branch", MAIN_SHA)

        kwargs = request_kwargs(client)
        assert kwargs["method"] == "POST"
        assert kwargs["path"] == f"/2.0/repositories/{REPO}/refs/branches"
        assert kwargs["data"] == {"name": "new-branch", "target": {"hash": MAIN_SHA}}
        assert result["data"] == {"ref": "new-branch", "sha": MAIN_SHA}

    def test_create_existing_branch(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("branch_exists")
        with pytest.raises(ResourceBadRequest):
            provider.create_branch("new-branch", MAIN_SHA)

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

        result = provider.get_git_ref("tags/test-tag")

        assert request_kwargs(client)["path"] == f"/2.0/repositories/{REPO}/refs/tags/test-tag"
        assert result["data"] == {"ref": "refs/tags/test-tag", "sha": FEATURE_SHA}

    def test_encodes_ref_name(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("tag")
        provider.get_git_ref("tags/v#1")
        assert request_kwargs(client)["path"] == f"/2.0/repositories/{REPO}/refs/tags/v%231"

    @pytest.mark.parametrize("ref", ["main", "heads/", "pull/1/head", "refs/heads/main"])
    def test_unsupported_ref(self, provider: BitbucketProvider, client: unittest.mock.MagicMock, ref: str) -> None:
        with pytest.raises(ResourceNotFound):
            provider.get_git_ref(ref)
        client.request.assert_not_called()


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
        assert request_kwargs(client)["params"]["fields"] == (
            "next,values.path,values.type,values.size,values.attributes,values.commit.hash"
        )
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

    def test_null_attributes_is_a_regular_file(self) -> None:
        raw = {"path": "a.py", "type": "commit_file", "size": 1, "attributes": None, "commit": {"hash": COMMIT}}
        assert map_tree_entry(raw, "")["mode"] == "100644"

    def test_quotes_path(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("src_subdirectory")

        provider.get_tree(f"{COMMIT}:a dir")

        assert request_kwargs(client)["path"] == f"/2.0/repositories/{REPO}/src/{COMMIT}/a%20dir/"

    def test_resolves_ref_with_slash(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.side_effect = [recorded("commits_branch_head"), recorded("src_subdirectory")]

        result = provider.get_tree("feature/x:src")

        paths = [call.kwargs["path"] for call in client.request.call_args_list]
        assert paths == [
            f"/2.0/repositories/{REPO}/commits/feature/x",
            f"/2.0/repositories/{REPO}/src/{COMMIT}/src/",
        ]
        assert result["data"]["sha"] == "feature/x:src"

    def test_file_path_is_not_a_tree(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        response = requests.Response()
        response.status_code = 200
        response.headers["content-type"] = "text/plain"
        response._content = b"#!/bin/sh\necho hi\n"
        client.request.return_value = response

        with pytest.raises(PathIsNotDirectory):
            provider.get_tree(f"{COMMIT}:run.sh")

    def test_dot_segments_raise(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        with pytest.raises(ResourceBadRequest):
            provider.get_tree(f"{COMMIT}:../../../other-repo/src/HEAD/docs")
        client.request.assert_not_called()


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

    def test_resolves_ref_with_slash_once(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.side_effect = [
            recorded("commits_branch_head"),
            recorded("src_recursive_page_1"),
            recorded("src_recursive_page_2"),
        ]

        result = provider.get_full_tree("feature/x")

        paths = [call.kwargs["path"] for call in client.request.call_args_list]
        assert paths == [
            f"/2.0/repositories/{REPO}/commits/feature/x",
            f"/2.0/repositories/{REPO}/src/{COMMIT}/",
            f"/2.0/repositories/{REPO}/src/{COMMIT}/",
        ]
        assert result["data"]["sha"] == "feature/x"
        assert len(result["data"]["tree"]) == 8
