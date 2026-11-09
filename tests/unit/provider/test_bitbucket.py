import base64
import json
import pathlib
import unittest.mock
from datetime import UTC, date, datetime
from typing import Any

import pytest
import requests

from scm.errors import PathIsDirectory, PathIsNotDirectory, ReadmeNotFound, ResourceBadRequest, ResourceNotFound
from scm.providers.bitbucket.provider import (
    BitbucketProvider,
    map_app_installation,
    map_commit_author,
    map_paginated_action,
    map_repository,
    next_cursor,
    parse_scopes,
    split_patches,
)
from scm.types import (
    CompareCommitsProtocol,
    CreateBranchProtocol,
    DeleteBranchProtocol,
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
    GetGitRefProtocol,
    GetGitRemoteProtocol,
    GetPullRequestTemplateProtocol,
    GetReadmeProtocol,
    GetRepositoryProtocol,
    Repository,
    UpdateBranchProtocol,
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
    CreateBranchProtocol,
    DeleteBranchProtocol,
    GetBranchProtocol,
    GetGitRefProtocol,
]
COMMIT_SHA = "e723f15089c22737bb51aee8db06e0eab91e4ba1"
HEAD_SHA = "e3ef83d1a831b7b26c838372f668b1627bbf9a1a"
BRANCH_SHA = "c495d0431cfae535ec9a05e4def54229d0eb2ad2"
TEST_AUTHOR = {"name": "Test User", "email": "test@example.com", "date": datetime(2026, 11, 5, 20, 16, 34, tzinfo=UTC)}


MAIN_SHA = "a682892aa0f4502ef7a242f75d72d0b41eaa27b9"

FEATURE_SHA = "1c0671c60997ab4bd70884bf9f3bd0c41d7043f1"


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

    def test_split_patches_skips_files_without_hunks(self) -> None:
        diff = (
            "diff --git a/old.md b/new.md\nsimilarity index 100%\nrename from old.md\nrename to new.md\n"
            "diff --git a/img.png b/img.png\nBinary files a/img.png and b/img.png differ\n"
            "diff --git a/a.py b/a.py\n--- a/a.py\n+++ b/a.py\n@@ -1 +1 @@\n-x\n+y\n"
        )
        assert split_patches(diff) == {"a.py": "@@ -1 +1 @@\n-x\n+y\n"}


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
