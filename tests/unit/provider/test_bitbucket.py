import json
import pathlib
import unittest.mock
from datetime import UTC, date, datetime
from typing import Any

import pytest
import requests

from scm.errors import ResourceBadRequest, ResourceNotFound
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
    GetAppInstallationProtocol,
    GetCommitChangesProtocol,
    GetCommitProtocol,
    GetCommitsByPathProtocol,
    GetCommitsProtocol,
    GetCommitsUrlProtocol,
    GetCommitUrlProtocol,
    GetRepositoryProtocol,
    Repository,
)

FIXTURES = pathlib.Path(__file__).parent / "fixtures" / "bitbucket"
REPO = "test-workspace/test-repo"

SUPPORTED_PROTOCOLS = [
    CompareCommitsProtocol,
    GetAppInstallationProtocol,
    GetCommitChangesProtocol,
    GetCommitProtocol,
    GetCommitsByPathProtocol,
    GetCommitsProtocol,
    GetCommitsUrlProtocol,
    GetCommitUrlProtocol,
    GetRepositoryProtocol,
]
COMMIT_SHA = "e723f15089c22737bb51aee8db06e0eab91e4ba1"
HEAD_SHA = "e3ef83d1a831b7b26c838372f668b1627bbf9a1a"
BRANCH_SHA = "c495d0431cfae535ec9a05e4def54229d0eb2ad2"
TEST_AUTHOR = {"name": "Test User", "email": "test@example.com", "date": datetime(2026, 11, 5, 20, 16, 34, tzinfo=UTC)}


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


def requested_paths(client: unittest.mock.MagicMock) -> list[str]:
    return [call.kwargs["path"] for call in client.request.call_args_list]


class TestCommitUrls:
    def test_commit_url(self, provider: BitbucketProvider) -> None:
        assert provider.get_commit_url("abc") == f"https://bitbucket.org/{REPO}/commits/abc"

    def test_file_history_url(self, provider: BitbucketProvider) -> None:
        url = provider.get_commits_url("abc", file_path="src/my file.py")
        assert url == f"https://bitbucket.org/{REPO}/history-node/abc/src/my%20file.py"

    @pytest.mark.parametrize(
        "kwargs",
        [
            {},
            {"file_path": "README.md", "since": date(2026, 1, 1)},
            {"file_path": "README.md", "until": date(2026, 1, 1)},
        ],
    )
    def test_unsupported_history_url(self, provider: BitbucketProvider, kwargs: dict[str, Any]) -> None:
        with pytest.raises(ResourceBadRequest):
            provider.get_commits_url("abc", **kwargs)


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

        assert request_kwargs(client)["path"] == f"/2.0/repositories/{REPO}/commits/feature%2Fx"
        assert request_kwargs(client)["params"] == {"pagelen": "2"}
        assert [c["id"] for c in result["data"]] == [HEAD_SHA, COMMIT_SHA]
        assert result["data"][0]["message"] == "Update README\n"
        assert result["data"][0]["author"] == TEST_AUTHOR
        assert result["meta"]["next_cursor"] == "2"

    def test_defaults_to_main_branch(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("commits_page_1")
        provider.get_commits()
        assert request_kwargs(client)["path"] == f"/2.0/repositories/{REPO}/commits/HEAD"

    def test_lists_commits_by_path(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = recorded("commits_by_path_page_1")

        result = provider.get_commits_by_path("src/test_app/math.py", ref="s4-commits", pagination={"per_page": 1})

        assert request_kwargs(client)["path"] == f"/2.0/repositories/{REPO}/commits/s4-commits"
        assert request_kwargs(client)["params"] == {"path": "src/test_app/math.py", "pagelen": "1"}
        assert [c["id"] for c in result["data"]] == [COMMIT_SHA]
        assert result["meta"]["next_cursor"] == "2"

    @pytest.mark.parametrize("kwargs", [{"since": datetime(2026, 1, 1)}, {"until": datetime(2026, 1, 1)}])
    def test_no_date_filters(self, provider: BitbucketProvider, kwargs: dict[str, Any]) -> None:
        with pytest.raises(ResourceBadRequest):
            provider.get_commits(**kwargs)
        with pytest.raises(ResourceBadRequest):
            provider.get_commits_by_path("README.md", **kwargs)


class TestCompareCommits:
    def test_compares_against_merge_base(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.side_effect = [
            recorded("compare_commits"),
            recorded("compare_diffstat"),
            recorded("compare_diff"),
        ]

        result = provider.compare_commits(BRANCH_SHA, HEAD_SHA)

        calls = client.request.call_args_list
        assert calls[0].kwargs["params"] == {"include": HEAD_SHA, "exclude": BRANCH_SHA, "pagelen": "100"}
        assert requested_paths(client)[1:] == [
            f"/2.0/repositories/{REPO}/diffstat/{HEAD_SHA}..{BRANCH_SHA}",
            f"/2.0/repositories/{REPO}/diff/{HEAD_SHA}..{BRANCH_SHA}",
        ]
        comparison = result["data"]
        assert comparison["ahead_by"] == 2
        assert "behind_by" not in comparison
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

    def test_include_behind(self, provider: BitbucketProvider, client: unittest.mock.MagicMock) -> None:
        client.request.side_effect = [
            recorded("compare_commits"),
            recorded("compare_diffstat"),
            recorded("compare_diff"),
            recorded("compare_behind"),
        ]

        result = provider.compare_commits(BRANCH_SHA, HEAD_SHA, include_behind=True)

        assert request_kwargs(client)["params"] == {"include": BRANCH_SHA, "exclude": HEAD_SHA, "pagelen": "100"}
        assert (result["data"]["ahead_by"], result["data"]["behind_by"]) == (2, 1)

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
