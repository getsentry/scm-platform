import unittest.mock
from datetime import UTC, datetime
from typing import Any

import pytest

from scm.errors import (
    PathIsDirectory,
    PathIsNotDirectory,
    ReadmeNotFound,
    ResourceBadRequest,
    ResourceConflict,
    ResourceGatewayTimeout,
    ResourceNotFound,
    ResourceServerError,
    StaleBranchHead,
    UnexpectedResponseFormat,
)
from scm.helpers import iter_all_pages
from scm.providers.cursor_origin.provider import (
    CursorOriginProvider,
    inline_anchor,
    map_app_installation,
    map_author,
    map_check_run,
    map_commit,
    map_file_content,
    map_git_ref,
    map_git_tree,
    map_repository,
    map_review_threads,
)
from scm.types import (
    ChmodCommitAction,
    CommitAuthorParam,
    DeleteCommitAction,
    MoveCommitAction,
    Repository,
    WriteCommitAction,
)

REPO = "acme/rocket"


@pytest.fixture
def client() -> unittest.mock.MagicMock:
    return unittest.mock.MagicMock(_name="client")


@pytest.fixture
def provider(client: unittest.mock.MagicMock) -> CursorOriginProvider:
    return CursorOriginProvider(
        client=client,
        organization_id=1,
        repository=Repository(
            id=1,
            integration_id=1,
            name=REPO,
            organization_id=1,
            is_active=True,
            external_id="r_01example",
            provider_name="cursor_origin",
            web_base_url=None,
            installation_id="inst_01example",
        ),
        installation_id="inst_01example",
    )


def _response(json_data: Any, status_code: int = 200) -> unittest.mock.MagicMock:
    response = unittest.mock.MagicMock()
    response.json.return_value = json_data
    response.status_code = status_code
    response.headers = {}
    response.content = b"{}"
    return response


REPOSITORY_RAW = {
    "id": "r_01example",
    "name": "rocket",
    "fullName": REPO,
    "owner": {"slug": "acme", "id": "ns_01example", "type": "team"},
    "defaultBranch": "main",
    "cloneUrl": "https://git.cursor.com/acme/rocket.git",
    "visibility": "private",
}


class TestGetRepository:
    def test_it_reads_the_repository(self, provider: CursorOriginProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = _response(REPOSITORY_RAW)

        result = provider.get_repository()

        assert client.request.call_args.kwargs["path"] == f"/repos/{REPO}"
        assert client.request.call_args.kwargs["method"] == "GET"
        assert result["type"] == "cursor_origin"
        assert result["data"] == {
            "full_name": REPO,
            "default_branch": "main",
            "clone_url": "https://git.cursor.com/acme/rocket.git",
            "private": True,
            "size": None,
            "description": None,
            "topics": [],
        }

    def test_an_internal_repository_is_private(self) -> None:
        """Visible across the owner but not public, as GitHub reports it."""
        assert map_repository({**REPOSITORY_RAW, "visibility": "internal"})["private"] is True

    def test_a_failure_is_raised_as_a_coded_error(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response({"message": "not found"}, status_code=404)

        with pytest.raises(ResourceNotFound):
            provider.get_repository()


class TestGetBranch:
    def test_a_branch_is_read_as_a_git_ref(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        """Origin has no per-branch route, only the git ref one."""
        client.request.return_value = _response(
            {"ref": "refs/heads/main", "object": {"sha": "9a41f0c3", "type": "commit"}}
        )

        result = provider.get_branch("main")

        assert client.request.call_args.kwargs["path"] == f"/repos/{REPO}/git/ref/heads/main"
        assert result["data"] == {"ref": "main", "sha": "9a41f0c3"}

    def test_the_refs_prefix_is_stripped(self) -> None:
        raw = {"ref": "refs/heads/release/2.0", "object": {"sha": "abc", "type": "commit"}}

        assert map_git_ref(raw)["ref"] == "release/2.0"


class TestPagination:
    def test_a_cursor_is_sent_as_origins_page_token(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response(REPOSITORY_RAW)

        provider.get(f"/repos/{REPO}", pagination={"cursor": "opaque", "per_page": 50})

        assert client.request.call_args.kwargs["params"] == {
            "pageSize": "50",
            "pageToken": "opaque",
        }

    def test_a_first_page_sends_no_token(self, provider: CursorOriginProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = _response(REPOSITORY_RAW)

        provider.get(f"/repos/{REPO}", pagination={"cursor": "", "per_page": 30})

        assert client.request.call_args.kwargs["params"] == {"pageSize": "30"}

    def test_the_first_page_cursor_sends_no_token(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response(REPOSITORY_RAW)

        provider.get(f"/repos/{REPO}", pagination={"cursor": "1", "per_page": 30})

        assert client.request.call_args.kwargs["params"] == {"pageSize": "30"}


FILE_RAW = {
    "type": "file",
    "encoding": "base64",
    "size": "42",
    "name": "app.py",
    "path": "src/app.py",
    "sha": "b10b5ha",
    "content": "cHJpbnQoImhpIikK",
}

DIRECTORY_RAW = {
    "type": "dir",
    "name": "src",
    "path": "src",
    "sha": "7ree5ha",
    "entries": [
        {"type": "file", "name": "app.py", "path": "src/app.py", "sha": "b10b5ha", "size": "42"},
        {"type": "dir", "name": "web", "path": "src/web", "sha": "d1r5ha", "size": "0"},
    ],
}


class TestGetFileContent:
    def test_a_file_is_read_at_a_ref(self, provider: CursorOriginProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = _response(FILE_RAW)

        result = provider.get_file_content("src/app.py", "main")

        assert client.request.call_args.kwargs["path"] == f"/repos/{REPO}/contents"
        assert client.request.call_args.kwargs["params"] == {"path": "src/app.py", "ref": "main"}
        assert result["data"]["content"] == "cHJpbnQoImhpIikK"
        assert result["data"]["size"] == 42

    def test_the_size_string_becomes_an_integer(self) -> None:
        """Origin sends 64-bit integers as JSON strings."""
        assert map_file_content(FILE_RAW)["size"] == 42

    def test_a_directory_asked_for_as_a_file_is_refused(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response(DIRECTORY_RAW)

        with pytest.raises(PathIsDirectory):
            provider.get_file_content("src", "main")


class TestGetDirectoryContents:
    def test_the_children_are_listed(self, provider: CursorOriginProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = _response(DIRECTORY_RAW)

        result = provider.get_directory_contents("src", ref="main")

        assert [entry["path"] for entry in result["data"]] == ["src/app.py", "src/web"]
        assert result["data"][1]["type"] == "directory"
        assert result["meta"]["next_cursor"] is None

    def test_a_file_asked_for_as_a_directory_is_refused(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response(FILE_RAW)

        with pytest.raises(PathIsNotDirectory):
            provider.get_directory_contents("src/app.py")


TREE_RAW = {
    "sha": "7ree5ha",
    "tree": [
        {"path": "src", "mode": "040000", "type": "tree", "sha": "d1r5ha"},
        {"path": "src/app.py", "mode": "100644", "type": "blob", "sha": "b10b5ha", "size": 42},
    ],
    "truncated": False,
}


class TestGetTree:
    def test_a_recursive_tree_is_read(self, provider: CursorOriginProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = _response(TREE_RAW)

        result = provider.get_tree("HEAD")

        assert client.request.call_args.kwargs["path"] == f"/repos/{REPO}/git/trees/HEAD"
        assert client.request.call_args.kwargs["params"] == {"recursive": "true"}
        assert [entry["path"] for entry in result["data"]["tree"]] == ["src", "src/app.py"]
        assert result["data"]["truncated"] is False

    def test_immediate_children_only_when_recursion_is_off(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response(TREE_RAW)

        provider.get_tree("HEAD", recursive=False)

        assert client.request.call_args.kwargs["params"] == {}

    def test_a_tree_entry_has_no_size(self) -> None:
        assert map_git_tree(TREE_RAW)["tree"][0]["size"] is None


def _redirect(location: str) -> unittest.mock.MagicMock:
    response = unittest.mock.MagicMock()
    response.status_code = 302
    response.headers = {"Location": location}
    response.content = b""
    return response


def _streamed_archive() -> unittest.mock.MagicMock:
    response = unittest.mock.MagicMock()
    response.status_code = 200
    response.headers = {}
    response.content = b""
    return response


SIGNED_URL = "https://artifacts.origin.cursor.com/tarballs/abc.tar.gz?Signature=EXAMPLE"


class TestGetArchiveLink:
    def test_a_cached_archive_answers_with_a_signed_url(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _redirect(SIGNED_URL)

        result = provider.get_archive_link("release/test", request_options={"timeout": 600.0})

        assert client.request.call_args.kwargs["path"] == f"/repos/{REPO}/tarball"
        assert client.request.call_args.kwargs["params"] == {"ref": "release/test"}
        assert client.request.call_args.kwargs["allow_redirects"] is False
        assert client.request.call_args.kwargs["timeout"] == 600.0
        assert client.request.return_value.close.called
        assert result["data"] == {"url": SIGNED_URL, "headers": {}}

    def test_the_first_request_builds_the_archive_and_the_second_links_it(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        """Origin streams the archive inline the first time a commit is asked for."""
        streamed = _streamed_archive()
        client.request.side_effect = [streamed, _redirect(SIGNED_URL)]

        result = provider.get_archive_link("HEAD")

        assert client.request.call_count == 2
        assert streamed.close.called
        assert result["data"]["url"] == SIGNED_URL

    def test_a_response_with_no_location_is_refused(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        response = _redirect(SIGNED_URL)
        response.headers = {}
        client.request.return_value = response

        with pytest.raises(UnexpectedResponseFormat):
            provider.get_archive_link("HEAD")

    def test_only_tarballs_are_offered(self, provider: CursorOriginProvider) -> None:
        """Origin has one archive route, and it is a gzipped tarball."""
        with pytest.raises(ResourceBadRequest):
            provider.get_archive_link("HEAD", archive_format="zip")


class TestDownloadArchive:
    def test_the_archive_is_streamed_with_redirects_followed(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        archive = _streamed_archive()
        client.request.return_value = archive

        response = provider.download_archive("abc123", request_options={"timeout": 600.0})

        assert response is archive
        assert client.request.call_args.kwargs["path"] == f"/repos/{REPO}/tarball"
        assert client.request.call_args.kwargs["params"] == {"ref": "abc123"}
        assert client.request.call_args.kwargs["allow_redirects"] is None
        assert client.request.call_args.kwargs["timeout"] == 600.0

    def test_only_tarballs_are_offered(self, provider: CursorOriginProvider) -> None:
        with pytest.raises(ResourceBadRequest):
            provider.download_archive("abc123", archive_format="zip")


def _entry(path: str, entry_type: str = "file") -> dict[str, Any]:
    return {"type": entry_type, "name": path.rsplit("/", 1)[-1], "path": path, "sha": "s", "size": "1"}


class TestAuthenticatedActor:
    def test_the_app_is_read_with_its_own_credentials(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response({"id": "app_01example", "displayName": "Sentry"})

        result = provider.get_authenticated_actor()

        assert client.request.call_args.kwargs["path"] == "/app"
        assert client.request.call_args.kwargs["credentials_set"] == "application"
        assert result["data"] == {"id": "app_01example", "username": "Sentry"}


class TestPullRequestTemplate:
    def test_a_template_in_the_root_is_read(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.side_effect = [
            _response({"entries": [_entry("PULL_REQUEST_TEMPLATE.md"), _entry("README.md")]}),
            _response({**FILE_RAW, "path": "PULL_REQUEST_TEMPLATE.md"}),
            _response({"entries": []}),
        ]

        templates = list(provider.get_pull_request_template("main"))

        assert [template["data"]["path"] for template in templates] == ["PULL_REQUEST_TEMPLATE.md"]
        assert client.request.call_args_list[0].kwargs["params"] == {"path": "", "ref": "main"}
        assert client.request.call_args_list[2].kwargs["params"] == {"path": "docs", "ref": "main"}

    def test_every_template_in_a_template_directory_is_read(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.side_effect = [
            _response({"entries": []}),
            _response({"entries": [_entry("docs/PULL_REQUEST_TEMPLATE", "dir")]}),
            _response(
                {
                    "entries": [
                        _entry("docs/PULL_REQUEST_TEMPLATE/bug.md"),
                        _entry("docs/PULL_REQUEST_TEMPLATE/notes.txt"),
                    ]
                }
            ),
            _response({**FILE_RAW, "path": "docs/PULL_REQUEST_TEMPLATE/bug.md"}),
        ]

        templates = list(provider.get_pull_request_template("main"))

        assert [template["data"]["path"] for template in templates] == ["docs/PULL_REQUEST_TEMPLATE/bug.md"]

    def test_a_repository_with_no_template_yields_nothing(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response({"message": "not found"}, status_code=404)

        assert list(provider.get_pull_request_template("main")) == []

    def test_a_failure_that_is_not_a_missing_path_is_raised(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response({"message": "boom"}, status_code=500)

        with pytest.raises(ResourceServerError):
            list(provider.get_pull_request_template("main"))


class TestReadme:
    def test_the_readme_is_found_in_the_root(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.side_effect = [
            _response({"entries": [_entry("src", "dir"), _entry("README.md")]}),
            _response({**FILE_RAW, "path": "README.md"}),
        ]

        result = provider.get_readme("main")

        assert client.request.call_args_list[0].kwargs["params"] == {"path": "", "ref": "main"}
        assert result["data"]["path"] == "README.md"

    def test_a_repository_without_one_is_refused(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response({"entries": [_entry("src/app.py")]})

        with pytest.raises(ReadmeNotFound):
            provider.get_readme("main")

    def test_a_repository_has_no_topics(self, provider: CursorOriginProvider) -> None:
        assert provider.get_repository_topics()["data"] == []


def _check_run_raw(**overrides: Any) -> dict[str, Any]:
    return {
        "id": "cr_01example",
        "checkSuite": {"id": "crg_01example"},
        "sha": "head123",
        "key": "Seer",
        "name": "Seer",
        "status": "in_progress",
        "detailsUrl": "https://sentry.io/seer/run/1",
        "externalId": "Seer:head123",
        "startedAt": "2026-08-01T09:30:00Z",
        **overrides,
    }


class TestCheckRuns:
    def test_a_check_run_is_read_by_its_id(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response(_check_run_raw(status="completed", conclusion="success"))

        result = provider.get_check_run("cr_01example")

        assert client.request.call_args.kwargs["path"] == f"/repos/{REPO}/check-runs/cr_01example"
        assert result["data"] == {
            "id": "cr_01example",
            "name": "Seer",
            "status": "completed",
            "conclusion": "success",
            "html_url": "https://sentry.io/seer/run/1",
        }

    def test_a_rerequested_run_is_pending_again(self) -> None:
        assert map_check_run(_check_run_raw(status="rerequested"))["status"] == "pending"

    def test_a_rerequested_run_hides_the_superseded_conclusion(self) -> None:
        assert map_check_run(_check_run_raw(status="rerequested", conclusion="failure"))["conclusion"] is None

    def test_a_stale_conclusion_is_unknown(self) -> None:
        assert map_check_run(_check_run_raw(status="completed", conclusion="stale"))["conclusion"] == "unknown"

    def test_a_run_is_posted_with_its_suite(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response({"checkRun": _check_run_raw(), "checkSuite": {}})

        provider.create_check_run(
            "Seer",
            "head123",
            status="running",
            started_at="2026-08-01T09:30:00Z",
            output={"title": "Seer", "summary": "Reviewing"},
        )

        data = client.request.call_args.kwargs["data"]
        assert client.request.call_args.kwargs["path"] == f"/repos/{REPO}/check-runs"
        assert data["headSha"] == "head123"
        assert data["checkSuite"] == {"key": "Seer", "name": "Seer", "externalId": "Seer:head123"}
        assert data["checkRun"]["status"] == "in_progress"
        assert data["checkRun"]["key"] == "Seer"
        assert data["checkRun"]["externalId"] == "Seer:head123"
        assert data["checkRun"]["startedAt"] == "2026-08-01T09:30:00Z"
        assert data["checkRun"]["output"] == {"title": "Seer", "summary": "Reviewing"}
        assert data["checkRun"]["externalUpdatedAt"].endswith("Z")

    def test_an_unknown_conclusion_is_posted_as_neutral(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response({"checkRun": _check_run_raw(), "checkSuite": {}})

        provider.create_check_run("Seer", "head123", status="completed", conclusion="unknown")

        assert client.request.call_args.kwargs["data"]["checkRun"]["conclusion"] == "neutral"

    def test_a_neutral_conclusion_is_read_as_neutral(self) -> None:
        assert map_check_run(_check_run_raw(status="completed", conclusion="neutral"))["conclusion"] == "neutral"

    def test_a_queued_run_is_the_default(self, provider: CursorOriginProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = _response({"checkRun": _check_run_raw(), "checkSuite": {}})

        provider.create_check_run("Seer", "head123")

        assert client.request.call_args.kwargs["data"]["checkRun"]["status"] == "queued"

    def test_an_update_reads_the_run_and_posts_it_again(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.side_effect = [
            _response(_check_run_raw()),
            _response({"checkRun": _check_run_raw(status="completed", conclusion="success"), "checkSuite": {}}),
        ]

        result = provider.update_check_run(
            "cr_01example", status="completed", conclusion="success", output={"title": "Seer", "summary": "Done"}
        )

        read, post = client.request.call_args_list
        assert read.kwargs["path"] == f"/repos/{REPO}/check-runs/cr_01example"
        assert post.kwargs["data"]["headSha"] == "head123"
        assert post.kwargs["data"]["checkSuite"]["externalId"] == "Seer:head123"
        assert post.kwargs["data"]["checkRun"]["status"] == "completed"
        assert post.kwargs["data"]["checkRun"]["conclusion"] == "success"
        assert post.kwargs["data"]["checkRun"]["startedAt"] == "2026-08-01T09:30:00Z"
        assert post.kwargs["data"]["checkRun"]["detailsUrl"] == "https://sentry.io/seer/run/1"
        assert result["data"]["conclusion"] == "success"

    def test_an_update_keeps_what_it_does_not_change(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        """Origin's post replaces the run, so an output-only update must not drop the rest."""
        stored = _check_run_raw(
            status="completed",
            conclusion="failure",
            completedAt="2026-08-01T09:40:00Z",
            output={"title": "Seer", "summary": "Found 2 issues"},
        )
        client.request.side_effect = [
            _response(stored),
            _response({"checkRun": stored, "checkSuite": {}}),
        ]

        provider.update_check_run("cr_01example")

        posted = client.request.call_args.kwargs["data"]["checkRun"]
        assert posted["status"] == "completed"
        assert posted["conclusion"] == "failure"
        assert posted["completedAt"] == "2026-08-01T09:40:00Z"
        assert posted["output"] == {"title": "Seer", "summary": "Found 2 issues"}
        assert posted["detailsUrl"] == "https://sentry.io/seer/run/1"

    def test_a_rerequested_run_is_posted_as_queued(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        """Only Origin sets `rerequested`, and it refuses it on a post."""
        stored = _check_run_raw(status="rerequested", conclusion="failure")
        client.request.side_effect = [
            _response(stored),
            _response({"checkRun": _check_run_raw(status="queued"), "checkSuite": {}}),
        ]

        provider.update_check_run("cr_01example", output={"title": "Seer", "summary": "Re-running"})

        posted = client.request.call_args.kwargs["data"]["checkRun"]
        assert posted["status"] == "queued"
        assert "conclusion" not in posted

    def test_a_commits_runs_are_listed(self, provider: CursorOriginProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = _response({"checkRuns": [_check_run_raw()], "nextPageToken": ""})

        result = provider.list_check_runs_for_ref("head123", check_name="Seer", status="in_progress")

        assert client.request.call_args.kwargs["path"] == f"/repos/{REPO}/commits/head123/check-runs"
        assert client.request.call_args.kwargs["params"] == {"checkName": "Seer", "status": "in_progress"}
        assert [run["id"] for run in result["data"]] == ["cr_01example"]
        assert result["meta"]["next_cursor"] is None


def _pull_request_raw(**overrides: Any) -> dict[str, Any]:
    return {
        "id": "pr_01example",
        "number": "7",
        "state": "open",
        "draft": False,
        "merged": False,
        "title": "Fix the thing",
        "body": "Details",
        "head": {"ref": "refs/heads/fix", "sha": "head123"},
        "base": {"ref": "main", "sha": "base123"},
        "author": {"user": {"id": "user_01example", "email": "jane@example.com", "handle": "jane"}},
        "additions": 1,
        "deletions": 0,
        "changedFiles": 1,
        **overrides,
    }


PULL_REQUEST = {
    "id": "7",
    "internal_id": "pr_01example",
    "title": "Fix the thing",
    "body": "Details",
    "state": "open",
    "merged": False,
    "html_url": f"https://cursor.com/codebase/{REPO}/pull/7",
    "head": {"sha": "head123", "ref": "fix"},
    "base": {"sha": "base123", "ref": "main"},
    "author": {"id": "user_01example", "username": "jane"},
}


class TestPullRequests:
    def test_a_pull_request_is_read_by_number(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response(_pull_request_raw())

        result = provider.get_pull_request("7")

        assert client.request.call_args.kwargs["path"] == f"/repos/{REPO}/pulls/7"
        assert result["data"] == PULL_REQUEST

    def test_an_empty_body_is_absent(self, provider: CursorOriginProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = _response(_pull_request_raw(body=""))

        assert provider.get_pull_request("7")["data"]["body"] is None

    def test_open_pull_requests_are_listed_by_head_branch(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response({"pullRequests": [_pull_request_raw()], "nextPageToken": ""})

        result = provider.get_pull_requests(state="open", head="acme:refs/heads/fix")

        assert client.request.call_args.kwargs["path"] == f"/repos/{REPO}/pulls"
        assert client.request.call_args.kwargs["params"] == {"state": "open", "head": "fix"}
        assert result["data"] == [PULL_REQUEST]
        assert result["meta"]["next_cursor"] is None

    def test_no_state_lists_every_pull_request(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response({"pullRequests": [], "nextPageToken": ""})

        provider.get_pull_requests(state=None)

        assert client.request.call_args.kwargs["params"] == {"state": "all"}

    def test_a_pull_request_is_created(self, provider: CursorOriginProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = _response(_pull_request_raw())

        result = provider.create_pull_request("Fix the thing", "Details", "fix", "main")

        assert client.request.call_args.kwargs["method"] == "POST"
        assert client.request.call_args.kwargs["path"] == f"/repos/{REPO}/pulls"
        assert client.request.call_args.kwargs["data"] == {
            "title": "Fix the thing",
            "body": "Details",
            "head": "fix",
            "base": "main",
        }
        assert result["data"] == PULL_REQUEST

    def test_a_draft_is_created(self, provider: CursorOriginProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = _response(_pull_request_raw(draft=True))

        provider.create_pull_request_draft("Fix the thing", "Details", "fix", "main")

        assert client.request.call_args.kwargs["data"]["draft"] is True

    def test_only_the_given_fields_are_updated(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response(_pull_request_raw(state="closed"))

        result = provider.update_pull_request("7", body="", state="closed")

        assert client.request.call_args.kwargs["method"] == "PATCH"
        assert client.request.call_args.kwargs["path"] == f"/repos/{REPO}/pulls/7"
        assert client.request.call_args.kwargs["data"] == {"body": "", "state": "closed"}
        assert result["data"]["state"] == "closed"


class TestMapAuthor:
    def test_a_user_without_a_public_handle_is_named(self) -> None:
        assert map_author({"user": {"id": "user_01", "email": "j@example.com", "displayName": "Jane Doe"}}) == {
            "id": "user_01",
            "username": "Jane Doe",
        }

    def test_an_app(self) -> None:
        assert map_author({"app": {"id": "app_01", "displayName": "Sentry"}}) == {"id": "app_01", "username": "Sentry"}

    def test_a_service_account(self) -> None:
        assert map_author({"serviceAccount": {"id": "sa_01"}}) == {"id": "sa_01", "username": ""}


AUTHOR = CommitAuthorParam(name="Jane Doe", email="jane@example.com")
CREATED = {"sha": "new123", "treeSha": "tree456", "previousHeadSha": "parent123"}


class TestCreateBranch:
    def test_a_branch_is_created_as_a_ref(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response(
            {"ref": "refs/heads/fix", "object": {"sha": "abc123", "type": "commit"}}
        )

        result = provider.create_branch("fix", "abc123")

        assert client.request.call_args.kwargs["method"] == "POST"
        assert client.request.call_args.kwargs["path"] == f"/repos/{REPO}/git/refs"
        assert client.request.call_args.kwargs["data"] == {"ref": "refs/heads/fix", "sha": "abc123"}
        assert result["data"] == {"ref": "fix", "sha": "abc123"}


class TestDeleteBranch:
    def test_a_branch_is_deleted_by_its_ref(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response({}, status_code=204)

        provider.delete_branch("fix")

        assert client.request.call_args.kwargs["method"] == "DELETE"
        assert client.request.call_args.kwargs["path"] == f"/repos/{REPO}/git/refs/heads/fix"


class TestCreateCommit:
    def test_files_are_committed_onto_the_parent(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response(CREATED)

        result = provider.create_commit(
            "fix",
            "parent123",
            "Fix the thing",
            [
                WriteCommitAction(action="update", filename="a.py", content="print(1)", encoding="utf-8"),
                DeleteCommitAction(filename="b.py"),
            ],
            author=AUTHOR,
        )

        assert client.request.call_args.kwargs["path"] == f"/repos/{REPO}/git/commits:createFromFiles"
        assert client.request.call_args.kwargs["data"] == {
            "targetBranch": "fix",
            "expectedHeadSha": "parent123",
            "message": "Fix the thing",
            "author": AUTHOR,
            "files": [
                {"path": "a.py", "content": "print(1)", "encoding": "utf-8"},
                {"path": "b.py", "delete": True},
            ],
        }
        assert result["data"] == {
            "id": "new123",
            "message": "Fix the thing",
            "author": {**AUTHOR, "date": None},
            "additions": None,
            "deletions": None,
        }

    def test_a_new_branch_is_created_at_the_parent_first(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.side_effect = [
            _response({"message": "not found"}, status_code=404),
            _response({"ref": "refs/heads/fix", "object": {"sha": "parent123", "type": "commit"}}),
            _response(CREATED),
        ]

        provider.create_commit(
            "fix", "parent123", "m", [DeleteCommitAction(filename="b.py")], create_branch=True, author=AUTHOR
        )

        lookup, create, commit = client.request.call_args_list
        assert lookup.kwargs["path"] == f"/repos/{REPO}/git/ref/heads/fix"
        assert create.kwargs["data"] == {"ref": "refs/heads/fix", "sha": "parent123"}
        assert commit.kwargs["data"]["expectedHeadSha"] == "parent123"

    def test_a_branch_that_already_exists_is_a_conflict(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        """Origin would accept a branch already at the parent; GitHub refuses any existing one."""
        client.request.return_value = _response({"ref": "refs/heads/fix", "object": {"sha": "parent123"}})

        with pytest.raises(ResourceConflict):
            provider.create_commit(
                "fix", "parent123", "m", [DeleteCommitAction(filename="b.py")], create_branch=True, author=AUTHOR
            )

        assert client.request.call_count == 1

    def test_a_failed_commit_removes_the_branch_it_created(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.side_effect = [
            _response({"message": "not found"}, status_code=404),
            _response({"ref": "refs/heads/fix", "object": {"sha": "parent123", "type": "commit"}}),
            _response({"message": "unchanged tree"}, status_code=400),
            _response({"ref": "refs/heads/fix", "object": {"sha": "parent123", "type": "commit"}}),
            _response({}, status_code=204),
        ]

        with pytest.raises(ResourceBadRequest):
            provider.create_commit(
                "fix", "parent123", "m", [DeleteCommitAction(filename="b.py")], create_branch=True, author=AUTHOR
            )

        assert client.request.call_args.kwargs["method"] == "DELETE"
        assert client.request.call_args.kwargs["path"] == f"/repos/{REPO}/git/refs/heads/fix"

    def test_a_branch_a_commit_reached_is_kept(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        """A timeout can hide a commit that landed, so a branch past the parent is left alone."""
        client.request.side_effect = [
            _response({"message": "not found"}, status_code=404),
            _response({"ref": "refs/heads/fix", "object": {"sha": "parent123", "type": "commit"}}),
            _response({"message": "gateway timeout"}, status_code=504),
            _response({"ref": "refs/heads/fix", "object": {"sha": "new123", "type": "commit"}}),
        ]

        with pytest.raises(ResourceGatewayTimeout):
            provider.create_commit(
                "fix", "parent123", "m", [DeleteCommitAction(filename="b.py")], create_branch=True, author=AUTHOR
            )

        assert client.request.call_count == 4
        assert client.request.call_args.kwargs["method"] == "GET"

    def test_a_move_and_a_mode_change_rewrite_the_file(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.side_effect = [_response(FILE_RAW), _response(FILE_RAW), _response(CREATED)]

        provider.create_commit(
            "fix",
            "parent123",
            "m",
            [
                MoveCommitAction(old_filename="src/app.py", new_filename="src/main.py"),
                ChmodCommitAction(executable=True, filename="src/app.py"),
            ],
            author=AUTHOR,
        )

        assert client.request.call_args_list[0].kwargs["params"] == {"path": "src/app.py", "ref": "parent123"}
        assert client.request.call_args.kwargs["data"]["files"] == [
            {"path": "src/app.py", "delete": True},
            {"path": "src/main.py", "content": FILE_RAW["content"], "encoding": "base64"},
            {"path": "src/app.py", "content": FILE_RAW["content"], "encoding": "base64", "mode": "executable"},
        ]

    def test_a_commit_without_an_author_is_the_apps(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.side_effect = [_response({"id": "app_01example", "displayName": "Sentry"}), _response(CREATED)]

        provider.create_commit("fix", "parent123", "m", [DeleteCommitAction(filename="b.py")])

        app_call, commit_call = client.request.call_args_list
        assert app_call.kwargs["path"] == "/app"
        assert app_call.kwargs["credentials_set"] == "application"
        assert commit_call.kwargs["data"]["author"] == {"name": "Sentry", "email": "noreply@sentry.io"}

    def test_a_branch_cannot_be_forced(self, provider: CursorOriginProvider) -> None:
        with pytest.raises(ResourceBadRequest):
            provider.create_commit("fix", "parent123", "m", [], force=True, author=AUTHOR)

    def test_the_expected_head_must_be_the_parent(self, provider: CursorOriginProvider) -> None:
        with pytest.raises(ResourceBadRequest):
            provider.create_commit("fix", "parent123", "m", [], author=AUTHOR, expected_head_sha="other")

    def test_a_moved_head_is_a_stale_branch(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.side_effect = [
            _response({"message": "branch moved"}, status_code=400),
            _response({"ref": "refs/heads/fix", "object": {"sha": "moved123", "type": "commit"}}),
        ]

        with pytest.raises(StaleBranchHead):
            provider.create_commit(
                "fix",
                "parent123",
                "m",
                [DeleteCommitAction(filename="b.py")],
                author=AUTHOR,
                expected_head_sha="parent123",
            )

    def test_a_refusal_at_the_expected_head_is_not_stale(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.side_effect = [
            _response({"message": "unchanged tree"}, status_code=400),
            _response({"ref": "refs/heads/fix", "object": {"sha": "parent123", "type": "commit"}}),
        ]

        with pytest.raises(ResourceBadRequest):
            provider.create_commit(
                "fix",
                "parent123",
                "m",
                [DeleteCommitAction(filename="b.py")],
                author=AUTHOR,
                expected_head_sha="parent123",
            )


class TestGetAppInstallation:
    def test_the_installation_is_read_with_the_app_credentials(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response({"id": "inst_01example", "scopes": ["repository:contents:read"]})

        result = provider.get_app_installation()

        assert client.request.call_args.kwargs["path"] == "/app/installations/inst_01example"
        assert client.request.call_args.kwargs["credentials_set"] == "application"
        assert result["data"] == {
            "has_read_access": True,
            "has_write_access": False,
            "has_check_run_write_access": False,
        }

    def test_writing_needs_contents_and_pull_requests(self) -> None:
        assert map_app_installation({"scopes": ["repository:contents:write", "repository:pull_requests:write"]}) == {
            "has_read_access": True,
            "has_write_access": True,
            "has_check_run_write_access": False,
        }
        assert not map_app_installation({"scopes": ["repository:contents:write"]})["has_write_access"]

    def test_check_runs_need_the_checks_write_scope(self) -> None:
        assert map_app_installation({"scopes": ["repository:checks:write"]}) == {
            "has_read_access": False,
            "has_write_access": False,
            "has_check_run_write_access": True,
        }


class TestGetGitCommit:
    def test_a_commit_is_read_with_its_tree(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response(
            {
                "sha": "abc123",
                "author": {"name": "A", "email": "a@example.com", "date": "2026-01-01T00:00:00Z"},
                "message": "Fix the thing",
                "tree": {"sha": "tree123"},
                "parents": [{"sha": "parent123"}],
            }
        )

        result = provider.get_git_commit("abc123")

        assert client.request.call_args.kwargs["path"] == f"/repos/{REPO}/git/commits/abc123"
        assert result["data"] == {
            "sha": "abc123",
            "tree": {"sha": "tree123"},
            "message": "Fix the thing",
        }


class TestGetFullTree:
    def test_the_whole_tree_is_read_in_one_response(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response(
            {
                "sha": "tree123",
                "tree": [{"path": "a.py", "mode": "100644", "type": "blob", "sha": "b1", "size": "4"}],
                "truncated": False,
            }
        )

        result = provider.get_full_tree("tree123")

        assert client.request.call_args.kwargs["path"] == f"/repos/{REPO}/git/trees/tree123"
        assert client.request.call_args.kwargs["params"] == {"recursive": "true"}
        assert result["data"]["tree"][0]["size"] == 4
        assert "next_cursor" not in result["meta"]


class TestWebUrls:
    def test_a_file_url_links_the_line_range(self, provider: CursorOriginProvider) -> None:
        assert (
            provider.get_file_url("src/app.py", "abc123", 5, 9)
            == f"https://cursor.com/codebase/{REPO}/blob/abc123/src/app.py#L5-L9"
        )

    def test_a_file_url_without_lines(self, provider: CursorOriginProvider) -> None:
        assert (
            provider.get_file_url("docs/read me.md", "abc123")
            == f"https://cursor.com/codebase/{REPO}/blob/abc123/docs/read%20me.md"
        )

    def test_a_file_url_at_a_branch_encodes_its_slash(self, provider: CursorOriginProvider) -> None:
        assert (
            provider.get_file_url("README.md", "release/test")
            == f"https://cursor.com/codebase/{REPO}/blob/release%2Ftest/README.md"
        )

    def test_a_history_url(self, provider: CursorOriginProvider) -> None:
        assert provider.get_commits_url("release/test") == f"https://cursor.com/codebase/{REPO}/commits/release%2Ftest"

    def test_a_filtered_history_url_is_refused(self, provider: CursorOriginProvider) -> None:
        with pytest.raises(ResourceBadRequest):
            provider.get_commits_url("abc123", file_path="src/app.py")

    def test_a_pull_request_url(self, provider: CursorOriginProvider) -> None:
        assert provider.get_pull_request_url("7") == f"https://cursor.com/codebase/{REPO}/pull/7"

    def test_a_commit_url(self, provider: CursorOriginProvider) -> None:
        assert provider.get_commit_url("abc123") == f"https://cursor.com/codebase/{REPO}/commit/abc123"


def _commit_raw(sha: str, date: str = "2026-08-01T09:30:00Z") -> dict[str, Any]:
    identity = {"name": "Jane Doe", "email": "jane@example.com", "date": date}
    return {
        "sha": sha,
        "commit": {
            "author": identity,
            "committer": identity,
            "message": "Add launch telemetry",
            "tree": {"sha": "tree123"},
        },
        "parents": [],
    }


class TestGetCommits:
    def test_commits_are_listed_from_a_ref(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response({"commits": [_commit_raw("abc123")], "nextPageToken": "t2"})

        result = provider.get_commits(ref="main", pagination={"cursor": "1", "per_page": 30})

        assert client.request.call_args.kwargs["path"] == f"/repos/{REPO}/commits"
        assert client.request.call_args.kwargs["params"] == {"sha": "main", "pageSize": "30"}
        assert result["data"] == [
            {
                "id": "abc123",
                "message": "Add launch telemetry",
                "author": {
                    "name": "Jane Doe",
                    "email": "jane@example.com",
                    "date": datetime(2026, 8, 1, 9, 30, tzinfo=UTC),
                },
                "additions": None,
                "deletions": None,
            }
        ]
        assert result["meta"]["next_cursor"] == "t2"

    def test_an_empty_date_is_absent(self) -> None:
        assert map_commit(_commit_raw("abc123", date=""))["author"] == {
            "name": "Jane Doe",
            "email": "jane@example.com",
            "date": None,
        }

    def test_every_page_is_followed_by_its_token(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.side_effect = [
            _response({"commits": [_commit_raw("a")], "nextPageToken": "t2"}),
            _response({"commits": [_commit_raw("b")], "nextPageToken": ""}),
        ]

        pages = list(iter_all_pages(lambda p: provider.get_commits(pagination=p), per_page=1))

        assert [page["data"][0]["id"] for page in pages] == ["a", "b"]
        assert client.request.call_args_list[1].kwargs["params"] == {"pageSize": "1", "pageToken": "t2"}
        assert pages[1]["meta"]["next_cursor"] is None

    def test_a_date_range_is_refused(self, provider: CursorOriginProvider) -> None:
        with pytest.raises(ResourceBadRequest):
            provider.get_commits(since=datetime(2026, 1, 1, tzinfo=UTC))


COMPARISON_RAW = {"status": "ahead", "aheadBy": 2, "behindBy": 0, "baseCommit": {}, "headCommit": {}}


class TestCompareCommits:
    def test_without_a_page_up_to_three_pages_of_files_are_read(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        """Origin's pages hold at most 100 files; GitHub's compare returns up to 300."""
        file = {"filename": "a.py", "status": "modified", "additions": 1, "deletions": 0, "changes": 1, "patch": "@@"}
        client.request.side_effect = [
            _response(COMPARISON_RAW),
            _response({"files": [file], "nextPageToken": "t2"}),
            _response({"files": [file], "nextPageToken": "t3"}),
            _response({"files": [file], "nextPageToken": "t4"}),
        ]

        result = provider.compare_commits("base123", "head123")

        _, first, second, third = client.request.call_args_list
        assert first.kwargs["params"] == {"pageSize": "100"}
        assert second.kwargs["params"] == {"pageSize": "100", "pageToken": "t2"}
        assert third.kwargs["params"] == {"pageSize": "100", "pageToken": "t3"}
        assert len(result["data"]["diff"]) == 3
        assert result["meta"]["next_cursor"] == "t4"

    def test_without_a_page_reading_stops_at_the_last_page(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        file = {"filename": "a.py", "status": "modified", "additions": 1, "deletions": 0, "changes": 1, "patch": "@@"}
        client.request.side_effect = [_response(COMPARISON_RAW), _response({"files": [file], "nextPageToken": ""})]

        result = provider.compare_commits("base123", "head123")

        assert client.request.call_count == 2
        assert len(result["data"]["diff"]) == 1

    def test_the_counts_and_the_changed_files_are_read(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.side_effect = [
            _response(COMPARISON_RAW),
            _response(
                {
                    "files": [
                        {
                            "filename": "src/new.py",
                            "status": "renamed",
                            "additions": 1,
                            "deletions": 0,
                            "changes": 1,
                            "patch": "@@ -1 +1 @@",
                            "previousFilename": "src/old.py",
                        },
                        {
                            "filename": "logo.png",
                            "status": "added",
                            "additions": 0,
                            "deletions": 0,
                            "changes": 0,
                            "patch": "",
                        },
                    ],
                    "nextPageToken": "t2",
                }
            ),
        ]

        result = provider.compare_commits("base123", "head123", pagination={"cursor": "1", "per_page": 50})

        summary, files = client.request.call_args_list
        assert summary.kwargs["path"] == f"/repos/{REPO}/compare/base123...head123"
        assert files.kwargs["path"] == f"/repos/{REPO}/compare/base123...head123/files"
        assert files.kwargs["params"] == {"pageSize": "50"}
        assert result["data"] == {
            "ahead_by": 2,
            "behind_by": 0,
            "commits": [],
            "diff": [
                {
                    "filename": "src/new.py",
                    "status": "renamed",
                    "patch": "@@ -1 +1 @@",
                    "additions": 1,
                    "deletions": 0,
                    "previous_filename": "src/old.py",
                },
                {
                    "filename": "logo.png",
                    "status": "added",
                    "patch": None,
                    "additions": 0,
                    "deletions": 0,
                    "previous_filename": None,
                },
            ],
        }
        assert result["meta"]["next_cursor"] == "t2"


class TestCommitDetail:
    def test_a_commit_is_read_with_its_changed_files(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.side_effect = [
            _response({**_commit_raw("abc123"), "stats": {"additions": 6, "deletions": 3, "total": 9}}),
            _response(
                {
                    "files": [
                        {
                            "filename": "src/app.py",
                            "status": "modified",
                            "additions": 6,
                            "deletions": 3,
                            "changes": 9,
                            "patch": "@@ -1 +1 @@",
                        }
                    ],
                    "nextPageToken": "",
                }
            ),
        ]

        result = provider.get_commit("abc123")

        commit, files = client.request.call_args_list
        assert commit.kwargs["path"] == f"/repos/{REPO}/commits/abc123"
        assert files.kwargs["path"] == f"/repos/{REPO}/commits/abc123/files"
        assert files.kwargs["params"] == {"pageSize": "100"}
        assert result["data"]["id"] == "abc123"
        assert (result["data"]["additions"], result["data"]["deletions"]) == (6, 3)
        assert result["data"]["files"] == [
            {
                "filename": "src/app.py",
                "status": "modified",
                "patch": "@@ -1 +1 @@",
                "additions": 6,
                "deletions": 3,
                "previous_filename": None,
            }
        ]

    def test_the_changed_files_are_paged(self, provider: CursorOriginProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = _response(
            {
                "files": [
                    {
                        "filename": "logo.png",
                        "status": "added",
                        "additions": 0,
                        "deletions": 0,
                        "changes": 0,
                        "patch": "",
                    }
                ],
                "nextPageToken": "t2",
            }
        )

        result = provider.get_commit_changes("abc123", pagination={"cursor": "1", "per_page": 30})

        assert client.request.call_args.kwargs["params"] == {"pageSize": "30"}
        assert result["data"][0]["patch"] is None
        assert result["meta"]["next_cursor"] == "t2"


class TestPullRequestDiff:
    def test_the_changed_files_are_listed(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response(
            {
                "files": [
                    {
                        "filename": "src/new.py",
                        "status": "renamed",
                        "additions": 1,
                        "deletions": 1,
                        "changes": 2,
                        "patch": "@@ -1 +1 @@",
                        "previousFilename": "src/old.py",
                    }
                ],
                "nextPageToken": "t2",
            }
        )

        result = provider.get_pull_request_files("7", pagination={"cursor": "1", "per_page": 100})

        assert client.request.call_args.kwargs["path"] == f"/repos/{REPO}/pulls/7/files"
        assert client.request.call_args.kwargs["params"] == {"pageSize": "100"}
        assert result["data"] == [
            {
                "filename": "src/new.py",
                "status": "renamed",
                "patch": "@@ -1 +1 @@",
                "changes": 2,
                "sha": "",
                "previous_filename": "src/old.py",
            }
        ]
        assert result["meta"]["next_cursor"] == "t2"

    def test_the_commits_are_listed(self, provider: CursorOriginProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = _response({"commits": [_commit_raw("abc123")], "nextPageToken": ""})

        result = provider.get_pull_request_commits("7")

        assert client.request.call_args.kwargs["path"] == f"/repos/{REPO}/pulls/7/commits"
        assert result["data"] == [
            {
                "sha": "abc123",
                "message": "Add launch telemetry",
                "author": {
                    "name": "Jane Doe",
                    "email": "jane@example.com",
                    "date": datetime(2026, 8, 1, 9, 30, tzinfo=UTC),
                },
            }
        ]
        assert result["meta"]["next_cursor"] is None


VERSION = {"number": "1", "headSha": "head123", "baseSha": "base123", "createdAt": "2026-08-01T09:30:00Z"}
USER = {"user": {"id": "user_01", "email": "jane@example.com", "handle": "jane"}}
APP = {"app": {"id": "app_01", "displayName": "Sentry"}}


def _comment_raw(
    comment_id: str, thread_id: str, path: str = "", author: dict[str, Any] = USER, **thread: Any
) -> dict[str, Any]:
    return {
        "id": comment_id,
        "thread": {
            "id": thread_id,
            "version": VERSION,
            "path": path,
            "startLine": 0,
            "endLine": 0,
            "createdAt": "2026-08-01T09:30:00Z",
            "updatedAt": "2026-08-01T09:30:00Z",
            **thread,
        },
        "body": f"Comment {comment_id}",
        "author": author,
        "createdAt": "2026-08-01T09:30:00Z",
        "updatedAt": "2026-08-01T09:31:00Z",
    }


class TestPullRequestComments:
    def test_only_the_general_discussion_is_listed_from_every_page(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        """A first page of inline comments alone must not end the listing."""
        client.request.side_effect = [
            _response(
                {
                    "comments": [_comment_raw("c1", "t1", path="src/app.py", side="right", startLine=3)],
                    "nextPageToken": "p2",
                }
            ),
            _response({"comments": [_comment_raw("c2", "g1")], "nextPageToken": ""}),
        ]

        result = provider.get_pull_request_comments("7", pagination={"cursor": "1", "per_page": 30})

        first, second = client.request.call_args_list
        assert first.kwargs["path"] == f"/repos/{REPO}/pulls/7/comments"
        assert second.kwargs["params"] == {"pageSize": "100", "pageToken": "p2"}
        assert result["data"] == [
            {
                "id": "c2",
                "body": "Comment c2",
                "author": {"id": "user_01", "username": "jane"},
                "created_at": "2026-08-01T09:30:00Z",
                "author_association": None,
            }
        ]
        assert result["meta"]["next_cursor"] is None


class TestReviewThreads:
    def test_comments_are_grouped_into_threads_across_pages(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        anchor: dict[str, Any] = {"path": "src/app.py", "side": "right", "startLine": 3, "endLine": 5}
        client.request.side_effect = [
            _response(
                {"comments": [_comment_raw("c1", "t1", **anchor), _comment_raw("c2", "g1")], "nextPageToken": "p2"}
            ),
            _response({"comments": [_comment_raw("c3", "t1", author=APP, **anchor)], "nextPageToken": ""}),
        ]

        result = provider.get_pull_request_review_threads("7")

        first, second = client.request.call_args_list
        assert first.kwargs["params"] == {"pageSize": "100"}
        assert second.kwargs["params"] == {"pageSize": "100", "pageToken": "p2"}
        assert result["meta"]["next_cursor"] is None
        [thread] = result["data"]
        assert thread["id"] == "t1"
        assert thread["file_path"] == "src/app.py"
        assert (thread["start_line"], thread["line"]) == (3, 5)
        assert thread["is_resolved"] is False
        assert [(c["id"], c["is_bot"], c["commit_sha"]) for c in thread["comments"]] == [
            ("c1", False, "head123"),
            ("c3", True, "head123"),
        ]

    def test_a_single_line_thread(self) -> None:
        [thread] = map_review_threads([_comment_raw("c1", "t1", path="a.py", side="right", startLine=3)])

        assert (thread["start_line"], thread["line"]) == (None, 3)

    def test_a_file_thread_has_no_lines(self) -> None:
        [thread] = map_review_threads([_comment_raw("c1", "t1", path="a.py", side="right")])

        assert (thread["start_line"], thread["line"]) == (None, None)

    def test_a_resolved_thread(self) -> None:
        [thread] = map_review_threads(
            [_comment_raw("c1", "t1", path="a.py", side="right", resolvedAt="2026-08-02T00:00:00Z")]
        )

        assert thread["is_resolved"] is True


class TestCommentWrites:
    def test_a_general_comment_opens_a_discussion(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response(_comment_raw("c1", "t1"))

        result = provider.create_pull_request_comment("7", "Looks good")

        assert client.request.call_args.kwargs["method"] == "POST"
        assert client.request.call_args.kwargs["path"] == f"/repos/{REPO}/pulls/7/comments"
        assert client.request.call_args.kwargs["data"] == {"body": "Looks good"}
        assert result["data"]["id"] == "c1"

    def test_a_review_comment_anchors_a_line_range(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response(
            _comment_raw("c1", "t1", path="src/app.py", side="right", startLine=3, endLine=5)
        )

        result = provider.create_review_comment(
            "7", "head123", "Fix this", "src/app.py", line={"head": 5}, start_line={"base": 2, "head": 3}
        )

        assert client.request.call_args.kwargs["data"] == {
            "body": "Fix this",
            "inline": {"path": "src/app.py", "side": "right", "startLine": 3, "endLine": 5},
        }
        assert result["data"]["file_path"] == "src/app.py"
        assert result["data"]["line"] == {"head": 5}
        assert result["data"]["start_line"] == {"head": 3}
        assert result["data"]["thread_id"] == "t1"
        assert result["data"]["commit_sha"] == "head123"

    def test_a_removed_line_anchors_the_left_side(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response(_comment_raw("c1", "t1", path="src/app.py", side="left", startLine=17))

        result = provider.create_review_comment("7", "head123", "Why?", "src/app.py", line={"base": 17})

        assert client.request.call_args.kwargs["data"]["inline"] == {
            "path": "src/app.py",
            "side": "left",
            "startLine": 17,
        }
        assert result["data"]["line"] == {"base": 17}
        assert result["data"]["start_line"] is None

    def test_a_comment_is_updated_by_its_id(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response({**_comment_raw("c1", "t1"), "thread": {"id": "t1"}})

        result = provider.update_review_comment("7", "c1", "Edited")

        assert client.request.call_args.kwargs["method"] == "PATCH"
        assert client.request.call_args.kwargs["path"] == f"/repos/{REPO}/pulls/comments/c1"
        assert client.request.call_args.kwargs["data"] == {"body": "Edited"}
        assert result["data"]["thread_id"] == "t1"
        assert result["data"]["line"] is None
        assert result["data"]["commit_sha"] is None

    def test_collapsing_resolves_the_thread(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.side_effect = [
            _response({**_comment_raw("c1", "t1"), "thread": {"id": "t1"}}),
            _response({"id": "t1"}),
        ]

        provider.update_and_collapse_pull_request_comment("7", "t1", "c1", "c1", "Outdated")

        update, resolve = client.request.call_args_list
        assert update.kwargs["path"] == f"/repos/{REPO}/pulls/comments/c1"
        assert resolve.kwargs["method"] == "PATCH"
        assert resolve.kwargs["path"] == f"/repos/{REPO}/pulls/threads/t1"
        assert resolve.kwargs["data"] == {"resolved": True}


class TestInlineAnchor:
    def test_a_range_on_one_side(self) -> None:
        assert inline_anchor("a.py", {"head": 5}, {"head": 3}) == {
            "path": "a.py",
            "side": "right",
            "startLine": 3,
            "endLine": 5,
        }

    def test_the_start_is_read_on_the_end_side(self) -> None:
        """A context line carries both numbers; the range uses the one on the end's side."""
        assert inline_anchor("a.py", {"base": 17}, {"base": 15, "head": 20}) == {
            "path": "a.py",
            "side": "left",
            "startLine": 15,
            "endLine": 17,
        }

    def test_a_start_on_the_other_side_anchors_the_end_alone(self) -> None:
        assert inline_anchor("a.py", {"head": 5}, {"base": 3}) == {"path": "a.py", "side": "right", "startLine": 5}

    def test_a_single_line(self) -> None:
        assert inline_anchor("a.py", {"base": 9}, None) == {"path": "a.py", "side": "left", "startLine": 9}


def _review_raw(**overrides: Any) -> dict[str, Any]:
    return {
        "id": "rev_01example",
        "author": USER,
        "verdict": "approve",
        "body": "Looks good",
        "submittedAt": "2026-08-01T09:30:00Z",
        "pullRequestVersion": {"number": "1", "headSha": "head123", "baseSha": "base123"},
        **overrides,
    }


class TestReviews:
    def test_submitted_reviews_are_listed(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response({"reviews": [_review_raw()], "nextPageToken": ""})

        result = provider.list_pull_request_reviews("7")

        assert client.request.call_args.kwargs["path"] == f"/repos/{REPO}/pulls/7/reviews"
        assert result["data"] == [
            {
                "id": "rev_01example",
                "html_url": f"https://cursor.com/codebase/{REPO}/pull/7",
                "state": "approved",
                "author": {"id": "user_01", "username": "jane"},
                "body": "Looks good",
                "submitted_at": "2026-08-01T09:30:00Z",
                "commit_id": "head123",
            }
        ]

    def test_a_dismissed_review(self, provider: CursorOriginProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = _response(
            {"reviews": [_review_raw(dismissal={"dismissedBy": USER})], "nextPageToken": ""}
        )

        assert provider.list_pull_request_reviews("7")["data"][0]["state"] == "dismissed"

    def test_a_review_carries_its_comments(
        self, provider: CursorOriginProvider, client: unittest.mock.MagicMock
    ) -> None:
        client.request.return_value = _response(_review_raw(verdict="request_changes"))

        result = provider.create_review(
            "7",
            "head123",
            "change_request",
            [
                {"path": "src/app.py", "body": "Fix this", "line": {"head": 5}, "start_line": {"head": 3}},
                {"path": "src/other.py", "body": "And this", "line": {"base": 9}},
                {"path": "src/whole.py", "body": "This file"},
            ],
            body="Some changes needed",
        )

        assert client.request.call_args.kwargs["method"] == "POST"
        assert client.request.call_args.kwargs["path"] == f"/repos/{REPO}/pulls/7/reviews"
        assert client.request.call_args.kwargs["data"] == {
            "verdict": "request_changes",
            "body": "Some changes needed",
            "comments": [
                {
                    "body": "Fix this",
                    "inline": {"path": "src/app.py", "side": "right", "startLine": 3, "endLine": 5},
                },
                {"body": "And this", "inline": {"path": "src/other.py", "side": "left", "startLine": 9}},
                {"body": "This file", "file": {"path": "src/whole.py"}},
            ],
        }
        assert result["data"]["state"] == "changes_requested"

    def test_a_review_without_a_body(self, provider: CursorOriginProvider, client: unittest.mock.MagicMock) -> None:
        client.request.return_value = _response(_review_raw(verdict="comment", body=""))

        result = provider.create_review("7", "head123", "comment", [])

        assert client.request.call_args.kwargs["data"] == {"verdict": "comment", "comments": []}
        assert result["data"]["body"] is None
        assert result["data"]["state"] == "commented"
