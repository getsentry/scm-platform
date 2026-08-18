"""Tests for the Cursor Origin provider.

These fake the ``ApiClient`` rather than the provider's own ``get``/``post``, because
almost everything that distinguishes this provider from the GitHub one lives in the
transport layer: how pagination is spelled, which parameters are forwarded, and which
are refused outright. Swapping out ``provider.get`` would skip exactly the code worth
testing.

Payloads are the shapes observed against the live API on sentry/nuget-trends, including
the awkward parts -- ``size`` as a string, absent zero-valued counts, and collections
that arrive as ``{}`` rather than as an empty list.
"""

import json
from datetime import UTC, datetime
from typing import Any, cast

import pytest
import requests

from scm.errors import (
    PathIsDirectory,
    PathIsNotDirectory,
    RateLimitExceeded,
    ReadmeNotFound,
    ResourceBadRequest,
    ResourceNotFound,
    ResourceUnauthorized,
    UnexpectedResponseFormat,
)
from scm.providers.cursor_origin.provider import (
    CURSOR_ORIGIN_WEB_BASE_URL,
    CursorOriginProvider,
    format_comment_location,
    map_actor,
    map_check_run,
    map_commit,
    map_commit_file,
    map_file_content,
    map_git_tree,
    map_repository,
    map_review_comment,
)
from scm.types import CoPilotChatExtension, CredentialsSet, Repository

# --------------------------------------------------------------------------- harness


def make_repository() -> Repository:
    return {
        "id": 1,
        "integration_id": 1,
        "name": "test-owner/test-repo",
        "organization_id": 1,
        "is_active": True,
        "external_id": "r_01test",
        "provider_name": "cursor_origin",
        "web_base_url": None,
    }


class FakeResponse:
    def __init__(self, payload: Any, *, status_code: int = 200, headers: dict[str, str] | None = None) -> None:
        self._payload = payload
        self.content = json.dumps(payload).encode()
        self.status_code = status_code
        self.headers = headers or {}
        self.request = type("R", (), {"headers": {}, "body": None, "url": "https://api.cursor.com", "method": "GET"})()

    def json(self) -> Any:
        return self._payload


class FakeApiClient:
    """Records every call and replays queued responses in order."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.responses: list[FakeResponse] = []

    def queue(self, payload: Any, *, status_code: int = 200, headers: dict[str, str] | None = None) -> None:
        self.responses.append(FakeResponse(payload, status_code=status_code, headers=headers))

    # Spelled out rather than **kwargs so the fake actually satisfies the ApiClient
    # protocol -- a fake that is looser than the real thing hides signature drift.
    def request(
        self,
        method: str,
        path: str,
        headers: dict[str, str] | None = None,
        data: dict[str, Any] | None = None,
        params: dict[str, str] | None = None,
        allow_redirects: bool | None = None,
        stream: bool = True,
        raw_response: bool = True,
        credentials_set: CredentialsSet = "installation",
        timeout: float | tuple[float, float] | None = None,
    ) -> requests.Response:
        self.calls.append(
            {
                "method": method,
                "path": path,
                "headers": headers,
                "data": data,
                "params": params,
                "allow_redirects": allow_redirects,
                "stream": stream,
                "raw_response": raw_response,
                "credentials_set": credentials_set,
                "timeout": timeout,
            }
        )
        if not self.responses:
            raise AssertionError(f"No queued response for {method} {path}")
        return cast(requests.Response, self.responses.pop(0))

    @property
    def last(self) -> dict[str, Any]:
        return self.calls[-1]

    @property
    def paths(self) -> list[str]:
        return [c["path"] for c in self.calls]


def make_provider(**kwargs: Any) -> tuple[CursorOriginProvider, FakeApiClient]:
    client = FakeApiClient()
    return CursorOriginProvider(client, organization_id=1, repository=make_repository(), **kwargs), client


# --------------------------------------------------------------------------- payloads

REPOSITORY = {
    "id": "r_01test",
    "name": "test-repo",
    "fullName": "test-owner/test-repo",
    "defaultBranch": "main",
    "cloneUrl": "https://origin.cursor.com/test-owner/test-repo.git",
    "owner": {"slug": "test-owner", "id": "ns_01test"},
    "pushedAt": "2026-08-17T21:28:49.503Z",
}

# Two commits, the newer one with no deletions -- Origin omits zero-valued keys rather
# than sending 0.
COMMIT_HEAD = {
    "sha": "a" * 40,
    "commit": {
        "author": {"name": "Ada", "email": "ada@example.com", "date": "2026-03-15T22:29:12-04:00"},
        "committer": {"name": "Ada", "email": "ada@example.com", "date": "2026-03-15T22:29:12-04:00"},
        "message": "add a thing",
        "tree": {"sha": "t" * 40},
    },
    "parents": [{"sha": "b" * 40}],
    "stats": {"additions": 167, "total": 167},
}

COMMIT_PARENT = {
    "sha": "b" * 40,
    "commit": {
        "author": {"name": "Ada", "email": "ada@example.com", "date": "2026-03-15T20:37:14-04:00"},
        "message": "bump deps",
        "tree": {"sha": "u" * 40},
    },
    "parents": [{"sha": "c" * 40}],
    "stats": {"additions": 16, "deletions": 17, "total": 33},
}

PULL_REQUEST = {
    "id": "pr_01test",
    # Origin serializes 64-bit integers as JSON strings.
    "number": "1",
    "state": "open",
    "draft": False,
    "merged": False,
    "title": "a pull request",
    "body": "with a body",
    "head": {"ref": "topic", "sha": "a" * 40},
    "base": {"ref": "main", "sha": "b" * 40},
    "author": {"app": {"id": "app_01test", "slug": "test-app"}},
    "createdAt": "2026-08-17T22:17:08.063Z",
}

CHECK_RUN = {
    "id": "cr_01test",
    "sha": "a" * 40,
    "key": "seer/review",
    "name": "seer/review",
    "externalId": "seer/review",
    "status": "in_progress",
    "checkSuite": {"id": "crg_01test"},
}


# --------------------------------------------------------------------------- transport


def test_pagination_uses_page_size_and_page_token() -> None:
    provider, client = make_provider()
    client.queue({"commits": [COMMIT_HEAD], "nextPageToken": "tok_next"})

    result = provider.get_commits(pagination={"per_page": 25, "cursor": "tok_prev"})

    assert client.last["params"] == {"pageSize": "25", "pageToken": "tok_prev"}
    assert result["meta"]["next_cursor"] == "tok_next"


def test_page_size_is_clamped_to_the_server_maximum() -> None:
    provider, client = make_provider()
    client.queue({"commits": []})

    provider.get_commits(pagination={"per_page": 500})

    assert client.last["params"]["pageSize"] == "100"


def test_iter_all_pages_sentinel_cursor_is_dropped() -> None:
    """``iter_all_pages`` seeds cursor="1", a GitHub page number Origin would reject."""
    provider, client = make_provider()
    client.queue({"commits": [COMMIT_HEAD]})

    provider.get_commits(pagination={"per_page": 10, "cursor": "1"})

    assert "pageToken" not in client.last["params"]
    assert client.last["params"]["pageSize"] == "10"


def test_absent_next_page_token_ends_pagination() -> None:
    provider, client = make_provider()
    client.queue({"commits": [COMMIT_HEAD]})

    assert provider.get_commits()["meta"]["next_cursor"] is None


def test_empty_collection_arrives_as_an_empty_object() -> None:
    """Origin omits the collection key entirely rather than sending []."""
    provider, client = make_provider()
    client.queue({})

    result = provider.get_pull_requests()

    assert result["data"] == []
    assert result["meta"]["next_cursor"] is None


def test_conditional_request_forwards_if_none_match() -> None:
    provider, client = make_provider()
    client.queue({"ref": "refs/heads/main", "object": {"sha": "a" * 40}})

    provider.get_branch("main", request_options={"if_none_match": '"abc"'})

    assert client.last["headers"]["If-None-Match"] == '"abc"'


def test_etag_is_surfaced_in_meta() -> None:
    provider, client = make_provider()
    client.queue({"ref": "refs/heads/main", "object": {"sha": "a" * 40}}, headers={"ETag": '"a"'})

    assert provider.get_branch("main")["meta"]["etag"] == '"a"'


@pytest.mark.parametrize(
    ("status_code", "expected"),
    [
        (401, ResourceUnauthorized),
        (404, ResourceNotFound),
        (429, RateLimitExceeded),
    ],
)
def test_error_statuses_raise_their_mapped_class(status_code: int, expected: type[Exception]) -> None:
    provider, client = make_provider()
    client.queue({"code": 5, "message": "nope", "details": []}, status_code=status_code)

    with pytest.raises(expected):
        provider.get_repository()


def test_app_endpoints_use_application_credentials() -> None:
    """``/app`` rejects an installation token with a 403."""
    provider, client = make_provider()
    client.queue({"id": "app_01test", "slug": "test-app", "displayName": "Test App"})

    actor = provider.get_authenticated_actor()

    assert client.last["credentials_set"] == "application"
    assert actor["data"] == {"id": "app_01test", "username": "test-app"}


# ------------------------------------------------------- refusals (silently-ignored params)


def test_get_commits_rejects_date_filters() -> None:
    """Origin ignores since/until, so forwarding them would return the wrong page."""
    provider, client = make_provider()

    with pytest.raises(ResourceBadRequest, match="does not support filtering commits by date"):
        provider.get_commits(since=datetime(2020, 1, 1, tzinfo=UTC))
    with pytest.raises(ResourceBadRequest):
        provider.get_commits(until=datetime(2020, 1, 1, tzinfo=UTC))

    assert client.calls == []


def test_get_commits_spells_the_ref_parameter_sha() -> None:
    """``ref`` is accepted and ignored by Origin; ``sha`` is the one that works."""
    provider, client = make_provider()
    client.queue({"commits": []})

    provider.get_commits(ref="topic")

    assert client.last["params"] == {"sha": "topic"}


def test_create_pull_request_comment_rejects_copilot_extensions() -> None:
    provider, client = make_provider()

    with pytest.raises(ResourceBadRequest, match="Copilot chat extensions"):
        provider.create_pull_request_comment("1", "body", [CoPilotChatExtension(name="n", prompt="p")])

    assert client.calls == []


# --------------------------------------------------------------------------- contents


def test_get_file_content_passes_path_as_a_query_parameter() -> None:
    """The documented /contents/{path} form 404s; the query form is the real one."""
    provider, client = make_provider()
    client.queue(
        {
            "type": "file",
            "encoding": "base64",
            "size": "2534",
            "name": "README.md",
            "path": "README.md",
            "sha": "f" * 40,
            "content": "aGk=",
        }
    )

    result = provider.get_file_content("README.md", ref="main")

    assert client.last["path"] == "/repos/test-owner/test-repo/contents"
    assert client.last["params"] == {"path": "README.md", "ref": "main"}
    assert result["data"]["size"] == 2534  # the string is coerced
    assert result["data"]["type"] == "file"


def test_get_file_content_on_a_directory_raises() -> None:
    provider, client = make_provider()
    client.queue({"type": "dir", "sha": "d" * 40, "entries": []})

    with pytest.raises(PathIsDirectory):
        provider.get_file_content("src", ref="main")


def test_get_directory_contents_maps_entries_and_never_paginates() -> None:
    provider, client = make_provider()
    client.queue(
        {
            "type": "dir",
            "sha": "d" * 40,
            "entries": [
                {"type": "dir", "name": "sub", "path": "src/sub", "sha": "e" * 40},
                {"type": "file", "name": "a.cs", "path": "src/a.cs", "sha": "f" * 40},
            ],
        }
    )

    result = provider.get_directory_contents("src", ref="main")

    assert [e["type"] for e in result["data"]] == ["directory", "file"]
    # Directory entries carry no size.
    assert [e["size"] for e in result["data"]] == [0, 0]
    assert result["meta"]["next_cursor"] is None


def test_get_directory_contents_on_a_file_raises() -> None:
    provider, client = make_provider()
    client.queue({"type": "file", "path": "README.md", "sha": "f" * 40, "size": "1"})

    with pytest.raises(PathIsNotDirectory):
        provider.get_directory_contents("README.md")


def test_get_directory_contents_omits_an_empty_path() -> None:
    """The root listing is the contents endpoint with no path at all."""
    provider, client = make_provider()
    client.queue({"type": "dir", "sha": "d" * 40, "entries": []})

    provider.get_directory_contents("", ref="main")

    assert client.last["params"] == {"ref": "main"}


def test_get_readme_finds_the_file_by_name() -> None:
    provider, client = make_provider()
    client.queue(
        {
            "type": "dir",
            "sha": "d" * 40,
            "entries": [
                {"type": "file", "name": "LICENSE", "path": "LICENSE", "sha": "1" * 40},
                {"type": "file", "name": "README.md", "path": "README.md", "sha": "2" * 40},
            ],
        }
    )
    client.queue(
        {"type": "file", "encoding": "base64", "size": "3", "path": "README.md", "sha": "2" * 40, "content": "aGk="}
    )

    result = provider.get_readme("main")

    assert result["data"]["path"] == "README.md"
    assert client.calls[1]["params"] == {"path": "README.md", "ref": "main"}


def test_get_readme_without_one_raises() -> None:
    provider, client = make_provider()
    client.queue({"type": "dir", "sha": "d" * 40, "entries": [{"type": "file", "name": "a.cs", "path": "a.cs"}]})

    with pytest.raises(ReadmeNotFound):
        provider.get_readme("main")


# --------------------------------------------------------------------------- trees


def test_get_tree_sets_recursive() -> None:
    provider, client = make_provider()
    client.queue({"sha": "t" * 40, "tree": []})

    provider.get_tree("main")

    assert client.last["params"] == {"recursive": "1"}


def test_get_tree_omits_recursive_entirely_when_not_recursing() -> None:
    """Origin treats any non-empty value as true -- including the string "false"."""
    provider, client = make_provider()
    client.queue({"sha": "t" * 40, "tree": []})

    provider.get_tree("main", recursive=False)

    assert client.last["params"] == {}


def test_get_tree_reports_untruncated_without_the_flag() -> None:
    provider, client = make_provider()
    client.queue(
        {
            "sha": "t" * 40,
            "tree": [
                {"path": "a.cs", "mode": "100644", "type": "blob", "sha": "1" * 40, "size": 10},
                {"path": "src", "mode": "040000", "type": "tree", "sha": "2" * 40},
            ],
        }
    )

    result = provider.get_tree("main")

    assert result["data"]["truncated"] is False
    assert result["data"]["tree"][0]["size"] == 10
    assert result["data"]["tree"][1]["size"] is None
    assert result["meta"]["next_cursor"] is None


# --------------------------------------------------------------------------- commits


def test_get_branch_reads_the_git_ref_endpoint() -> None:
    """There is no /branches/{name} route, and /branches cannot be filtered."""
    provider, client = make_provider()
    client.queue({"ref": "refs/heads/topic/nested", "object": {"sha": "a" * 40, "type": "commit"}})

    result = provider.get_branch("topic/nested")

    assert client.last["path"] == "/repos/test-owner/test-repo/git/ref/heads/topic/nested"
    assert result["data"] == {"ref": "topic/nested", "sha": "a" * 40}


def test_get_commit_walks_the_separate_file_endpoint() -> None:
    provider, client = make_provider()
    client.queue(COMMIT_HEAD)
    client.queue(
        {"files": [{"filename": "a.cs", "status": "added", "additions": 4, "changes": 4}], "nextPageToken": "t2"}
    )
    client.queue({"files": [{"filename": "b.cs", "status": "modified", "additions": 1, "deletions": 2, "changes": 3}]})

    result = provider.get_commit("a" * 40)

    assert client.paths == [
        "/repos/test-owner/test-repo/commits/" + "a" * 40,
        "/repos/test-owner/test-repo/commits/" + "a" * 40 + "/files",
        "/repos/test-owner/test-repo/commits/" + "a" * 40 + "/files",
    ]
    assert client.calls[2]["params"]["pageToken"] == "t2"
    files = result["data"]["files"]
    assert files is not None
    assert [f["filename"] for f in files] == ["a.cs", "b.cs"]
    # An absent deletions key means zero, not unknown.
    assert files[0]["deletions"] == 0
    assert files[1]["deletions"] == 2


def test_compare_commits_walks_back_to_the_merge_base() -> None:
    provider, client = make_provider()
    client.queue(
        {
            "status": "ahead",
            "aheadBy": 1,
            "behindBy": 0,
            "baseCommit": {"sha": "b" * 40},
            "headCommit": {"sha": "a" * 40},
            "mergeBaseCommit": {"sha": "b" * 40},
        }
    )
    client.queue({"commits": [COMMIT_HEAD, COMMIT_PARENT], "nextPageToken": "more"})

    result = provider.compare_commits("b" * 40, "a" * 40)

    assert result["data"]["ahead_by"] == 1
    assert result["data"]["behind_by"] == 0
    # The walk stops at the merge base and does not include it.
    assert [c["id"] for c in result["data"]["commits"]] == ["a" * 40]
    # Origin offered another page, but the comparison is complete.
    assert result["meta"]["next_cursor"] is None
    # No endpoint reports the files changed between two arbitrary commits.
    assert result["data"]["diff"] == []


def test_compare_commits_keeps_paging_when_the_merge_base_is_further_back() -> None:
    provider, client = make_provider()
    client.queue(
        {
            "aheadBy": 9,
            "behindBy": 0,
            "baseCommit": {"sha": "z" * 40},
            "headCommit": {"sha": "a" * 40},
            "mergeBaseCommit": {"sha": "z" * 40},
        }
    )
    client.queue({"commits": [COMMIT_HEAD, COMMIT_PARENT], "nextPageToken": "more"})

    result = provider.compare_commits("z" * 40, "a" * 40)

    assert [c["id"] for c in result["data"]["commits"]] == ["a" * 40, "b" * 40]
    assert result["meta"]["next_cursor"] == "more"


# ---------------------------------------------------------------------- pull requests


def test_create_pull_request_draft_sets_the_draft_flag() -> None:
    provider, client = make_provider()
    client.queue({**PULL_REQUEST, "draft": True})

    result = provider.create_pull_request_draft("t", "b", "topic", "main")

    assert client.last["data"] == {"title": "t", "body": "b", "head": "topic", "base": "main", "draft": True}
    assert result["data"]["id"] == "1"


def test_marking_ready_for_review_is_a_single_patch() -> None:
    """draft is a plain field, so unlike GitHub there is no read-first and no mutation."""
    provider, client = make_provider()
    client.queue(PULL_REQUEST)

    provider.mark_pull_request_ready_for_review("1")

    assert len(client.calls) == 1
    assert client.last["method"] == "PATCH"
    assert client.last["data"] == {"draft": False}


def test_pull_request_maps_to_a_locally_built_web_url() -> None:
    provider, client = make_provider()
    client.queue(PULL_REQUEST)

    result = provider.get_pull_request("1")

    assert result["data"]["html_url"] == "https://cursor.com/codebase/test-owner/test-repo/pull/1"
    assert result["data"]["author"] == {"id": "app_01test", "username": "test-app"}


def test_create_review_sends_a_verdict() -> None:
    provider, client = make_provider()
    client.queue(
        {
            "id": "rev_01test",
            "verdict": "request_changes",
            "body": "please fix",
            "submittedAt": "2026-08-17T22:17:33.878Z",
            "author": {"app": {"id": "app_01test", "slug": "test-app"}},
            "pullRequestVersion": {"number": "1", "headSha": "a" * 40},
        }
    )

    result = provider.create_review("1", "ignored-sha", "change_request", [], body="please fix")

    assert client.last["data"] == {"verdict": "request_changes", "body": "please fix"}
    assert result["data"]["state"] == "changes_requested"
    assert result["data"]["commit_id"] == "a" * 40


# ------------------------------------------------- review comments (degraded)

COMMENT = {
    "id": "cmt_01test",
    "body": "posted body",
    "author": {"app": {"id": "app_01test", "slug": "test-app"}},
    "createdAt": "2026-08-17T22:17:32.816Z",
    "thread": {"id": "thr_01test"},
}


def test_create_review_comment_states_its_location_in_the_body() -> None:
    """Origin has no diff anchor, so the location has to survive as text."""
    provider, client = make_provider()
    client.queue(COMMENT)

    result = provider.create_review_comment("1", "a" * 40, "this leaks", "src/a.cs", {"head": 42})

    assert client.last["path"] == "/repos/test-owner/test-repo/pulls/1/comments"
    assert client.last["data"]["body"] == "**`src/a.cs`** line 42\n\nthis leaks"
    # The location echoes the request; Origin is not holding an anchor.
    assert result["data"]["file_path"] == "src/a.cs"
    assert result["data"]["line"] == {"head": 42}
    assert result["data"]["thread_id"] == "thr_01test"


def test_create_review_comment_labels_a_base_side_line() -> None:
    provider, client = make_provider()
    client.queue(COMMENT)

    provider.create_review_comment("1", "a" * 40, "gone", "src/a.cs", {"base": 17})

    assert client.last["data"]["body"].startswith("**`src/a.cs`** line 17 (before)")


def test_create_review_comment_renders_a_line_range() -> None:
    provider, client = make_provider()
    client.queue(COMMENT)

    provider.create_review_comment("1", "a" * 40, "b", "src/a.cs", {"head": 20}, start_line={"head": 10})

    assert client.last["data"]["body"].startswith("**`src/a.cs`** lines 10-20")


def test_create_review_comment_file_omits_a_line() -> None:
    provider, client = make_provider()
    client.queue(COMMENT)

    result = provider.create_review_comment_file("1", "a" * 40, "whole file", "src/a.cs", "head")

    assert client.last["data"]["body"] == "**`src/a.cs`**\n\nwhole file"
    assert result["data"]["line"] is None


def test_the_location_header_carries_no_link() -> None:
    """The blob URL shape is unverified, and a dead link is worse than a searchable path."""
    provider, client = make_provider()
    client.queue(COMMENT)

    provider.create_review_comment("1", "a" * 40, "b", "src/a.cs", {"head": 1})

    assert "http" not in client.last["data"]["body"]


def test_create_review_comment_reply_resolves_the_thread_first() -> None:
    provider, client = make_provider()
    client.queue(COMMENT)  # the parent, read for its thread id
    client.queue({**COMMENT, "id": "cmt_02"})

    result = provider.create_review_comment_reply("1", "replying", "cmt_01test")

    assert [c["method"] for c in client.calls] == ["GET", "POST"]
    assert client.calls[0]["path"] == "/repos/test-owner/test-repo/pulls/comments/cmt_01test"
    assert client.calls[1]["data"] == {"body": "replying", "threadId": "thr_01test"}
    assert result["data"]["id"] == "cmt_02"


def test_create_review_comment_reply_without_a_thread_raises() -> None:
    provider, client = make_provider()
    client.queue({"id": "cmt_01test", "body": "x"})

    with pytest.raises(UnexpectedResponseFormat):
        provider.create_review_comment_reply("1", "replying", "cmt_01test")


def test_update_review_comment_patches_in_place() -> None:
    provider, client = make_provider()
    client.queue({**COMMENT, "body": "edited"})

    result = provider.update_review_comment("1", "cmt_01test", "edited")

    assert client.last["method"] == "PATCH"
    assert client.last["path"] == "/repos/test-owner/test-repo/pulls/comments/cmt_01test"
    assert result["data"]["body"] == "edited"


def test_create_review_posts_the_verdict_then_each_finding() -> None:
    provider, client = make_provider()
    client.queue(
        {
            "id": "rev_01test",
            "verdict": "comment",
            "author": {"app": {"id": "app_01test", "slug": "test-app"}},
            "pullRequestVersion": {"headSha": "a" * 40},
        }
    )
    client.queue(COMMENT)
    client.queue(COMMENT)

    result = provider.create_review(
        "1",
        "a" * 40,
        "comment",
        [
            {"path": "src/a.cs", "body": "first", "line": {"head": 4}},
            {"path": "src/b.cs", "body": "second"},
        ],
        body="overall",
    )

    # The verdict lands first: a partial failure should leave a review missing detail,
    # not findings with no review attached.
    assert client.paths == [
        "/repos/test-owner/test-repo/pulls/1/reviews",
        "/repos/test-owner/test-repo/pulls/1/comments",
        "/repos/test-owner/test-repo/pulls/1/comments",
    ]
    assert client.calls[0]["data"] == {"verdict": "comment", "body": "overall"}
    assert client.calls[1]["data"]["body"] == "**`src/a.cs`** line 4\n\nfirst"
    assert client.calls[2]["data"]["body"] == "**`src/b.cs`**\n\nsecond"
    assert result["data"]["id"] == "rev_01test"


def test_create_review_without_comments_makes_one_request() -> None:
    provider, client = make_provider()
    client.queue({"id": "rev_01test", "verdict": "approve"})

    provider.create_review("1", "a" * 40, "approve", [])

    assert len(client.calls) == 1


@pytest.mark.parametrize(
    ("path", "line", "start_line", "expected"),
    [
        ("a.py", None, None, "**`a.py`**"),
        ("a.py", {"head": 3}, None, "**`a.py`** line 3"),
        ("a.py", {"base": 3}, None, "**`a.py`** line 3 (before)"),
        ("a.py", {"base": 3, "head": 9}, None, "**`a.py`** line 9"),
        ("a.py", {"head": 9}, {"head": 4}, "**`a.py`** lines 4-9"),
        ("a.py", {}, None, "**`a.py`** line ?"),
    ],
)
def test_format_comment_location(path: str, line: Any, start_line: Any, expected: str) -> None:
    assert format_comment_location(path, line, start_line) == expected


def test_map_review_comment_leaves_the_location_unset_when_read_back() -> None:
    """A comment fetched rather than written carries its location only in body text."""
    mapped = map_review_comment(COMMENT)

    assert mapped["file_path"] is None
    assert mapped["line"] is None
    assert mapped["thread_id"] == "thr_01test"
    assert mapped["author"] == {"id": "app_01test", "username": "test-app"}


# ------------------------------------------------------------------------ check runs


def test_create_check_run_builds_a_suite_alongside_the_run() -> None:
    provider, client = make_provider()
    client.queue({"checkSuite": {"id": "crg_01test"}, "checkRun": CHECK_RUN})

    result = provider.create_check_run("seer/review", "a" * 40, status="running", external_id="ext-1")

    sent = client.last["data"]
    assert sent["headSha"] == "a" * 40
    assert sent["checkSuite"] == {"key": "ext-1", "name": "seer/review", "externalId": "ext-1"}
    assert sent["checkRun"]["key"] == "ext-1"
    assert sent["checkRun"]["status"] == "in_progress"
    # Mandatory, and what Origin uses to order concurrent writes.
    assert sent["checkRun"]["externalUpdatedAt"].endswith("Z")
    assert result["data"]["status"] == "running"


def test_create_check_run_falls_back_to_the_name_as_key() -> None:
    provider, client = make_provider()
    client.queue({"checkSuite": {"id": "crg_01test"}, "checkRun": CHECK_RUN})

    provider.create_check_run("seer/review", "a" * 40)

    assert client.last["data"]["checkRun"]["key"] == "seer/review"
    assert client.last["data"]["checkRun"]["status"] == "queued"


def test_update_check_run_reads_back_then_upserts() -> None:
    """There is no PATCH; an update is a re-POST needing the run's and suite's keys."""
    provider, client = make_provider()
    client.queue(CHECK_RUN)
    client.queue({"id": "crg_01test", "key": "suite-key", "name": "suite", "externalId": "suite-ext"})
    client.queue(
        {"checkSuite": {"id": "crg_01test"}, "checkRun": {**CHECK_RUN, "status": "completed", "conclusion": "success"}}
    )

    result = provider.update_check_run("cr_01test", status="completed", conclusion="success")

    assert [c["method"] for c in client.calls] == ["GET", "GET", "POST"]
    sent = client.calls[2]["data"]
    assert sent["headSha"] == "a" * 40
    assert sent["checkSuite"]["key"] == "suite-key"
    assert sent["checkRun"]["key"] == "seer/review"
    assert sent["checkRun"]["conclusion"] == "success"
    assert result["data"]["conclusion"] == "success"


def test_update_check_run_without_a_suite_id_raises() -> None:
    provider, client = make_provider()
    client.queue({**CHECK_RUN, "checkSuite": {}})

    with pytest.raises(UnexpectedResponseFormat):
        provider.update_check_run("cr_01test", status="completed")


def test_check_run_lists_filter_client_side() -> None:
    """Origin's check-run lists take no filters, so they are applied after the fetch."""
    provider, client = make_provider()
    client.queue(
        {
            "checkRuns": [
                CHECK_RUN,
                {**CHECK_RUN, "id": "cr_02", "name": "other", "status": "completed"},
            ],
            "nextPageToken": "more",
        }
    )

    result = provider.list_check_runs_for_ref("a" * 40, check_name="other")

    assert client.last["params"] == {}
    assert [r["id"] for r in result["data"]] == ["cr_02"]
    # Filtering can empty a page while later pages still hold matches.
    assert result["meta"]["next_cursor"] == "more"


# --------------------------------------------------------------------------- web urls


def test_web_urls_are_built_from_the_codebase_root() -> None:
    provider, _ = make_provider()

    assert provider.get_pull_request_url("7") == "https://cursor.com/codebase/test-owner/test-repo/pull/7"
    assert provider.get_commit_url("abc") == "https://cursor.com/codebase/test-owner/test-repo/commit/abc"
    assert (
        provider.get_file_url("src/a.cs", "abc", 10, 20)
        == "https://cursor.com/codebase/test-owner/test-repo/blob/abc/src/a.cs#L10-L20"
    )
    assert (
        provider.get_file_url("src/a.cs", "abc") == "https://cursor.com/codebase/test-owner/test-repo/blob/abc/src/a.cs"
    )


def test_the_default_web_base_matches_sentrys_constant() -> None:
    """Sentry hands its own CURSOR_ORIGIN_WEB_BASE_URL through as web_base_url; if the two
    disagreed about whether /codebase is included, links would double it up."""
    provider, _ = make_provider(web_base_url=CURSOR_ORIGIN_WEB_BASE_URL)

    assert provider.get_pull_request_url("7") == "https://cursor.com/codebase/test-owner/test-repo/pull/7"


def test_trailing_slash_on_the_web_base_is_tolerated() -> None:
    provider, _ = make_provider(web_base_url="https://cursor.com/codebase/")

    assert provider.get_pull_request_url("7") == "https://cursor.com/codebase/test-owner/test-repo/pull/7"


# --------------------------------------------------------------------------- mappers


def test_map_repository_defaults_the_fields_origin_omits() -> None:
    mapped = map_repository(REPOSITORY)

    assert mapped["full_name"] == "test-owner/test-repo"
    # Origin reports no visibility; unknown is treated as private.
    assert mapped["private"] is True
    # No size hint anywhere, which is why language detection weighs tree blob sizes.
    assert mapped["size"] == 0
    assert mapped["description"] is None
    assert mapped["topics"] == []


def test_map_commit_leaves_stats_none_when_absent() -> None:
    """The list endpoint carries no stats at all; absent is unknown, not zero."""
    mapped = map_commit({k: v for k, v in COMMIT_HEAD.items() if k != "stats"})

    assert mapped["additions"] is None
    assert mapped["deletions"] is None
    # Origin attributes commits to no account, so the login keys stay unset.
    assert "author_login" not in mapped
    assert "committer_login" not in mapped


def test_map_commit_reads_present_stats() -> None:
    mapped = map_commit(COMMIT_PARENT)

    assert (mapped["additions"], mapped["deletions"]) == (16, 17)
    assert mapped["author"] == {
        "name": "Ada",
        "email": "ada@example.com",
        "date": datetime.fromisoformat("2026-03-15T20:37:14-04:00"),
    }


def test_map_commit_file_treats_an_unknown_status_as_unknown() -> None:
    mapped = map_commit_file({"filename": "a.cs", "status": "teleported", "changes": 1})

    assert mapped["status"] == "unknown"
    assert mapped["patch"] is None


@pytest.mark.parametrize(
    ("actor", "expected"),
    [
        ({"app": {"id": "app_1", "slug": "seer"}}, {"id": "app_1", "username": "seer"}),
        ({"user": {"id": "u_1", "email": "a@b.c"}}, {"id": "u_1", "username": "a@b.c"}),
        ({"serviceAccount": {"id": "sa_1"}}, {"id": "sa_1", "username": "sa_1"}),
        (None, None),
        ({}, None),
    ],
)
def test_map_actor_handles_every_arm_of_the_union(
    actor: dict[str, Any] | None, expected: dict[str, str] | None
) -> None:
    assert map_actor(actor) == expected


def test_map_file_content_coerces_a_string_size() -> None:
    mapped = map_file_content({"path": "a.md", "sha": "f" * 40, "size": "2534", "type": "file"})

    assert mapped["size"] == 2534


def test_map_git_tree_handles_a_missing_tree_key() -> None:
    assert map_git_tree({"sha": "t" * 40}) == {"sha": "t" * 40, "tree": [], "truncated": False}


@pytest.mark.parametrize(
    ("raw_status", "raw_conclusion", "expected"),
    [
        ("queued", None, ("pending", None)),
        ("in_progress", None, ("running", None)),
        ("completed", "success", ("completed", "success")),
        # Origin's only conclusion GitHub does not share.
        ("completed", "stale", ("completed", "unknown")),
        ("something-new", None, ("pending", None)),
    ],
)
def test_map_check_run_normalizes_status_and_conclusion(
    raw_status: str, raw_conclusion: str | None, expected: tuple[str, str | None]
) -> None:
    mapped = map_check_run({"id": "cr_1", "name": "n", "status": raw_status, "conclusion": raw_conclusion})

    assert (mapped["status"], mapped["conclusion"]) == expected
    # detailsUrl is caller-supplied and we never set it.
    assert mapped["html_url"] == ""
