import datetime
import unittest.mock
from typing import Any

import pytest

from scm.errors import (
    MalformedExternalId,
    ResourceForbidden,
    ResourceNotFound,
    ResourceUnprocessableContent,
)
from scm.facade import Facade
from scm.providers.gitea.provider import (
    GITEA_MAX_PAGE_SIZE,
    ApiClient,
    GiteaProvider,
    _latest_status_per_context,
    _strip_wip_prefix,
    map_check_run,
    map_commit,
    map_pull_request,
    map_pull_request_file,
    map_reaction,
    map_review_comment,
)
from scm.types import ALL_PROTOCOLS, PaginationParams, Repository

WEB_BASE_URL = "https://gitea.example.com/gitea"


@pytest.fixture
def client() -> ApiClient:
    return unittest.mock.MagicMock(_name="client")


def _make_repository(*, name: str = "acme/widgets") -> Repository:
    return Repository(
        id=1,
        integration_id=1,
        name=name,
        organization_id=1,
        is_active=True,
        external_id="gitea.example.com:42",
        provider_name="gitea",
        web_base_url=WEB_BASE_URL,
    )


def _make_gitea_provider(client: ApiClient, *, name: str = "acme/widgets") -> GiteaProvider:
    return GiteaProvider(
        client=client,
        organization_id=1,
        repository=_make_repository(name=name),
        web_base_url=WEB_BASE_URL,
    )


@pytest.fixture
def provider(client: ApiClient) -> GiteaProvider:
    return _make_gitea_provider(client)


def _mock_response(json_data: Any, *, status_code: int = 200, headers: dict[str, str] | None = None):
    response = unittest.mock.MagicMock()
    response.json.return_value = json_data
    response.status_code = status_code
    response.headers = headers or {}
    response.content = b"{}"
    return response


# Construction


def test_requires_owner_and_name(client) -> None:
    """A bare repository name would build a URL one segment short and hit an unrelated route."""
    for bad in ("widgets", "acme/widgets/extra", "acme/", "/widgets"):
        with pytest.raises(MalformedExternalId):
            _make_gitea_provider(client, name=bad)


def test_requires_web_base_url(client) -> None:
    with pytest.raises(MalformedExternalId):
        GiteaProvider(client, 1, _make_repository(), web_base_url="")


def test_build_url_preserves_sub_path_install(provider: GiteaProvider) -> None:
    """Gitea's ROOT_URL is free-form, so a sub-path install must survive into every API URL."""
    assert provider.build_url("/repos/acme/widgets") == "https://gitea.example.com/gitea/api/v1/repos/acme/widgets"


def test_build_url_strips_trailing_slash(client) -> None:
    p = GiteaProvider(client, 1, _make_repository(), web_base_url="https://gitea.example.com/")
    assert p.build_url("/user") == "https://gitea.example.com/api/v1/user"


# Capability detection
#
# Capabilities are structural: a signature that drifts from its protocol drops
# the capability silently rather than failing at import. These two tests are the
# regression guard on that.

SEER_SUPPORTED = {
    "CreatePullRequestCommentProtocol",
    "CreatePullRequestCommentReactionProtocol",
    "CreatePullRequestReactionProtocol",
    "DeletePullRequestCommentReactionProtocol",
    "DeletePullRequestReactionProtocol",
    "GetAuthenticatedActorProtocol",
    "GetBranchProtocol",
    "GetCommitsByPathProtocol",
    "GetPullRequestCommentReactionsProtocol",
    "GetPullRequestFilesProtocol",
    "GetPullRequestProtocol",
    "GetPullRequestReactionsProtocol",
    "GetPullRequestReviewProtocol",
    "GetRepositoryProtocol",
    "GetRepositoryUserPermissionProtocol",
    "GetReviewCommentsProtocol",
    "ListCheckRunsForRefProtocol",
    "MarkPullRequestDraftStateProtocol",
    "RequestReviewProtocol",
    "UpdateIssueProtocol",
}

# Gitea has reactions on issues/PRs and on issue comments, but none on review
# comments -- GitHub's /pulls/comments/{id}/reactions has no counterpart.
SEER_UNSUPPORTED = {
    "CreateReviewCommentReactionProtocol",
    "DeleteReviewCommentReactionProtocol",
    "GetReviewCommentReactionsProtocol",
}


def test_facade_detects_supported_capabilities(provider: GiteaProvider) -> None:
    facade = Facade(provider, record_count=lambda *a: None)
    by_name = {p.__name__: p for p in ALL_PROTOCOLS}
    for name in sorted(SEER_SUPPORTED):
        assert isinstance(facade, by_name[name]), f"{name} should be supported"


def test_facade_reports_unsupported_capabilities(provider: GiteaProvider) -> None:
    """Callers guard with isinstance and skip the behavior; that is the intended degradation."""
    facade = Facade(provider, record_count=lambda *a: None)
    by_name = {p.__name__: p for p in ALL_PROTOCOLS}
    for name in sorted(SEER_UNSUPPORTED):
        assert not isinstance(facade, by_name[name]), f"{name} has no Gitea endpoint"


# Pagination


def test_pagination_uses_limit_not_per_page(provider: GiteaProvider, client) -> None:
    """Gitea names the page-size parameter `limit`; `per_page` would be ignored."""
    client.request.return_value = _mock_response([])
    provider.get("/repos/acme/widgets/commits", pagination=PaginationParams(per_page=25, cursor="3"))

    params = client.request.call_args.kwargs["params"]
    assert params == {"limit": "25", "page": "3"}
    assert "per_page" not in params


def test_pagination_clamps_to_gitea_cap(provider: GiteaProvider, client) -> None:
    """Gitea silently clamps a limit above MAX_RESPONSE_ITEMS, which would truncate a page loop."""
    client.request.return_value = _mock_response([])
    provider.get("/repos/acme/widgets/commits", pagination=PaginationParams(per_page=100, cursor="1"))

    assert client.request.call_args.kwargs["params"]["limit"] == str(GITEA_MAX_PAGE_SIZE)


def test_next_cursor_advances_blindly(provider: GiteaProvider, client) -> None:
    """Gitea sends no next-page header, so the cursor increments and the loop stops on an empty page."""
    client.request.return_value = _mock_response([{"sha": "abc", "commit": {"message": "m"}}])
    result = provider.get_commits_by_path("src/app.py", pagination=PaginationParams(per_page=50, cursor="4"))
    assert result["meta"]["next_cursor"] == "5"
    assert result["type"] == "gitea"


# Error mapping


@pytest.mark.parametrize(
    "status_code,expected",
    [(403, ResourceForbidden), (404, ResourceNotFound), (422, ResourceUnprocessableContent)],
)
def test_error_status_is_mapped(provider: GiteaProvider, client, status_code, expected) -> None:
    client.request.return_value = _mock_response({}, status_code=status_code)
    with pytest.raises(expected):
        provider.get_repository()


# Draft state


def test_mark_as_draft_adds_wip_prefix(provider: GiteaProvider, client) -> None:
    """Gitea's PR edit payload has no `draft` field, so draft state is written via the title."""
    client.request.return_value = _mock_response({"title": "Fix the bug"})
    provider.mark_pull_request_as_draft("7")

    patch_call = client.request.call_args
    assert patch_call.kwargs["method"] == "PATCH"
    assert patch_call.kwargs["data"] == {"title": "WIP: Fix the bug"}


def test_mark_as_draft_is_idempotent(provider: GiteaProvider, client) -> None:
    """Without the read-then-patch, a repeated call would stack prefixes."""
    client.request.return_value = _mock_response({"title": "WIP: Fix the bug"})
    provider.mark_pull_request_as_draft("7")

    assert client.request.call_count == 1  # the read only; no PATCH issued
    assert client.request.call_args.kwargs["method"] == "GET"


def test_ready_for_review_strips_prefix(provider: GiteaProvider, client) -> None:
    client.request.return_value = _mock_response({"title": "[WIP] Fix the bug"})
    provider.mark_pull_request_ready_for_review("7")
    assert client.request.call_args.kwargs["data"] == {"title": "Fix the bug"}


def test_ready_for_review_on_non_draft_is_a_noop(provider: GiteaProvider, client) -> None:
    client.request.return_value = _mock_response({"title": "Fix the bug"})
    provider.mark_pull_request_ready_for_review("7")
    assert client.request.call_count == 1


@pytest.mark.parametrize(
    "title,expected",
    [
        ("WIP: thing", "thing"),
        ("[WIP] thing", "thing"),
        ("wip: thing", "thing"),  # prefix matching is case-insensitive
        ("thing", "thing"),
        ("WIPE the slate", "WIPE the slate"),  # not a prefix match
    ],
)
def test_strip_wip_prefix(title: str, expected: str) -> None:
    assert _strip_wip_prefix(title) == expected


# Reactions


def test_reaction_id_is_its_content(provider: GiteaProvider, client) -> None:
    """Gitea assigns no reaction id, so content doubles as the identifier."""
    client.request.return_value = _mock_response({"content": "rocket", "user": {"id": 3, "login": "bot"}})
    result = provider.create_pull_request_reaction("7", "rocket")
    assert result["data"]["id"] == "rocket"
    assert result["data"]["content"] == "rocket"


def test_delete_reaction_sends_content_in_body(provider: GiteaProvider, client) -> None:
    """Gitea's delete endpoint takes the content in a body, not an id in the path."""
    client.request.return_value = _mock_response(None, status_code=200)
    provider.delete_pull_request_reaction("7", "rocket")

    call = client.request.call_args
    assert call.kwargs["method"] == "DELETE"
    assert call.kwargs["path"] == "/repos/acme/widgets/issues/7/reactions"
    assert call.kwargs["data"] == {"content": "rocket"}


def test_pull_request_comment_reaction_uses_issue_comment_route(provider: GiteaProvider, client) -> None:
    """Gitea models pull requests as issues, so PR comments live on the issue comment routes."""
    client.request.return_value = _mock_response({"content": "+1", "user": {"id": 3, "login": "bot"}})
    provider.create_pull_request_comment_reaction("7", "912", "+1")
    assert client.request.call_args.kwargs["path"] == "/repos/acme/widgets/issues/comments/912/reactions"


# Check runs


def test_check_run_id_is_composite(provider: GiteaProvider, client) -> None:
    """A Gitea status id changes on every update; the context is what is stable across runs."""
    run = map_check_run({"context": "ci/build", "status": "success", "target_url": "https://ci/1"}, "deadbeef")
    assert run["id"] == "deadbeef:ci/build"
    assert run["name"] == "ci/build"
    assert run["status"] == "completed"
    assert run["conclusion"] == "success"


@pytest.mark.parametrize(
    "state,expected",
    [
        ("pending", ("pending", None)),
        ("success", ("completed", "success")),
        ("failure", ("completed", "failure")),
        ("error", ("completed", "failure")),
        ("warning", ("completed", "neutral")),
        ("skipped", ("completed", "skipped")),
    ],
)
def test_check_run_state_mapping(state: str, expected: tuple) -> None:
    run = map_check_run({"context": "ci", "status": state}, "sha")
    assert (run["status"], run["conclusion"]) == expected


def test_latest_status_per_context_dedupes() -> None:
    """Gitea appends a row per status update, so one context can appear many times."""
    statuses = [
        {"context": "ci/build", "status": "success"},
        {"context": "ci/test", "status": "failure"},
        {"context": "ci/build", "status": "pending"},  # older; sort=recentupdate puts newest first
    ]
    result = _latest_status_per_context(statuses)
    assert [s["context"] for s in result] == ["ci/build", "ci/test"]
    assert result[0]["status"] == "success"


def test_list_check_runs_filters_by_name(provider: GiteaProvider, client) -> None:
    client.request.return_value = _mock_response(
        [
            {"context": "ci/build", "status": "success"},
            {"context": "ci/test", "status": "failure"},
        ]
    )
    result = provider.list_check_runs_for_ref("deadbeef", check_name="ci/test")
    assert [r["name"] for r in result["data"]] == ["ci/test"]


def test_list_check_runs_all_keeps_duplicate_contexts(provider: GiteaProvider, client) -> None:
    client.request.return_value = _mock_response(
        [
            {"context": "ci/build", "status": "success"},
            {"context": "ci/build", "status": "pending"},
        ]
    )
    result = provider.list_check_runs_for_ref("deadbeef", timestamp_filter="all")
    assert len(result["data"]) == 2


# Mappers


def test_map_pull_request_uses_number_not_id() -> None:
    """Every Gitea pull-request route addresses `number`; `id` is a global row id."""
    pr = map_pull_request(
        {
            "id": 90210,
            "number": 7,
            "title": "WIP: Fix it",
            "body": "details",
            "state": "open",
            "merged": False,
            "html_url": "https://gitea.example.com/gitea/acme/widgets/pulls/7",
            "head": {"sha": "aaa", "ref": "feature"},
            "base": {"sha": "bbb", "ref": "main"},
            "user": {"id": 5, "login": "dev"},
        }
    )
    assert pr["id"] == "7"
    assert pr["internal_id"] == "90210"
    assert pr["title"] == "Fix it"  # the WIP marker is draft state, not part of the title
    assert pr["head"]["ref"] == "feature"
    assert pr["author"]["username"] == "dev"


def test_map_commit_populates_account_logins() -> None:
    """Unlike GitLab, Gitea attaches resolved accounts alongside the git identities."""
    commit = map_commit(
        {
            "sha": "deadbeef",
            "commit": {
                "message": "Fix it",
                "author": {"name": "Dev", "email": "dev@example.com", "date": "2026-01-02T03:04:05Z"},
            },
            "author": {"id": 5, "login": "dev"},
            "committer": {"id": 6, "login": "ci-bot"},
            "stats": {"additions": 10, "deletions": 2},
        }
    )
    assert commit["id"] == "deadbeef"
    assert commit["author_login"] == "dev"
    assert commit["committer_login"] == "ci-bot"
    author = commit["author"]
    assert author is not None
    assert author["date"] == datetime.datetime(2026, 1, 2, 3, 4, 5, tzinfo=datetime.UTC)
    assert commit["additions"] == 10


def test_map_commit_omits_logins_when_unattributed() -> None:
    """Gitea leaves the account null when the commit email matches no user."""
    commit = map_commit({"sha": "abc", "commit": {"message": "m"}, "author": None, "committer": None})
    assert "author_login" not in commit
    assert "committer_login" not in commit


def test_map_pull_request_file_has_no_patch_or_sha() -> None:
    """Gitea's changed-file entry carries counts and URLs but no diff body and no blob sha."""
    f = map_pull_request_file({"filename": "src/app.py", "status": "modified", "changes": 12, "previous_filename": ""})
    assert f["filename"] == "src/app.py"
    assert f["status"] == "modified"
    assert f["changes"] == 12
    assert f["patch"] is None
    assert f["sha"] == ""
    assert f["previous_filename"] is None


@pytest.mark.parametrize(
    "gitea_status,expected",
    [
        ("added", "added"),
        ("deleted", "removed"),
        ("removed", "removed"),
        ("renamed", "renamed"),
        ("modified", "modified"),
        ("bogus", "unknown"),
    ],
)
def test_map_file_status(gitea_status: str, expected: str) -> None:
    assert map_pull_request_file({"filename": "f", "status": gitea_status})["status"] == expected


def test_map_review_comment_leaves_line_unset() -> None:
    """Gitea's `position` indexes the diff hunk, not the file, so it cannot become a DiffLine."""
    c = map_review_comment(
        {
            "id": 55,
            "body": "nit",
            "path": "src/app.py",
            "position": 4,
            "commit_id": "deadbeef",
            "diff_hunk": "@@ -1 +1 @@",
            "user": {"id": 5, "login": "dev"},
            "pull_request_review_id": 12,
        },
        review_id="12",
    )
    assert c["line"] is None
    assert c["start_line"] is None
    assert c["thread_id"] is None  # Gitea has no review-thread entity
    assert c["review_id"] == "12"
    assert c["file_path"] == "src/app.py"
    assert c["commit_sha"] == "deadbeef"


def test_map_reaction_without_user() -> None:
    assert map_reaction({"content": "heart"})["author"] is None


# Routing


def test_get_repository_uses_owner_name_path(provider: GiteaProvider, client) -> None:
    client.request.return_value = _mock_response(
        {
            "full_name": "acme/widgets",
            "default_branch": "main",
            "clone_url": "https://x.git",
            "private": False,
            "size": 10,
            "description": None,
            "topics": [],
        }
    )
    result = provider.get_repository()
    assert client.request.call_args.kwargs["path"] == "/repos/acme/widgets"
    assert result["data"]["full_name"] == "acme/widgets"


def test_get_commits_by_path_forwards_filters(provider: GiteaProvider, client) -> None:
    client.request.return_value = _mock_response([])
    provider.get_commits_by_path(
        "src/app.py",
        ref="main",
        since=datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC),
        until=datetime.datetime(2026, 2, 1, tzinfo=datetime.UTC),
    )
    params = client.request.call_args.kwargs["params"]
    assert params["path"] == "src/app.py"
    assert params["sha"] == "main"  # Gitea names the ref filter `sha`
    assert params["since"].startswith("2026-01-01")
    assert params["until"].startswith("2026-02-01")


def test_get_repository_user_permission_maps_owner_to_admin(provider: GiteaProvider, client) -> None:
    """Gitea's `owner` sits above `admin`, but the generic model tops out at admin."""
    client.request.return_value = _mock_response({"permission": "owner", "user": {"id": 5, "login": "dev"}})
    result = provider.get_repository_user_permission("dev")
    assert result["data"]["perms"] == "admin"
    assert result["data"]["login"] == "dev"


def test_update_issue_resolves_label_names_to_ids(provider: GiteaProvider, client) -> None:
    """Gitea's issue-edit payload takes label ids; an unknown name is dropped, not fatal."""
    client.request.side_effect = [
        _mock_response([{"id": 1, "name": "bug"}, {"id": 2, "name": "p1"}]),
        _mock_response({"number": 7, "title": "t", "body": None, "state": "closed", "html_url": "u"}),
    ]
    provider.update_issue("7", state="closed", labels=["bug", "nonexistent"])

    patch_call = client.request.call_args
    assert patch_call.kwargs["data"] == {"state": "closed", "labels": [1]}


def test_request_review_posts_reviewers(provider: GiteaProvider, client) -> None:
    client.request.return_value = _mock_response({})
    provider.request_review("7", ["alice", "bob"])
    call = client.request.call_args
    assert call.kwargs["path"] == "/repos/acme/widgets/pulls/7/requested_reviewers"
    assert call.kwargs["data"] == {"reviewers": ["alice", "bob"]}


def test_map_commit_never_claims_bot_status() -> None:
    """Gitea's user object has no bot marker, so the key stays absent rather
    than asserting something the API never said."""
    commit = map_commit(
        {
            "sha": "abc",
            "commit": {"message": "m"},
            "author": {"id": 5, "login": "ci-runner"},
        }
    )
    assert commit["author_login"] == "ci-runner"
    assert "author_is_bot" not in commit
