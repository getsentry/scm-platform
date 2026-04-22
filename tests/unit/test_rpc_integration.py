import inspect
import json
from collections.abc import Callable
from io import BytesIO
from typing import Any
from unittest.mock import MagicMock

import msgspec
import pytest
import requests

from scm import actions
from scm.rpc.client import SourceCodeManager, deserialize_repository
from scm.rpc.errors import deserialize_error
from scm.rpc.helpers import sign_get
from scm.rpc.server import RpcServer
from scm.test_fixtures import (
    make_github_assignee,
    make_github_branch,
    make_github_check_run,
    make_github_comment,
    make_github_commit,
    make_github_commit_comparison,
    make_github_file_content,
    make_github_git_blob,
    make_github_git_commit_object,
    make_github_git_ref,
    make_github_git_tree,
    make_github_issue,
    make_github_label,
    make_github_pull_request,
    make_github_pull_request_commit,
    make_github_pull_request_file,
    make_github_reaction,
    make_github_repository,
    make_github_review,
    make_github_review_comment,
)
from scm.types import Repository, WriteCommitAction

SIGNING_SECRET = "test-secret"
BASE_URL = "http://rpc-server"


def make_repository(**overrides) -> Repository:
    defaults: Repository = {
        "id": 1,
        "external_id": "abc123",
        "integration_id": 1,
        "is_active": True,
        "name": "org/repo",
        "organization_id": 1,
        "provider_name": "github",
    }
    return {**defaults, **overrides}  # type: ignore[typeddict-item]


def make_github_api_response(
    body: dict | list | str,
    status_code: int = 200,
    headers: dict[str, str] | None = None,
) -> MagicMock:
    """A mock requests.Response as returned by the GitHub API."""
    if isinstance(body, str):
        content = body.encode()
    else:
        content = json.dumps(body).encode()
    response = MagicMock(spec=requests.Response)
    response.status_code = status_code
    response.headers = headers or {"Content-Type": "application/json"}
    response.iter_content.return_value = [content]
    response.__enter__ = lambda s: s
    response.__exit__ = MagicMock(return_value=False)
    return response


def make_rpc_server(repository: Repository, server_provider: MagicMock) -> RpcServer:
    return RpcServer(
        secrets=[SIGNING_SECRET],
        fetch_repository=lambda org_id, repo_id: repository,
        fetch_provider=lambda org_id, repo: server_provider,
        record_count=lambda name, value, tags: None,
        emit_error=lambda e: None,
    )


def bridged_fetch_repository(server: RpcServer):
    """Return a fetch_repository callable that routes through the RPC server."""

    def _fetch(url, signing_secret, org_id, repo_id, sess):
        headers = {
            "Authorization": sign_get(signing_secret, org_id, repo_id),
            "X-Organization-Id": str(org_id),
            "X-Repository-Id": msgspec.json.encode(repo_id).decode("utf-8"),
        }
        response = server.get(headers)
        if response.status_code != 200:
            deserialize_error(response.content)
        return deserialize_repository(response.content)

    return _fetch


def bridge_session_to_server(session: requests.Session, server: RpcServer) -> None:
    """Patch a requests.Session so POST calls route directly to the RPC server."""

    def fake_post(url, **kwargs):
        response = server.post(kwargs.get("data", b""), dict(kwargs.get("headers", {})))
        raw = BytesIO(b"".join(response.content))
        mock_resp = requests.Response()
        mock_resp.status_code = response.status_code
        mock_resp.headers.update(response.headers)
        mock_resp.raw = raw
        mock_resp._content = raw.getvalue()
        return mock_resp

    session.post = fake_post  # type: ignore[assignment]


def make_client_scm(organization_id, repository_id, server):
    """Build an RPC client SourceCodeManager wired to the given RPC server."""
    scm = SourceCodeManager.make_from_repository_id(
        organization_id,
        repository_id,
        base_url=BASE_URL,
        signing_secret=SIGNING_SECRET,
        fetch_repository=bridged_fetch_repository(server),
    )
    bridge_session_to_server(scm.provider.client.session, server)
    return scm


# Each entry: (action_name, action_callable, mock_response_body, status_code, extra_headers)
# action_callable receives the scm instance and calls the action with appropriate args.
ACTION_TEST_CASES: list[tuple[str, Callable, dict | list | str, int, dict[str, str] | None]] = [
    (
        "get_repository",
        lambda scm: actions.get_repository(scm),
        make_github_repository(),
        200,
        None,
    ),
    (
        "get_repository_assignees",
        lambda scm: actions.get_repository_assignees(scm),
        [make_github_assignee()],
        200,
        None,
    ),
    (
        "get_repository_labels",
        lambda scm: actions.get_repository_labels(scm),
        [make_github_label()],
        200,
        None,
    ),
    # Pull request operations
    (
        "get_pull_request",
        lambda scm: actions.get_pull_request(scm, "1"),
        make_github_pull_request(),
        200,
        None,
    ),
    (
        "get_pull_requests",
        lambda scm: actions.get_pull_requests(scm),
        [make_github_pull_request()],
        200,
        None,
    ),
    (
        "create_pull_request",
        lambda scm: actions.create_pull_request(scm, "Title", "Body", "feature", "main"),
        make_github_pull_request(),
        201,
        None,
    ),
    (
        "create_pull_request_draft",
        lambda scm: actions.create_pull_request_draft(scm, "Title", "Body", "feature", "main"),
        make_github_pull_request(),
        201,
        None,
    ),
    (
        "update_pull_request",
        lambda scm: actions.update_pull_request(scm, "1", title="Updated"),
        make_github_pull_request(title="Updated"),
        200,
        None,
    ),
    (
        "get_pull_request_files",
        lambda scm: actions.get_pull_request_files(scm, "1"),
        [make_github_pull_request_file()],
        200,
        None,
    ),
    (
        "get_pull_request_commits",
        lambda scm: actions.get_pull_request_commits(scm, "1"),
        [make_github_pull_request_commit()],
        200,
        None,
    ),
    (
        "get_pull_request_diff",
        lambda scm: actions.get_pull_request_diff(scm, "1"),
        "diff --git a/file.py b/file.py\n-old\n+new",
        200,
        None,
    ),
    (
        "request_review",
        lambda scm: actions.request_review(scm, "1", ["reviewer1"]),
        {"id": 1},
        200,
        None,
    ),
    # Issue operations
    (
        "get_issue",
        lambda scm: actions.get_issue(scm, "7"),
        make_github_issue(),
        200,
        None,
    ),
    (
        "create_issue",
        lambda scm: actions.create_issue(scm, "Title", "Body"),
        make_github_issue(title="Title", body="Body"),
        201,
        None,
    ),
    # Issue comment operations
    (
        "get_issue_comments",
        lambda scm: actions.get_issue_comments(scm, "10"),
        [make_github_comment()],
        200,
        None,
    ),
    (
        "create_issue_comment",
        lambda scm: actions.create_issue_comment(scm, issue_id="10", body="Hello"),
        make_github_comment(body="Hello"),
        201,
        None,
    ),
    (
        "delete_issue_comment",
        lambda scm: actions.delete_issue_comment(scm, "10", "1"),
        {},
        204,
        None,
    ),
    # Pull request comment operations
    (
        "get_pull_request_comments",
        lambda scm: actions.get_pull_request_comments(scm, "1"),
        [make_github_comment()],
        200,
        None,
    ),
    (
        "create_pull_request_comment",
        lambda scm: actions.create_pull_request_comment(scm, "1", "Nice work"),
        make_github_comment(body="Nice work"),
        201,
        None,
    ),
    (
        "delete_pull_request_comment",
        lambda scm: actions.delete_pull_request_comment(scm, "1", "5"),
        {},
        204,
        None,
    ),
    # Issue comment reaction operations
    (
        "get_issue_comment_reactions",
        lambda scm: actions.get_issue_comment_reactions(scm, "10", "1"),
        [make_github_reaction()],
        200,
        None,
    ),
    (
        "create_issue_comment_reaction",
        lambda scm: actions.create_issue_comment_reaction(scm, "10", "1", "+1"),
        make_github_reaction(content="+1"),
        201,
        None,
    ),
    (
        "delete_issue_comment_reaction",
        lambda scm: actions.delete_issue_comment_reaction(scm, "10", "1", "99"),
        {},
        204,
        None,
    ),
    # Pull request comment reaction operations
    (
        "get_pull_request_comment_reactions",
        lambda scm: actions.get_pull_request_comment_reactions(scm, "1", "5"),
        [make_github_reaction()],
        200,
        None,
    ),
    (
        "create_pull_request_comment_reaction",
        lambda scm: actions.create_pull_request_comment_reaction(scm, "1", "5", "heart"),
        make_github_reaction(content="heart"),
        201,
        None,
    ),
    (
        "delete_pull_request_comment_reaction",
        lambda scm: actions.delete_pull_request_comment_reaction(scm, "1", "5", "99"),
        {},
        204,
        None,
    ),
    # Issue reaction operations
    (
        "get_issue_reactions",
        lambda scm: actions.get_issue_reactions(scm, "10"),
        [make_github_reaction()],
        200,
        None,
    ),
    (
        "create_issue_reaction",
        lambda scm: actions.create_issue_reaction(scm, "10", "eyes"),
        make_github_reaction(content="eyes"),
        201,
        None,
    ),
    (
        "delete_issue_reaction",
        lambda scm: actions.delete_issue_reaction(scm, "10", "99"),
        {},
        204,
        None,
    ),
    # Pull request reaction operations
    (
        "get_pull_request_reactions",
        lambda scm: actions.get_pull_request_reactions(scm, "1"),
        [make_github_reaction()],
        200,
        None,
    ),
    (
        "create_pull_request_reaction",
        lambda scm: actions.create_pull_request_reaction(scm, "1", "rocket"),
        make_github_reaction(content="rocket"),
        201,
        None,
    ),
    (
        "delete_pull_request_reaction",
        lambda scm: actions.delete_pull_request_reaction(scm, "1", "99"),
        {},
        204,
        None,
    ),
    # Branch operations
    (
        "get_branch",
        lambda scm: actions.get_branch(scm, "main"),
        make_github_branch(),
        200,
        None,
    ),
    (
        "create_branch",
        lambda scm: actions.create_branch(scm, "new-branch", "abc123"),
        make_github_git_ref(),
        201,
        None,
    ),
    (
        "update_branch",
        lambda scm: actions.update_branch(scm, "main", "def456"),
        make_github_git_ref(sha="def456"),
        200,
        None,
    ),
    (
        "delete_branch",
        lambda scm: actions.delete_branch(scm, "feature"),
        {},
        204,
        None,
    ),
    (
        "get_git_ref",
        lambda scm: actions.get_git_ref(scm, "heads/main"),
        make_github_git_ref(),
        200,
        None,
    ),
    # Git blob operations
    (
        "create_git_blob",
        lambda scm: actions.create_git_blob(scm, "file content", "utf-8"),
        make_github_git_blob(),
        201,
        None,
    ),
    # File content operations
    (
        "get_file_content",
        lambda scm: actions.get_file_content(scm, "README.md"),
        make_github_file_content(),
        200,
        None,
    ),
    # Commit operations
    (
        "get_commit",
        lambda scm: actions.get_commit(scm, "abc123"),
        make_github_commit(),
        200,
        None,
    ),
    (
        "get_commits",
        lambda scm: actions.get_commits(scm),
        [make_github_commit()],
        200,
        None,
    ),
    (
        "get_commits_by_path",
        lambda scm: actions.get_commits_by_path(scm, "src/main.py"),
        [make_github_commit()],
        200,
        None,
    ),
    (
        "compare_commits",
        lambda scm: actions.compare_commits(scm, "abc123", "def456"),
        make_github_commit_comparison(commits=[make_github_commit()]),
        200,
        None,
    ),
    # Git tree and commit operations
    (
        "get_tree",
        lambda scm: actions.get_tree(scm, "tree_sha"),
        make_github_git_tree(),
        200,
        None,
    ),
    (
        "get_git_commit",
        lambda scm: actions.get_git_commit(scm, "abc123"),
        make_github_git_commit_object(),
        200,
        None,
    ),
    (
        "create_git_tree",
        lambda scm: actions.create_git_tree(scm, [{"path": "file.py", "mode": "100644", "type": "blob", "sha": "abc"}]),
        make_github_git_tree(),
        201,
        None,
    ),
    (
        "create_git_commit",
        lambda scm: actions.create_git_commit(scm, "commit msg", "tree_sha", ["parent_sha"]),
        make_github_git_commit_object(),
        201,
        None,
    ),
    # Review comment operations
    (
        "create_review_comment_file",
        lambda scm: actions.create_review_comment_file(scm, "1", "abc123", "Nice", "file.py", "head"),
        make_github_review_comment(),
        201,
        None,
    ),
    (
        "create_review_comment_line",
        lambda scm: actions.create_review_comment_line(scm, "1", "abc123", "Line comment", "file.py", "head", 3),
        make_github_review_comment(),
        201,
        None,
    ),
    (
        "create_review_comment_multiline",
        lambda scm: actions.create_review_comment_multiline(
            scm, "1", "abc123", "Span comment", "file.py", "head", "base", 1, 5
        ),
        make_github_review_comment(),
        201,
        None,
    ),
    (
        "create_review_comment_reply",
        lambda scm: actions.create_review_comment_reply(scm, "1", "Reply", "100"),
        make_github_review_comment(),
        201,
        None,
    ),
    (
        "create_review",
        lambda scm: actions.create_review(scm, "1", "abc123", "comment", []),
        make_github_review(),
        201,
        None,
    ),
    # Check run operations
    (
        "create_check_run",
        lambda scm: actions.create_check_run(scm, "CI", "abc123"),
        make_github_check_run(),
        201,
        None,
    ),
    (
        "get_check_run",
        lambda scm: actions.get_check_run(scm, "300"),
        make_github_check_run(),
        200,
        None,
    ),
    (
        "update_check_run",
        lambda scm: actions.update_check_run(scm, "300", status="completed", conclusion="success"),
        make_github_check_run(),
        200,
        None,
    ),
    # Minimize comment (GraphQL)
    (
        "minimize_comment",
        lambda scm: actions.minimize_comment(scm, "IC_abc123", "SPAM"),
        {"data": {"minimizeComment": {"minimizedComment": {"isMinimized": True}}}},
        200,
        None,
    ),
    # Archive link (redirect)
    (
        "get_archive_link",
        lambda scm: actions.get_archive_link(scm, "main"),
        "",
        302,
        {
            "Content-Type": "text/html",
            "Location": "https://codeload.github.com/org/repo/legacy.tar.gz/main",
        },
    ),
    # Download archive (raw bytes)
    (
        "download_archive",
        lambda scm: actions.download_archive(scm, "main"),
        "tarball-bytes",
        200,
        {"Content-Type": "application/x-gzip"},
    ),
]


# Multi-call actions issue several provider HTTP requests per invocation.
MULTI_CALL_ACTION_TEST_CASES: list[tuple[str, Callable, list[tuple[dict | list | str, int, dict[str, str] | None]]]] = [
    (
        "create_commit",
        lambda scm: actions.create_commit(
            scm,
            branch="topic",
            parent_sha="parent_sha",
            message="msg",
            actions=[WriteCommitAction(action="create", filename="f.py", content="x", encoding="utf-8")],
        ),
        [
            (make_github_git_commit_object(sha="parent_sha", tree_sha="parent_tree"), 200, None),
            (make_github_git_tree(sha="new_tree"), 201, None),
            (make_github_git_commit_object(sha="new_commit", message="msg"), 201, None),
            (make_github_git_ref(ref="refs/heads/topic", sha="new_commit"), 200, None),
        ],
    ),
]


# URL-building actions are computed locally from the provider's state and don't
# trigger a round-trip to the RPC server.
LOCAL_ACTION_TEST_CASES: list[tuple[str, Callable, Any]] = [
    (
        "get_file_url",
        lambda scm: actions.get_file_url(scm, "src/main.py", "abc123", start_line=10, end_line=20),
        "https://github.com/org/repo/blob/abc123/src/main.py#L10-L20",
    ),
    (
        "get_commit_url",
        lambda scm: actions.get_commit_url(scm, "abc123"),
        "https://github.com/org/repo/commit/abc123",
    ),
]


class TestRpcIntegration:
    @pytest.mark.parametrize(
        "action_name, action_fn, response_body, status_code, extra_headers",
        ACTION_TEST_CASES,
        ids=[case[0] for case in ACTION_TEST_CASES],
    )
    def test_action_through_rpc(
        self,
        action_name: str,
        action_fn: Callable[[Any], Any],
        response_body: dict | list | str,
        status_code: int,
        extra_headers: dict[str, str] | None,
    ):
        repo = make_repository()
        mock_response = make_github_api_response(response_body, status_code, extra_headers)

        server_provider = MagicMock()
        server_provider.repository = repo
        server_provider.is_rate_limited.return_value = False
        server_provider.__class__.__name__ = "GitHubProvider"
        server_provider.request.return_value = mock_response

        server = make_rpc_server(repo, server_provider)
        scm = make_client_scm(1, 1, server)

        action_fn(scm)

        server_provider.request.assert_called_once()

    def test_multi_chunk_streaming_response(self):
        """Verify that a response split across multiple chunks is reassembled correctly."""
        repo = make_repository()
        pr_json = make_github_pull_request()
        content = json.dumps(pr_json).encode()

        # Split content into 3 chunks with an empty chunk interleaved
        mid = len(content) // 2
        chunks = [content[:mid], b"", content[mid:]]

        mock_response = MagicMock(spec=requests.Response)
        mock_response.status_code = 200
        mock_response.headers = {"Content-Type": "application/json"}
        mock_response.iter_content.return_value = chunks
        mock_response.__enter__ = lambda s: s
        mock_response.__exit__ = MagicMock(return_value=False)

        server_provider = MagicMock()
        server_provider.repository = repo
        server_provider.is_rate_limited.return_value = False
        server_provider.__class__.__name__ = "GitHubProvider"
        server_provider.request.return_value = mock_response

        server = make_rpc_server(repo, server_provider)
        scm = make_client_scm(1, 1, server)

        result = actions.get_pull_request(scm, "1")

        assert result["data"]["title"] == "Test PR"

    @pytest.mark.parametrize(
        "action_name, action_fn, expected_result",
        LOCAL_ACTION_TEST_CASES,
        ids=[case[0] for case in LOCAL_ACTION_TEST_CASES],
    )
    def test_local_action_through_rpc(
        self,
        action_name: str,
        action_fn: Callable[[Any], Any],
        expected_result: Any,
    ):
        repo = make_repository()

        server_provider = MagicMock()
        server_provider.repository = repo
        server_provider.is_rate_limited.return_value = False
        server_provider.__class__.__name__ = "GitHubProvider"

        server = make_rpc_server(repo, server_provider)
        scm = make_client_scm(1, 1, server)

        assert action_fn(scm) == expected_result
        server_provider.request.assert_not_called()

    @pytest.mark.parametrize(
        "action_name, action_fn, responses",
        MULTI_CALL_ACTION_TEST_CASES,
        ids=[case[0] for case in MULTI_CALL_ACTION_TEST_CASES],
    )
    def test_multi_call_action_through_rpc(
        self,
        action_name: str,
        action_fn: Callable[[Any], Any],
        responses: list[tuple[dict | list | str, int, dict[str, str] | None]],
    ):
        repo = make_repository()
        mock_responses = [make_github_api_response(body, status, headers) for body, status, headers in responses]

        server_provider = MagicMock()
        server_provider.repository = repo
        server_provider.is_rate_limited.return_value = False
        server_provider.__class__.__name__ = "GitHubProvider"
        server_provider.request.side_effect = mock_responses

        server = make_rpc_server(repo, server_provider)
        scm = make_client_scm(1, 1, server)

        action_fn(scm)

        assert server_provider.request.call_count == len(responses)

    def test_all_actions_covered(self):
        tested_actions = (
            {case[0] for case in ACTION_TEST_CASES}
            | {case[0] for case in MULTI_CALL_ACTION_TEST_CASES}
            | {case[0] for case in LOCAL_ACTION_TEST_CASES}
        )
        all_action_fns = {
            name for name, obj in inspect.getmembers(actions, inspect.isfunction) if not name.startswith("_")
        }
        assert tested_actions == all_action_fns, (
            "Missing action function. Please add your action to the test case above."
        )
