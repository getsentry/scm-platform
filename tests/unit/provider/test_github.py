import json
from datetime import date, datetime
from typing import Any
from unittest.mock import MagicMock

import pytest

from scm.errors import (
    RateLimitExceeded,
    ResourceBadGateway,
    ResourceBadRequest,
    ResourceConflict,
    ResourceForbidden,
    ResourceGatewayTimeout,
    ResourceNotFound,
    ResourceServerError,
    ResourceServiceUnavailable,
    ResourceUnauthorized,
    ResourceUnprocessableContent,
    SCMCodedError,
    UnhandledException,
)
from scm.providers.github.provider import (
    GITHUB_CONCLUSION_MAP,
    MINIMIZE_COMMENT_MUTATION,
    RESOLVE_REVIEW_THREAD_MUTATION,
    REVIEW_THREAD_BY_COMMENT_QUERY,
    THREAD_COMMENTS_QUERY,
    UPDATE_AND_MINIMIZE_PULL_REQUEST_REVIEW_COMMENT_MUTATION,
    UPDATE_AND_RESOLVE_PULL_REQUEST_REVIEW_COMMENT_MUTATION,
    GitHubProvider,
    _graphql_review_thread_full_comments_query,
    _graphql_review_threads_query,
    map_app_installation,
    map_check_run,
    map_collaborator_permission_level,
    map_comment,
    map_github_repository_permission,
    map_reaction_rollup,
)
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
    make_github_workflow_job,
    make_github_workflow_run,
)
from scm.types import (
    ApiClient,
    ChmodCommitAction,
    CredentialsSet,
    DeleteCommitAction,
    DiffLine,
    MoveCommitAction,
    Referrer,
    Repository,
    WriteCommitAction,
)

REVIEW_THREADS_QUERY = _graphql_review_threads_query(include_reactions=False)
REVIEW_THREADS_WITH_REACTIONS_QUERY = _graphql_review_threads_query(include_reactions=True)
REVIEW_THREAD_FULL_COMMENTS_QUERY = _graphql_review_thread_full_comments_query(include_reactions=False)


def make_repository() -> Repository:
    return {
        "id": 1,
        "integration_id": 1,
        "name": "test-org/test-repo",
        "organization_id": 1,
        "is_active": True,
        "external_id": None,
        "provider_name": "github",
        "web_base_url": None,
    }


class FakeResponse:
    def __init__(
        self,
        payload: Any,
        *,
        headers: dict[str, str] | None = None,
        status_code: int | None = None,
        text: str | None = None,
        url: str = "",
    ) -> None:
        self._payload = payload
        self.content = json.dumps(payload).encode()
        self.headers = headers or {}
        self.status_code = status_code
        self.text = text if text is not None else ""
        self.url = url

    def json(self) -> Any:
        return self._payload


class RecordingClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.responses: dict[str, list[Any]] = {
            "get": [],
            "post": [],
            "patch": [],
            "delete": [],
            "request": [],
            "graphql": [],
        }

    def queue(self, operation: str, response: Any) -> None:
        self.responses[operation].append(response)

    def _pop(self, operation: str) -> Any:
        if not self.responses[operation]:
            raise AssertionError(f"No queued response for {operation}")
        return self.responses[operation].pop(0)

    def is_rate_limited(self, referrer: Referrer) -> bool:
        return False

    def get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        pagination: Any | None = None,
        request_options: Any | None = None,
        extra_headers: dict[str, str] | None = None,
        allow_redirects: bool | None = None,
        credentials_set: CredentialsSet = "installation",
    ) -> FakeResponse:
        self.calls.append(
            {
                "operation": "get",
                "path": path,
                "params": params,
                "pagination": pagination,
                "request_options": request_options,
                "extra_headers": extra_headers,
                "credentials_set": credentials_set,
                "timeout": request_options.get("timeout") if request_options else None,
            }
        )
        return self._pop("get")

    def post(
        self,
        path: str,
        data: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> FakeResponse:
        self.calls.append({"operation": "post", "path": path, "data": data, "headers": headers})
        return self._pop("post")

    def patch(
        self,
        path: str,
        data: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> FakeResponse:
        self.calls.append({"operation": "patch", "path": path, "data": data, "headers": headers})
        return self._pop("patch")

    def delete(self, path: str) -> FakeResponse:
        self.calls.append({"operation": "delete", "path": path})
        return self._pop("delete")

    def request(
        self,
        method: str,
        path: str,
        data: dict[str, Any] | None = None,
        params: dict[str, str] | None = None,
        headers: dict[str, str] | None = None,
    ) -> FakeResponse:
        self.calls.append(
            {
                "operation": "request",
                "method": method,
                "path": path,
                "data": data,
                "params": params,
                "headers": headers,
            }
        )
        return self._pop("request")

    def graphql(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        self.calls.append({"operation": "graphql", "query": query, "variables": variables})
        return self._pop("graphql")


class NoOpRateLimiter:
    def is_rate_limited(self, referrer: str) -> bool:
        return False

    def update_rate_limit_meta(self, capacity: int, consumed: int, next_window_start: int) -> None:
        pass


def make_provider(client: RecordingClient | None = None) -> tuple[GitHubProvider, RecordingClient]:
    transport = client or RecordingClient()
    provider = GitHubProvider(
        MagicMock(spec=ApiClient),
        organization_id=1,
        repository=make_repository(),
        rate_limiter=NoOpRateLimiter(),
    )
    provider.get = transport.get  # type: ignore[assignment]
    provider.post = transport.post  # type: ignore[assignment]
    provider.patch = transport.patch  # type: ignore[assignment]
    provider.delete = transport.delete  # type: ignore[assignment]
    provider.graphql = transport.graphql  # type: ignore[assignment]
    return provider, transport


def expected_repository(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "full_name": raw["full_name"],
        "default_branch": raw["default_branch"],
        "clone_url": raw["clone_url"],
        "private": raw["private"],
        "size": raw["size"],
        "description": raw.get("description"),
        "topics": raw["topics"],
    }


def expected_comment(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(raw["id"]),
        "body": raw["body"],
        "author": {"id": str(raw["user"]["id"]), "username": raw["user"]["login"]},
        "created_at": raw.get("created_at"),
        "author_association": raw.get("author_association"),
        "reactions": map_reaction_rollup(raw.get("reactions")),
    }


def expected_assignee(raw: dict[str, Any]) -> dict[str, Any]:
    return {"id": str(raw["id"]), "username": raw["login"]}


def make_collaborator(
    login: str = "testuser",
    user_id: int = 123,
    permissions: dict[str, bool] | None = None,
) -> dict[str, Any]:
    return {
        "login": login,
        "id": user_id,
        "permissions": permissions or {"pull": True, "triage": True, "push": False, "maintain": False, "admin": False},
    }


def make_collaborator_permission(
    login: str = "testuser",
    user_id: int = 123,
    permission: str = "read",
) -> dict[str, Any]:
    return {
        "permission": permission,
        "role_name": permission,
        "user": {
            "login": login,
            "id": user_id,
        },
    }


def expected_label(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(raw["id"]),
        "name": raw["name"],
        "color": raw["color"],
        "description": raw.get("description"),
    }


def expected_reaction(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(raw["id"]),
        "content": raw["content"],
        "author": {"id": str(raw["user"]["id"]), "username": raw["user"]["login"]},
    }


def expected_pull_request(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "internal_id": str(raw["id"]),
        "id": str(raw["number"]),
        "title": raw["title"],
        "body": raw.get("body"),
        "state": raw["state"],
        "merged": raw.get("merged_at") is not None,
        "html_url": raw.get("html_url", ""),
        "head": {"sha": raw["head"]["sha"], "ref": raw["head"]["ref"]},
        "base": {"sha": raw["base"]["sha"], "ref": raw["base"]["ref"]},
        "author": {"id": str(raw["user"]["id"]), "username": raw["user"]["login"]},
    }


def expected_issue(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(raw["number"]),
        "title": raw["title"],
        "body": raw.get("body"),
        "state": raw["state"],
        "html_url": raw.get("html_url", ""),
    }


def expected_git_ref_from_branch(raw: dict[str, Any]) -> dict[str, Any]:
    return {"ref": raw["name"], "sha": raw["commit"]["sha"]}


def expected_git_ref(raw: dict[str, Any]) -> dict[str, Any]:
    return {"ref": raw["ref"].removeprefix("refs/heads/"), "sha": raw["object"]["sha"]}


def expected_git_ref_full(raw: dict[str, Any]) -> dict[str, Any]:
    return {"ref": raw["ref"], "sha": raw["object"]["sha"]}


def expected_file_content(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "path": raw["path"],
        "sha": raw["sha"],
        "content": raw.get("content", ""),
        "encoding": raw.get("encoding", ""),
        "size": raw["size"],
        "type": "directory" if raw.get("type") == "dir" else raw.get("type", "file"),
    }


def expected_commit(raw: dict[str, Any]) -> dict[str, Any]:
    author = raw["commit"]["author"]
    stats = raw.get("stats") or {}
    return {
        "id": raw["sha"],
        "message": raw["commit"]["message"],
        "author": {
            "name": author["name"],
            "email": author["email"],
            "date": datetime.fromisoformat(author["date"]),
        },
        "additions": stats.get("additions"),
        "deletions": stats.get("deletions"),
    }


def expected_commit_with_changes(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        **expected_commit(raw),
        "files": [
            {
                "filename": entry["filename"],
                "status": entry.get("status", "modified"),
                "patch": entry.get("patch"),
                "additions": entry.get("additions"),
                "deletions": entry.get("deletions"),
                "previous_filename": entry.get("previous_filename"),
            }
            for entry in raw.get("files", [])
        ],
    }


def expected_tree(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "sha": raw["sha"],
        "tree": [
            {
                "path": entry["path"],
                "mode": entry["mode"],
                "type": entry["type"],
                "sha": entry["sha"],
                "size": entry.get("size"),
            }
            for entry in raw["tree"]
        ],
        "truncated": raw["truncated"],
    }


def expected_git_commit_object(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "sha": raw["sha"],
        "tree": {"sha": raw["tree"]["sha"]},
        "message": raw.get("message", ""),
    }


def expected_pull_request_file(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "filename": raw["filename"],
        "status": raw.get("status", "modified"),
        "patch": raw.get("patch"),
        "changes": raw.get("changes", 0),
        "sha": raw.get("sha", ""),
        "previous_filename": raw.get("previous_filename"),
    }


def expected_pull_request_commit(raw: dict[str, Any]) -> dict[str, Any]:
    author = raw["commit"]["author"]
    return {
        "sha": raw["sha"],
        "message": raw["commit"]["message"],
        "author": {
            "name": author["name"],
            "email": author["email"],
            "date": datetime.fromisoformat(author["date"]),
        },
    }


def expected_review_comment(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(raw["id"]),
        "unique_id": raw["node_id"],
        "url": raw["html_url"],
        "file_path": raw["path"],
        "body": raw["body"],
        "author": {"id": str(raw["user"]["id"]), "username": raw["user"]["login"]} if raw.get("user") else None,
        "created_at": "2025-01-01T00:00:00+00:00",
        "diff_hunk": raw["diff_hunk"],
        "review_id": str(raw["pull_request_review_id"]),
        "author_association": raw["author_association"],
        "commit_sha": raw["original_commit_id"],
        "head": raw["commit_id"],
        "thread_id": None,
    }


def expected_review(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(raw["id"]),
        "html_url": raw["html_url"],
        "state": raw["state"].lower(),
        "author": {"id": str(raw["user"]["id"]), "username": raw["user"]["login"]} if raw.get("user") else None,
        "body": raw["body"] or None,
        "submitted_at": raw["submitted_at"],
        "commit_id": raw["commit_id"],
    }


def expected_check_run(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(raw["id"]),
        "name": raw["name"],
        "status": "completed" if raw["status"] == "completed" else "pending",
        "conclusion": raw["conclusion"],
        "html_url": raw["html_url"],
    }


def expected_workflow_run(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(raw["id"]),
        "name": raw["name"],
        "status": "completed" if raw["status"] == "completed" else "pending",
        "conclusion": raw["conclusion"],
    }


def expected_workflow_job(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(raw["id"]),
        "name": raw["name"],
        "status": "completed" if raw["status"] == "completed" else "pending",
        "conclusion": raw["conclusion"],
    }


REPOSITORY_RAW = make_github_repository()
ASSIGNEE_RAW = make_github_assignee()
LABEL_RAW = make_github_label()
COMMENT_RAW = make_github_comment()
REACTION_RAW = make_github_reaction()
PULL_REQUEST_RAW = make_github_pull_request()
ISSUE_RAW = make_github_issue()
BRANCH_RAW = make_github_branch()
GIT_REF_RAW = make_github_git_ref()
GIT_BLOB_RAW = make_github_git_blob()
FILE_CONTENT_RAW = make_github_file_content()
COMMIT_RAW = make_github_commit()
COMPARISON_RAW = make_github_commit_comparison(commits=[COMMIT_RAW])
TREE_RAW = make_github_git_tree()
GIT_COMMIT_OBJECT_RAW = make_github_git_commit_object()
PULL_REQUEST_FILE_RAW = make_github_pull_request_file(previous_filename="src/old.py")
PULL_REQUEST_COMMIT_RAW = make_github_pull_request_commit()
REVIEW_COMMENT_RAW = make_github_review_comment(user={"id": 42, "login": "testuser"})
REVIEW_RAW = make_github_review()
CHECK_RUN_RAW = make_github_check_run()
WORKFLOW_RUN_RAW = make_github_workflow_run()
WORKFLOW_JOB_RAW = make_github_workflow_job()


PAGINATED_CASES: list[dict[str, Any]] = [
    {
        "name": "list_repositories",
        "kwargs": {},
        "path": "/installation/repositories",
        "params": None,
        "pagination": None,
        "raw": {"repositories": [REPOSITORY_RAW], "total_count": 1},
        "expected_data": [expected_repository(REPOSITORY_RAW)],
        "next_cursor": "2",
    },
    {
        "name": "get_repository_assignees",
        "kwargs": {},
        "path": "/repos/test-org/test-repo/assignees",
        "params": None,
        "pagination": None,
        "raw": [ASSIGNEE_RAW],
        "expected_data": [expected_assignee(ASSIGNEE_RAW)],
        "next_cursor": "2",
    },
    {
        "name": "list_repository_user_permissions",
        "kwargs": {},
        "path": "/repos/test-org/test-repo/collaborators",
        "params": None,
        "pagination": None,
        "raw": [
            make_collaborator(
                login="reader",
                user_id=1,
                permissions={"pull": True, "triage": True, "push": False, "maintain": False, "admin": False},
            ),
            make_collaborator(
                login="writer",
                user_id=2,
                permissions={"pull": True, "triage": True, "push": True, "maintain": True, "admin": False},
            ),
            make_collaborator(
                login="admin",
                user_id=3,
                permissions={"pull": True, "triage": True, "push": True, "maintain": True, "admin": True},
            ),
        ],
        "expected_data": [
            {"login": "reader", "id": "1", "perms": "read"},
            {"login": "writer", "id": "2", "perms": "write"},
            {"login": "admin", "id": "3", "perms": "admin"},
        ],
        "next_cursor": "2",
    },
    {
        "name": "get_repository_labels",
        "kwargs": {},
        "path": "/repos/test-org/test-repo/labels",
        "params": None,
        "pagination": None,
        "raw": [LABEL_RAW],
        "expected_data": [expected_label(LABEL_RAW)],
        "next_cursor": "2",
    },
    {
        "name": "get_issue_comments",
        "kwargs": {"issue_id": "42"},
        "path": "/repos/test-org/test-repo/issues/42/comments",
        "params": None,
        "pagination": None,
        "raw": [COMMENT_RAW],
        "expected_data": [expected_comment(COMMENT_RAW)],
        "next_cursor": "2",
    },
    {
        "name": "get_pull_request_comments",
        "kwargs": {"pull_request_id": "42", "pagination": {"cursor": "4", "per_page": 25}},
        "path": "/repos/test-org/test-repo/issues/42/comments",
        "params": None,
        "pagination": {"cursor": "4", "per_page": 25},
        "raw": [COMMENT_RAW],
        "expected_data": [expected_comment(COMMENT_RAW)],
        "next_cursor": "5",
    },
    {
        "name": "get_issue_comment_reactions",
        "kwargs": {"issue_id": "42", "comment_id": "99"},
        "path": "/repos/test-org/test-repo/issues/comments/99/reactions",
        "params": None,
        "pagination": None,
        "raw": [REACTION_RAW],
        "expected_data": [expected_reaction(REACTION_RAW)],
        "next_cursor": "2",
    },
    {
        "name": "get_issue_reactions",
        "kwargs": {"issue_id": "42"},
        "path": "/repos/test-org/test-repo/issues/42/reactions",
        "params": None,
        "pagination": None,
        "raw": [REACTION_RAW],
        "expected_data": [expected_reaction(REACTION_RAW)],
        "next_cursor": "2",
    },
    {
        "name": "get_review_comment_reactions",
        "kwargs": {"pull_request_id": "42", "comment_id": "99"},
        "path": "/repos/test-org/test-repo/pulls/comments/99/reactions",
        "params": None,
        "pagination": None,
        "raw": [REACTION_RAW],
        "expected_data": [expected_reaction(REACTION_RAW)],
        "next_cursor": "2",
    },
    {
        "name": "get_commits",
        "kwargs": {"ref": "main", "pagination": {"cursor": "3", "per_page": 10}},
        "path": "/repos/test-org/test-repo/commits",
        "params": {"sha": "main"},
        "pagination": {"cursor": "3", "per_page": 10},
        "raw": [COMMIT_RAW],
        "expected_data": [expected_commit(COMMIT_RAW)],
        "next_cursor": "4",
    },
    {
        "name": "get_commit_changes",
        "kwargs": {"sha": "abc123"},
        "path": "/repos/test-org/test-repo/commits/abc123",
        "params": None,
        "pagination": None,
        "raw": COMMIT_RAW,
        "expected_data": expected_commit_with_changes(COMMIT_RAW)["files"],
        "next_cursor": "2",
    },
    {
        "name": "get_commits_by_path",
        "kwargs": {"path": "src/main.py", "ref": "main"},
        "path": "/repos/test-org/test-repo/commits",
        "params": {"path": "src/main.py", "sha": "main"},
        "pagination": None,
        "raw": [COMMIT_RAW],
        "expected_data": [expected_commit(COMMIT_RAW)],
        "next_cursor": "2",
    },
    {
        "name": "compare_commits",
        "kwargs": {"start_sha": "aaa", "end_sha": "bbb"},
        "path": "/repos/test-org/test-repo/compare/aaa...bbb",
        "params": None,
        "pagination": None,
        "raw": COMPARISON_RAW,
        "expected_data": {
            "ahead_by": COMPARISON_RAW["ahead_by"],
            "behind_by": COMPARISON_RAW["behind_by"],
            "commits": [expected_commit(COMMIT_RAW)],
            "diff": [
                {
                    "filename": "src/main.py",
                    "status": "modified",
                    "patch": "@@ -1,3 +1,4 @@\n+new line",
                    "additions": 1,
                    "deletions": 0,
                    "previous_filename": None,
                }
            ],
        },
        "next_cursor": "2",
    },
    {
        "name": "get_directory_contents",
        "kwargs": {"path": "src", "ref": "main"},
        "path": "/repos/test-org/test-repo/contents/src",
        "params": {"ref": "main"},
        "pagination": None,
        "raw": [FILE_CONTENT_RAW, FILE_CONTENT_RAW],
        "expected_data": [expected_file_content(FILE_CONTENT_RAW), expected_file_content(FILE_CONTENT_RAW)],
        "next_cursor": None,
    },
    {
        # GitHub returns the whole tree in one response, so get_tree is a
        # single page with no next cursor.
        "name": "get_tree",
        "kwargs": {"tree_sha": "tree123", "recursive": False},
        "path": "/repos/test-org/test-repo/git/trees/tree123",
        "params": {},
        "pagination": None,
        "raw": TREE_RAW,
        "expected_data": expected_tree(TREE_RAW),
        "next_cursor": None,
    },
    {
        "name": "get_pull_request_files",
        "kwargs": {"pull_request_id": "42"},
        "path": "/repos/test-org/test-repo/pulls/42/files",
        "params": None,
        "pagination": None,
        "raw": [PULL_REQUEST_FILE_RAW],
        "expected_data": [expected_pull_request_file(PULL_REQUEST_FILE_RAW)],
        "next_cursor": "2",
    },
    {
        "name": "get_pull_request_commits",
        "kwargs": {"pull_request_id": "42"},
        "path": "/repos/test-org/test-repo/pulls/42/commits",
        "params": None,
        "pagination": None,
        "raw": [PULL_REQUEST_COMMIT_RAW],
        "expected_data": [expected_pull_request_commit(PULL_REQUEST_COMMIT_RAW)],
        "next_cursor": "2",
    },
    {
        "name": "get_pull_requests",
        "kwargs": {
            "state": None,
            "head": "octocat:feature",
            "pagination": {"cursor": "2", "per_page": 15},
        },
        "path": "/repos/test-org/test-repo/pulls",
        "params": {"state": "all", "head": "octocat:feature"},
        "pagination": {"cursor": "2", "per_page": 15},
        "raw": [PULL_REQUEST_RAW],
        "expected_data": [expected_pull_request(PULL_REQUEST_RAW)],
        "next_cursor": "3",
    },
    {
        "name": "get_review_comments",
        "kwargs": {"pull_request_id": "42", "review_id": "80"},
        "path": "/repos/test-org/test-repo/pulls/42/reviews/80/comments",
        "params": None,
        "pagination": None,
        "raw": [REVIEW_COMMENT_RAW],
        "expected_data": [expected_review_comment(REVIEW_COMMENT_RAW)],
        "next_cursor": "2",
    },
    {
        "name": "get_review_comments",
        "kwargs": {
            "pull_request_id": "42",
            "review_id": "80",
            "pagination": {"cursor": "3", "per_page": 10},
        },
        "path": "/repos/test-org/test-repo/pulls/42/reviews/80/comments",
        "params": None,
        "pagination": {"cursor": "3", "per_page": 10},
        "raw": [REVIEW_COMMENT_RAW],
        "expected_data": [expected_review_comment(REVIEW_COMMENT_RAW)],
        "next_cursor": "4",
    },
    {
        "name": "list_check_runs_in_check_suite",
        "kwargs": {"check_suite_id": "500"},
        "path": "/repos/test-org/test-repo/check-suites/500/check-runs",
        "params": {"filter": "latest"},
        "pagination": None,
        "raw": {"total_count": 1, "check_runs": [CHECK_RUN_RAW]},
        "expected_data": [expected_check_run(CHECK_RUN_RAW)],
        "next_cursor": "2",
    },
    {
        "name": "list_check_runs_in_check_suite",
        "kwargs": {
            "check_suite_id": "500",
            "check_name": "Seer Review",
            "status": "completed",
            "timestamp_filter": "all",
            "pagination": {"cursor": "3", "per_page": 10},
        },
        "path": "/repos/test-org/test-repo/check-suites/500/check-runs",
        "params": {"check_name": "Seer Review", "status": "completed", "filter": "all"},
        "pagination": {"cursor": "3", "per_page": 10},
        "raw": {"total_count": 1, "check_runs": [CHECK_RUN_RAW]},
        "expected_data": [expected_check_run(CHECK_RUN_RAW)],
        "next_cursor": "4",
    },
    {
        "name": "list_check_runs_for_ref",
        "kwargs": {"ref": "abc123"},
        "path": "/repos/test-org/test-repo/commits/abc123/check-runs",
        "params": {"filter": "latest"},
        "pagination": None,
        "raw": {"total_count": 1, "check_runs": [CHECK_RUN_RAW]},
        "expected_data": [expected_check_run(CHECK_RUN_RAW)],
        "next_cursor": "2",
    },
    {
        "name": "list_check_runs_for_ref",
        "kwargs": {
            "ref": "abc123",
            "check_name": "Seer Review",
            "status": "completed",
            "timestamp_filter": "all",
            "pagination": {"cursor": "3", "per_page": 10},
        },
        "path": "/repos/test-org/test-repo/commits/abc123/check-runs",
        "params": {"check_name": "Seer Review", "status": "completed", "filter": "all"},
        "pagination": {"cursor": "3", "per_page": 10},
        "raw": {"total_count": 1, "check_runs": [CHECK_RUN_RAW]},
        "expected_data": [expected_check_run(CHECK_RUN_RAW)],
        "next_cursor": "4",
    },
    {
        "name": "list_workflow_runs",
        "kwargs": {"head_sha": "abc123"},
        "path": "/repos/test-org/test-repo/actions/runs",
        "params": {"head_sha": "abc123"},
        "pagination": None,
        "raw": {"total_count": 1, "workflow_runs": [WORKFLOW_RUN_RAW]},
        "expected_data": [expected_workflow_run(WORKFLOW_RUN_RAW)],
        "next_cursor": "2",
    },
    {
        "name": "list_workflow_runs",
        "kwargs": {"head_sha": "abc123", "pagination": {"cursor": "3", "per_page": 10}},
        "path": "/repos/test-org/test-repo/actions/runs",
        "params": {"head_sha": "abc123"},
        "pagination": {"cursor": "3", "per_page": 10},
        "raw": {"total_count": 1, "workflow_runs": [WORKFLOW_RUN_RAW]},
        "expected_data": [expected_workflow_run(WORKFLOW_RUN_RAW)],
        "next_cursor": "4",
    },
    {
        "name": "list_workflow_jobs",
        "kwargs": {"workflow_run_id": "400"},
        "path": "/repos/test-org/test-repo/actions/runs/400/jobs",
        "params": None,
        "pagination": None,
        "raw": {"total_count": 1, "jobs": [WORKFLOW_JOB_RAW]},
        "expected_data": [expected_workflow_job(WORKFLOW_JOB_RAW)],
        "next_cursor": "2",
    },
    {
        "name": "list_workflow_jobs",
        "kwargs": {"workflow_run_id": "400", "pagination": {"cursor": "3", "per_page": 10}},
        "path": "/repos/test-org/test-repo/actions/runs/400/jobs",
        "params": None,
        "pagination": {"cursor": "3", "per_page": 10},
        "raw": {"total_count": 1, "jobs": [WORKFLOW_JOB_RAW]},
        "expected_data": [expected_workflow_job(WORKFLOW_JOB_RAW)],
        "next_cursor": "4",
    },
    {
        "name": "list_pull_request_reviews",
        "kwargs": {"pull_request_id": "1"},
        "path": "/repos/test-org/test-repo/pulls/1/reviews",
        "params": None,
        "pagination": None,
        "raw": [REVIEW_RAW],
        "expected_data": [expected_review(REVIEW_RAW)],
        "next_cursor": "2",
    },
    {
        "name": "list_pull_request_reviews",
        "kwargs": {"pull_request_id": "1", "pagination": {"cursor": "3", "per_page": 10}},
        "path": "/repos/test-org/test-repo/pulls/1/reviews",
        "params": None,
        "pagination": {"cursor": "3", "per_page": 10},
        "raw": [REVIEW_RAW],
        "expected_data": [expected_review(REVIEW_RAW)],
        "next_cursor": "4",
    },
]


ACTION_CASES: list[dict[str, Any]] = [
    {
        "name": "get_app_installation",
        "operation": "get",
        "kwargs": {},
        "path": "/repos/test-org/test-repo/installation",
        "raw": {"permissions": {"contents": "write", "pull_requests": "write"}, "bar": "baz"},
        "expected_data": {
            "has_read_access": True,
            "has_write_access": True,
            "has_check_run_write_access": False,
        },
        "credentials_set": "application",
    },
    {
        "name": "get_repository",
        "operation": "get",
        "kwargs": {},
        "path": "/repos/test-org/test-repo",
        "raw": REPOSITORY_RAW,
        "expected_data": expected_repository(REPOSITORY_RAW),
    },
    {
        "name": "get_repository_user_permission",
        "operation": "get",
        "kwargs": {"username": "testuser"},
        "path": "/repos/test-org/test-repo/collaborators/testuser/permission",
        "raw": make_collaborator_permission(permission="write"),
        "expected_data": {"login": "testuser", "id": "123", "perms": "write"},
    },
    {
        "name": "get_repository_topics",
        "operation": "get",
        "kwargs": {},
        "path": "/repos/test-org/test-repo/topics",
        "raw": {"names": ["python", "api"]},
        "expected_data": ["python", "api"],
    },
    {
        "name": "create_issue_comment",
        "operation": "post",
        "kwargs": {"issue_id": "42", "body": "hello"},
        "path": "/repos/test-org/test-repo/issues/42/comments",
        "data": {"body": "hello"},
        "raw": COMMENT_RAW,
        "expected_data": expected_comment(COMMENT_RAW),
    },
    {
        "name": "get_pull_request",
        "operation": "get",
        "kwargs": {"pull_request_id": "42"},
        "path": "/repos/test-org/test-repo/pulls/42",
        "raw": PULL_REQUEST_RAW,
        "expected_data": expected_pull_request(PULL_REQUEST_RAW),
    },
    {
        "name": "get_issue",
        "operation": "get",
        "kwargs": {"issue_id": "7"},
        "path": "/repos/test-org/test-repo/issues/7",
        "raw": ISSUE_RAW,
        "expected_data": expected_issue(ISSUE_RAW),
    },
    {
        "name": "create_issue",
        "operation": "post",
        "kwargs": {"title": "bug", "body": "it broke"},
        "path": "/repos/test-org/test-repo/issues",
        "data": {"title": "bug", "body": "it broke"},
        "raw": ISSUE_RAW,
        "expected_data": expected_issue(ISSUE_RAW),
    },
    {
        "name": "create_pull_request_comment",
        "operation": "post",
        "kwargs": {"pull_request_id": "42", "body": "hello"},
        "path": "/repos/test-org/test-repo/issues/42/comments",
        "data": {"body": "hello"},
        "raw": COMMENT_RAW,
        "expected_data": expected_comment(COMMENT_RAW),
    },
    {
        "name": "create_issue_comment_reaction",
        "operation": "post",
        "kwargs": {"issue_id": "42", "comment_id": "99", "reaction": "heart"},
        "path": "/repos/test-org/test-repo/issues/comments/99/reactions",
        "data": {"content": "heart"},
        "raw": REACTION_RAW,
        "expected_data": expected_reaction(REACTION_RAW),
    },
    {
        "name": "create_issue_reaction",
        "operation": "post",
        "kwargs": {"issue_id": "42", "reaction": "rocket"},
        "path": "/repos/test-org/test-repo/issues/42/reactions",
        "data": {"content": "rocket"},
        "raw": REACTION_RAW,
        "expected_data": expected_reaction(REACTION_RAW),
    },
    {
        "name": "create_review_comment_reaction",
        "operation": "post",
        "kwargs": {"pull_request_id": "42", "comment_id": "99", "reaction": "heart"},
        "path": "/repos/test-org/test-repo/pulls/comments/99/reactions",
        "data": {"content": "heart"},
        "raw": REACTION_RAW,
        "expected_data": expected_reaction(REACTION_RAW),
    },
    {
        "name": "get_branch",
        "operation": "get",
        "kwargs": {"branch": "main"},
        "path": "/repos/test-org/test-repo/branches/main",
        "raw": BRANCH_RAW,
        "expected_data": expected_git_ref_from_branch(BRANCH_RAW),
    },
    {
        "name": "create_branch",
        "operation": "post",
        "kwargs": {"branch": "feature", "sha": "abc123"},
        "path": "/repos/test-org/test-repo/git/refs",
        "data": {"ref": "refs/heads/feature", "sha": "abc123"},
        "raw": GIT_REF_RAW,
        "expected_data": expected_git_ref(GIT_REF_RAW),
    },
    {
        "name": "update_branch",
        "operation": "patch",
        "kwargs": {"branch": "feature", "sha": "abc123", "force": True},
        "path": "/repos/test-org/test-repo/git/refs/heads/feature",
        "data": {"sha": "abc123", "force": True},
        "raw": GIT_REF_RAW,
        "expected_data": expected_git_ref(GIT_REF_RAW),
    },
    {
        "name": "get_git_ref",
        "operation": "get",
        "kwargs": {"ref": "heads/main"},
        "path": "/repos/test-org/test-repo/git/ref/heads/main",
        "raw": GIT_REF_RAW,
        "expected_data": expected_git_ref_full(GIT_REF_RAW),
    },
    {
        "name": "create_git_blob",
        "operation": "post",
        "kwargs": {"content": "hello", "encoding": "utf-8"},
        "path": "/repos/test-org/test-repo/git/blobs",
        "data": {"content": "hello", "encoding": "utf-8"},
        "raw": GIT_BLOB_RAW,
        "expected_data": {"sha": GIT_BLOB_RAW["sha"]},
    },
    {
        "name": "get_file_content",
        "operation": "get",
        "kwargs": {"path": "README.md", "ref": "main"},
        "path": "/repos/test-org/test-repo/contents/README.md",
        "params": {"ref": "main"},
        "raw": FILE_CONTENT_RAW,
        "expected_data": expected_file_content(FILE_CONTENT_RAW),
    },
    {
        "name": "get_readme",
        "operation": "get",
        "kwargs": {"ref": "main"},
        "path": "/repos/test-org/test-repo/readme",
        "params": {"ref": "main"},
        "raw": FILE_CONTENT_RAW,
        "expected_data": expected_file_content(FILE_CONTENT_RAW),
    },
    {
        "name": "get_commit",
        "operation": "get",
        "kwargs": {"sha": "abc123"},
        "path": "/repos/test-org/test-repo/commits/abc123",
        "raw": COMMIT_RAW,
        "expected_data": expected_commit_with_changes(COMMIT_RAW),
    },
    {
        "name": "get_full_tree",
        "operation": "get",
        "kwargs": {"tree_sha": "tree123", "recursive": False},
        "path": "/repos/test-org/test-repo/git/trees/tree123",
        "params": {},
        "raw": TREE_RAW,
        "expected_data": expected_tree(TREE_RAW),
    },
    {
        "name": "get_git_commit",
        "operation": "get",
        "kwargs": {"sha": "abc123"},
        "path": "/repos/test-org/test-repo/git/commits/abc123",
        "raw": GIT_COMMIT_OBJECT_RAW,
        "expected_data": expected_git_commit_object(GIT_COMMIT_OBJECT_RAW),
    },
    {
        "name": "create_git_tree",
        "operation": "post",
        "kwargs": {
            "tree": [{"path": "f.py", "mode": "100644", "type": "blob", "sha": "abc"}],
            "base_tree": "base123",
        },
        "path": "/repos/test-org/test-repo/git/trees",
        "data": {
            "tree": [{"path": "f.py", "mode": "100644", "type": "blob", "sha": "abc"}],
            "base_tree": "base123",
        },
        "raw": TREE_RAW,
        "expected_data": expected_tree(TREE_RAW),
    },
    {
        "name": "create_git_commit",
        "operation": "post",
        "kwargs": {"message": "msg", "tree_sha": "tree123", "parent_shas": ["p1", "p2"]},
        "path": "/repos/test-org/test-repo/git/commits",
        "data": {"message": "msg", "tree": "tree123", "parents": ["p1", "p2"]},
        "raw": GIT_COMMIT_OBJECT_RAW,
        "expected_data": expected_git_commit_object(GIT_COMMIT_OBJECT_RAW),
    },
    {
        "name": "create_pull_request",
        "operation": "post",
        "kwargs": {"title": "T", "body": "B", "head": "feature", "base": "main"},
        "path": "/repos/test-org/test-repo/pulls",
        "data": {"title": "T", "body": "B", "head": "feature", "base": "main"},
        "raw": PULL_REQUEST_RAW,
        "expected_data": expected_pull_request(PULL_REQUEST_RAW),
    },
    {
        "name": "create_pull_request_draft",
        "operation": "post",
        "kwargs": {"title": "T", "body": "B", "head": "feature", "base": "main"},
        "path": "/repos/test-org/test-repo/pulls",
        "data": {"title": "T", "body": "B", "head": "feature", "base": "main", "draft": True},
        "raw": PULL_REQUEST_RAW,
        "expected_data": expected_pull_request(PULL_REQUEST_RAW),
    },
    {
        "name": "update_pull_request",
        "operation": "patch",
        "kwargs": {"pull_request_id": "42", "title": "New", "body": "Body", "state": "closed"},
        "path": "/repos/test-org/test-repo/pulls/42",
        "data": {"title": "New", "body": "Body", "state": "closed"},
        "raw": PULL_REQUEST_RAW,
        "expected_data": expected_pull_request(PULL_REQUEST_RAW),
    },
    {
        "name": "create_review_comment_file",
        "operation": "post",
        "kwargs": {
            "pull_request_id": "42",
            "commit_id": "abc123",
            "body": "Looks good",
            "path": "src/main.py",
            "side": "head",
        },
        "path": "/repos/test-org/test-repo/pulls/42/comments",
        "data": {
            "body": "Looks good",
            "commit_id": "abc123",
            "path": "src/main.py",
            "side": "RIGHT",
            "subject_type": "file",
        },
        "raw": REVIEW_COMMENT_RAW,
        "expected_data": expected_review_comment(REVIEW_COMMENT_RAW),
    },
    {
        "name": "create_review_comment",
        "operation": "post",
        "kwargs": {
            "pull_request_id": "42",
            "commit_id": "abc123",
            "body": "Looks good",
            "path": "src/main.py",
            "line": DiffLine(head=3),
        },
        "path": "/repos/test-org/test-repo/pulls/42/comments",
        "data": {
            "body": "Looks good",
            "commit_id": "abc123",
            "path": "src/main.py",
            "line": 3,
            "side": "RIGHT",
        },
        "raw": REVIEW_COMMENT_RAW,
        "expected_data": expected_review_comment(REVIEW_COMMENT_RAW),
    },
    {
        # A removed line (base only) anchors LEFT.
        "name": "create_review_comment",
        "operation": "post",
        "kwargs": {
            "pull_request_id": "42",
            "commit_id": "abc123",
            "body": "Looks good",
            "path": "src/main.py",
            "line": DiffLine(base=7),
        },
        "path": "/repos/test-org/test-repo/pulls/42/comments",
        "data": {
            "body": "Looks good",
            "commit_id": "abc123",
            "path": "src/main.py",
            "line": 7,
            "side": "LEFT",
        },
        "raw": REVIEW_COMMENT_RAW,
        "expected_data": expected_review_comment(REVIEW_COMMENT_RAW),
    },
    {
        # Multiline range: end on head line 5, start on base line 1.
        "name": "create_review_comment",
        "operation": "post",
        "kwargs": {
            "pull_request_id": "42",
            "commit_id": "abc123",
            "body": "Looks good",
            "path": "src/main.py",
            "line": DiffLine(head=5),
            "start_line": DiffLine(base=1),
        },
        "path": "/repos/test-org/test-repo/pulls/42/comments",
        "data": {
            "body": "Looks good",
            "commit_id": "abc123",
            "path": "src/main.py",
            "line": 5,
            "side": "RIGHT",
            "start_line": 1,
            "start_side": "LEFT",
        },
        "raw": REVIEW_COMMENT_RAW,
        "expected_data": expected_review_comment(REVIEW_COMMENT_RAW),
    },
    {
        "name": "create_review_comment_reply",
        "operation": "post",
        "kwargs": {"pull_request_id": "42", "body": "reply", "comment_id": "99"},
        "path": "/repos/test-org/test-repo/pulls/42/comments",
        "data": {"body": "reply", "in_reply_to": 99},
        "raw": REVIEW_COMMENT_RAW,
        "expected_data": expected_review_comment(REVIEW_COMMENT_RAW),
    },
    {
        "name": "update_review_comment",
        "operation": "patch",
        "kwargs": {"pull_request_id": "42", "comment_id": "99", "body": "updated body"},
        "path": "/repos/test-org/test-repo/pulls/comments/99",
        "data": {"body": "updated body"},
        "raw": REVIEW_COMMENT_RAW,
        "expected_data": expected_review_comment(REVIEW_COMMENT_RAW),
    },
    {
        "name": "create_review",
        "operation": "post",
        "kwargs": {
            "pull_request_id": "42",
            "commit_sha": "abc123",
            "event": "approve",
            "comments": [{"path": "f.py", "body": "fix", "line": DiffLine(head=3)}],
            "body": "overall",
        },
        "path": "/repos/test-org/test-repo/pulls/42/reviews",
        "data": {
            "commit_id": "abc123",
            "event": "APPROVE",
            "comments": [{"path": "f.py", "body": "fix", "line": 3, "side": "RIGHT"}],
            "body": "overall",
        },
        "raw": REVIEW_RAW,
        "expected_data": expected_review(REVIEW_RAW),
    },
    {
        "name": "create_review",
        "operation": "post",
        "kwargs": {
            "pull_request_id": "42",
            "commit_sha": "abc123",
            "event": "comment",
            "comments": [{"path": "f.py", "body": "file-level note"}],
        },
        "path": "/repos/test-org/test-repo/pulls/42/reviews",
        "data": {
            "commit_id": "abc123",
            "event": "COMMENT",
            "comments": [{"path": "f.py", "body": "file-level note"}],
        },
        "raw": REVIEW_RAW,
        "expected_data": expected_review(REVIEW_RAW),
    },
    {
        "name": "create_check_run",
        "operation": "post",
        "kwargs": {
            "name": "Seer Review",
            "head_sha": "abc123",
            "status": "running",
            "conclusion": "success",
            "external_id": "ext-1",
            "started_at": "2026-02-04T10:00:00Z",
            "completed_at": "2026-02-04T10:05:00Z",
            "output": {"title": "Review", "summary": "All good"},
        },
        "path": "/repos/test-org/test-repo/check-runs",
        "data": {
            "name": "Seer Review",
            "head_sha": "abc123",
            "status": "in_progress",
            "conclusion": "success",
            "external_id": "ext-1",
            "started_at": "2026-02-04T10:00:00Z",
            "completed_at": "2026-02-04T10:05:00Z",
            "output": {"title": "Review", "summary": "All good"},
        },
        "raw": CHECK_RUN_RAW,
        "expected_data": expected_check_run(CHECK_RUN_RAW),
    },
    {
        "name": "get_check_run",
        "operation": "get",
        "kwargs": {"check_run_id": "300"},
        "path": "/repos/test-org/test-repo/check-runs/300",
        "raw": CHECK_RUN_RAW,
        "expected_data": expected_check_run(CHECK_RUN_RAW),
    },
    {
        "name": "update_check_run",
        "operation": "patch",
        "kwargs": {
            "check_run_id": "300",
            "status": "completed",
            "conclusion": "failure",
            "output": {"title": "Done", "summary": "Failed"},
        },
        "path": "/repos/test-org/test-repo/check-runs/300",
        "data": {
            "status": "completed",
            "conclusion": "failure",
            "output": {"title": "Done", "summary": "Failed"},
        },
        "raw": CHECK_RUN_RAW,
        "expected_data": expected_check_run(CHECK_RUN_RAW),
    },
    {
        "name": "get_archive_link",
        "id": "get_archive_link_tarball",
        "operation": "get",
        "status_code": 302,
        "kwargs": {"ref": "main"},
        "path": "/repos/test-org/test-repo/tarball/main",
        "headers": {"Location": "https://codeload.github.com/test-org/test-repo/legacy.tar.gz/refs/heads/main"},
        "raw": "https://codeload.github.com/test-org/test-repo/legacy.tar.gz/refs/heads/main",
        "expected_data": {
            "url": "https://codeload.github.com/test-org/test-repo/legacy.tar.gz/refs/heads/main",
            "headers": {},
        },
    },
    {
        "name": "get_archive_link",
        "id": "get_archive_link_zip",
        "operation": "get",
        "status_code": 302,
        "kwargs": {"ref": "main", "archive_format": "zip"},
        "path": "/repos/test-org/test-repo/zipball/main",
        "headers": {"Location": "https://codeload.github.com/test-org/test-repo/legacy.zip/refs/heads/main"},
        "raw": "https://codeload.github.com/test-org/test-repo/legacy.zip/refs/heads/main",
        "expected_data": {
            "url": "https://codeload.github.com/test-org/test-repo/legacy.zip/refs/heads/main",
            "headers": {},
        },
    },
]


VOID_CASES: list[dict[str, Any]] = [
    {
        "name": "delete_branch",
        "operation": "delete",
        "kwargs": {"branch": "feature"},
        "path": "/repos/test-org/test-repo/git/refs/heads/feature",
    },
    {
        "name": "delete_issue_comment",
        "operation": "delete",
        "kwargs": {"issue_id": "42", "comment_id": "99"},
        "path": "/repos/test-org/test-repo/issues/comments/99",
    },
    {
        "name": "delete_pull_request_comment",
        "operation": "delete",
        "kwargs": {"pull_request_id": "42", "comment_id": "99"},
        "path": "/repos/test-org/test-repo/issues/comments/99",
    },
    {
        "name": "delete_issue_comment_reaction",
        "operation": "delete",
        "kwargs": {"issue_id": "42", "comment_id": "99", "reaction_id": "5"},
        "path": "/repos/test-org/test-repo/issues/comments/99/reactions/5",
    },
    {
        "name": "delete_issue_reaction",
        "operation": "delete",
        "kwargs": {"issue_id": "42", "reaction_id": "5"},
        "path": "/repos/test-org/test-repo/issues/42/reactions/5",
    },
    {
        "name": "delete_review_comment_reaction",
        "operation": "delete",
        "kwargs": {"pull_request_id": "42", "comment_id": "99", "reaction_id": "5"},
        "path": "/repos/test-org/test-repo/pulls/comments/99/reactions/5",
    },
    {
        "name": "request_review",
        "operation": "post",
        "kwargs": {"pull_request_id": "42", "reviewers": ["octocat"]},
        "path": "/repos/test-org/test-repo/pulls/42/requested_reviewers",
        "data": {"reviewers": ["octocat"]},
    },
    {
        "name": "minimize_comment",
        "operation": "graphql",
        "kwargs": {"comment_node_id": "IC_123", "reason": "OUTDATED"},
        "query": MINIMIZE_COMMENT_MUTATION,
        "variables": {"commentId": "IC_123", "reason": "OUTDATED"},
    },
    {
        "name": "resolve_review_thread",
        "operation": "graphql",
        "kwargs": {"pull_request_id": "42", "thread_id": "PRRT_456"},
        "query": RESOLVE_REVIEW_THREAD_MUTATION,
        "variables": {"threadId": "PRRT_456"},
    },
]


ALIAS_METHODS: dict[str, tuple[str, dict[str, Any], tuple[Any, ...], Any]] = {
    "get_pull_request_comment_reactions": (
        "get_issue_comment_reactions",
        {"pull_request_id": "42", "comment_id": "99", "pagination": {"cursor": "2", "per_page": 5}},
        ("42", "99", {"cursor": "2", "per_page": 5}, None),
        {"data": ["ok"], "type": "github", "raw": [], "meta": {"next_cursor": "3"}},
    ),
    "create_pull_request_comment_reaction": (
        "create_issue_comment_reaction",
        {"pull_request_id": "42", "comment_id": "99", "reaction": "heart"},
        ("42", "99", "heart"),
        {"data": {"id": "1"}, "type": "github", "raw": {}, "meta": {}},
    ),
    "delete_pull_request_comment_reaction": (
        "delete_issue_comment_reaction",
        {"pull_request_id": "42", "comment_id": "99", "reaction_id": "5"},
        ("42", "99", "5"),
        None,
    ),
    "get_pull_request_reactions": (
        "get_issue_reactions",
        {"pull_request_id": "42", "pagination": {"cursor": "2", "per_page": 5}},
        ("42", {"cursor": "2", "per_page": 5}, None),
        {"data": ["ok"], "type": "github", "raw": [], "meta": {"next_cursor": "3"}},
    ),
    "create_pull_request_reaction": (
        "create_issue_reaction",
        {"pull_request_id": "42", "reaction": "rocket"},
        ("42", "rocket"),
        {"data": {"id": "1"}, "type": "github", "raw": {}, "meta": {}},
    ),
    "delete_pull_request_reaction": (
        "delete_issue_reaction",
        {"pull_request_id": "42", "reaction_id": "5"},
        ("42", "5"),
        None,
    ),
}


@pytest.mark.parametrize("case", PAGINATED_CASES)
def test_paginated_methods(case: dict[str, Any]) -> None:
    provider, client = make_provider()
    client.queue("get", FakeResponse(case["raw"]))

    result = getattr(provider, case["name"])(**case["kwargs"])

    assert result["type"] == "github"
    assert result["raw"] == {"data": case["raw"], "headers": {}}
    assert result["data"] == case["expected_data"]
    assert result["meta"] == {"next_cursor": case["next_cursor"]}

    assert client.calls == [
        {
            "operation": "get",
            "path": case["path"],
            "params": case["params"],
            "pagination": case["pagination"],
            "request_options": None,
            "extra_headers": None,
            "credentials_set": "installation",
            "timeout": None,
        }
    ]


@pytest.mark.parametrize("case", ACTION_CASES)
def test_action_methods(case: dict[str, Any]) -> None:
    provider, client = make_provider()
    client.queue(
        case["operation"],
        FakeResponse(case["raw"], headers=case.get("headers"), status_code=case.get("status_code")),
    )

    result = getattr(provider, case["name"])(**case["kwargs"])

    assert result["type"] == "github"
    assert result["raw"] == {"data": case["raw"], "headers": case.get("headers", {})}
    assert result["data"] == case["expected_data"]
    assert result["meta"] == {}

    expected_call = {"operation": case["operation"], "path": case["path"]}
    if "data" in case:
        expected_call["data"] = case["data"]
    if case["operation"] == "get":
        expected_call["params"] = case.get("params")
        expected_call["pagination"] = None
        expected_call["request_options"] = None
        expected_call["extra_headers"] = None
        expected_call["credentials_set"] = case.get("credentials_set", "installation")
        expected_call["timeout"] = None
    else:
        if "params" in case:
            expected_call["params"] = case["params"]
        expected_call["headers"] = case.get("headers")
    assert client.calls == [expected_call]


def test_get_authenticated_actor_uses_app_slug_for_bot_user() -> None:
    provider, client = make_provider()
    client.queue("get", FakeResponse({"id": 3028048, "slug": "sentry"}))
    client.queue("get", FakeResponse({"id": 177979347, "login": "sentry[bot]"}))

    result = provider.get_authenticated_actor()

    assert result["type"] == "github"
    assert result["data"] == {"id": "177979347", "username": "sentry[bot]"}
    assert result["raw"] == {"data": {"id": 177979347, "login": "sentry[bot]"}, "headers": {}}
    assert client.calls == [
        {
            "operation": "get",
            "path": "/app",
            "params": None,
            "pagination": None,
            "request_options": None,
            "extra_headers": None,
            "credentials_set": "application",
            "timeout": None,
        },
        {
            "operation": "get",
            "path": "/users/sentry[bot]",
            "params": None,
            "pagination": None,
            "request_options": None,
            "extra_headers": None,
            "credentials_set": "installation",
            "timeout": None,
        },
    ]


def test_create_pull_request_comment_forwards_copilot_chat_extensions() -> None:
    from scm.types import CoPilotChatExtension

    provider, client = make_provider()
    client.queue("post", FakeResponse(COMMENT_RAW))

    provider.create_pull_request_comment(
        pull_request_id="42",
        body="hello",
        extensions=[
            CoPilotChatExtension(name="explain", prompt="Explain this PR"),
            CoPilotChatExtension(name="review", prompt="Review this PR"),
        ],
    )

    assert client.calls == [
        {
            "operation": "post",
            "path": "/repos/test-org/test-repo/issues/42/comments",
            "data": {
                "body": "hello",
                "actions": [
                    {"name": "explain", "type": "copilot-chat", "prompt": "Explain this PR"},
                    {"name": "review", "type": "copilot-chat", "prompt": "Review this PR"},
                ],
            },
            "headers": None,
        }
    ]


def test_create_pull_request_comment_omits_actions_when_no_extensions() -> None:
    provider, client = make_provider()
    client.queue("post", FakeResponse(COMMENT_RAW))

    provider.create_pull_request_comment(pull_request_id="42", body="hello", extensions=None)

    assert client.calls == [
        {
            "operation": "post",
            "path": "/repos/test-org/test-repo/issues/42/comments",
            "data": {"body": "hello"},
            "headers": None,
        }
    ]


def test_create_issue_forwards_assignees_and_labels() -> None:
    provider, client = make_provider()
    client.queue("post", FakeResponse(ISSUE_RAW))

    provider.create_issue(title="bug", body="it broke", assignees=["alice", "bob"], labels=["bug", "p1"])

    assert client.calls == [
        {
            "operation": "post",
            "path": "/repos/test-org/test-repo/issues",
            "data": {
                "title": "bug",
                "body": "it broke",
                "assignees": ["alice", "bob"],
                "labels": ["bug", "p1"],
            },
            "headers": None,
        }
    ]


def test_update_issue_forwards_all_fields() -> None:
    provider, client = make_provider()
    client.queue("patch", FakeResponse(ISSUE_RAW))

    provider.update_issue(issue_id="42", state="closed", assignees=["alice"], labels=["bug"])

    assert client.calls == [
        {
            "operation": "patch",
            "path": "/repos/test-org/test-repo/issues/42",
            "data": {
                "state": "closed",
                "assignees": ["alice"],
                "labels": ["bug"],
            },
            "headers": None,
        }
    ]


def test_update_issue_omits_none_fields() -> None:
    provider, client = make_provider()
    client.queue("patch", FakeResponse(ISSUE_RAW))

    provider.update_issue(issue_id="42", state="open")

    assert client.calls == [
        {
            "operation": "patch",
            "path": "/repos/test-org/test-repo/issues/42",
            "data": {"state": "open"},
            "headers": None,
        }
    ]


def test_update_issue_empty_lists_clear_fields() -> None:
    provider, client = make_provider()
    client.queue("patch", FakeResponse(ISSUE_RAW))

    provider.update_issue(issue_id="42", assignees=[], labels=[])

    assert client.calls == [
        {
            "operation": "patch",
            "path": "/repos/test-org/test-repo/issues/42",
            "data": {"assignees": [], "labels": []},
            "headers": None,
        }
    ]


def test_get_file_content_raises_when_path_is_directory() -> None:
    provider, client = make_provider()
    client.queue("get", FakeResponse([FILE_CONTENT_RAW, FILE_CONTENT_RAW]))

    with pytest.raises(SCMCodedError) as exc_info:
        provider.get_file_content(path="src", ref="main")

    assert exc_info.value.code == "path_is_directory"
    assert exc_info.value.detail == "src"


def test_get_readme_raises_readme_not_found_on_404() -> None:
    provider, _ = make_provider()
    provider.get = MagicMock(side_effect=SCMCodedError(code="resource_not_found"))  # type: ignore[method-assign]

    with pytest.raises(SCMCodedError) as exc_info:
        provider.get_readme(ref="main")

    assert exc_info.value.code == "readme_not_found"


def test_get_pull_request_template_yields_root_and_multi_template_dir() -> None:
    provider, client = make_provider()
    template_root = make_github_file_content(path=".github/PULL_REQUEST_TEMPLATE.md")
    template_a = make_github_file_content(path=".github/PULL_REQUEST_TEMPLATE/feature.md")
    template_b = make_github_file_content(path=".github/PULL_REQUEST_TEMPLATE/bug.md")

    # Listing of .github/ — contains the single-template file and the multi-template dir.
    client.queue(
        "get",
        FakeResponse(
            [
                {"path": ".github/PULL_REQUEST_TEMPLATE.md", "type": "file", "sha": "a", "size": 1},
                {"path": ".github/PULL_REQUEST_TEMPLATE", "type": "dir", "sha": "b", "size": 0},
            ]
        ),
    )
    client.queue("get", FakeResponse(template_root))
    # Listing of .github/PULL_REQUEST_TEMPLATE/.
    client.queue(
        "get",
        FakeResponse(
            [
                {"path": ".github/PULL_REQUEST_TEMPLATE/feature.md", "type": "file", "sha": "c", "size": 1},
                {"path": ".github/PULL_REQUEST_TEMPLATE/bug.md", "type": "file", "sha": "d", "size": 1},
                {"path": ".github/PULL_REQUEST_TEMPLATE/notes.txt", "type": "file", "sha": "e", "size": 1},
            ]
        ),
    )
    client.queue("get", FakeResponse(template_a))
    client.queue("get", FakeResponse(template_b))
    # Listings of "" and "docs" — empty.
    client.queue("get", FakeResponse([]))
    client.queue("get", FakeResponse([]))

    results = list(provider.get_pull_request_template(ref="main"))

    assert [r["data"]["path"] for r in results] == [
        ".github/PULL_REQUEST_TEMPLATE.md",
        ".github/PULL_REQUEST_TEMPLATE/feature.md",
        ".github/PULL_REQUEST_TEMPLATE/bug.md",
    ]


def test_get_pull_request_template_skips_missing_parent_dir() -> None:
    provider, client = make_provider()
    # .github 404 -> skip, "" empty, docs/ empty. No templates.
    client.queue("get", FakeResponse({}, status_code=404))

    def get_with_404(*args, **kwargs):
        if not client.responses["get"]:
            return FakeResponse([])
        resp = client.responses["get"].pop(0)
        if resp.status_code == 404:
            raise SCMCodedError(code="resource_not_found")
        return resp

    provider.get = get_with_404  # type: ignore[assignment]

    results = list(provider.get_pull_request_template(ref="main"))

    assert results == []


def test_get_directory_contents_raises_when_path_is_not_directory() -> None:
    provider, client = make_provider()
    client.queue("get", FakeResponse(FILE_CONTENT_RAW))

    with pytest.raises(SCMCodedError) as exc_info:
        provider.get_directory_contents(path="README.md", ref="main")

    assert exc_info.value.code == "path_is_not_directory"
    assert exc_info.value.detail == "README.md"


def test_get_pull_request_diff_uses_raw_request_and_extracts_meta() -> None:
    provider, client = make_provider()
    response = FakeResponse(
        {},
        headers={
            "ETag": '"etag-123"',
            "Last-Modified": "Tue, 04 Feb 2026 10:00:00 GMT",
        },
        text="diff --git a/f.py b/f.py",
    )
    client.queue("get", response)

    result = provider.get_pull_request_diff("42")

    assert result["type"] == "github"
    assert result["raw"] == {
        "data": "diff --git a/f.py b/f.py",
        "headers": {
            "ETag": '"etag-123"',
            "Last-Modified": "Tue, 04 Feb 2026 10:00:00 GMT",
        },
    }
    assert result["data"] == "diff --git a/f.py b/f.py"
    assert result["meta"]["etag"] == '"etag-123"'
    assert result["meta"]["last_modified"].isoformat() == "2026-02-04T10:00:00+00:00"
    assert client.calls == [
        {
            "operation": "get",
            "path": "/repos/test-org/test-repo/pulls/42",
            "params": None,
            "pagination": None,
            "request_options": None,
            "extra_headers": {"Accept": "application/vnd.github.v3.diff"},
            "credentials_set": "installation",
            "timeout": None,
        }
    ]


@pytest.mark.parametrize("case", VOID_CASES)
def test_void_methods(case: dict[str, Any]) -> None:
    provider, client = make_provider()
    client.queue(case["operation"], {} if case["operation"] == "graphql" else FakeResponse({}))

    result = getattr(provider, case["name"])(**case["kwargs"])

    assert result is None
    if case["operation"] == "graphql":
        assert client.calls == [
            {
                "operation": "graphql",
                "query": case["query"],
                "variables": case["variables"],
            }
        ]
    elif case["operation"] == "post":
        assert client.calls == [
            {
                "operation": "post",
                "path": case["path"],
                "data": case["data"],
                "headers": None,
            }
        ]
    else:
        assert client.calls == [{"operation": "delete", "path": case["path"]}]


@pytest.mark.parametrize("method_name", sorted(ALIAS_METHODS))
def test_alias_methods_delegate_to_issue_methods(method_name: str) -> None:
    delegated_name, kwargs, expected_args, expected_result = ALIAS_METHODS[method_name]
    provider, _ = make_provider()
    delegated = MagicMock(return_value=expected_result)
    setattr(provider, delegated_name, delegated)

    result = getattr(provider, method_name)(**kwargs)

    delegated.assert_called_once_with(*expected_args)
    assert result == expected_result


def test_provider_initialization_wraps_api_client() -> None:
    raw_client = MagicMock(spec=ApiClient)
    repository = make_repository()

    provider = GitHubProvider(
        raw_client,
        organization_id=99,
        repository=repository,
        rate_limiter=NoOpRateLimiter(),
    )

    assert provider.organization_id == 99
    assert provider.repository == repository


def test_is_rate_limited_returns_false() -> None:
    provider, _ = make_provider()

    assert provider.is_rate_limited("shared") is False


def _make_api_client() -> GitHubProvider:
    return GitHubProvider(
        client=MagicMock(spec=ApiClient),
        organization_id=1,
        repository=make_repository(),
        rate_limiter=NoOpRateLimiter(),
    )


class TestGitHubProviderApiClientGraphql:
    def test_returns_data_on_success(self) -> None:
        api_client = _make_api_client()
        api_client.post = MagicMock(  # type: ignore[method-assign]
            return_value=FakeResponse({"data": {"viewer": {"login": "octocat"}}})
        )

        result = api_client.graphql("{ viewer { login } }", {})

        assert result == {"viewer": {"login": "octocat"}}
        api_client.post.assert_called_once_with("/graphql", data={"query": "{ viewer { login } }"}, headers={})

    def test_includes_variables_when_provided(self) -> None:
        api_client = _make_api_client()
        api_client.post = MagicMock(  # type: ignore[method-assign]
            return_value=FakeResponse({"data": {"node": {"id": "123"}}})
        )

        api_client.graphql("query($id: ID!) { node(id: $id) { id } }", {"id": "123"})

        call_data = (
            api_client.post.call_args[1]["data"] if api_client.post.call_args[1] else api_client.post.call_args[0][1]
        )
        assert call_data["variables"] == {"id": "123"}

    def test_excludes_variables_when_empty(self) -> None:
        api_client = _make_api_client()
        api_client.post = MagicMock(  # type: ignore[method-assign]
            return_value=FakeResponse({"data": {}})
        )

        api_client.graphql("{ viewer { login } }", {})

        call_data = (
            api_client.post.call_args[1]["data"] if api_client.post.call_args[1] else api_client.post.call_args[0][1]
        )
        assert "variables" not in call_data

    def test_raises_on_non_dict_response(self) -> None:
        api_client = _make_api_client()
        api_client.post = MagicMock(  # type: ignore[method-assign]
            return_value=FakeResponse([{"unexpected": "list"}])
        )

        with pytest.raises(SCMCodedError) as exc_info:
            api_client.graphql("{ viewer { login } }", {})
        assert exc_info.value.code == "unexpected_response_format"

    def test_raises_on_response_missing_data_and_errors(self) -> None:
        api_client = _make_api_client()
        api_client.post = MagicMock(  # type: ignore[method-assign]
            return_value=FakeResponse({"something": "else"})
        )

        with pytest.raises(SCMCodedError) as exc_info:
            api_client.graphql("{ viewer { login } }", {})
        assert exc_info.value.code == "unexpected_response_format"

    def test_raises_on_errors_without_data(self) -> None:
        api_client = _make_api_client()
        api_client.post = MagicMock(  # type: ignore[method-assign]
            return_value=FakeResponse({"errors": [{"message": "Field not found"}, {"message": "Unauthorized"}]})
        )

        with pytest.raises(SCMCodedError) as exc_info:
            api_client.graphql("{ viewer { login } }", {})
        assert exc_info.value.code == "resource_bad_request"
        assert exc_info.value.detail == "Field not found\nUnauthorized"

    def test_returns_data_on_partial_success_with_errors(self) -> None:
        api_client = _make_api_client()
        api_client.post = MagicMock(  # type: ignore[method-assign]
            return_value=FakeResponse(
                {
                    "data": {"viewer": {"login": "octocat"}},
                    "errors": [{"message": "Some warning"}],
                }
            )
        )

        result = api_client.graphql("{ viewer { login } }", {})

        assert result == {"viewer": {"login": "octocat"}}

    def test_returns_empty_dict_when_data_key_missing_but_errors_empty(self) -> None:
        api_client = _make_api_client()
        api_client.post = MagicMock(  # type: ignore[method-assign]
            return_value=FakeResponse({"errors": []})
        )

        result = api_client.graphql("{ viewer { login } }", {})

        assert result == {}


def _queue_raw_bytes(client: RecordingClient, content: bytes) -> None:
    response = FakeResponse({})
    response.content = content
    client.queue("get", response)


def test_download_archive_returns_bytes_from_response() -> None:
    provider, client = make_provider()
    _queue_raw_bytes(client, b"tarball-bytes")

    result = provider.download_archive("main", request_options={"timeout": 10.5})

    assert result.content == b"tarball-bytes"
    assert client.calls == [
        {
            "operation": "get",
            "path": "/repos/test-org/test-repo/tarball/main",
            "params": None,
            "pagination": None,
            "request_options": {"timeout": 10.5},
            "extra_headers": None,
            "credentials_set": "installation",
            "timeout": 10.5,
        }
    ]


def test_download_archive_zip_uses_zipball_path() -> None:
    provider, client = make_provider()
    _queue_raw_bytes(client, b"zip-bytes")

    result = provider.download_archive("main", archive_format="zip", request_options={"timeout": (10, 300)})

    assert result.content == b"zip-bytes"
    assert client.calls[0]["path"] == "/repos/test-org/test-repo/zipball/main"
    assert client.calls[0]["timeout"] == (10, 300)


def test_download_workflow_job_log_returns_bytes_from_response() -> None:
    provider, client = make_provider()
    _queue_raw_bytes(client, b"job-log-bytes")

    result = provider.download_workflow_job_log("400", request_options={"timeout": 10.5})

    assert result.content == b"job-log-bytes"
    assert client.calls == [
        {
            "operation": "get",
            "path": "/repos/test-org/test-repo/actions/jobs/400/logs",
            "params": None,
            "pagination": None,
            "request_options": {"timeout": 10.5},
            "extra_headers": None,
            "credentials_set": "installation",
            "timeout": 10.5,
        }
    ]


def test_get_file_url_builds_blob_url() -> None:
    provider, _ = make_provider()

    assert provider.get_file_url("src/main.py", "abc123") == (
        "https://github.com/test-org/test-repo/blob/abc123/src/main.py"
    )
    assert provider.get_file_url("src/main.py", "abc123", start_line=10) == (
        "https://github.com/test-org/test-repo/blob/abc123/src/main.py#L10"
    )
    assert provider.get_file_url("src/main.py", "abc123", start_line=10, end_line=20) == (
        "https://github.com/test-org/test-repo/blob/abc123/src/main.py#L10-L20"
    )
    assert provider.get_file_url("src/main.py", "abc123", end_line=20) == (
        "https://github.com/test-org/test-repo/blob/abc123/src/main.py#L20"
    )


def test_get_commit_url_builds_commit_url() -> None:
    provider, _ = make_provider()

    assert provider.get_commit_url("abc123") == "https://github.com/test-org/test-repo/commit/abc123"


def test_get_commits_url_builds_commits_list_url() -> None:
    provider, _ = make_provider()

    assert provider.get_commits_url("abc123") == "https://github.com/test-org/test-repo/commits/abc123"
    assert (
        provider.get_commits_url("abc123", file_path="src/foo/bar.py")
        == "https://github.com/test-org/test-repo/commits/abc123/src/foo/bar.py"
    )
    assert (
        provider.get_commits_url("abc123", since=date(2026, 1, 15))
        == "https://github.com/test-org/test-repo/commits/abc123?since=2026-01-15"
    )
    assert (
        provider.get_commits_url("abc123", until=date(2026, 3, 20))
        == "https://github.com/test-org/test-repo/commits/abc123?until=2026-03-20"
    )
    assert (
        provider.get_commits_url("abc123", since=date(2026, 1, 15), until=date(2026, 3, 20))
        == "https://github.com/test-org/test-repo/commits/abc123?since=2026-01-15&until=2026-03-20"
    )
    assert (
        provider.get_commits_url("abc123", file_path="src/foo/bar.py", since=date(2026, 1, 15), until=date(2026, 3, 20))
        == "https://github.com/test-org/test-repo/commits/abc123/src/foo/bar.py?since=2026-01-15&until=2026-03-20"
    )


def test_get_pull_request_url_builds_pr_url() -> None:
    provider, _ = make_provider()

    assert provider.get_pull_request_url("42") == "https://github.com/test-org/test-repo/pull/42"


def test_ghe_web_base_url_used_in_url_methods() -> None:
    provider = GitHubProvider(
        MagicMock(spec=ApiClient),
        organization_id=1,
        repository=make_repository(),
        rate_limiter=NoOpRateLimiter(),
        web_base_url="https://github.example.com",
    )

    assert provider.get_file_url("src/main.py", "abc123") == (
        "https://github.example.com/test-org/test-repo/blob/abc123/src/main.py"
    )
    assert provider.get_commit_url("abc123") == ("https://github.example.com/test-org/test-repo/commit/abc123")
    assert provider.get_commits_url("abc123", since=date(2026, 1, 15)) == (
        "https://github.example.com/test-org/test-repo/commits/abc123?since=2026-01-15"
    )
    assert provider.get_pull_request_url("42") == ("https://github.example.com/test-org/test-repo/pull/42")


def test_create_commit_chains_low_level_git_calls() -> None:
    provider, client = make_provider()

    client.queue("post", FakeResponse(make_github_git_blob(sha="blob_upd")))
    client.queue("get", FakeResponse(make_github_file_content(path="old.md", sha="blob_moved")))
    client.queue("get", FakeResponse(make_github_file_content(path="run.sh", sha="blob_chmod")))
    client.queue("get", FakeResponse(make_github_git_commit_object(sha="parent_sha", tree_sha="parent_tree")))
    client.queue("post", FakeResponse(make_github_git_tree(sha="new_tree_sha")))
    client.queue(
        "post",
        FakeResponse(make_github_git_commit_object(sha="new_commit_sha", tree_sha="new_tree_sha", message="Edits")),
    )
    client.queue("patch", FakeResponse(make_github_git_ref(ref="refs/heads/topic", sha="new_commit_sha")))

    result = provider.create_commit(
        branch="topic",
        parent_sha="parent_sha",
        message="Edits",
        actions=[
            WriteCommitAction(action="create", filename="new.md", content="hello", encoding="utf-8"),
            WriteCommitAction(action="update", filename="README.md", content="Zm9v", encoding="base64"),
            DeleteCommitAction(filename="obsolete.md"),
            MoveCommitAction(old_filename="old.md", new_filename="renamed.md"),
            ChmodCommitAction(filename="run.sh", executable=True),
        ],
    )

    assert result["type"] == "github"
    assert result["data"]["id"] == "new_commit_sha"
    assert result["data"]["message"] == "Edits"
    assert "files" not in result["data"]

    expected_tree_entries = [
        {"path": "new.md", "mode": "100644", "type": "blob", "content": "hello"},
        {"path": "README.md", "mode": "100644", "type": "blob", "sha": "blob_upd"},
        {"path": "obsolete.md", "mode": "100644", "type": "blob", "sha": None},
        {"path": "old.md", "mode": "100644", "type": "blob", "sha": None},
        {"path": "renamed.md", "mode": "100644", "type": "blob", "sha": "blob_moved"},
        {"path": "run.sh", "mode": "100755", "type": "blob", "sha": "blob_chmod"},
    ]

    assert client.calls == [
        {
            "operation": "post",
            "path": "/repos/test-org/test-repo/git/blobs",
            "data": {"content": "Zm9v", "encoding": "base64"},
            "headers": None,
        },
        {
            "operation": "get",
            "path": "/repos/test-org/test-repo/contents/old.md",
            "params": {"ref": "parent_sha"},
            "pagination": None,
            "request_options": None,
            "extra_headers": None,
            "credentials_set": "installation",
            "timeout": None,
        },
        {
            "operation": "get",
            "path": "/repos/test-org/test-repo/contents/run.sh",
            "params": {"ref": "parent_sha"},
            "pagination": None,
            "request_options": None,
            "extra_headers": None,
            "credentials_set": "installation",
            "timeout": None,
        },
        {
            "operation": "get",
            "path": "/repos/test-org/test-repo/git/commits/parent_sha",
            "params": None,
            "pagination": None,
            "request_options": None,
            "extra_headers": None,
            "credentials_set": "installation",
            "timeout": None,
        },
        {
            "operation": "post",
            "path": "/repos/test-org/test-repo/git/trees",
            "data": {"tree": expected_tree_entries, "base_tree": "parent_tree"},
            "headers": None,
        },
        {
            "operation": "post",
            "path": "/repos/test-org/test-repo/git/commits",
            "data": {"message": "Edits", "tree": "new_tree_sha", "parents": ["parent_sha"]},
            "headers": None,
        },
        {
            "operation": "patch",
            "path": "/repos/test-org/test-repo/git/refs/heads/topic",
            "data": {"sha": "new_commit_sha", "force": False},
            "headers": None,
        },
    ]


def test_create_commit_calls_create_branch_when_create_branch_true() -> None:
    provider, client = make_provider()

    client.queue("get", FakeResponse(make_github_git_commit_object(sha="parent_sha", tree_sha="parent_tree")))
    client.queue("post", FakeResponse(make_github_git_tree(sha="new_tree_sha")))
    client.queue(
        "post",
        FakeResponse(make_github_git_commit_object(sha="new_commit_sha", tree_sha="new_tree_sha", message="Edits")),
    )
    client.queue("post", FakeResponse(make_github_git_ref(ref="refs/heads/topic", sha="new_commit_sha")))

    result = provider.create_commit(
        branch="topic",
        parent_sha="parent_sha",
        message="Edits",
        actions=[WriteCommitAction(action="create", filename="new.md", content="hello", encoding="utf-8")],
        create_branch=True,
    )

    assert result["data"]["id"] == "new_commit_sha"

    final_call = client.calls[-1]
    assert final_call["operation"] == "post"
    assert final_call["path"] == "/repos/test-org/test-repo/git/refs"
    assert final_call["data"] == {"ref": "refs/heads/topic", "sha": "new_commit_sha"}


def test_create_pull_request_draft_raises_coded_error_when_drafts_not_supported() -> None:
    provider, client = make_provider()
    provider.post = MagicMock(  # type: ignore[assignment]
        side_effect=SCMCodedError(
            code="resource_unprocessable_content",
            detail="Draft pull requests are not supported for this repository",
        ),
    )

    with pytest.raises(SCMCodedError) as exc_info:
        provider.create_pull_request_draft(title="T", body="B", head="feature", base="main")

    assert exc_info.value.code == "draft_pull_request_not_supported"
    assert exc_info.value.__cause__ is not None
    assert exc_info.value.__cause__.code == "resource_unprocessable_content"  # type: ignore[attr-defined]


def test_create_pull_request_draft_reraises_unrelated_unprocessable_content_error() -> None:
    provider, client = make_provider()
    provider.post = MagicMock(  # type: ignore[assignment]
        side_effect=SCMCodedError(
            code="resource_unprocessable_content",
            detail="Validation Failed: something else went wrong",
        ),
    )

    with pytest.raises(SCMCodedError) as exc_info:
        provider.create_pull_request_draft(title="T", body="B", head="feature", base="main")

    assert exc_info.value.code == "resource_unprocessable_content"


def _thread_node(
    thread_id: str, comment_ids: list[str], *, has_more_comments: bool = False, end_cursor: str | None = None
) -> dict[str, Any]:
    return {
        "id": thread_id,
        "comments": {
            "pageInfo": {"hasNextPage": has_more_comments, "endCursor": end_cursor},
            "nodes": [{"id": cid} for cid in comment_ids],
        },
    }


def test_get_thread_id_from_review_comment_unique_id_returns_match_in_first_page() -> None:
    provider, client = make_provider()
    client.queue(
        "graphql",
        {
            "repository": {
                "pullRequest": {
                    "reviewThreads": {
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                        "nodes": [
                            _thread_node("PRRT_other", ["PRRC_unrelated"]),
                            _thread_node("PRRT_target", ["PRRC_a", "PRRC_match"]),
                        ],
                    }
                }
            }
        },
    )

    result = provider.get_thread_id_from_review_comment_unique_id("42", "PRRC_match")

    assert result == "PRRT_target"
    assert client.calls == [
        {
            "operation": "graphql",
            "query": REVIEW_THREAD_BY_COMMENT_QUERY,
            "variables": {"owner": "test-org", "name": "test-repo", "number": 42, "cursor": None},
        }
    ]


def test_get_thread_id_from_review_comment_unique_id_paginates_until_found() -> None:
    provider, client = make_provider()
    client.queue(
        "graphql",
        {
            "repository": {
                "pullRequest": {
                    "reviewThreads": {
                        "pageInfo": {"hasNextPage": True, "endCursor": "page2"},
                        "nodes": [_thread_node("PRRT_1", ["PRRC_other"])],
                    }
                }
            }
        },
    )
    client.queue(
        "graphql",
        {
            "repository": {
                "pullRequest": {
                    "reviewThreads": {
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                        "nodes": [_thread_node("PRRT_2", ["PRRC_match"])],
                    }
                }
            }
        },
    )

    result = provider.get_thread_id_from_review_comment_unique_id("42", "PRRC_match")

    assert result == "PRRT_2"
    assert [call["variables"]["cursor"] for call in client.calls] == [None, "page2"]


def test_get_thread_id_from_review_comment_unique_id_paginates_inner_comments() -> None:
    """Threads with more than 100 comments require an inner pagination loop."""
    provider, client = make_provider()
    client.queue(
        "graphql",
        {
            "repository": {
                "pullRequest": {
                    "reviewThreads": {
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                        "nodes": [
                            _thread_node(
                                "PRRT_target",
                                ["PRRC_first_page"],
                                has_more_comments=True,
                                end_cursor="comments_p2",
                            ),
                        ],
                    }
                }
            }
        },
    )
    client.queue(
        "graphql",
        {
            "node": {
                "comments": {
                    "pageInfo": {"hasNextPage": True, "endCursor": "comments_p3"},
                    "nodes": [{"id": "PRRC_second_page"}],
                }
            }
        },
    )
    client.queue(
        "graphql",
        {
            "node": {
                "comments": {
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                    "nodes": [{"id": "PRRC_match"}],
                }
            }
        },
    )

    result = provider.get_thread_id_from_review_comment_unique_id("42", "PRRC_match")

    assert result == "PRRT_target"
    assert [call["query"] for call in client.calls] == [
        REVIEW_THREAD_BY_COMMENT_QUERY,
        THREAD_COMMENTS_QUERY,
        THREAD_COMMENTS_QUERY,
    ]
    assert [call["variables"] for call in client.calls[1:]] == [
        {"threadId": "PRRT_target", "cursor": "comments_p2"},
        {"threadId": "PRRT_target", "cursor": "comments_p3"},
    ]


def test_get_thread_id_from_review_comment_unique_id_raises_when_pull_request_missing() -> None:
    provider, client = make_provider()
    client.queue("graphql", {"repository": {"pullRequest": None}})

    with pytest.raises(SCMCodedError) as exc_info:
        provider.get_thread_id_from_review_comment_unique_id("999", "PRRC_match")

    assert exc_info.value.code == "resource_not_found"


def test_get_thread_id_from_review_comment_unique_id_raises_when_repository_missing() -> None:
    provider, client = make_provider()
    client.queue("graphql", {"repository": None})

    with pytest.raises(SCMCodedError) as exc_info:
        provider.get_thread_id_from_review_comment_unique_id("42", "PRRC_match")

    assert exc_info.value.code == "resource_not_found"


def test_get_thread_id_from_review_comment_unique_id_handles_deleted_thread_during_inner_pagination() -> None:
    provider, client = make_provider()
    client.queue(
        "graphql",
        {
            "repository": {
                "pullRequest": {
                    "reviewThreads": {
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                        "nodes": [
                            _thread_node(
                                "PRRT_target",
                                ["PRRC_first_page"],
                                has_more_comments=True,
                                end_cursor="comments_p2",
                            ),
                        ],
                    }
                }
            }
        },
    )
    client.queue("graphql", {"node": None})

    assert provider.get_thread_id_from_review_comment_unique_id("42", "PRRC_match") is None


def test_get_thread_id_from_review_comment_unique_id_returns_none_when_not_found() -> None:
    provider, client = make_provider()
    client.queue(
        "graphql",
        {
            "repository": {
                "pullRequest": {
                    "reviewThreads": {
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                        "nodes": [_thread_node("PRRT_1", ["PRRC_other"])],
                    }
                }
            }
        },
    )

    assert provider.get_thread_id_from_review_comment_unique_id("42", "PRRC_missing") is None
    assert len(client.calls) == 1


def _review_threads_graphql_payload(
    nodes: list[dict[str, Any]],
    *,
    has_next_page: bool = False,
    end_cursor: str | None = None,
) -> dict[str, Any]:
    return {
        "repository": {
            "pullRequest": {
                "reviewThreads": {
                    "pageInfo": {"hasNextPage": has_next_page, "endCursor": end_cursor},
                    "nodes": nodes,
                }
            }
        }
    }


def _review_thread_node(
    thread_id: str,
    comment_nodes: list[dict[str, Any]],
    *,
    is_resolved: bool = False,
    is_outdated: bool = False,
    path: str = "src/main.py",
    line: int | None = 10,
    start_line: int | None = None,
    comments_has_next_page: bool = False,
    comments_end_cursor: str | None = None,
) -> dict[str, Any]:
    return {
        "id": thread_id,
        "isResolved": is_resolved,
        "isOutdated": is_outdated,
        "path": path,
        "line": line,
        "startLine": start_line,
        "comments": {
            "pageInfo": {"hasNextPage": comments_has_next_page, "endCursor": comments_end_cursor},
            "nodes": comment_nodes,
        },
    }


def _make_thread_comment_node(
    node_id: str = "PRRC_a",
    full_database_id: int | None = 1001,
    body: str = "hello",
    author_login: str = "reviewer",
    author_database_id: int | None = 42,
    author_typename: str = "User",
    created_at: str = "2026-02-04T10:00:00Z",
    updated_at: str = "2026-02-04T10:00:00Z",
    is_minimized: bool = False,
    reactions: list[dict[str, Any]] | None = None,
    url: str | None = "https://github.com/test-org/test-repo/pull/42#r1001",
    diff_hunk: str | None = "@@ -1 +1 @@",
    author_association: str | None = "MEMBER",
    review_database_id: int | None = 555,
    commit_oid: str | None = "headsha",
    original_commit_oid: str | None = "origsha",
) -> dict[str, Any]:
    node: dict[str, Any] = {
        "id": node_id,
        "fullDatabaseId": full_database_id,
        "url": url,
        "body": body,
        "isMinimized": is_minimized,
        "diffHunk": diff_hunk,
        "createdAt": created_at,
        "updatedAt": updated_at,
        "authorAssociation": author_association,
        "commit": {"oid": commit_oid} if commit_oid is not None else None,
        "originalCommit": {"oid": original_commit_oid} if original_commit_oid is not None else None,
        "pullRequestReview": ({"databaseId": review_database_id} if review_database_id is not None else None),
        "author": {
            "login": author_login,
            "__typename": author_typename,
            "databaseId": author_database_id,
        },
    }
    if reactions is not None:
        node["reactions"] = {"nodes": reactions}
    return node


def test_get_pull_request_review_threads_returns_threads_with_comments() -> None:
    provider, client = make_provider()
    client.queue(
        "graphql",
        _review_threads_graphql_payload(
            [
                _review_thread_node(
                    "PRRT_1",
                    [
                        _make_thread_comment_node(
                            node_id="PRRC_a",
                            full_database_id=1001,
                            author_login="sentry-bot",
                            author_typename="Bot",
                            author_database_id=None,
                            is_minimized=True,
                        ),
                    ],
                    is_outdated=True,
                    start_line=5,
                ),
            ]
        ),
    )

    result = provider.get_pull_request_review_threads("42")

    assert result["type"] == "github"
    assert result["meta"] == {"next_cursor": None}
    assert result["data"] == [
        {
            "id": "PRRT_1",
            "is_resolved": False,
            "is_outdated": True,
            "file_path": "src/main.py",
            "line": 10,
            "start_line": 5,
            "comments": [
                {
                    "id": "1001",
                    "unique_id": "PRRC_a",
                    "body": "hello",
                    "author": {"id": "sentry-bot", "username": "sentry-bot"},
                    "is_bot": True,
                    "created_at": "2026-02-04T10:00:00Z",
                    "updated_at": "2026-02-04T10:00:00Z",
                    "url": "https://github.com/test-org/test-repo/pull/42#r1001",
                    "diff_hunk": "@@ -1 +1 @@",
                    "author_association": "MEMBER",
                    "review_id": "555",
                    "commit_sha": "origsha",
                    "is_minimized": True,
                },
            ],
        },
    ]
    assert client.calls == [
        {
            "operation": "graphql",
            "query": REVIEW_THREADS_QUERY,
            "variables": {
                "owner": "test-org",
                "name": "test-repo",
                "number": 42,
                "cursor": None,
                "perPage": 100,
            },
        }
    ]


def test_get_pull_request_review_threads_is_resolved_from_graphql() -> None:
    provider, client = make_provider()
    client.queue(
        "graphql",
        _review_threads_graphql_payload(
            [_review_thread_node("PRRT_1", [_make_thread_comment_node()], is_resolved=True)],
        ),
    )

    result = provider.get_pull_request_review_threads("42")

    assert result["data"][0]["is_resolved"] is True


def test_get_pull_request_review_threads_include_reactions() -> None:
    provider, client = make_provider()
    client.queue(
        "graphql",
        _review_threads_graphql_payload(
            [
                _review_thread_node(
                    "PRRT_1",
                    [
                        _make_thread_comment_node(
                            reactions=[
                                {
                                    "databaseId": 10,
                                    "content": "THUMBS_UP",
                                    "user": {
                                        "login": "alice",
                                        "__typename": "User",
                                        "databaseId": 100,
                                    },
                                },
                                {
                                    "databaseId": 11,
                                    "content": "HEART",
                                    "user": {
                                        "login": "bob",
                                        "__typename": "User",
                                        "databaseId": 101,
                                    },
                                },
                            ],
                        ),
                    ],
                ),
            ]
        ),
    )

    result = provider.get_pull_request_review_threads("42", include_reactions=True)

    comment = result["data"][0]["comments"][0]
    assert comment["reactions"] == [
        {"id": "10", "content": "+1", "author": {"id": "100", "username": "alice"}},
        {"id": "11", "content": "heart", "author": {"id": "101", "username": "bob"}},
    ]
    assert client.calls[0]["query"] == REVIEW_THREADS_WITH_REACTIONS_QUERY
    assert "reactions(first: 10)" in client.calls[0]["query"]


def test_get_pull_request_review_threads_omits_reactions_when_not_requested() -> None:
    provider, client = make_provider()
    client.queue(
        "graphql",
        _review_threads_graphql_payload([_review_thread_node("PRRT_1", [_make_thread_comment_node()])]),
    )

    result = provider.get_pull_request_review_threads("42", include_reactions=False)

    assert "reactions" not in result["data"][0]["comments"][0]


def test_get_pull_request_review_threads_commit_sha_falls_back_to_commit() -> None:
    provider, client = make_provider()
    client.queue(
        "graphql",
        _review_threads_graphql_payload(
            [
                _review_thread_node(
                    "PRRT_1",
                    [
                        _make_thread_comment_node(
                            original_commit_oid=None,
                            commit_oid="headsha",
                        ),
                    ],
                ),
            ]
        ),
    )

    result = provider.get_pull_request_review_threads("42")

    # originalCommit absent -> falls back to the mutable commit HEAD.
    assert result["data"][0]["comments"][0]["commit_sha"] == "headsha"


def test_get_pull_request_review_threads_forwards_pagination_cursor_and_returns_next() -> None:
    provider, client = make_provider()
    client.queue(
        "graphql",
        {
            "repository": {
                "pullRequest": {
                    "reviewThreads": {
                        "pageInfo": {"hasNextPage": True, "endCursor": "next-page-cursor"},
                        "nodes": [],
                    }
                }
            }
        },
    )

    result = provider.get_pull_request_review_threads(
        "42",
        pagination={"cursor": "current-cursor", "per_page": 25},
    )

    assert result["meta"] == {"next_cursor": "next-page-cursor"}
    assert client.calls[0]["variables"] == {
        "owner": "test-org",
        "name": "test-repo",
        "number": 42,
        "cursor": "current-cursor",
        "perPage": 25,
    }


def test_get_pull_request_review_threads_paginates_inner_comments() -> None:
    provider, client = make_provider()
    client.queue(
        "graphql",
        _review_threads_graphql_payload(
            [
                _review_thread_node(
                    "PRRT_big",
                    [_make_thread_comment_node(node_id="PRRC_1", full_database_id=1)],
                    path="f.py",
                    line=1,
                    comments_has_next_page=True,
                    comments_end_cursor="c1",
                ),
            ]
        ),
    )
    client.queue(
        "graphql",
        {
            "node": {
                "comments": {
                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                    "nodes": [_make_thread_comment_node(node_id="PRRC_2", full_database_id=2)],
                }
            }
        },
    )

    result = provider.get_pull_request_review_threads("42")

    assert [c["id"] for c in result["data"][0]["comments"]] == ["1", "2"]
    assert [call["query"] for call in client.calls] == [
        REVIEW_THREADS_QUERY,
        REVIEW_THREAD_FULL_COMMENTS_QUERY,
    ]
    assert client.calls[1]["variables"] == {"threadId": "PRRT_big", "cursor": "c1"}


def test_get_pull_request_review_threads_skips_inner_pages_when_thread_deleted() -> None:
    provider, client = make_provider()
    client.queue(
        "graphql",
        _review_threads_graphql_payload(
            [
                _review_thread_node(
                    "PRRT_ghost",
                    [_make_thread_comment_node(node_id="PRRC_1", full_database_id=1)],
                    path="f.py",
                    line=1,
                    comments_has_next_page=True,
                    comments_end_cursor="c1",
                ),
            ]
        ),
    )
    client.queue("graphql", {"node": None})

    result = provider.get_pull_request_review_threads("42")

    # The thread is still returned with the comments fetched before deletion.
    assert [c["id"] for c in result["data"][0]["comments"]] == ["1"]


def test_get_pull_request_review_threads_raises_when_pull_request_missing() -> None:
    provider, client = make_provider()
    client.queue("graphql", {"repository": {"pullRequest": None}})

    with pytest.raises(SCMCodedError) as exc_info:
        provider.get_pull_request_review_threads("999")

    assert exc_info.value.code == "resource_not_found"


def test_collapse_pull_request_comment_resolves_when_contents_write() -> None:
    provider, client = make_provider()
    client.queue("get", FakeResponse({"permissions": {"contents": "write", "pull_requests": "read"}}))
    client.queue("graphql", {})

    provider.collapse_pull_request_comment("42", "PRRT_456", "IC_123", "OUTDATED")

    assert client.calls[0]["operation"] == "get"
    assert client.calls[0]["path"] == "/repos/test-org/test-repo/installation"
    assert client.calls[1] == {
        "operation": "graphql",
        "query": RESOLVE_REVIEW_THREAD_MUTATION,
        "variables": {"threadId": "PRRT_456"},
    }


def _graphql_updated_review_comment() -> dict[str, Any]:
    return {
        "updatePullRequestReviewComment": {
            "pullRequestReviewComment": {
                "id": "PRRC_123",
                "fullDatabaseId": 99,
                "body": "updated body",
                "createdAt": "2026-02-04T10:00:00Z",
                "updatedAt": "2026-02-04T10:05:00Z",
                "author": {"__typename": "User", "databaseId": 1, "login": "octocat"},
                "pullRequestReview": {"databaseId": 200},
            }
        }
    }


def test_update_and_collapse_pull_request_comment_resolves_when_contents_write() -> None:
    provider, client = make_provider()
    client.queue("get", FakeResponse({"permissions": {"contents": "write", "pull_requests": "read"}}))
    client.queue(
        "graphql",
        {
            **_graphql_updated_review_comment(),
            "resolveReviewThread": {"thread": {"isResolved": True}},
        },
    )

    result = provider.update_and_collapse_pull_request_comment(
        "42",
        "PRRT_456",
        "99",
        "PRRC_123",
        "updated body",
        "OUTDATED",
    )

    assert client.calls[0]["operation"] == "get"
    assert client.calls[1] == {
        "operation": "graphql",
        "query": UPDATE_AND_RESOLVE_PULL_REQUEST_REVIEW_COMMENT_MUTATION,
        "variables": {"commentId": "PRRC_123", "body": "updated body", "threadId": "PRRT_456"},
    }
    assert result["data"]["body"] == "updated body"
    assert result["data"]["id"] == "99"
    assert result["data"]["unique_id"] == "PRRC_123"


def test_update_and_collapse_pull_request_comment_minimizes_without_contents_write() -> None:
    provider, client = make_provider()
    client.queue("get", FakeResponse({"permissions": {"contents": "read", "pull_requests": "write"}}))
    client.queue(
        "graphql",
        {
            **_graphql_updated_review_comment(),
            "minimizeComment": {"minimizedComment": {"isMinimized": True}},
        },
    )

    provider.update_and_collapse_pull_request_comment(
        "42",
        "PRRT_456",
        "99",
        "PRRC_123",
        "updated body",
        "OUTDATED",
    )

    assert client.calls[1] == {
        "operation": "graphql",
        "query": UPDATE_AND_MINIMIZE_PULL_REQUEST_REVIEW_COMMENT_MUTATION,
        "variables": {"commentId": "PRRC_123", "body": "updated body", "reason": "OUTDATED"},
    }


def test_collapse_pull_request_comment_minimizes_without_contents_write() -> None:
    provider, client = make_provider()
    client.queue("get", FakeResponse({"permissions": {"contents": "read", "pull_requests": "write"}}))
    client.queue("graphql", {})

    provider.collapse_pull_request_comment("42", "PRRT_456", "IC_123", "OUTDATED")

    assert client.calls[0]["operation"] == "get"
    assert client.calls[1] == {
        "operation": "graphql",
        "query": MINIMIZE_COMMENT_MUTATION,
        "variables": {"commentId": "IC_123", "reason": "OUTDATED"},
    }


def test_get_pull_request_review_threads_handles_null_author() -> None:
    provider, client = make_provider()
    client.queue(
        "graphql",
        {
            "repository": {
                "pullRequest": {
                    "reviewThreads": {
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                        "nodes": [
                            {
                                "id": "PRRT_anon",
                                "isResolved": False,
                                "isOutdated": False,
                                "path": "f.py",
                                "line": 1,
                                "startLine": None,
                                "comments": {
                                    "pageInfo": {"hasNextPage": False, "endCursor": None},
                                    "nodes": [
                                        {
                                            "id": "PRRC_x",
                                            "fullDatabaseId": 7,
                                            "body": "anon",
                                            "createdAt": "2026-02-04T10:00:00Z",
                                            "updatedAt": "2026-02-04T10:00:00Z",
                                            "author": None,
                                        }
                                    ],
                                },
                            }
                        ],
                    }
                }
            }
        },
    )

    [thread] = provider.get_pull_request_review_threads("42")["data"]

    assert thread["comments"][0]["author"] is None
    assert thread["comments"][0]["is_bot"] is False


@pytest.mark.parametrize(
    ("permissions", "expected"),
    [
        (
            {"contents": "write", "pull_requests": "write"},
            {"has_read_access": True, "has_write_access": True, "has_check_run_write_access": False},
        ),
        (
            {"contents": "write", "pull_requests": "write", "checks": "write"},
            {"has_read_access": True, "has_write_access": True, "has_check_run_write_access": True},
        ),
        (
            {"checks": "write"},
            {"has_read_access": True, "has_write_access": False, "has_check_run_write_access": True},
        ),
    ],
)
def test_map_app_installation_checks_permission(permissions: dict[str, str], expected: dict[str, bool]) -> None:
    assert map_app_installation({"permissions": permissions}) == expected


def test_map_check_run_normalizes_startup_failure() -> None:
    raw = make_github_check_run(conclusion="startup_failure")

    assert GITHUB_CONCLUSION_MAP["startup_failure"] == "failure"
    assert map_check_run(raw)["conclusion"] == "failure"


@pytest.mark.parametrize(
    ("permissions", "expected"),
    [
        ({"pull": True, "triage": True}, "read"),
        ({"pull": False, "triage": True}, "read"),
        ({"pull": True, "triage": True, "push": True, "maintain": False, "admin": False}, "write"),
        ({"pull": True, "triage": True, "push": False, "maintain": True, "admin": False}, "write"),
        ({"pull": True, "triage": True, "push": True, "maintain": True, "admin": True}, "admin"),
        ({"pull": False, "triage": False, "push": False, "maintain": False, "admin": False}, "none"),
        ({}, "none"),
    ],
)
def test_map_github_repository_permission(permissions: dict[str, bool], expected: str) -> None:
    assert map_github_repository_permission(permissions) == expected


# The /collaborators/{username}/permission endpoint only returns GitHub's legacy base
# roles in the top-level "permission" field; "maintain" and "triage" are already
# collapsed to "write"/"read" by GitHub (those granular flags are exercised against the
# list endpoint in test_map_github_repository_permission above).
@pytest.mark.parametrize(
    ("permission", "expected"),
    [
        ("admin", "admin"),
        ("write", "write"),
        ("read", "read"),
        ("none", "none"),
    ],
)
def test_map_collaborator_permission_level(permission: str, expected: str) -> None:
    assert map_collaborator_permission_level(permission) == expected


def test_map_collaborator_permission_level_rejects_unknown() -> None:
    with pytest.raises(ValueError, match="unmappable repository permission"):
        map_collaborator_permission_level("bogus")


@pytest.mark.parametrize("rollup", [None, {}, {"+1": 0, "heart": 0, "total_count": 0}])
def test_map_reaction_rollup_empty(rollup: dict[str, Any] | None) -> None:
    assert map_reaction_rollup(rollup) == []


def test_map_reaction_rollup_expands_counts_in_canonical_order() -> None:
    # Rollup keys are unordered (and carry a total_count); the output is one entry
    # per reaction in the canonical content order, with no per-reaction author/id.
    rollup = {"url": "https://x", "total_count": 4, "heart": 1, "+1": 2, "eyes": 1}

    assert map_reaction_rollup(rollup) == [
        {"id": "", "content": "+1", "author": None},
        {"id": "", "content": "+1", "author": None},
        {"id": "", "content": "heart", "author": None},
        {"id": "", "content": "eyes", "author": None},
    ]


def test_map_comment_populates_metadata_from_rest_payload() -> None:
    raw = make_github_comment(
        author_association="OWNER",
        reactions={"total_count": 3, "+1": 2, "rocket": 1},
    )

    comment = map_comment(raw)

    assert comment["created_at"] == raw["created_at"]
    assert comment["author_association"] == "OWNER"
    assert comment["reactions"] == [
        {"id": "", "content": "+1", "author": None},
        {"id": "", "content": "+1", "author": None},
        {"id": "", "content": "rocket", "author": None},
    ]


def _error_response(status_code: int, body: bytes = b'{"message":"boom"}') -> Any:
    response = MagicMock()
    response.status_code = status_code
    response.content = body
    response.headers = {}
    response.request = MagicMock(headers={}, body=None, url="https://api.github.com/x", method="GET")
    return response


@pytest.mark.parametrize(
    ("status_code", "expected_code", "expected_type"),
    [
        (400, "resource_bad_request", ResourceBadRequest),
        (401, "resource_unauthorized", ResourceUnauthorized),
        (403, "resource_forbidden", ResourceForbidden),
        (404, "resource_not_found", ResourceNotFound),
        (409, "resource_conflict", ResourceConflict),
        (422, "resource_unprocessable_content", ResourceUnprocessableContent),
        (429, "rate_limit_exceeded", RateLimitExceeded),
        (500, "resource_server_error", ResourceServerError),
        (502, "resource_bad_gateway", ResourceBadGateway),
        (503, "resource_service_unavailable", ResourceServiceUnavailable),
        (504, "resource_gateway_timeout", ResourceGatewayTimeout),
        (418, "unhandled_exception", UnhandledException),
    ],
)
def test_request_maps_status_code_to_error(
    status_code: int, expected_code: str, expected_type: type[SCMCodedError]
) -> None:
    client = MagicMock(spec=ApiClient)
    client.request.return_value = _error_response(status_code, body=b'{"message":"upstream said no"}')
    provider = GitHubProvider(
        client,
        organization_id=1,
        repository=make_repository(),
        rate_limiter=NoOpRateLimiter(),
    )

    with pytest.raises(expected_type) as exc_info:
        provider.request("GET", "/repos/test-org/test-repo")

    assert exc_info.value.code == expected_code
    assert exc_info.value.detail == '{"message":"upstream said no"}'


def test_public_methods_are_accounted_for() -> None:
    covered_methods = {
        "request",
        "is_rate_limited",
        "get_pull_request_diff",
        "get_pull_request_template",
        "get_pull_request_review_threads",
        "download_archive",
        "download_workflow_job_log",
        "get_file_url",
        "get_commit_url",
        "get_commits_url",
        "get_pull_request_url",
        "create_commit",
        "update_issue",
        "get_thread_id_from_review_comment_unique_id",
        "collapse_pull_request_comment",
        "update_and_collapse_pull_request_comment",
        "get_authenticated_actor",
        *{case["name"] for case in PAGINATED_CASES},
        *{case["name"] for case in ACTION_CASES},
        *{case["name"] for case in VOID_CASES},
        *set(ALIAS_METHODS),
    }
    # Transport methods are tested via TestGitHubProviderApiClientGraphql and
    # implicitly by every action test that routes through them.
    transport_methods = {"get", "post", "patch", "delete", "graphql"}
    public_methods = {
        name for name, value in GitHubProvider.__dict__.items() if callable(value) and not name.startswith("_")
    } - transport_methods

    assert public_methods == covered_methods
