import json
from datetime import datetime
from typing import Any
from unittest.mock import MagicMock

import pytest

from scm.errors import SCMCodedError
from scm.providers.github.provider import (
    MINIMIZE_COMMENT_MUTATION,
    GitHubProvider,
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
)
from scm.types import (
    ApiClient,
    ChmodCommitAction,
    DeleteCommitAction,
    MoveCommitAction,
    Referrer,
    Repository,
    WriteCommitAction,
)


def make_repository() -> Repository:
    return {
        "id": 1,
        "integration_id": 1,
        "name": "test-org/test-repo",
        "organization_id": 1,
        "is_active": True,
        "external_id": None,
        "provider_name": "github",
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
    ) -> FakeResponse:
        self.calls.append(
            {
                "operation": "get",
                "path": path,
                "params": params,
                "pagination": pagination,
                "request_options": request_options,
                "extra_headers": extra_headers,
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


class NoOpRateLimitProvider:
    def get_and_set_rate_limit(self, total_key: str, usage_key: str, expiration: int) -> tuple[int | None, int]:
        return (None, 0)

    def get_accounted_usage(self, keys: list[str]) -> int:
        return 0

    def set_key_values(self, kvs: dict[str, tuple[int, int | None]]) -> None:
        pass


def make_provider(client: RecordingClient | None = None) -> tuple[GitHubProvider, RecordingClient]:
    transport = client or RecordingClient()
    provider = GitHubProvider(
        MagicMock(spec=ApiClient),
        organization_id=1,
        repository=make_repository(),
        rate_limit_provider=NoOpRateLimitProvider(),
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
    }


def expected_comment(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(raw["id"]),
        "body": raw["body"],
        "author": {"id": str(raw["user"]["id"]), "username": raw["user"]["login"]},
    }


def expected_assignee(raw: dict[str, Any]) -> dict[str, Any]:
    return {"id": str(raw["id"]), "username": raw["login"]}


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
        "iid": str(raw["id"]),
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
        "additions": stats.get("additions"),
        "deletions": stats.get("deletions"),
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
    }


def expected_review(raw: dict[str, Any]) -> dict[str, Any]:
    return {"id": str(raw["id"]), "html_url": raw["html_url"]}


def expected_check_run(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(raw["id"]),
        "name": raw["name"],
        "status": "completed" if raw["status"] == "completed" else "pending",
        "conclusion": raw["conclusion"],
        "html_url": raw["html_url"],
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


PAGINATED_CASES: list[dict[str, Any]] = [
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
        "expected_data": [expected_commit(COMMIT_RAW)],
        "next_cursor": "2",
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
]


ACTION_CASES: list[dict[str, Any]] = [
    {
        "name": "get_repository",
        "operation": "get",
        "kwargs": {},
        "path": "/repos/test-org/test-repo",
        "raw": REPOSITORY_RAW,
        "expected_data": expected_repository(REPOSITORY_RAW),
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
        "name": "get_commit",
        "operation": "get",
        "kwargs": {"sha": "abc123"},
        "path": "/repos/test-org/test-repo/commits/abc123",
        "raw": COMMIT_RAW,
        "expected_data": expected_commit(COMMIT_RAW),
    },
    {
        "name": "get_tree",
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
        "name": "create_review_comment_line",
        "operation": "post",
        "kwargs": {
            "pull_request_id": "42",
            "commit_id": "abc123",
            "body": "Looks good",
            "path": "src/main.py",
            "side": "head",
            "line": 3,
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
        "name": "create_review_comment_multiline",
        "operation": "post",
        "kwargs": {
            "pull_request_id": "42",
            "commit_id": "abc123",
            "body": "Looks good",
            "path": "src/main.py",
            "side": "head",
            "start_side": "base",
            "start_line": 1,
            "end_line": 5,
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
        "name": "create_review",
        "operation": "post",
        "kwargs": {
            "pull_request_id": "42",
            "commit_sha": "abc123",
            "event": "approve",
            "comments": [{"path": "f.py", "body": "fix"}],
            "body": "overall",
        },
        "path": "/repos/test-org/test-repo/pulls/42/reviews",
        "data": {
            "commit_id": "abc123",
            "event": "APPROVE",
            "comments": [{"path": "f.py", "body": "fix"}],
            "body": "overall",
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
    else:
        if "params" in case:
            expected_call["params"] = case["params"]
        expected_call["headers"] = case.get("headers")
    assert client.calls == [expected_call]


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


def test_get_file_content_raises_when_path_is_directory() -> None:
    provider, client = make_provider()
    client.queue("get", FakeResponse([FILE_CONTENT_RAW, FILE_CONTENT_RAW]))

    with pytest.raises(SCMCodedError) as exc_info:
        provider.get_file_content(path="src", ref="main")

    assert exc_info.value.code == "path_is_directory"
    assert exc_info.value.detail == "src"


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
        rate_limit_provider=NoOpRateLimitProvider(),
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
        rate_limit_provider=NoOpRateLimitProvider(),
        get_time_in_seconds=lambda: 0,
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

    result = provider.download_archive("main")

    assert result == b"tarball-bytes"
    assert client.calls == [
        {
            "operation": "get",
            "path": "/repos/test-org/test-repo/tarball/main",
            "params": None,
            "pagination": None,
            "request_options": None,
            "extra_headers": None,
        }
    ]


def test_download_archive_zip_uses_zipball_path() -> None:
    provider, client = make_provider()
    _queue_raw_bytes(client, b"zip-bytes")

    result = provider.download_archive("main", archive_format="zip")

    assert result == b"zip-bytes"
    assert client.calls[0]["path"] == "/repos/test-org/test-repo/zipball/main"


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


def test_get_pull_request_url_builds_pr_url() -> None:
    provider, _ = make_provider()

    assert provider.get_pull_request_url("42") == "https://github.com/test-org/test-repo/pull/42"


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
    assert result["data"]["files"] is None

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
        },
        {
            "operation": "get",
            "path": "/repos/test-org/test-repo/contents/run.sh",
            "params": {"ref": "parent_sha"},
            "pagination": None,
            "request_options": None,
            "extra_headers": None,
        },
        {
            "operation": "get",
            "path": "/repos/test-org/test-repo/git/commits/parent_sha",
            "params": None,
            "pagination": None,
            "request_options": None,
            "extra_headers": None,
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


def test_public_methods_are_accounted_for() -> None:
    covered_methods = {
        "request",
        "is_rate_limited",
        "get_pull_request_diff",
        "download_archive",
        "get_file_url",
        "get_commit_url",
        "get_pull_request_url",
        "create_commit",
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
