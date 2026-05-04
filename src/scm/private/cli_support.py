import argparse
import json
import os
import pathlib
import sys
import time
from typing import Any

import jwt
import requests

from scm.manager import SourceCodeManager
from scm.providers.gitlab.provider import API_VERSION
from scm.types import (
    ChmodCommitAction,
    CompareCommitsProtocol,
    CreateCommitProtocol,
    CreateIssueCommentProtocol,
    CreateIssueProtocol,
    CreatePullRequestCommentProtocol,
    CreateReviewCommentLineProtocol,
    CreateReviewCommentMultilineProtocol,
    CredentialsSet,
    DeleteBranchProtocol,
    DeleteCommitAction,
    DownloadArchiveProtocol,
    GetAppInstallationProtocol,
    GetArchiveLinkProtocol,
    GetBranchProtocol,
    GetCommitProtocol,
    GetCommitUrlProtocol,
    GetFileContentProtocol,
    GetFileUrlProtocol,
    GetGitRefProtocol,
    GetIssueCommentsProtocol,
    GetIssueProtocol,
    GetPullRequestCommentsProtocol,
    GetPullRequestCommitsProtocol,
    GetPullRequestDiffProtocol,
    GetPullRequestFilesProtocol,
    GetPullRequestProtocol,
    GetPullRequestsProtocol,
    GetPullRequestUrlProtocol,
    GetRepositoryAssigneesProtocol,
    GetRepositoryLabelsProtocol,
    GetRepositoryProtocol,
    MoveCommitAction,
    WriteCommitAction,
)


def load_credentials() -> dict[str, str]:
    """Load KEY=VALUE pairs from .credentials, skipping blanks and comments."""
    creds: dict[str, str] = {}
    path = pathlib.Path(__file__).resolve().parent.parent.parent.parent / ".credentials"
    if not path.exists():
        return creds
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        value = value.strip()
        if value:
            creds[key.strip()] = value
    return creds


def resolve(key: str, cli_value: str | None, creds: dict[str, str]) -> str | None:
    """CLI arg wins, then env var, then .credentials file."""
    return cli_value or os.environ.get(key) or creds.get(key)


def add_commands(parser: argparse.ArgumentParser) -> None:
    sub = parser.add_subparsers(dest="command")

    p = sub.add_parser("get-pull-request")
    p.add_argument("id")

    p = sub.add_parser("get-pull-requests")
    p.add_argument("--state", default="open", choices=["open", "closed"])

    p = sub.add_parser("get-pull-request-files")
    p.add_argument("id")

    p = sub.add_parser("get-pull-request-commits")
    p.add_argument("id")

    p = sub.add_parser("get-pull-request-diff")
    p.add_argument("id")

    p = sub.add_parser("get-pull-request-url")
    p.add_argument("id")

    p = sub.add_parser("get-issue")
    p.add_argument("id")

    p = sub.add_parser("create-issue")
    p.add_argument("title")
    p.add_argument("body")
    p.add_argument("--assignee", action="append", dest="assignees", default=None)
    p.add_argument("--label", action="append", dest="labels", default=None)

    p = sub.add_parser("get-issue-comments")
    p.add_argument("issue_id")

    p = sub.add_parser("get-pull-request-comments")
    p.add_argument("pr_id")

    p = sub.add_parser("create-issue-comment")
    p.add_argument("issue_id")
    p.add_argument("body")

    p = sub.add_parser("create-pull-request-comment")
    p.add_argument("pr_id")
    p.add_argument("body")

    p = sub.add_parser("create-review-comment-line")
    p.add_argument("pr_id")
    p.add_argument("commit_id")
    p.add_argument("body")
    p.add_argument("path")
    p.add_argument("side", choices=["base", "head"])
    p.add_argument("line", type=int)

    p = sub.add_parser("create-review-comment-multiline")
    p.add_argument("pr_id")
    p.add_argument("commit_id")
    p.add_argument("body")
    p.add_argument("path")
    p.add_argument("side", choices=["base", "head"])
    p.add_argument("start_side", choices=["base", "head"])
    p.add_argument("start_line", type=int)
    p.add_argument("end_line", type=int)

    p = sub.add_parser("get-branch")
    p.add_argument("name")

    p = sub.add_parser("delete-branch")
    p.add_argument("name")

    p = sub.add_parser("get-git-ref")
    p.add_argument("ref", help="e.g. heads/main or tags/v1.0.0")

    p = sub.add_parser("download-archive")
    p.add_argument("ref")

    p = sub.add_parser("get-archive-link")
    p.add_argument("ref")

    p = sub.add_parser("get-commit")
    p.add_argument("sha")

    p = sub.add_parser("get-commit-url")
    p.add_argument("sha")

    p = sub.add_parser("get-file-content")
    p.add_argument("path")
    p.add_argument("--ref", default=None)

    p = sub.add_parser("get-file-url")
    p.add_argument("path")
    p.add_argument("sha")
    p.add_argument("--start-line", type=int, default=None)
    p.add_argument("--end-line", type=int, default=None)

    p = sub.add_parser("compare-commits")
    p.add_argument("start_sha")
    p.add_argument("end_sha")

    p = sub.add_parser("create-commit")
    p.add_argument("branch")
    p.add_argument("parent_sha")
    p.add_argument("message")
    p.add_argument("--create", action="append", dest="creates", default=None, metavar="FILENAME=CONTENT")
    p.add_argument("--update", action="append", dest="updates", default=None, metavar="FILENAME=CONTENT")
    p.add_argument("--delete", action="append", dest="deletes", default=None, metavar="FILENAME")
    p.add_argument("--move", action="append", dest="moves", default=None, metavar="OLD:NEW")
    p.add_argument("--chmod", action="append", dest="chmods", default=None, metavar="FILENAME:0|1")
    p.add_argument("--force", action="store_true")

    sub.add_parser("get-repository")
    sub.add_parser("get-repository-assignees")
    sub.add_parser("get-repository-labels")
    sub.add_parser("get-app-installation")


def execute_command(args: argparse.Namespace, scm: SourceCodeManager) -> None:
    if args.command == "get-pull-request":
        assert isinstance(scm, GetPullRequestProtocol)
        dump(scm.get_pull_request(args.id))

    elif args.command == "get-pull-requests":
        assert isinstance(scm, GetPullRequestsProtocol)
        dump(scm.get_pull_requests(state=args.state))

    elif args.command == "get-pull-request-files":
        assert isinstance(scm, GetPullRequestFilesProtocol)
        dump(scm.get_pull_request_files(args.id))

    elif args.command == "get-pull-request-commits":
        assert isinstance(scm, GetPullRequestCommitsProtocol)
        dump(scm.get_pull_request_commits(args.id))

    elif args.command == "get-pull-request-diff":
        assert isinstance(scm, GetPullRequestDiffProtocol)
        dump(scm.get_pull_request_diff(args.id))

    elif args.command == "get-pull-request-url":
        assert isinstance(scm, GetPullRequestUrlProtocol)
        dump(scm.get_pull_request_url(args.id))

    elif args.command == "get-issue":
        assert isinstance(scm, GetIssueProtocol)
        dump(scm.get_issue(args.id))

    elif args.command == "create-issue":
        assert isinstance(scm, CreateIssueProtocol)
        dump(scm.create_issue(args.title, args.body, assignees=args.assignees, labels=args.labels))

    elif args.command == "get-issue-comments":
        assert isinstance(scm, GetIssueCommentsProtocol)
        dump(scm.get_issue_comments(args.issue_id))

    elif args.command == "get-pull-request-comments":
        assert isinstance(scm, GetPullRequestCommentsProtocol)
        dump(scm.get_pull_request_comments(args.pr_id))

    elif args.command == "create-issue-comment":
        assert isinstance(scm, CreateIssueCommentProtocol)
        dump(scm.create_issue_comment(args.issue_id, args.body))

    elif args.command == "create-pull-request-comment":
        assert isinstance(scm, CreatePullRequestCommentProtocol)
        dump(scm.create_pull_request_comment(args.pr_id, args.body))

    elif args.command == "create-review-comment-line":
        assert isinstance(scm, CreateReviewCommentLineProtocol)
        dump(scm.create_review_comment_line(args.pr_id, args.commit_id, args.body, args.path, args.side, args.line))

    elif args.command == "create-review-comment-multiline":
        assert isinstance(scm, CreateReviewCommentMultilineProtocol)
        dump(
            scm.create_review_comment_multiline(
                args.pr_id,
                args.commit_id,
                args.body,
                args.path,
                args.side,
                args.start_side,
                args.start_line,
                args.end_line,
            )
        )

    elif args.command == "get-branch":
        assert isinstance(scm, GetBranchProtocol)
        dump(scm.get_branch(args.name))

    elif args.command == "delete-branch":
        assert isinstance(scm, DeleteBranchProtocol)
        scm.delete_branch(args.name)

    elif args.command == "get-git-ref":
        assert isinstance(scm, GetGitRefProtocol)
        dump(scm.get_git_ref(args.ref))

    elif args.command == "get-commit":
        assert isinstance(scm, GetCommitProtocol)
        dump(scm.get_commit(args.sha))

    elif args.command == "get-commit-url":
        assert isinstance(scm, GetCommitUrlProtocol)
        dump(scm.get_commit_url(args.sha))

    elif args.command == "get-file-content":
        assert isinstance(scm, GetFileContentProtocol)
        dump(scm.get_file_content(args.path, ref=args.ref))

    elif args.command == "get-file-url":
        assert isinstance(scm, GetFileUrlProtocol)
        dump(scm.get_file_url(args.path, args.sha, start_line=args.start_line, end_line=args.end_line))

    elif args.command == "compare-commits":
        assert isinstance(scm, CompareCommitsProtocol)
        dump(scm.compare_commits(args.start_sha, args.end_sha))

    elif args.command == "create-commit":
        assert isinstance(scm, CreateCommitProtocol)
        actions: list = []
        for spec in args.creates or []:
            filename, _, content = spec.partition("=")
            actions.append(WriteCommitAction(action="create", filename=filename, content=content, encoding="utf-8"))
        for spec in args.updates or []:
            filename, _, content = spec.partition("=")
            actions.append(WriteCommitAction(action="update", filename=filename, content=content, encoding="utf-8"))
        for filename in args.deletes or []:
            actions.append(DeleteCommitAction(filename=filename))
        for spec in args.moves or []:
            old, _, new = spec.partition(":")
            actions.append(MoveCommitAction(old_filename=old, new_filename=new))
        for spec in args.chmods or []:
            filename, _, flag = spec.rpartition(":")
            actions.append(ChmodCommitAction(filename=filename, executable=flag == "1"))
        dump(scm.create_commit(args.branch, args.parent_sha, args.message, actions, force=args.force))

    elif args.command == "download-archive":
        assert isinstance(scm, DownloadArchiveProtocol)
        sys.stdout.buffer.write(scm.download_archive(args.ref))

    elif args.command == "get-archive-link":
        assert isinstance(scm, GetArchiveLinkProtocol)
        dump(scm.get_archive_link(args.ref))

    elif args.command == "get-repository":
        assert isinstance(scm, GetRepositoryProtocol)
        dump(scm.get_repository())

    elif args.command == "get-repository-assignees":
        assert isinstance(scm, GetRepositoryAssigneesProtocol)
        dump(scm.get_repository_assignees())

    elif args.command == "get-repository-labels":
        assert isinstance(scm, GetRepositoryLabelsProtocol)
        dump(scm.get_repository_labels())

    elif args.command == "get-app-installation":
        assert isinstance(scm, GetAppInstallationProtocol)
        dump(scm.get_app_installation())


def dump(data: Any) -> None:
    json.dump(data, sys.stdout, indent=2, default=str)
    sys.stdout.write("\n")


class GitLabApiClient:
    """API client that makes authenticated HTTP requests to the GitLab API."""

    def __init__(self, base_url: str, access_token: str) -> None:
        self.base_url = base_url.rstrip("/")
        self.access_token = access_token

    def request(
        self,
        method: str,
        path: str,
        headers: dict[str, str] | None = None,
        data: dict[str, Any] | None = None,
        params: dict[str, str] | None = None,
        allow_redirects: bool | None = None,
        stream: bool | None = None,
        raw_response: bool = True,
        credentials_set: CredentialsSet = "installation",
    ) -> requests.Response:
        url = f"{self.base_url}{API_VERSION}{path}"
        req_headers = {"Authorization": f"Bearer {self.access_token}"}
        if headers:
            req_headers.update(headers)

        kwargs: dict[str, Any] = {"headers": req_headers}
        if data is not None:
            kwargs["json"] = data
        if params is not None:
            kwargs["params"] = params
        if allow_redirects is not None:
            kwargs["allow_redirects"] = allow_redirects
        if stream is not None:
            kwargs["stream"] = stream

        return requests.request(method, url, **kwargs)


GITHUB_API_BASE = "https://api.github.com"


class GitHubInstallationTokenManager:
    """Manages GitHub App installation access tokens with automatic refresh."""

    def __init__(self, app_id: str, private_key: str, installation_id: str) -> None:
        self.app_id = app_id
        self.private_key = private_key
        self.installation_id = installation_id
        self._token: str | None = None
        self._expires_at: float = 0

    def _make_jwt(self) -> str:
        now = int(time.time())
        payload = {
            "iat": now - 60,
            "exp": now + (10 * 60),
            "iss": self.app_id,
        }
        return jwt.encode(payload, self.private_key, algorithm="RS256")

    def _refresh(self) -> None:
        token = self._make_jwt()
        response = requests.post(
            f"{GITHUB_API_BASE}/app/installations/{self.installation_id}/access_tokens",
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/vnd.github+json",
            },
        )
        response.raise_for_status()
        data = response.json()
        self._token = data["token"]
        # Refresh 5 minutes before actual expiry to avoid races.
        self._expires_at = time.time() + 3300

    @property
    def application_token(self) -> str:
        """Returns a JWT for the GitHub App itself (not tied to an installation)."""
        return self._make_jwt()

    @property
    def installation_token(self) -> str:
        """Returns a valid access token for the installation, refreshing if needed."""
        if self._token is None or time.time() >= self._expires_at:
            self._refresh()
        assert self._token is not None
        return self._token


class GitHubApiClient:
    """API client that makes authenticated HTTP requests to the GitHub API."""

    def __init__(self, token_manager: GitHubInstallationTokenManager) -> None:
        self.token_manager = token_manager

    def request(
        self,
        method: str,
        path: str,
        headers: dict[str, str] | None = None,
        data: dict[str, Any] | None = None,
        params: dict[str, str] | None = None,
        allow_redirects: bool | None = None,
        stream: bool | None = None,
        raw_response: bool = True,
        credentials_set: CredentialsSet = "installation",
    ) -> requests.Response:
        url = f"{GITHUB_API_BASE}{path}"
        token = (
            self.token_manager.installation_token
            if credentials_set == "installation"
            else self.token_manager.application_token
        )
        req_headers = {"Authorization": f"Bearer {token}"}
        if headers:
            req_headers.update(headers)

        kwargs: dict[str, Any] = {"headers": req_headers}
        if data is not None:
            kwargs["json"] = data
        if params is not None:
            kwargs["params"] = params
        if allow_redirects is not None:
            kwargs["allow_redirects"] = allow_redirects
        if stream is not None:
            kwargs["stream"] = stream

        return requests.request(method, url, **kwargs)
