import time
from collections.abc import Callable
from datetime import datetime
from email.utils import format_datetime, parsedate_to_datetime
from typing import Any, cast

import msgspec
import requests

from scm.errors import SCMCodedError
from scm.providers.github.types import GitHubPullRequestReviewComment
from scm.rate_limit import (
    DynamicRateLimiter,
    RateLimitProvider,
)
from scm.types import (
    SHA,
    ActionResult,
    ApiClient,
    ArchiveFormat,
    ArchiveLink,
    Author,
    BranchName,
    BuildConclusion,
    BuildStatus,
    CheckRun,
    CheckRunOutput,
    ChmodCommitAction,
    Comment,
    Commit,
    CommitAuthor,
    CommitFile,
    DeleteCommitAction,
    FileContent,
    FileStatus,
    GitBlob,
    GitCommitObject,
    GitCommitTree,
    GitRef,
    GitRepository,
    GitTree,
    InputTreeEntry,
    Issue,
    Label,
    MoveCommitAction,
    PaginatedActionResult,
    PaginatedResponseMeta,
    PaginationParams,
    PullRequest,
    PullRequestBranch,
    PullRequestCommit,
    PullRequestFile,
    PullRequestState,
    Reaction,
    ReactionResult,
    Referrer,
    Repository,
    RequestOptions,
    ResourceId,
    ResponseMeta,
    Review,
    ReviewComment,
    ReviewCommentInput,
    ReviewEvent,
    ReviewSide,
    TreeEntry,
    WriteCommitAction,
)

# GitHub's Checks API status values map to generic BuildStatus.
# "requested", "waiting", and "pending" are GitHub Actions-internal states that
# cannot be set via the API; we treat them as "pending" when reading.
GITHUB_STATUS_MAP: dict[str, BuildStatus] = {
    "queued": "pending",
    "requested": "pending",
    "waiting": "pending",
    "pending": "pending",
    "in_progress": "running",
    "completed": "completed",
}

# GitHub's conclusion values map 1-to-1 except "stale" (GitHub-internal, set
# automatically after 14 days) which we surface as "unknown".
GITHUB_CONCLUSION_MAP: dict[str, BuildConclusion] = {
    "success": "success",
    "failure": "failure",
    "neutral": "neutral",
    "cancelled": "cancelled",
    "skipped": "skipped",
    "timed_out": "timed_out",
    "action_required": "action_required",
    "stale": "unknown",
}

# Reverse maps for writing to GitHub's Checks API.
# "pending" maps to "queued" (the only writable in-queue state).
# "unknown" has no GitHub equivalent and is omitted; callers should not write it.
GITHUB_STATUS_WRITE_MAP: dict[BuildStatus, str] = {
    "pending": "queued",
    "running": "in_progress",
    "completed": "completed",
}

GITHUB_CONCLUSION_WRITE_MAP: dict[BuildConclusion, str] = {
    "success": "success",
    "failure": "failure",
    "neutral": "neutral",
    "cancelled": "cancelled",
    "skipped": "skipped",
    "timed_out": "timed_out",
    "action_required": "action_required",
    "unknown": "neutral",
}

GITHUB_ARCHIVE_FORMAT_MAP: dict[ArchiveFormat, str] = {
    "tarball": "tarball",
    "zip": "zipball",
}

GITHUB_REVIEW_EVENT_MAP: dict[ReviewEvent, str] = {
    "approve": "APPROVE",
    "change_request": "REQUEST_CHANGES",
    "comment": "COMMENT",
}

GITHUB_REVIEW_SIDE_MAP: dict[ReviewSide, str] = {
    "base": "LEFT",
    "head": "RIGHT",
}


MINIMIZE_COMMENT_MUTATION = """
mutation MinimizeComment($commentId: ID!, $reason: ReportedContentClassifiers!) {
    minimizeComment(input: {subjectId: $commentId, classifier: $reason}) {
        minimizedComment { isMinimized }
    }
}
"""


# Mapping of referrer, percentage pairs. For a given referrer X% of quota is reserved for that
# identifier. Excess use of the allocated quota does not result in a rate-limit error. Once
# reserved quota is exhausted the referrer will fall back to the shared quota pool.
#
# WARN: "shared" is a reserved referrer name and may not be used.
REFERRER_ALLOCATION: dict[Referrer, float] = {"emerge": 0.05}
assert "shared" not in REFERRER_ALLOCATION

GITHUB_RATE_LIMIT_WINDOW = 3600
GITHUB_RATE_LIMIT_CAPACITY = "x-ratelimit-limit"
GITHUB_RATE_LIMIT_USED = "x-ratelimit-used"
GITHUB_RATE_LIMIT_RESET = "x-ratelimit-reset"
GITHUB_RATE_LIMIT_REMAINING = "x-ratelimit-remaining"
GITHUB_RATE_LIMIT_RETRY_AFTER = "retry-after"

GITHUB_WEB_BASE_URL = "https://github.com"


def _extract_response_meta(response: requests.Response) -> ResponseMeta:
    meta: ResponseMeta = {}
    if etag := response.headers.get("ETag"):
        meta["etag"] = etag
    if last_modified := response.headers.get("Last-Modified"):
        meta["last_modified"] = parsedate_to_datetime(last_modified)
    return meta


class GitHubProvider:
    def __init__(
        self,
        client: ApiClient,
        organization_id: int,
        repository: Repository,
        rate_limit_provider: RateLimitProvider,
        get_time_in_seconds: Callable[[], int] = lambda: int(time.time()),
    ) -> None:
        self.client = client
        self.organization_id = organization_id
        self.repository = repository
        self.rate_limiter = DynamicRateLimiter(
            get_time_in_seconds=get_time_in_seconds,
            organization_id=organization_id,
            provider="github",
            rate_limit_provider=rate_limit_provider,
            rate_limit_window_seconds=GITHUB_RATE_LIMIT_WINDOW,
            referrer_allocation=REFERRER_ALLOCATION,
            recorded_capacity=None,
        )

    def is_rate_limited(self, referrer: Referrer) -> bool:
        """Return true if access to the resource has been blocked."""
        # If the referrer has allocated quota and that quota has not been exhausted we eagerly
        # exit by returning false. Otherwise we consume from the shared quota pool.
        if referrer in self.rate_limiter.referrer_allocation and not self.rate_limiter.is_rate_limited(referrer):
            return False
        else:
            return self.rate_limiter.is_rate_limited("shared")

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
    ) -> requests.Response:
        response = self.client.request(
            method=method,
            path=path,
            headers=headers,
            data=data,
            params=params,
            raw_response=raw_response,
            allow_redirects=allow_redirects,
            stream=stream,
        )

        if (
            GITHUB_RATE_LIMIT_CAPACITY in response.headers
            and GITHUB_RATE_LIMIT_USED in response.headers
            and GITHUB_RATE_LIMIT_RESET in response.headers
        ):
            self.rate_limiter.update_rate_limit_meta(
                capacity=int(response.headers[GITHUB_RATE_LIMIT_CAPACITY]),
                consumed=int(response.headers[GITHUB_RATE_LIMIT_USED]),
                next_window_start=int(response.headers[GITHUB_RATE_LIMIT_RESET]),
            )

        if response.status_code == 403:
            raise SCMCodedError(code="resource_forbidden")
        elif response.status_code == 404:
            raise SCMCodedError(code="resource_not_found")
        elif response.status_code == 409:
            raise SCMCodedError(code="resource_conflict")
        elif response.status_code == 422:
            raise SCMCodedError(code="resource_unprocessable_content")
        elif response.status_code >= 400:
            raise SCMCodedError(code="unhandled_exception")

        return response

    def get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
        extra_headers: dict[str, str] | None = None,
        allow_redirects: bool | None = None,
    ) -> requests.Response:
        headers = {"Accept": "application/vnd.github+json"}

        if request_options:
            if_none_match = request_options.get("if_none_match")
            if if_none_match is not None:
                headers["If-None-Match"] = if_none_match

            if_modified_since = request_options.get("if_modified_since")
            if if_modified_since is not None:
                headers["If-Modified-Since"] = format_datetime(if_modified_since)

        if extra_headers:
            headers.update(extra_headers)

        params = params or {}
        if pagination:
            params["per_page"] = str(pagination["per_page"])
            params["page"] = str(pagination["cursor"])

        return self.request(
            "GET",
            path=path,
            params=params,
            headers=headers,
            allow_redirects=allow_redirects,
        )

    def post(
        self,
        path: str,
        data: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> requests.Response:
        return self.request("POST", path=path, data=data, headers=headers)

    def patch(
        self,
        path: str,
        data: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> requests.Response:
        return self.request("PATCH", path=path, data=data, headers=headers)

    def delete(self, path: str) -> requests.Response:
        return self.request("DELETE", path=path)

    def graphql(
        self,
        query: str,
        variables: dict[str, Any],
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"query": query}
        if variables:
            payload["variables"] = variables

        response = self.post("/graphql", data=payload, headers={})
        response_data = response.json()

        if not isinstance(response_data, dict) or ("data" not in response_data and "errors" not in response_data):
            raise SCMCodedError(code="unexpected_response_format", detail="GraphQL response is not in expected format")

        errors = response_data.get("errors", [])
        if errors and not response_data.get("data"):
            raise SCMCodedError(code="resource_bad_request", detail="\n".join(e.get("message", "") for e in errors))

        return response_data.get("data", {})

    def get_repository(self) -> ActionResult[GitRepository]:
        response = self.get(f"/repos/{self.repository['name']}")
        return map_action(response, map_repository)

    def get_repository_assignees(
        self,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[Author]:
        response = self.get(
            f"/repos/{self.repository['name']}/assignees",
            pagination=pagination,
            request_options=request_options,
        )
        return map_paginated_action(
            pagination, response, lambda r: [Author(id=str(u["id"]), username=u["login"]) for u in r]
        )

    def get_repository_labels(
        self,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[Label]:
        response = self.get(
            f"/repos/{self.repository['name']}/labels",
            pagination=pagination,
            request_options=request_options,
        )
        return map_paginated_action(pagination, response, lambda r: [map_label(label) for label in r])

    def get_issue_comments(
        self,
        issue_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[Comment]:
        response = self.get(
            f"/repos/{self.repository['name']}/issues/{issue_id}/comments",
            pagination=pagination,
            request_options=request_options,
        )
        return map_paginated_action(pagination, response, lambda r: [map_comment(c) for c in r])

    def create_issue_comment(self, issue_id: str, body: str) -> ActionResult[Comment]:
        response = self.post(
            f"/repos/{self.repository['name']}/issues/{issue_id}/comments",
            data={"body": body},
        )
        return map_action(response, map_comment)

    def delete_issue_comment(self, issue_id: str, comment_id: str) -> None:
        self.delete(f"/repos/{self.repository['name']}/issues/comments/{comment_id}")

    def get_issue(
        self,
        issue_id: str,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[Issue]:
        response = self.get(
            f"/repos/{self.repository['name']}/issues/{issue_id}",
            request_options=request_options,
        )
        return map_action(response, map_issue)

    def create_issue(
        self,
        title: str,
        body: str,
        assignees: list[str] | None = None,
        labels: list[str] | None = None,
    ) -> ActionResult[Issue]:
        data: dict[str, Any] = {"title": title, "body": body}
        if assignees is not None:
            data["assignees"] = assignees
        if labels is not None:
            data["labels"] = labels
        response = self.post(
            f"/repos/{self.repository['name']}/issues",
            data=data,
        )
        return map_action(response, map_issue)

    def get_pull_request(
        self,
        pull_request_id: str,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[PullRequest]:
        response = self.get(
            f"/repos/{self.repository['name']}/pulls/{pull_request_id}",
            request_options=request_options,
        )
        return map_action(response, map_pull_request)

    def get_pull_request_comments(
        self,
        pull_request_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[Comment]:
        response = self.get(
            f"/repos/{self.repository['name']}/issues/{pull_request_id}/comments",
            pagination=pagination,
            request_options=request_options,
        )
        return map_paginated_action(pagination, response, lambda r: [map_comment(c) for c in r])

    def create_pull_request_comment(self, pull_request_id: str, body: str) -> ActionResult[Comment]:
        response = self.post(
            f"/repos/{self.repository['name']}/issues/{pull_request_id}/comments",
            data={"body": body},
        )
        return map_action(response, map_comment)

    def delete_pull_request_comment(self, pull_request_id: str, comment_id: str) -> None:
        self.delete(f"/repos/{self.repository['name']}/issues/comments/{comment_id}")

    def get_issue_comment_reactions(
        self,
        issue_id: str,
        comment_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[ReactionResult]:
        response = self.get(
            f"/repos/{self.repository['name']}/issues/comments/{comment_id}/reactions",
            pagination=pagination,
            request_options=request_options,
        )
        return map_paginated_action(pagination, response, lambda r: [map_reaction(c) for c in r])

    def create_issue_comment_reaction(
        self, issue_id: str, comment_id: str, reaction: Reaction
    ) -> ActionResult[ReactionResult]:
        response = self.post(
            f"/repos/{self.repository['name']}/issues/comments/{comment_id}/reactions",
            data={"content": reaction},
        )
        return map_action(response, map_reaction)

    def delete_issue_comment_reaction(self, issue_id: str, comment_id: str, reaction_id: str) -> None:
        self.delete(f"/repos/{self.repository['name']}/issues/comments/{comment_id}/reactions/{reaction_id}")

    def get_pull_request_comment_reactions(
        self,
        pull_request_id: str,
        comment_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[ReactionResult]:
        return self.get_issue_comment_reactions(pull_request_id, comment_id, pagination, request_options)

    def create_pull_request_comment_reaction(
        self, pull_request_id: str, comment_id: str, reaction: Reaction
    ) -> ActionResult[ReactionResult]:
        return self.create_issue_comment_reaction(pull_request_id, comment_id, reaction)

    def delete_pull_request_comment_reaction(self, pull_request_id: str, comment_id: str, reaction_id: str) -> None:
        return self.delete_issue_comment_reaction(pull_request_id, comment_id, reaction_id)

    def get_issue_reactions(
        self,
        issue_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[ReactionResult]:
        response = self.get(
            f"/repos/{self.repository['name']}/issues/{issue_id}/reactions",
            pagination=pagination,
            request_options=request_options,
        )
        return map_paginated_action(pagination, response, lambda r: [map_reaction(c) for c in r])

    def create_issue_reaction(self, issue_id: str, reaction: Reaction) -> ActionResult[ReactionResult]:
        response = self.post(
            f"/repos/{self.repository['name']}/issues/{issue_id}/reactions",
            data={"content": reaction},
        )
        return map_action(response, map_reaction)

    def delete_issue_reaction(self, issue_id: str, reaction_id: str) -> None:
        self.delete(f"/repos/{self.repository['name']}/issues/{issue_id}/reactions/{reaction_id}")

    def get_pull_request_reactions(
        self,
        pull_request_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[ReactionResult]:
        return self.get_issue_reactions(pull_request_id, pagination, request_options)

    def create_pull_request_reaction(self, pull_request_id: str, reaction: Reaction) -> ActionResult[ReactionResult]:
        return self.create_issue_reaction(pull_request_id, reaction)

    def delete_pull_request_reaction(self, pull_request_id: str, reaction_id: str) -> None:
        return self.delete_issue_reaction(pull_request_id, reaction_id)

    def get_branch(
        self,
        branch: BranchName,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[GitRef]:
        response = self.get(
            f"/repos/{self.repository['name']}/branches/{branch}",
            request_options=request_options,
        )
        return map_action(response, lambda r: GitRef(ref=r["name"], sha=r["commit"]["sha"]))

    def create_branch(self, branch: BranchName, sha: SHA) -> ActionResult[GitRef]:
        ref = f"refs/heads/{branch}"
        response = self.post(
            f"/repos/{self.repository['name']}/git/refs",
            data={"ref": ref, "sha": sha},
        )
        return map_action(
            response,
            lambda r: GitRef(ref=r["ref"].removeprefix("refs/heads/"), sha=r["object"]["sha"]),
        )

    def update_branch(self, branch: BranchName, sha: SHA, force: bool = False) -> ActionResult[GitRef]:
        response = self.patch(
            f"/repos/{self.repository['name']}/git/refs/heads/{branch}",
            data={"sha": sha, "force": force},
        )
        return map_action(
            response,
            lambda r: GitRef(ref=r["ref"].removeprefix("refs/heads/"), sha=r["object"]["sha"]),
        )

    def delete_branch(self, branch: BranchName) -> None:
        self.delete(f"/repos/{self.repository['name']}/git/refs/heads/{branch}")

    def get_git_ref(
        self,
        ref: str,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[GitRef]:
        response = self.get(
            f"/repos/{self.repository['name']}/git/ref/{ref}",
            request_options=request_options,
        )
        return map_action(response, lambda r: GitRef(ref=r["ref"], sha=r["object"]["sha"]))

    def get_file_url(
        self,
        file_path: str,
        sha: SHA,
        start_line: int | None = None,
        end_line: int | None = None,
    ) -> str:
        url = f"{GITHUB_WEB_BASE_URL}/{self.repository['name']}/blob/{sha}/{file_path}"
        if start_line:
            url += f"#L{start_line}"
        if start_line and end_line:
            url += f"-L{end_line}"
        elif end_line:
            url += f"#L{end_line}"
        return url

    def get_commit_url(self, commit_sha: SHA) -> str:
        return f"{GITHUB_WEB_BASE_URL}/{self.repository['name']}/commit/{commit_sha}"

    def get_pull_request_url(self, pull_request_id: str) -> str:
        return f"{GITHUB_WEB_BASE_URL}/{self.repository['name']}/pull/{pull_request_id}"

    def create_git_blob(self, content: str, encoding: str) -> ActionResult[GitBlob]:
        response = self.post(
            f"/repos/{self.repository['name']}/git/blobs",
            data={"content": content, "encoding": encoding},
        )
        return map_action(response, map_git_blob)

    def get_file_content(
        self,
        path: str,
        ref: str | None = None,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[FileContent]:
        params: dict[str, str] = {}
        if ref:
            params["ref"] = ref
        response = self.get(
            f"/repos/{self.repository['name']}/contents/{path}",
            params=params,
            request_options=request_options,
        )
        if isinstance(response.json(), list):
            raise SCMCodedError(code="path_is_directory", detail=path)
        return map_action(response, map_file_content)

    def get_commit(
        self,
        sha: SHA,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[Commit]:
        response = self.get(
            f"/repos/{self.repository['name']}/commits/{sha}",
            request_options=request_options,
        )
        return map_action(response, map_commit)

    def get_commits(
        self,
        ref: str | None = None,
        pagination: PaginationParams | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[Commit]:
        params: dict[str, str] = {}
        if ref:
            params["sha"] = ref
        if since:
            params["since"] = since.isoformat()
        if until:
            params["until"] = until.isoformat()
        response = self.get(
            f"/repos/{self.repository['name']}/commits",
            params=params,
            pagination=pagination,
            request_options=request_options,
        )
        return map_paginated_action(pagination, response, lambda r: [map_commit(c) for c in r])

    def get_commits_by_path(
        self,
        path: str,
        ref: str | None = None,
        pagination: PaginationParams | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[Commit]:
        params: dict[str, str] = {"path": path}
        if ref:
            params["sha"] = ref
        if since:
            params["since"] = since.isoformat()
        if until:
            params["until"] = until.isoformat()
        response = self.get(
            f"/repos/{self.repository['name']}/commits",
            params=params,
            pagination=pagination,
            request_options=request_options,
        )
        return map_paginated_action(pagination, response, lambda r: [map_commit(c) for c in r])

    def compare_commits(
        self,
        start_sha: SHA,
        end_sha: SHA,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[Commit]:
        response = self.get(
            f"/repos/{self.repository['name']}/compare/{start_sha}...{end_sha}",
            pagination=pagination,
            request_options=request_options,
        )
        return map_paginated_action(pagination, response, lambda r: [map_commit(c) for c in r["commits"]])

    def create_commit(
        self,
        branch: BranchName,
        parent_sha: SHA,
        message: str,
        actions: list[ChmodCommitAction | DeleteCommitAction | MoveCommitAction | WriteCommitAction],
        force: bool = False,
    ) -> ActionResult[Commit]:
        tree_entries: list[InputTreeEntry] = []
        for action in actions:
            if isinstance(action, WriteCommitAction):
                if action.encoding == "utf-8":
                    tree_entries.append(
                        InputTreeEntry(
                            path=action.filename,
                            mode="100644",
                            type="blob",
                            content=action.content,
                        )
                    )
                else:
                    blob = self.create_git_blob(action.content, action.encoding)["data"]
                    tree_entries.append(
                        InputTreeEntry(
                            path=action.filename,
                            mode="100644",
                            type="blob",
                            sha=blob["sha"],
                        )
                    )
            elif isinstance(action, DeleteCommitAction):
                tree_entries.append(
                    InputTreeEntry(
                        path=action.filename,
                        mode="100644",
                        type="blob",
                        sha=None,
                    )
                )
            elif isinstance(action, MoveCommitAction):
                existing = self.get_file_content(action.old_filename, ref=parent_sha)["data"]
                tree_entries.append(
                    InputTreeEntry(
                        path=action.old_filename,
                        mode="100644",
                        type="blob",
                        sha=None,
                    )
                )
                tree_entries.append(
                    InputTreeEntry(
                        path=action.new_filename,
                        mode="100644",
                        type="blob",
                        sha=existing["sha"],
                    )
                )
            else:
                existing = self.get_file_content(action.filename, ref=parent_sha)["data"]
                tree_entries.append(
                    InputTreeEntry(
                        path=action.filename,
                        mode="100755" if action.executable else "100644",
                        type="blob",
                        sha=existing["sha"],
                    )
                )

        parent_commit = self.get_git_commit(parent_sha)["data"]
        new_tree = self.create_git_tree(tree_entries, base_tree=parent_commit["tree"]["sha"])["data"]
        new_commit = self.create_git_commit(message, new_tree["sha"], [parent_sha])
        self.update_branch(branch, new_commit["data"]["sha"], force=force)

        raw = new_commit["raw"]["data"]
        return ActionResult(
            data=Commit(
                id=raw["sha"],
                message=raw.get("message", ""),
                author=map_commit_author(raw.get("author")),
                files=None,
                additions=None,
                deletions=None,
            ),
            type="github",
            raw=new_commit["raw"],
            meta=new_commit["meta"],
        )

    def get_tree(
        self,
        tree_sha: SHA,
        recursive: bool = True,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[GitTree]:
        params: dict[str, Any] = {}
        if recursive:
            params["recursive"] = 1
        response = self.get(
            f"/repos/{self.repository['name']}/git/trees/{tree_sha}",
            params=params,
            request_options=request_options,
        )
        return map_action(response, map_git_tree)

    def get_git_commit(
        self,
        sha: SHA,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[GitCommitObject]:
        response = self.get(
            f"/repos/{self.repository['name']}/git/commits/{sha}",
            request_options=request_options,
        )
        return map_action(response, map_git_commit_object)

    def create_git_tree(
        self,
        tree: list[InputTreeEntry],
        base_tree: SHA | None = None,
    ) -> ActionResult[GitTree]:
        data: dict[str, Any] = {"tree": tree}
        if base_tree is not None:
            data["base_tree"] = base_tree
        response = self.post(
            f"/repos/{self.repository['name']}/git/trees",
            data=data,
        )
        return map_action(response, map_git_tree)

    def create_git_commit(
        self,
        message: str,
        tree_sha: SHA,
        parent_shas: list[SHA],
    ) -> ActionResult[GitCommitObject]:
        response = self.post(
            f"/repos/{self.repository['name']}/git/commits",
            data={
                "message": message,
                "tree": tree_sha,
                "parents": parent_shas,
            },
        )
        return map_action(response, map_git_commit_object)

    def get_pull_request_files(
        self,
        pull_request_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[PullRequestFile]:
        response = self.get(
            f"/repos/{self.repository['name']}/pulls/{pull_request_id}/files",
            pagination=pagination,
            request_options=request_options,
        )
        return map_paginated_action(pagination, response, lambda r: [map_pull_request_file(f) for f in r])

    def get_pull_request_commits(
        self,
        pull_request_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[PullRequestCommit]:
        response = self.get(
            f"/repos/{self.repository['name']}/pulls/{pull_request_id}/commits",
            pagination=pagination,
            request_options=request_options,
        )
        return map_paginated_action(pagination, response, lambda r: [map_pull_request_commit(c) for c in r])

    def get_pull_request_diff(
        self,
        pull_request_id: str,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[str]:
        response = self.get(
            f"/repos/{self.repository['name']}/pulls/{pull_request_id}",
            request_options=request_options,
            extra_headers={"Accept": "application/vnd.github.v3.diff"},
        )
        return {
            "data": response.text,
            "type": "github",
            "raw": {"data": response.text, "headers": dict(response.headers)},
            "meta": _extract_response_meta(response),
        }

    def get_pull_requests(
        self,
        state: PullRequestState | None = "open",
        head: BranchName | None = None,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[PullRequest]:
        params: dict[str, Any] = {"state": state if state is not None else "all"}
        if head:
            params["head"] = head

        response = self.get(
            f"/repos/{self.repository['name']}/pulls",
            params=params,
            pagination=pagination,
            request_options=request_options,
        )
        return map_paginated_action(pagination, response, lambda r: [map_pull_request(pr) for pr in r])

    def create_pull_request(
        self,
        title: str,
        body: str,
        head: str,
        base: str,
    ) -> ActionResult[PullRequest]:
        data: dict[str, Any] = {
            "title": title,
            "body": body,
            "head": head,
            "base": base,
        }
        response = self.post(f"/repos/{self.repository['name']}/pulls", data=data)
        return map_action(response, map_pull_request)

    def create_pull_request_draft(
        self,
        title: str,
        body: str,
        head: str,
        base: str,
    ) -> ActionResult[PullRequest]:
        response = self.post(
            f"/repos/{self.repository['name']}/pulls",
            data={"title": title, "body": body, "head": head, "base": base, "draft": True},
        )
        return map_action(response, map_pull_request)

    def update_pull_request(
        self,
        pull_request_id: str,
        title: str | None = None,
        body: str | None = None,
        state: PullRequestState | None = None,
    ) -> ActionResult[PullRequest]:
        data: dict[str, Any] = {}
        if title is not None:
            data["title"] = title
        if body is not None:
            data["body"] = body
        if state is not None:
            data["state"] = state
        response = self.patch(f"/repos/{self.repository['name']}/pulls/{pull_request_id}", data=data)
        return map_action(response, map_pull_request)

    def request_review(self, pull_request_id: str, reviewers: list[str]) -> None:
        self.post(
            f"/repos/{self.repository['name']}/pulls/{pull_request_id}/requested_reviewers",
            data={"reviewers": reviewers},
        )

    def create_review_comment_file(
        self,
        pull_request_id: str,
        commit_id: SHA,
        body: str,
        path: str,
        side: ReviewSide,
    ) -> ActionResult[ReviewComment]:
        """Leave a review comment on a file."""
        response = self.post(
            f"/repos/{self.repository['name']}/pulls/{pull_request_id}/comments",
            data={
                "body": body,
                "commit_id": commit_id,
                "path": path,
                "side": GITHUB_REVIEW_SIDE_MAP[side],
                "subject_type": "file",
            },
        )
        return deserialize_action(response, deserialize_pull_request_review_comment)

    def create_review_comment_line(
        self,
        pull_request_id: str,
        commit_id: SHA,
        body: str,
        path: str,
        side: ReviewSide,
        line: int,
    ) -> ActionResult[ReviewComment]:
        """Leave a review comment on a line."""
        response = self.post(
            f"/repos/{self.repository['name']}/pulls/{pull_request_id}/comments",
            data={
                "body": body,
                "commit_id": commit_id,
                "path": path,
                "line": line,
                "side": GITHUB_REVIEW_SIDE_MAP[side],
            },
        )
        return deserialize_action(response, deserialize_pull_request_review_comment)

    def create_review_comment_multiline(
        self,
        pull_request_id: str,
        commit_id: SHA,
        body: str,
        path: str,
        side: ReviewSide,
        start_side: ReviewSide,
        start_line: int,
        end_line: int,
    ) -> ActionResult[ReviewComment]:
        """Leave a review comment on a line span."""
        response = self.post(
            f"/repos/{self.repository['name']}/pulls/{pull_request_id}/comments",
            data={
                "body": body,
                "commit_id": commit_id,
                "path": path,
                "line": end_line,
                "side": GITHUB_REVIEW_SIDE_MAP[side],
                "start_line": start_line,
                "start_side": GITHUB_REVIEW_SIDE_MAP[start_side],
            },
        )
        return deserialize_action(response, deserialize_pull_request_review_comment)

    def create_review_comment_reply(
        self,
        pull_request_id: str,
        body: str,
        comment_id: str,
    ) -> ActionResult[ReviewComment]:
        """Leave a review comment in reply to another review comment."""
        response = self.post(
            f"/repos/{self.repository['name']}/pulls/{pull_request_id}/comments",
            data={
                "body": body,
                "in_reply_to": int(comment_id),
            },
        )
        return deserialize_action(response, deserialize_pull_request_review_comment)

    def create_review(
        self,
        pull_request_id: str,
        commit_sha: SHA,
        event: ReviewEvent,
        comments: list[ReviewCommentInput],
        body: str | None = None,
    ) -> ActionResult[Review]:
        translated_comments: list[dict[str, Any]] = []
        for comment in comments:
            translated: dict[str, Any] = dict(comment)
            if "side" in translated:
                translated["side"] = GITHUB_REVIEW_SIDE_MAP[translated["side"]]
            if "start_side" in translated:
                translated["start_side"] = GITHUB_REVIEW_SIDE_MAP[translated["start_side"]]
            translated_comments.append(translated)

        data: dict[str, Any] = {
            "commit_id": commit_sha,
            "event": GITHUB_REVIEW_EVENT_MAP[event],
            "comments": translated_comments,
        }
        if body is not None:
            data["body"] = body
        response = self.post(
            f"/repos/{self.repository['name']}/pulls/{pull_request_id}/reviews",
            data=data,
        )
        return map_action(response, map_review)

    def create_check_run(
        self,
        name: str,
        head_sha: SHA,
        status: BuildStatus | None = None,
        conclusion: BuildConclusion | None = None,
        external_id: str | None = None,
        started_at: str | None = None,
        completed_at: str | None = None,
        output: CheckRunOutput | None = None,
    ) -> ActionResult[CheckRun]:
        data: dict[str, Any] = {
            "name": name,
            "head_sha": head_sha,
        }
        if status is not None:
            data["status"] = GITHUB_STATUS_WRITE_MAP[status]
        if conclusion is not None:
            data["conclusion"] = GITHUB_CONCLUSION_WRITE_MAP[conclusion]
        if external_id is not None:
            data["external_id"] = external_id
        if started_at is not None:
            data["started_at"] = started_at
        if completed_at is not None:
            data["completed_at"] = completed_at
        if output is not None:
            data["output"] = output
        response = self.post(
            f"/repos/{self.repository['name']}/check-runs",
            data=data,
        )
        return map_action(response, map_check_run)

    def get_check_run(
        self,
        check_run_id: ResourceId,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[CheckRun]:
        response = self.get(
            f"/repos/{self.repository['name']}/check-runs/{check_run_id}",
            request_options=request_options,
        )
        return map_action(response, map_check_run)

    def update_check_run(
        self,
        check_run_id: ResourceId,
        status: BuildStatus | None = None,
        conclusion: BuildConclusion | None = None,
        output: CheckRunOutput | None = None,
    ) -> ActionResult[CheckRun]:
        data: dict[str, Any] = {}
        if status is not None:
            data["status"] = GITHUB_STATUS_WRITE_MAP[status]
        if conclusion is not None:
            data["conclusion"] = GITHUB_CONCLUSION_WRITE_MAP[conclusion]
        if output is not None:
            data["output"] = output
        response = self.patch(
            f"/repos/{self.repository['name']}/check-runs/{check_run_id}",
            data=data,
        )
        return map_action(response, map_check_run)

    def get_archive_link(
        self,
        ref: str,
        archive_format: ArchiveFormat = "tarball",
        request_options: RequestOptions | None = None,
    ) -> ActionResult[ArchiveLink]:
        response = self.get(
            f"/repos/{self.repository['name']}/{GITHUB_ARCHIVE_FORMAT_MAP[archive_format]}/{ref}",
            request_options=request_options,
            allow_redirects=False,
        )
        if response.status_code != 302 or "Location" not in response.headers:
            raise SCMCodedError(code="unexpected_response_format", detail="Could not extract 'Location' header.")

        return {
            "data": ArchiveLink(url=response.headers["Location"], headers={}),
            "type": "github",
            "raw": {"data": response.headers["Location"], "headers": dict(response.headers)},
            "meta": _extract_response_meta(response),
        }

    def download_archive(
        self,
        ref: str,
        archive_format: ArchiveFormat = "tarball",
        request_options: RequestOptions | None = None,
    ) -> bytes:
        return self.get(
            f"/repos/{self.repository['name']}/{GITHUB_ARCHIVE_FORMAT_MAP[archive_format]}/{ref}",
            request_options=request_options,
        ).content

    def minimize_comment(self, comment_node_id: str, reason: str) -> None:
        self.graphql(
            MINIMIZE_COMMENT_MUTATION,
            {"commentId": comment_node_id, "reason": reason},
        )

    # resolve_review_thread: not supported


def map_author(raw_user: dict[str, Any] | None) -> Author | None:
    if raw_user is None:
        return None
    return Author(id=str(raw_user["id"]), username=raw_user["login"])


def map_comment(raw: dict[str, Any]) -> Comment:
    return Comment(
        id=str(raw["id"]),
        body=raw["body"],
        author=map_author(raw.get("user")),
    )


def map_label(raw: dict[str, Any]) -> Label:
    return Label(
        id=str(raw["id"]),
        name=raw["name"],
        color=raw["color"],
        description=raw.get("description"),
    )


def map_reaction(raw: dict[str, Any]) -> ReactionResult:
    return ReactionResult(
        id=str(raw["id"]),
        content=raw["content"],
        author=map_author(raw.get("user")),
    )


def map_git_blob(raw: dict[str, Any]) -> GitBlob:
    return GitBlob(sha=raw["sha"])


def map_file_content(raw: dict[str, Any]) -> FileContent:
    return FileContent(
        path=raw["path"],
        sha=raw["sha"],
        content=raw.get("content", ""),
        encoding=raw.get("encoding", ""),
        size=raw["size"],
    )


def map_commit_author(raw_author: dict[str, Any] | None) -> CommitAuthor | None:
    if raw_author is None:
        return None

    raw_date = raw_author.get("date")
    date = datetime.fromisoformat(raw_date) if raw_date else None

    return CommitAuthor(
        name=raw_author.get("name", ""),
        email=raw_author.get("email", ""),
        date=date,
    )


_VALID_FILE_STATUSES: set[str] = {
    "added",
    "removed",
    "modified",
    "renamed",
    "copied",
    "changed",
    "unchanged",
}


def map_commit_file(raw_file: dict[str, Any]) -> CommitFile:
    raw_status = raw_file.get("status", "modified")
    status = raw_status if raw_status in _VALID_FILE_STATUSES else "unknown"
    return CommitFile(
        filename=raw_file["filename"],
        status=cast(FileStatus, status),
        patch=raw_file.get("patch"),
        additions=raw_file.get("additions"),
        deletions=raw_file.get("deletions"),
        previous_filename=raw_file.get("previous_filename"),
    )


def map_commit(raw: dict[str, Any]) -> Commit:
    commit = raw.get("commit", {})
    stats = raw.get("stats") or {}
    return Commit(
        id=raw["sha"],
        message=commit.get("message", ""),
        author=map_commit_author(commit.get("author")),
        files=[map_commit_file(f) for f in raw.get("files", [])],
        additions=stats.get("additions"),
        deletions=stats.get("deletions"),
    )


def map_tree_entry(raw_entry: dict[str, Any]) -> TreeEntry:
    return TreeEntry(
        path=raw_entry["path"],
        mode=raw_entry["mode"],
        type=raw_entry["type"],
        sha=raw_entry["sha"],
        size=raw_entry.get("size"),
    )


def map_git_tree(raw: dict[str, Any]) -> GitTree:
    """Transform a full git tree API response (from create_git_tree)."""
    return GitTree(
        sha=raw["sha"],
        tree=[map_tree_entry(e) for e in raw["tree"]],
        truncated=raw["truncated"],
    )


def map_git_commit_object(raw: dict[str, Any]) -> GitCommitObject:
    return GitCommitObject(
        sha=raw["sha"],
        tree=GitCommitTree(sha=raw["tree"]["sha"]),
        message=raw.get("message", ""),
    )


def map_review(raw: dict[str, Any]) -> Review:
    return Review(
        id=str(raw["id"]),
        html_url=raw["html_url"],
    )


def map_check_run(raw: dict[str, Any]) -> CheckRun:
    raw_status = raw.get("status", "")
    raw_conclusion = raw.get("conclusion")
    return CheckRun(
        id=str(raw["id"]),
        name=raw.get("name", ""),
        status=GITHUB_STATUS_MAP.get(raw_status, "pending"),
        conclusion=GITHUB_CONCLUSION_MAP.get(raw_conclusion) if raw_conclusion else None,
        html_url=raw.get("html_url", ""),
    )


def map_pull_request_file(raw_file: dict[str, Any]) -> PullRequestFile:
    raw_status = raw_file.get("status", "modified")
    status = raw_status if raw_status in _VALID_FILE_STATUSES else "unknown"
    return PullRequestFile(
        filename=raw_file["filename"],
        status=cast(FileStatus, status),
        patch=raw_file.get("patch"),
        changes=raw_file.get("changes", 0),
        sha=raw_file.get("sha", ""),
        previous_filename=raw_file.get("previous_filename"),
    )


def map_pull_request_commit(raw: dict[str, Any]) -> PullRequestCommit:
    raw_author = raw.get("commit", {}).get("author")
    return PullRequestCommit(
        sha=raw["sha"],
        message=raw.get("commit", {}).get("message", ""),
        author=map_commit_author(raw_author),
    )


def map_pull_request(raw: dict[str, Any]) -> PullRequest:
    return PullRequest(
        iid=str(raw["id"]),
        id=str(raw["number"]),
        title=raw["title"],
        body=raw.get("body"),
        state=raw["state"],
        merged=raw.get("merged_at") is not None,
        html_url=raw.get("html_url", ""),
        head=PullRequestBranch(sha=raw["head"]["sha"], ref=raw["head"]["ref"]),
        base=PullRequestBranch(sha=raw["base"]["sha"], ref=raw["base"]["ref"]),
    )


def map_issue(raw: dict[str, Any]) -> Issue:
    return Issue(
        id=str(raw["number"]),
        title=raw["title"],
        body=raw.get("body"),
        state=raw["state"],
        html_url=raw.get("html_url", ""),
    )


def map_repository(raw: dict[str, Any]) -> GitRepository:
    return GitRepository(
        full_name=raw["full_name"],
        default_branch=raw["default_branch"],
        clone_url=raw["clone_url"],
        private=raw["private"],
        size=raw["size"],
    )


def map_action[T](response: requests.Response, fn: Callable[[dict[str, Any]], T]) -> ActionResult[T]:
    raw = response.json()
    return {
        "data": fn(raw),
        "type": "github",
        "raw": {"data": raw, "headers": dict(response.headers)},
        "meta": _extract_response_meta(response),
    }


def map_paginated_action[T](
    pagination: PaginationParams | None,
    response: requests.Response,
    fn: Callable[[Any], list[T]],
) -> PaginatedActionResult[T]:
    raw = response.json()
    meta: PaginatedResponseMeta = {
        **_extract_response_meta(response),
        "next_cursor": str(int(pagination["cursor"]) + 1 if pagination else 2),
    }
    return {
        "data": fn(raw),
        "type": "github",
        "raw": {"data": raw, "headers": dict(response.headers)},
        "meta": meta,
    }


def deserialize_action[T](response: requests.Response, fn: Callable[[bytes], T]) -> ActionResult[T]:
    return {
        "data": fn(response.content),
        "type": "github",
        "raw": {"data": response.json(), "headers": dict(response.headers)},
        "meta": _extract_response_meta(response),
    }


def deserialize_pull_request_review_comment(content: bytes) -> ReviewComment:
    comment = msgspec.json.decode(content, type=GitHubPullRequestReviewComment)
    return {
        "author_association": comment.author_association,
        "author": Author(id=str(comment.user.id), username=comment.user.login) if comment.user else None,
        "body": comment.body,
        "commit_sha": comment.original_commit_id,
        "created_at": comment.created_at.isoformat(),
        "diff_hunk": comment.diff_hunk,
        "file_path": comment.path,
        "head": comment.commit_id,
        "id": str(comment.id),
        "review_id": str(comment.pull_request_review_id),
        "unique_id": comment.node_id,
        "url": comment.html_url,
    }
