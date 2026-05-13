import time
from collections.abc import Callable, Iterator
from datetime import datetime
from email.utils import format_datetime, parsedate_to_datetime
from typing import Any

import msgspec
import requests

from scm.errors import ErrorCode, SCMCodedError
from scm.helpers import iter_all_pages
from scm.providers.github.types import (
    GitHubAppInstallationResponse,
    GitHubBranchResponse,
    GitHubCheckRunResponse,
    GitHubCommentResponse,
    GitHubCommitAuthorDetail,
    GitHubCommitComparisonResponse,
    GitHubCommitFileResponse,
    GitHubCommitResponse,
    GitHubFileContentResponse,
    GitHubFileContentType,
    GitHubFileStatus,
    GitHubGitBlobResponse,
    GitHubGitCommitObjectResponse,
    GitHubGitRefResponse,
    GitHubGitTreeResponse,
    GitHubIssueResponse,
    GitHubLabelResponse,
    GitHubPullRequestCommitResponse,
    GitHubPullRequestFileResponse,
    GitHubPullRequestResponse,
    GitHubPullRequestReviewComment,
    GitHubReactionResponse,
    GitHubRepositoryResponse,
    GitHubReviewResponse,
    GitHubTopicsResponse,
    GitHubUser,
)
from scm.rate_limit import (
    DynamicRateLimiter,
    RateLimitProvider,
)
from scm.types import (
    SHA,
    ActionResult,
    ApiClient,
    AppInstallation,
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
    CommitWithChanges,
    CoPilotChatExtension,
    CredentialsSet,
    DeleteCommitAction,
    FileContent,
    FileContentType,
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
    ProviderName,
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
    ReviewThread,
    ReviewThreadComment,
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

GITHUB_FILE_CONTENT_TYPE_MAP: dict[GitHubFileContentType, FileContentType] = {
    "file": "file",
    "dir": "directory",
    "symlink": "symlink",
    "submodule": "submodule",
}

GITHUB_FILE_STATUS_MAP: dict[GitHubFileStatus, FileStatus] = {
    "added": "added",
    "removed": "removed",
    "modified": "modified",
    "renamed": "renamed",
    "copied": "copied",
    "changed": "changed",
    "unchanged": "unchanged",
}


MINIMIZE_COMMENT_MUTATION = """
mutation MinimizeComment($commentId: ID!, $reason: ReportedContentClassifiers!) {
    minimizeComment(input: {subjectId: $commentId, classifier: $reason}) {
        minimizedComment { isMinimized }
    }
}
"""

RESOLVE_REVIEW_THREAD_MUTATION = """
mutation ResolveReviewThread($threadId: ID!) {
    resolveReviewThread(input: {threadId: $threadId}) {
        thread { isResolved }
    }
}
"""

REVIEW_THREAD_BY_COMMENT_QUERY = """
query ReviewThreadByComment($owner: String!, $name: String!, $number: Int!, $cursor: String) {
    repository(owner: $owner, name: $name) {
        pullRequest(number: $number) {
            reviewThreads(first: 100, after: $cursor) {
                pageInfo { hasNextPage endCursor }
                nodes {
                    id
                    comments(first: 100) {
                        pageInfo { hasNextPage endCursor }
                        nodes { id }
                    }
                }
            }
        }
    }
}
"""

THREAD_COMMENTS_QUERY = """
query ThreadComments($threadId: ID!, $cursor: String) {
    node(id: $threadId) {
        ... on PullRequestReviewThread {
            comments(first: 100, after: $cursor) {
                pageInfo { hasNextPage endCursor }
                nodes { id }
            }
        }
    }
}
"""

REVIEW_THREADS_QUERY = """
query ReviewThreads($owner: String!, $name: String!, $number: Int!, $cursor: String, $perPage: Int!) {
    repository(owner: $owner, name: $name) {
        pullRequest(number: $number) {
            reviewThreads(first: $perPage, after: $cursor) {
                pageInfo { hasNextPage endCursor }
                nodes {
                    id
                    isResolved
                    isOutdated
                    path
                    line
                    startLine
                    comments(first: 100) {
                        pageInfo { hasNextPage endCursor }
                        nodes {
                            id
                            fullDatabaseId
                            body
                            createdAt
                            updatedAt
                            author { login __typename ... on User { databaseId } }
                        }
                    }
                }
            }
        }
    }
}
"""

REVIEW_THREAD_FULL_COMMENTS_QUERY = """
query ReviewThreadFullComments($threadId: ID!, $cursor: String) {
    node(id: $threadId) {
        ... on PullRequestReviewThread {
            comments(first: 100, after: $cursor) {
                pageInfo { hasNextPage endCursor }
                nodes {
                    id
                    fullDatabaseId
                    body
                    createdAt
                    updatedAt
                    author { login __typename ... on User { databaseId } }
                }
            }
        }
    }
}
"""

# Default page size for the reviewThreads connection. GitHub caps `first` at 100.
GITHUB_REVIEW_THREADS_DEFAULT_PAGE_SIZE = 100


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

# Directories where GitHub recognizes pull-request templates. Single-template
# files live as PULL_REQUEST_TEMPLATE.md inside one of these directories;
# multi-template repos use a PULL_REQUEST_TEMPLATE/ subdirectory under .github.
# Names are matched case-insensitively to mirror GitHub's own behavior.
PULL_REQUEST_TEMPLATE_PARENT_DIRS = (".github", "", "docs")
PULL_REQUEST_TEMPLATE_FILENAME = "pull_request_template.md"
PULL_REQUEST_TEMPLATE_DIRNAME = "pull_request_template"


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
        web_base_url: str = GITHUB_WEB_BASE_URL,
        provider_name: ProviderName = "github",
        referrer_allocation: dict[Referrer, float] | None = None,
    ) -> None:
        self.client = client
        self.organization_id = organization_id
        self.repository = repository
        self._web_base_url = web_base_url
        self.rate_limiter = DynamicRateLimiter(
            get_time_in_seconds=get_time_in_seconds,
            organization_id=organization_id,
            provider=provider_name,
            rate_limit_provider=rate_limit_provider,
            rate_limit_window_seconds=GITHUB_RATE_LIMIT_WINDOW,
            referrer_allocation=referrer_allocation if referrer_allocation is not None else REFERRER_ALLOCATION,
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
        stream: bool = True,
        raw_response: bool = True,
        credentials_set: CredentialsSet = "installation",
        timeout: float | tuple[float, float] | None = None,
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
            credentials_set=credentials_set,
            timeout=timeout,
        )

        if (
            credentials_set == "installation"
            and GITHUB_RATE_LIMIT_CAPACITY in response.headers
            and GITHUB_RATE_LIMIT_USED in response.headers
            and GITHUB_RATE_LIMIT_RESET in response.headers
        ):
            self.rate_limiter.update_rate_limit_meta(
                capacity=int(response.headers[GITHUB_RATE_LIMIT_CAPACITY]),
                consumed=int(response.headers[GITHUB_RATE_LIMIT_USED]),
                next_window_start=int(response.headers[GITHUB_RATE_LIMIT_RESET]),
            )

        if response.status_code >= 400:
            if response.status_code == 401:
                code: ErrorCode = "resource_unauthorized"  # type: ignore[no-redef]
            elif response.status_code == 403:
                code: ErrorCode = "resource_forbidden"  # type: ignore[no-redef]
            elif response.status_code == 404:
                code: ErrorCode = "resource_not_found"  # type: ignore[no-redef]
            elif response.status_code == 409:
                code: ErrorCode = "resource_conflict"  # type: ignore[no-redef]
            elif response.status_code == 422:
                code: ErrorCode = "resource_unprocessable_content"  # type: ignore[no-redef]
            else:
                code: ErrorCode = "unhandled_exception"  # type: ignore[no-redef]

            raise SCMCodedError(
                code=code,
                detail=response.content.decode("utf-8"),
                response_content=response.content.decode("utf-8"),
                request_headers=response.request.headers,
                request_body=response.request.body,
                request_url=response.request.url,
                request_method=response.request.method,
            )

        return response

    def get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
        extra_headers: dict[str, str] | None = None,
        allow_redirects: bool | None = None,
        credentials_set: CredentialsSet = "installation",
    ) -> requests.Response:
        headers = {}
        options = request_options or {}

        if_none_match = options.get("if_none_match")
        if if_none_match is not None:
            headers["If-None-Match"] = if_none_match

        if_modified_since = options.get("if_modified_since")
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
            credentials_set=credentials_set,
            timeout=options.get("timeout"),
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

    def get_app_installation(self) -> ActionResult[AppInstallation]:
        response = self.get(f"/repos/{self.repository['name']}/installation", credentials_set="application")
        return deserialize_action(response, deserialize_app_installation)

    def get_repository(self) -> ActionResult[GitRepository]:
        response = self.get(f"/repos/{self.repository['name']}")
        return deserialize_action(response, deserialize_repository)

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
        return deserialize_paginated_action(pagination, response, deserialize_assignees)

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
        return deserialize_paginated_action(pagination, response, deserialize_labels)

    def get_repository_topics(
        self,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[list[str]]:
        response = self.get(
            f"/repos/{self.repository['name']}/topics",
            request_options=request_options,
        )
        return deserialize_action(response, deserialize_topics)

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
        return deserialize_paginated_action(pagination, response, deserialize_comments)

    def create_issue_comment(self, issue_id: str, body: str) -> ActionResult[Comment]:
        response = self.post(
            f"/repos/{self.repository['name']}/issues/{issue_id}/comments",
            data={"body": body},
        )
        return deserialize_action(response, deserialize_comment)

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
        return deserialize_action(response, deserialize_issue)

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
        return deserialize_action(response, deserialize_issue)

    def get_pull_request(
        self,
        pull_request_id: str,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[PullRequest]:
        response = self.get(
            f"/repos/{self.repository['name']}/pulls/{pull_request_id}",
            request_options=request_options,
        )
        return deserialize_action(response, deserialize_pull_request)

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
        return deserialize_paginated_action(pagination, response, deserialize_comments)

    def create_pull_request_comment(
        self,
        pull_request_id: str,
        body: str,
        extensions: list[CoPilotChatExtension] | None = None,
    ) -> ActionResult[Comment]:
        data: dict[str, Any] = {"body": body}

        if extensions:
            data["actions"] = [
                {
                    "name": extension.name,
                    "type": "copilot-chat",
                    "prompt": extension.prompt,
                }
                for extension in extensions
            ]

        response = self.post(
            f"/repos/{self.repository['name']}/issues/{pull_request_id}/comments",
            data=data,
        )
        return deserialize_action(response, deserialize_comment)

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
        return deserialize_paginated_action(pagination, response, deserialize_reactions)

    def create_issue_comment_reaction(
        self, issue_id: str, comment_id: str, reaction: Reaction
    ) -> ActionResult[ReactionResult]:
        response = self.post(
            f"/repos/{self.repository['name']}/issues/comments/{comment_id}/reactions",
            data={"content": reaction},
        )
        return deserialize_action(response, deserialize_reaction)

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
        return deserialize_paginated_action(pagination, response, deserialize_reactions)

    def create_issue_reaction(self, issue_id: str, reaction: Reaction) -> ActionResult[ReactionResult]:
        response = self.post(
            f"/repos/{self.repository['name']}/issues/{issue_id}/reactions",
            data={"content": reaction},
        )
        return deserialize_action(response, deserialize_reaction)

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
        return deserialize_action(response, deserialize_branch)

    def create_branch(self, branch: BranchName, sha: SHA) -> ActionResult[GitRef]:
        ref = f"refs/heads/{branch}"
        response = self.post(
            f"/repos/{self.repository['name']}/git/refs",
            data={"ref": ref, "sha": sha},
        )
        return deserialize_action(response, deserialize_git_ref_branch)

    def update_branch(self, branch: BranchName, sha: SHA, force: bool = False) -> ActionResult[GitRef]:
        response = self.patch(
            f"/repos/{self.repository['name']}/git/refs/heads/{branch}",
            data={"sha": sha, "force": force},
        )
        return deserialize_action(response, deserialize_git_ref_branch)

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
        return deserialize_action(response, deserialize_git_ref)

    def get_file_url(
        self,
        file_path: str,
        sha: SHA,
        start_line: int | None = None,
        end_line: int | None = None,
    ) -> str:
        url = f"{self._web_base_url}/{self.repository['name']}/blob/{sha}/{file_path}"
        if start_line:
            url += f"#L{start_line}"
        if start_line and end_line:
            url += f"-L{end_line}"
        elif end_line:
            url += f"#L{end_line}"
        return url

    def get_commit_url(self, commit_sha: SHA) -> str:
        return f"{self._web_base_url}/{self.repository['name']}/commit/{commit_sha}"

    def get_pull_request_url(self, pull_request_id: str) -> str:
        return f"{self._web_base_url}/{self.repository['name']}/pull/{pull_request_id}"

    def create_git_blob(self, content: str, encoding: str) -> ActionResult[GitBlob]:
        response = self.post(
            f"/repos/{self.repository['name']}/git/blobs",
            data={"content": content, "encoding": encoding},
        )
        return deserialize_action(response, deserialize_git_blob)

    def get_file_content(
        self,
        path: str,
        ref: str,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[FileContent]:
        response = self.get(
            f"/repos/{self.repository['name']}/contents/{path}",
            params={"ref": ref},
            request_options=request_options,
        )
        if isinstance(response.json(), list):
            raise SCMCodedError(code="path_is_directory", detail=path)
        return deserialize_action(response, deserialize_file_content)

    def get_readme(
        self,
        ref: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[FileContent]:
        try:
            response = self.get(
                f"/repos/{self.repository['name']}/readme",
                params={"ref": ref},
                request_options=request_options,
            )
        except SCMCodedError as e:
            if e.code == "resource_not_found":
                raise SCMCodedError(code="readme_not_found") from e
            raise
        return deserialize_action(response, deserialize_file_content)

    def get_pull_request_template(
        self,
        ref: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> Iterator[ActionResult[FileContent]]:
        # Force pagination params to the empty pagination params type if specified as none. This is so we can
        # destructure it in the pagination-helper calls.
        iter_kwargs = pagination or {}

        for parent_dir in PULL_REQUEST_TEMPLATE_PARENT_DIRS:
            for path in self._find_pull_request_template_paths(parent_dir, ref, request_options, iter_kwargs):
                yield self.get_file_content(path, ref=ref, request_options=request_options)

    def _find_pull_request_template_paths(
        self,
        parent_dir: str,
        ref: str,
        ro: RequestOptions | None,
        pagination: PaginationParams,
    ) -> Iterator[str]:
        try:
            for page in iter_all_pages(
                lambda p: self.get_directory_contents(parent_dir, ref=ref, pagination=p, request_options=ro),
                **pagination,
            ):
                for entry in page["data"]:
                    basename = entry["path"].rsplit("/", 1)[-1].lower()
                    if entry["type"] == "file" and basename == PULL_REQUEST_TEMPLATE_FILENAME:
                        yield entry["path"]
                    elif entry["type"] == "directory" and basename == PULL_REQUEST_TEMPLATE_DIRNAME:
                        yield from self._iter_template_directory(entry["path"], ref, ro, pagination)
        except SCMCodedError as e:
            if e.code == "resource_not_found":
                return
            raise

    def _iter_template_directory(
        self,
        directory: str,
        ref: str,
        ro: RequestOptions | None,
        pagination: PaginationParams,
    ):
        for page in iter_all_pages(
            lambda p: self.get_directory_contents(directory, ref=ref, pagination=p, request_options=ro),
            **pagination,
        ):
            for entry in page["data"]:
                if entry["type"] == "file" and entry["path"].lower().endswith(".md"):
                    yield entry["path"]

    def get_directory_contents(
        self,
        path: str,
        ref: str | None = None,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[FileContent]:
        params: dict[str, str] = {}
        if ref:
            params["ref"] = ref
        response = self.get(
            f"/repos/{self.repository['name']}/contents/{path}",
            params=params,
            pagination=pagination,
            request_options=request_options,
        )
        raw = response.json()
        if not isinstance(raw, list):
            raise SCMCodedError(code="path_is_not_directory", detail=path)
        return {
            "data": deserialize_file_contents(response.content),
            "type": "github",
            "raw": {"data": response.text, "headers": dict(response.headers)},
            "meta": {**_extract_response_meta(response), "next_cursor": None},
        }

    def get_commit(
        self,
        sha: SHA,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[CommitWithChanges]:
        response = self.get(
            f"/repos/{self.repository['name']}/commits/{sha}",
            request_options=request_options,
        )
        return deserialize_action(response, deserialize_commit_with_changes)

    def get_commit_changes(
        self,
        sha: SHA,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[CommitFile]:
        response = self.get(
            f"/repos/{self.repository['name']}/commits/{sha}",
            pagination=pagination,
            request_options=request_options,
        )
        return deserialize_paginated_action(pagination, response, deserialize_commit_files)

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
        return deserialize_paginated_action(pagination, response, deserialize_commits)

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
        return deserialize_paginated_action(pagination, response, deserialize_commits)

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
        return deserialize_paginated_action(pagination, response, deserialize_commit_comparison)

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

        commit_obj = new_commit["data"]
        return ActionResult(
            data=Commit(
                id=commit_obj["sha"],
                message=commit_obj["message"],
                author=None,
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
        return deserialize_action(response, deserialize_git_tree)

    def get_git_commit(
        self,
        sha: SHA,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[GitCommitObject]:
        response = self.get(
            f"/repos/{self.repository['name']}/git/commits/{sha}",
            request_options=request_options,
        )
        return deserialize_action(response, deserialize_git_commit_object)

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
        return deserialize_action(response, deserialize_git_tree)

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
        return deserialize_action(response, deserialize_git_commit_object)

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
        return deserialize_paginated_action(pagination, response, deserialize_pull_request_files)

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
        return deserialize_paginated_action(pagination, response, deserialize_pull_request_commits)

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
        return deserialize_paginated_action(pagination, response, deserialize_pull_requests)

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
        return deserialize_action(response, deserialize_pull_request)

    def create_pull_request_draft(
        self,
        title: str,
        body: str,
        head: str,
        base: str,
    ) -> ActionResult[PullRequest]:
        try:
            response = self.post(
                f"/repos/{self.repository['name']}/pulls",
                data={"title": title, "body": body, "head": head, "base": base, "draft": True},
            )
        except SCMCodedError as e:
            if (
                e.code == "resource_unprocessable_content"
                and e.detail
                and "Draft pull requests are not supported for this repository" in e.detail
            ):
                raise SCMCodedError(code="draft_pull_request_not_supported") from e
            else:
                raise

        return deserialize_action(response, deserialize_pull_request)

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
        return deserialize_action(response, deserialize_pull_request)

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

    def update_review_comment(
        self,
        pull_request_id: str,
        comment_id: str,
        body: str,
    ) -> ActionResult[ReviewComment]:
        response = self.patch(
            f"/repos/{self.repository['name']}/pulls/comments/{comment_id}",
            data={"body": body},
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
        return deserialize_action(response, deserialize_review)

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
        return deserialize_action(response, deserialize_check_run)

    def get_check_run(
        self,
        check_run_id: ResourceId,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[CheckRun]:
        response = self.get(
            f"/repos/{self.repository['name']}/check-runs/{check_run_id}",
            request_options=request_options,
        )
        return deserialize_action(response, deserialize_check_run)

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
        return deserialize_action(response, deserialize_check_run)

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
    ) -> requests.Response:
        return self.get(
            f"/repos/{self.repository['name']}/{GITHUB_ARCHIVE_FORMAT_MAP[archive_format]}/{ref}",
            request_options=request_options,
        )

    def minimize_comment(self, comment_node_id: str, reason: str) -> None:
        self.graphql(
            MINIMIZE_COMMENT_MUTATION,
            {"commentId": comment_node_id, "reason": reason},
        )

    def resolve_review_thread(self, pull_request_id: str, thread_id: str) -> None:
        self.graphql(RESOLVE_REVIEW_THREAD_MUTATION, {"threadId": thread_id})

    def get_pull_request_review_threads(
        self,
        pull_request_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[ReviewThread]:
        owner, name = self.repository["name"].split("/", 1)
        cursor: str | None = (pagination or {}).get("cursor") or None
        per_page = (pagination or {}).get("per_page") or GITHUB_REVIEW_THREADS_DEFAULT_PAGE_SIZE

        data = self.graphql(
            REVIEW_THREADS_QUERY,
            {
                "owner": owner,
                "name": name,
                "number": int(pull_request_id),
                "cursor": cursor,
                "perPage": per_page,
            },
        )
        repository = data.get("repository") or {}
        pull_request = repository.get("pullRequest")
        if pull_request is None:
            raise SCMCodedError(
                code="resource_not_found",
                detail=f"pull request {self.repository['name']}#{pull_request_id}",
            )

        review_threads = pull_request["reviewThreads"]
        threads: list[ReviewThread] = []
        for raw_thread in review_threads["nodes"]:
            comments = list(self._iter_review_thread_comments(raw_thread))
            threads.append(
                ReviewThread(
                    id=raw_thread["id"],
                    is_resolved=raw_thread["isResolved"],
                    is_outdated=raw_thread["isOutdated"],
                    file_path=raw_thread.get("path"),
                    line=raw_thread.get("line"),
                    start_line=raw_thread.get("startLine"),
                    comments=comments,
                )
            )

        page_info = review_threads["pageInfo"]
        next_cursor = page_info["endCursor"] if page_info["hasNextPage"] else None
        return {
            "data": threads,
            "type": "github",
            "raw": {"data": data, "headers": None},
            "meta": {"next_cursor": next_cursor},
        }

    def _iter_review_thread_comments(self, raw_thread: dict[str, Any]) -> Iterator[ReviewThreadComment]:
        comments_page = raw_thread["comments"]
        for raw_comment in comments_page["nodes"]:
            yield map_graphql_review_thread_comment(raw_comment)
        page_info = comments_page["pageInfo"]
        cursor = page_info["endCursor"] if page_info["hasNextPage"] else None
        while cursor is not None:
            data = self.graphql(REVIEW_THREAD_FULL_COMMENTS_QUERY, {"threadId": raw_thread["id"], "cursor": cursor})
            node = data.get("node")
            if node is None:
                # Thread was deleted between the outer query and this follow-up.
                return
            page = node["comments"]
            for raw_comment in page["nodes"]:
                yield map_graphql_review_thread_comment(raw_comment)
            cursor = page["pageInfo"]["endCursor"] if page["pageInfo"]["hasNextPage"] else None

    def get_thread_id_from_review_comment_unique_id(
        self, pull_request_id: str, review_comment_unique_id: str
    ) -> str | None:
        owner, name = self.repository["name"].split("/", 1)
        cursor: str | None = None
        while True:
            data = self.graphql(
                REVIEW_THREAD_BY_COMMENT_QUERY,
                {"owner": owner, "name": name, "number": int(pull_request_id), "cursor": cursor},
            )
            repository = data.get("repository") or {}
            pull_request = repository.get("pullRequest")
            if pull_request is None:
                raise SCMCodedError(
                    code="resource_not_found",
                    detail=f"pull request {self.repository['name']}#{pull_request_id}",
                )
            review_threads = pull_request["reviewThreads"]
            for thread in review_threads["nodes"]:
                if self._thread_contains_review_comment(thread, review_comment_unique_id):
                    return thread["id"]
            page_info = review_threads["pageInfo"]
            if not page_info["hasNextPage"]:
                return None
            cursor = page_info["endCursor"]

    def _thread_contains_review_comment(self, thread: dict[str, Any], review_comment_unique_id: str) -> bool:
        comments = thread["comments"]
        for comment in comments["nodes"]:
            if comment["id"] == review_comment_unique_id:
                return True
        cursor = comments["pageInfo"]["endCursor"] if comments["pageInfo"]["hasNextPage"] else None
        while cursor is not None:
            data = self.graphql(THREAD_COMMENTS_QUERY, {"threadId": thread["id"], "cursor": cursor})
            node = data.get("node")
            if node is None:
                # Thread was deleted between the outer query and this follow-up.
                return False
            page = node["comments"]
            for comment in page["nodes"]:
                if comment["id"] == review_comment_unique_id:
                    return True
            cursor = page["pageInfo"]["endCursor"] if page["pageInfo"]["hasNextPage"] else None
        return False


def _map_user(user: GitHubUser | None) -> Author | None:
    if user is None:
        return None
    return Author(id=str(user.id), username=user.login)


def _map_commit_author(author: GitHubCommitAuthorDetail | None) -> CommitAuthor | None:
    if author is None:
        return None
    return CommitAuthor(
        name=author.name,
        email=author.email,
        date=datetime.fromisoformat(author.date) if author.date else None,
    )


def deserialize_action[T](response: requests.Response, fn: Callable[[bytes], T]) -> ActionResult[T]:
    return {
        "data": fn(response.content),
        "type": "github",
        "raw": {"data": response.text, "headers": dict(response.headers)},
        "meta": _extract_response_meta(response),
    }


def deserialize_paginated_action[T](
    pagination: PaginationParams | None,
    response: requests.Response,
    fn: Callable[[bytes], list[T]],
) -> PaginatedActionResult[T]:
    meta: PaginatedResponseMeta = {
        **_extract_response_meta(response),
        "next_cursor": str(int(pagination["cursor"]) + 1 if pagination else 2),
    }
    return {
        "data": fn(response.content),
        "type": "github",
        "raw": {"data": response.text, "headers": dict(response.headers)},
        "meta": meta,
    }


def map_graphql_author(raw_author: dict[str, Any] | None) -> tuple[Author | None, bool]:
    """Map a GraphQL author selection to ``(Author, is_bot)``.

    The author may be ``None`` when the account was deleted, in which case we
    return ``(None, False)``. Otherwise ``Author.id`` uses ``databaseId`` when
    available (Users) and falls back to ``login`` for actor types where
    GraphQL does not expose a stable numeric id (e.g. ``Mannequin``)."""
    if raw_author is None:
        return None, False
    typename = raw_author.get("__typename")
    is_bot = typename == "Bot"
    raw_id = raw_author.get("databaseId")
    return Author(id=str(raw_id) if raw_id is not None else raw_author["login"], username=raw_author["login"]), is_bot


def map_graphql_review_thread_comment(raw: dict[str, Any]) -> ReviewThreadComment:
    author, is_bot = map_graphql_author(raw.get("author"))
    full_database_id = raw.get("fullDatabaseId")
    return ReviewThreadComment(
        id=str(full_database_id) if full_database_id is not None else raw["id"],
        unique_id=raw["id"],
        body=raw.get("body", ""),
        author=author,
        is_bot=is_bot,
        created_at=raw.get("createdAt"),
        updated_at=raw.get("updatedAt"),
    )


def deserialize_app_installation(content: bytes) -> AppInstallation:
    r = msgspec.json.decode(content, type=GitHubAppInstallationResponse)
    return AppInstallation(
        has_read_access=True,
        has_write_access=r.permissions.contents == "write" and r.permissions.pull_requests == "write",
    )


def deserialize_repository(content: bytes) -> GitRepository:
    r = msgspec.json.decode(content, type=GitHubRepositoryResponse)
    return GitRepository(
        full_name=r.full_name,
        default_branch=r.default_branch,
        clone_url=r.clone_url,
        private=r.private,
        size=r.size,
        description=r.description,
        topics=list(r.topics),
    )


def deserialize_assignees(content: bytes) -> list[Author]:
    users = msgspec.json.decode(content, type=list[GitHubUser])
    return [Author(id=str(u.id), username=u.login) for u in users]


def deserialize_labels(content: bytes) -> list[Label]:
    labels = msgspec.json.decode(content, type=list[GitHubLabelResponse])
    return [Label(id=str(lb.id), name=lb.name, color=lb.color, description=lb.description) for lb in labels]


def deserialize_topics(content: bytes) -> list[str]:
    r = msgspec.json.decode(content, type=GitHubTopicsResponse)
    return list(r.names)


def deserialize_comment(content: bytes) -> Comment:
    c = msgspec.json.decode(content, type=GitHubCommentResponse)
    return Comment(id=str(c.id), body=c.body, author=_map_user(c.user))


def deserialize_comments(content: bytes) -> list[Comment]:
    comments = msgspec.json.decode(content, type=list[GitHubCommentResponse])
    return [Comment(id=str(c.id), body=c.body, author=_map_user(c.user)) for c in comments]


def deserialize_reaction(content: bytes) -> ReactionResult:
    r = msgspec.json.decode(content, type=GitHubReactionResponse)
    return ReactionResult(id=str(r.id), content=r.content, author=_map_user(r.user))


def deserialize_reactions(content: bytes) -> list[ReactionResult]:
    reactions = msgspec.json.decode(content, type=list[GitHubReactionResponse])
    return [ReactionResult(id=str(r.id), content=r.content, author=_map_user(r.user)) for r in reactions]


def deserialize_branch(content: bytes) -> GitRef:
    r = msgspec.json.decode(content, type=GitHubBranchResponse)
    return GitRef(ref=r.name, sha=r.commit.sha)


def deserialize_git_ref(content: bytes) -> GitRef:
    r = msgspec.json.decode(content, type=GitHubGitRefResponse)
    return GitRef(ref=r.ref, sha=r.object.sha)


def deserialize_git_ref_branch(content: bytes) -> GitRef:
    r = msgspec.json.decode(content, type=GitHubGitRefResponse)
    return GitRef(ref=r.ref.removeprefix("refs/heads/"), sha=r.object.sha)


def deserialize_git_blob(content: bytes) -> GitBlob:
    r = msgspec.json.decode(content, type=GitHubGitBlobResponse)
    return GitBlob(sha=r.sha)


def deserialize_file_content(content: bytes) -> FileContent:
    r = msgspec.json.decode(content, type=GitHubFileContentResponse)
    return FileContent(
        path=r.path,
        sha=r.sha,
        content=r.content,
        encoding=r.encoding,
        size=r.size,
        type=GITHUB_FILE_CONTENT_TYPE_MAP[r.type],
    )


def deserialize_file_contents(content: bytes) -> list[FileContent]:
    items = msgspec.json.decode(content, type=list[GitHubFileContentResponse])
    return [
        FileContent(
            path=r.path,
            sha=r.sha,
            content=r.content,
            encoding=r.encoding,
            size=r.size,
            type=GITHUB_FILE_CONTENT_TYPE_MAP[r.type],
        )
        for r in items
    ]


def deserialize_commit_with_changes(content: bytes) -> CommitWithChanges:
    r = msgspec.json.decode(content, type=GitHubCommitResponse)
    return _map_commit_with_changes(r)


def deserialize_commit_files(content: bytes) -> list[CommitFile]:
    r = msgspec.json.decode(content, type=GitHubCommitResponse)
    return [_map_commit_file(f) for f in r.files]


def deserialize_commits(content: bytes) -> list[Commit]:
    commits = msgspec.json.decode(content, type=list[GitHubCommitResponse])
    return [_map_commit(c) for c in commits]


def deserialize_commit_comparison(content: bytes) -> list[Commit]:
    r = msgspec.json.decode(content, type=GitHubCommitComparisonResponse)
    return [_map_commit(c) for c in r.commits]


def _map_commit(r: GitHubCommitResponse) -> Commit:
    return Commit(
        id=r.sha,
        message=r.commit.message,
        author=_map_commit_author(r.commit.author),
        additions=r.stats.additions if r.stats else None,
        deletions=r.stats.deletions if r.stats else None,
    )


def _map_commit_with_changes(r: GitHubCommitResponse) -> CommitWithChanges:
    return CommitWithChanges(
        id=r.sha,
        message=r.commit.message,
        author=_map_commit_author(r.commit.author),
        files=[_map_commit_file(f) for f in r.files],
        additions=r.stats.additions if r.stats else None,
        deletions=r.stats.deletions if r.stats else None,
    )


def _map_commit_file(f: GitHubCommitFileResponse) -> CommitFile:
    return CommitFile(
        filename=f.filename,
        status=GITHUB_FILE_STATUS_MAP[f.status],
        patch=f.patch,
        additions=f.additions,
        deletions=f.deletions,
        previous_filename=f.previous_filename,
    )


def deserialize_git_tree(content: bytes) -> GitTree:
    r = msgspec.json.decode(content, type=GitHubGitTreeResponse)
    return GitTree(
        sha=r.sha,
        tree=[TreeEntry(path=e.path, mode=e.mode, type=e.type, sha=e.sha, size=e.size) for e in r.tree],
        truncated=r.truncated,
    )


def deserialize_git_commit_object(content: bytes) -> GitCommitObject:
    r = msgspec.json.decode(content, type=GitHubGitCommitObjectResponse)
    return GitCommitObject(sha=r.sha, tree=GitCommitTree(sha=r.tree.sha), message=r.message)


def deserialize_review(content: bytes) -> Review:
    r = msgspec.json.decode(content, type=GitHubReviewResponse)
    return Review(id=str(r.id), html_url=r.html_url)


def deserialize_check_run(content: bytes) -> CheckRun:
    r = msgspec.json.decode(content, type=GitHubCheckRunResponse)
    return CheckRun(
        id=str(r.id),
        name=r.name,
        status=GITHUB_STATUS_MAP.get(r.status, "pending"),
        conclusion=GITHUB_CONCLUSION_MAP.get(r.conclusion) if r.conclusion else None,
        html_url=r.html_url or "",
    )


def deserialize_pull_request_files(content: bytes) -> list[PullRequestFile]:
    files = msgspec.json.decode(content, type=list[GitHubPullRequestFileResponse])
    return [
        PullRequestFile(
            filename=f.filename,
            status=GITHUB_FILE_STATUS_MAP[f.status],
            patch=f.patch,
            changes=f.changes,
            sha=f.sha or "",
            previous_filename=f.previous_filename,
        )
        for f in files
    ]


def deserialize_pull_request_commits(content: bytes) -> list[PullRequestCommit]:
    commits = msgspec.json.decode(content, type=list[GitHubPullRequestCommitResponse])
    return [
        PullRequestCommit(
            sha=c.sha,
            message=c.commit.message,
            author=_map_commit_author(c.commit.author),
        )
        for c in commits
    ]


def _map_pull_request(r: GitHubPullRequestResponse) -> PullRequest:
    return PullRequest(
        internal_id=str(r.id),
        id=str(r.number),
        title=r.title,
        body=r.body,
        state=r.state,
        merged=r.merged_at is not None,
        html_url=r.html_url,
        head=PullRequestBranch(sha=r.head.sha, ref=r.head.ref),
        base=PullRequestBranch(sha=r.base.sha, ref=r.base.ref),
        author=Author(id=str(r.user.id), username=r.user.login),
    )


def deserialize_pull_request(content: bytes) -> PullRequest:
    return _map_pull_request(msgspec.json.decode(content, type=GitHubPullRequestResponse))


def deserialize_pull_requests(content: bytes) -> list[PullRequest]:
    return [_map_pull_request(r) for r in msgspec.json.decode(content, type=list[GitHubPullRequestResponse])]


def deserialize_issue(content: bytes) -> Issue:
    r = msgspec.json.decode(content, type=GitHubIssueResponse)
    return Issue(id=str(r.number), title=r.title, body=r.body, state=r.state, html_url=r.html_url)


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
        "thread_id": None,
    }
