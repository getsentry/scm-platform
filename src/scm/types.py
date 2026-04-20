from collections.abc import MutableMapping
from datetime import datetime
from typing import Any, Literal, Protocol, Required, TypedDict, runtime_checkable

import requests

type Action = Literal["check_run", "comment", "pull_request"]
type EventTypeHint = Literal["check_run", "comment", "pull_request"]
type HybridCloudSilo = Literal["control", "region"]


type ProviderName = Literal["bitbucket", "github", "github_enterprise", "gitlab"]
"""The SCM provider that owns an integration or repository."""

PROVIDER_SET: set[ProviderName] = set(["bitbucket", "github", "github_enterprise", "gitlab"])

type ExternalId = str
"""
Identifier whose origin is an external, source-code-management provider. Refers specifically to
the unique identifier of a repository.
"""

type ResourceId = str
"""An opaque provider-assigned identifier for a resource (pull request, review, check run, etc.).

Represented as a string to accommodate providers that use non-integer IDs (e.g. GitLab uses
integers but Bitbucket uses UUIDs). Callers should treat this as opaque and not assume numeric
ordering or format.
"""

type Reaction = Literal["+1", "-1", "laugh", "confused", "heart", "hooray", "rocket", "eyes"]
"""Normalized reaction identifiers shared across all SCM providers."""

type Referrer = str
"""
Identifies the caller so providers can apply per-referrer rate-limit policies and emit metrics
scoped by referrer.
"""

type RepositoryId = int | tuple[ProviderName, ExternalId]
"""A repository can be identified by its internal DB id or by a (provider, external_id) pair."""

type FileStatus = Literal["added", "removed", "modified", "renamed", "copied", "changed", "unchanged", "unknown"]
"""The change type applied to a file in a commit or pull request.

- added: file was created
- removed: file was deleted
- modified: file contents changed
- renamed: file was moved; see previous_filename for the old path
- copied: file was duplicated from another path
- changed: file metadata changed without content change (e.g. mode)
- unchanged: file appears in the diff context but was not modified
- unknown: file status could not be positively identified
"""

type ArchiveFormat = Literal["tarball", "zip"]
"""Normalized archive format identifiers shared across all SCM providers."""


class ArchiveLink(TypedDict):
    """A download URL bundled with the authentication headers required to fetch it."""

    url: str
    headers: dict[str, str]


type BuildStatus = Literal["pending", "running", "completed"]
"""The lifecycle stage of a CI build.

- pending: created or queued but not yet running
- running: actively executing
- completed: finished; see BuildConclusion for the outcome
"""

type BuildConclusion = Literal[
    "success",
    "failure",
    "cancelled",
    "skipped",
    "timed_out",
    "neutral",
    "action_required",
    "unknown",
]
"""The terminal outcome of a completed build.

- success: all checks passed
- failure: one or more checks failed
- cancelled: stopped before completion
- skipped: deliberately bypassed
- timed_out: exceeded the time limit
- neutral: completed without a pass/fail determination
- action_required: requires manual intervention before proceeding
- unknown: outcome could not be determined
"""

type TreeEntryMode = Literal["100644", "100755", "040000", "160000", "120000"]
"""UNIX file mode for a git tree entry, as stored in a git tree object.

- 100644: regular file (non-executable)
- 100755: executable file
- 040000: directory (subtree)
- 160000: git submodule (gitlink)
- 120000: symbolic link
"""

type TreeEntryType = Literal["blob", "tree", "commit"]
"""The object type stored at a git tree entry.

- blob: a file
- tree: a directory (subtree)
- commit: a submodule reference
"""

type ReviewSide = Literal["LEFT", "RIGHT"]
"""Which side of a diff a review comment is anchored to.

- LEFT: the base (original) side of the diff
- RIGHT: the head (modified) side of the diff
"""

type BranchName = str
type SHA = str
type IssueState = Literal["open", "closed"]
type PullRequestState = Literal["open", "closed"]
type ReviewEvent = Literal["approve", "change_request", "comment"]


class PaginationParams(TypedDict, total=False):
    """Controls page traversal for list endpoints.

    - cursor: an opaque token returned from a previous page's `next_cursor`
    - per_page: how many items to return per page
    """

    cursor: str
    per_page: int


class RequestOptions(TypedDict, total=False):
    """Transport-level options for single-resource fetches.

    - if_none_match: send an `If-None-Match` header (ETag-based caching)
    - if_modified_since: send an `If-Modified-Since` header (UTC datetime)
    """

    if_none_match: str
    if_modified_since: datetime


class ResponseMeta(TypedDict, total=False):
    """Transport-level metadata attached to a single-resource provider response.

    - etag: the `ETag` header value, usable in a subsequent `if_none_match`
    - last_modified: UTC datetime parsed from the `Last-Modified` header
    """

    etag: str
    last_modified: datetime


class PaginatedResponseMeta(TypedDict, total=False):
    """Transport-level metadata attached to a paginated provider response.

    Carries all fields from `ResponseMeta` plus a required `next_cursor`
    that callers can pass back to `PaginationParams.cursor` to fetch the
    next page. `None` means there are no more pages.
    """

    etag: str
    last_modified: datetime
    next_cursor: Required[str | None]


class Author(TypedDict):
    """Normalized author identity returned by all SCM providers."""

    id: ResourceId
    username: str


class Comment(TypedDict):
    """Provider-agnostic representation of an issue or pull-request comment."""

    id: ResourceId
    body: str | None
    author: Author | None


class Label(TypedDict):
    """Provider-agnostic representation of a repository label."""

    id: ResourceId
    name: str
    color: str
    description: str | None


class ReactionResult(TypedDict):
    """Provider-agnostic representation of a reaction on an issue, comment, or pull request."""

    id: ResourceId
    content: Reaction
    author: Author | None


class PullRequestBranch(TypedDict):
    """A branch reference within a pull request (head or base)."""

    sha: SHA | None
    ref: BranchName


class PullRequest(TypedDict):
    """Provider-agnostic representation of a pull request."""

    # @todo Why do we have two ids here? Confusing.
    id: ResourceId
    number: str
    title: str
    body: str | None
    state: PullRequestState
    merged: bool
    html_url: str
    head: PullRequestBranch
    base: PullRequestBranch


class Issue(TypedDict):
    """Provider-agnostic representation of an issue."""

    id: ResourceId
    title: str
    body: str | None
    state: IssueState
    html_url: str


class RawResult(TypedDict):
    headers: MutableMapping[str, str] | None
    data: Any


class ActionResult[T](TypedDict):
    """Wraps a provider response with metadata and the original API payload.

    Pairs a normalized domain object with the provider name and raw API
    payload. This lets callers work with a stable interface while still
    having access to provider-specific fields when needed.

    The `meta` field carries transport-level metadata such as ETags.
    Pass an empty dict when the provider does not supply any metadata.
    """

    data: T
    type: ProviderName
    raw: RawResult
    meta: ResponseMeta


class PaginatedActionResult[T](TypedDict):
    """Wraps a paginated provider response.

    Identical to `ActionResult` but carries a `PaginatedResponseMeta` with a required
    `page_info`, guaranteeing that callers of list endpoints always have access to pagination
    state.
    """

    data: list[T]
    type: ProviderName
    raw: RawResult
    meta: PaginatedResponseMeta


class Repository(TypedDict):
    """Identifies a repository within a Sentry integration."""

    external_id: str | None
    id: int
    integration_id: int
    is_active: bool
    name: str
    organization_id: int
    provider_name: ProviderName


class GitRepository(TypedDict):
    """Information about a git repository at an SCM provider."""

    full_name: str
    default_branch: str | None
    clone_url: str
    private: bool
    size: int


class GitRef(TypedDict):
    """A git reference (branch pointer)."""

    ref: BranchName
    sha: SHA


class GitBlob(TypedDict):
    sha: SHA


class FileContent(TypedDict):
    path: str
    sha: SHA
    content: str  # base64-encoded
    encoding: str
    size: int


class CommitAuthor(TypedDict):
    name: str
    email: str
    date: datetime | None


class CommitFile(TypedDict):
    filename: str
    status: FileStatus
    patch: str | None


class Commit(TypedDict):
    id: SHA
    message: str
    author: CommitAuthor | None
    files: list[CommitFile] | None


class CommitComparison(TypedDict):
    ahead_by: int
    behind_by: int
    commits: list[Commit]


class TreeEntry(TypedDict):
    path: str
    mode: TreeEntryMode
    type: TreeEntryType
    sha: SHA
    size: int | None


class InputTreeEntry(TypedDict):
    path: str
    mode: TreeEntryMode
    type: TreeEntryType
    sha: SHA | None  # None for deletions


class GitTree(TypedDict):
    sha: SHA
    tree: list[TreeEntry]
    truncated: bool


class GitCommitTree(TypedDict):
    sha: SHA


class GitCommitObject(TypedDict):
    sha: SHA
    tree: GitCommitTree
    message: str


class PullRequestFile(TypedDict):
    filename: str
    status: FileStatus
    patch: str | None
    changes: int
    sha: SHA
    previous_filename: str | None


class PullRequestCommit(TypedDict):
    sha: SHA
    message: str
    author: CommitAuthor | None


class ReviewCommentInput(TypedDict, total=False):
    """Input for an inline comment within a batch review."""

    path: Required[str]
    body: Required[str]
    line: int
    side: ReviewSide
    start_line: int
    start_side: ReviewSide


class ReviewComment(TypedDict):
    """Provider-agnostic representation of a review comment."""

    id: ResourceId
    unique_id: str | None
    url: str | None
    file_path: str
    body: str
    author: Author | None
    created_at: str | None
    diff_hunk: str | None
    review_id: ResourceId | None
    author_association: str | None
    commit_sha: str | None
    head: str | None


class Review(TypedDict):
    """Provider-agnostic representation of a pull request review."""

    id: ResourceId
    html_url: str


class CheckRunOutput(TypedDict, total=False):
    """Output annotation for a check run."""

    title: Required[str]
    summary: Required[str]
    text: str


class CheckRun(TypedDict):
    """Provider-agnostic representation of a check run."""

    id: ResourceId
    name: str
    status: BuildStatus
    conclusion: BuildConclusion | None
    html_url: str


type CheckRunAction = Literal["completed", "created", "requested_action", "rerequested"]


class CheckRunEventData(TypedDict):
    external_id: str
    html_url: str


type CommentAction = Literal["created", "deleted", "edited", "pinned", "unpinned"]
type CommentType = Literal["issue", "pull_request"]


class CommentEventData(TypedDict):
    id: str
    body: str | None
    author: Author | None


type PullRequestAction = Literal[
    "assigned",
    "auto_merge_disabled",
    "auto_merge_enabled",
    "closed",
    "converted_to_draft",
    "demilestoned",  # Removed a milestone.
    "dequeued",  # Removed from merge queue.
    "edited",
    "enqueued",  # Added to merge queue.
    "labeled",
    "locked",
    "milestoned",  # Added a milestone.
    "opened",
    "ready_for_review",
    "reopened",
    "review_request_removed",
    "review_requested",
    "synchronize",  # Commits were pushed.
    "unassigned",
    "unlabeled",
    "unlocked",
]


class PullRequestEventData(TypedDict):
    id: str
    title: str
    description: str | None
    head: PullRequestBranch
    base: PullRequestBranch
    is_private_repo: bool
    author: Author | None


# Basic protocols


@runtime_checkable
class GetRepositoryProtocol(Protocol):
    def get_repository(self) -> ActionResult[GitRepository]: ...


@runtime_checkable
class GetRepositoryAssigneesProtocol(Protocol):
    def get_repository_assignees(
        self,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[Author]: ...


@runtime_checkable
class GetRepositoryLabelsProtocol(Protocol):
    def get_repository_labels(
        self,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[Label]: ...


# Issue Protocols


@runtime_checkable
class GetIssueProtocol(Protocol):
    def get_issue(
        self,
        issue_id: str,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[Issue]: ...


@runtime_checkable
class CreateIssueProtocol(Protocol):
    def create_issue(
        self,
        title: str,
        body: str,
        assignees: list[str] | None = None,
        labels: list[str] | None = None,
    ) -> ActionResult[Issue]: ...


# Issue Comment Protocols


@runtime_checkable
class GetIssueCommentsProtocol(Protocol):
    def get_issue_comments(
        self,
        issue_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[Comment]: ...


@runtime_checkable
class CreateIssueCommentProtocol(Protocol):
    def create_issue_comment(self, issue_id: str, body: str) -> ActionResult[Comment]: ...


@runtime_checkable
class DeleteIssueCommentProtocol(Protocol):
    def delete_issue_comment(self, issue_id: str, comment_id: str) -> None: ...


# Pull Request Comment Protocols


@runtime_checkable
class GetPullRequestCommentsProtocol(Protocol):
    def get_pull_request_comments(
        self,
        pull_request_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[Comment]: ...


@runtime_checkable
class CreatePullRequestCommentProtocol(Protocol):
    def create_pull_request_comment(self, pull_request_id: str, body: str) -> ActionResult[Comment]: ...


@runtime_checkable
class DeletePullRequestCommentProtocol(Protocol):
    def delete_pull_request_comment(self, pull_request_id: str, comment_id: str) -> None: ...


# Issue Comment Reaction Protocols


@runtime_checkable
class GetIssueCommentReactionsProtocol(Protocol):
    def get_issue_comment_reactions(
        self,
        issue_id: str,
        comment_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[ReactionResult]: ...


@runtime_checkable
class CreateIssueCommentReactionProtocol(Protocol):
    def create_issue_comment_reaction(
        self, issue_id: str, comment_id: str, reaction: Reaction
    ) -> ActionResult[ReactionResult]: ...


@runtime_checkable
class DeleteIssueCommentReactionProtocol(Protocol):
    def delete_issue_comment_reaction(self, issue_id: str, comment_id: str, reaction_id: str) -> None: ...


# Pull Request Comment Reaction Protocols


@runtime_checkable
class GetPullRequestCommentReactionsProtocol(Protocol):
    def get_pull_request_comment_reactions(
        self,
        pull_request_id: str,
        comment_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[ReactionResult]: ...


@runtime_checkable
class CreatePullRequestCommentReactionProtocol(Protocol):
    def create_pull_request_comment_reaction(
        self, pull_request_id: str, comment_id: str, reaction: Reaction
    ) -> ActionResult[ReactionResult]: ...


@runtime_checkable
class DeletePullRequestCommentReactionProtocol(Protocol):
    def delete_pull_request_comment_reaction(self, pull_request_id: str, comment_id: str, reaction_id: str) -> None: ...


# Issue Reaction Protocols


@runtime_checkable
class GetIssueReactionsProtocol(Protocol):
    def get_issue_reactions(
        self,
        issue_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[ReactionResult]: ...


@runtime_checkable
class CreateIssueReactionProtocol(Protocol):
    def create_issue_reaction(self, issue_id: str, reaction: Reaction) -> ActionResult[ReactionResult]: ...


@runtime_checkable
class DeleteIssueReactionProtocol(Protocol):
    def delete_issue_reaction(self, issue_id: str, reaction_id: str) -> None: ...


# Pull Request Reaction Protocols


@runtime_checkable
class GetPullRequestReactionsProtocol(Protocol):
    def get_pull_request_reactions(
        self,
        pull_request_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[ReactionResult]: ...


@runtime_checkable
class CreatePullRequestReactionProtocol(Protocol):
    def create_pull_request_reaction(
        self, pull_request_id: str, reaction: Reaction
    ) -> ActionResult[ReactionResult]: ...


@runtime_checkable
class DeletePullRequestReactionProtocol(Protocol):
    def delete_pull_request_reaction(self, pull_request_id: str, reaction_id: str) -> None: ...


# Branch Protocols


@runtime_checkable
class GetBranchProtocol(Protocol):
    def get_branch(
        self,
        branch: BranchName,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[GitRef]: ...


@runtime_checkable
class CreateBranchProtocol(Protocol):
    def create_branch(self, branch: BranchName, sha: SHA) -> ActionResult[GitRef]: ...


@runtime_checkable
class UpdateBranchProtocol(Protocol):
    def update_branch(self, branch: BranchName, sha: SHA, force: bool = False) -> ActionResult[GitRef]: ...


# Commit Protocols


@runtime_checkable
class GetCommitProtocol(Protocol):
    def get_commit(
        self,
        sha: SHA,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[Commit]: ...


@runtime_checkable
class GetCommitsProtocol(Protocol):
    def get_commits(
        self,
        ref: str | None = None,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[Commit]: ...


@runtime_checkable
class GetCommitsByPathProtocol(Protocol):
    def get_commits_by_path(
        self,
        path: str,
        ref: str | None = None,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[Commit]: ...


@runtime_checkable
class CompareCommitsProtocol(Protocol):
    def compare_commits(
        self,
        start_sha: SHA,
        end_sha: SHA,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[Commit]: ...


# Pull Request Protocols


@runtime_checkable
class GetPullRequestProtocol(Protocol):
    def get_pull_request(
        self,
        pull_request_id: str,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[PullRequest]: ...


@runtime_checkable
class GetPullRequestsProtocol(Protocol):
    def get_pull_requests(
        self,
        state: PullRequestState | None = "open",
        head: BranchName | None = None,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[PullRequest]: ...


@runtime_checkable
class GetPullRequestFilesProtocol(Protocol):
    def get_pull_request_files(
        self,
        pull_request_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[PullRequestFile]: ...


@runtime_checkable
class GetPullRequestCommitsProtocol(Protocol):
    def get_pull_request_commits(
        self,
        pull_request_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[PullRequestCommit]: ...


@runtime_checkable
class GetPullRequestDiffProtocol(Protocol):
    def get_pull_request_diff(
        self,
        pull_request_id: str,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[str]: ...


@runtime_checkable
class CreatePullRequestProtocol(Protocol):
    def create_pull_request(
        self,
        title: str,
        body: str,
        head: BranchName,
        base: BranchName,
    ) -> ActionResult[PullRequest]: ...


@runtime_checkable
class CreatePullRequestDraftProtocol(Protocol):
    def create_pull_request_draft(
        self,
        title: str,
        body: str,
        head: BranchName,
        base: BranchName,
    ) -> ActionResult[PullRequest]: ...


@runtime_checkable
class UpdatePullRequestProtocol(Protocol):
    def update_pull_request(
        self,
        pull_request_id: str,
        title: str | None = None,
        body: str | None = None,
        state: PullRequestState | None = None,
    ) -> ActionResult[PullRequest]: ...


@runtime_checkable
class RequestReviewProtocol(Protocol):
    def request_review(self, pull_request_id: str, reviewers: list[str]) -> None: ...


# Git Object Protocols


@runtime_checkable
class GetTreeProtocol(Protocol):
    def get_tree(
        self,
        tree_sha: SHA,
        recursive: bool = True,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[GitTree]: ...


@runtime_checkable
class GetGitCommitProtocol(Protocol):
    def get_git_commit(
        self,
        sha: SHA,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[GitCommitObject]: ...


@runtime_checkable
class CreateGitBlobProtocol(Protocol):
    def create_git_blob(self, content: str, encoding: str) -> ActionResult[GitBlob]: ...


@runtime_checkable
class CreateGitTreeProtocol(Protocol):
    def create_git_tree(self, tree: list[InputTreeEntry], base_tree: SHA | None = None) -> ActionResult[GitTree]: ...


@runtime_checkable
class CreateGitCommitProtocol(Protocol):
    def create_git_commit(
        self, message: str, tree_sha: SHA, parent_shas: list[SHA]
    ) -> ActionResult[GitCommitObject]: ...


# File Content Protocol


@runtime_checkable
class GetFileContentProtocol(Protocol):
    def get_file_content(
        self,
        path: str,
        ref: str | None = None,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[FileContent]: ...


# Archive Protocols


@runtime_checkable
class GetArchiveLinkProtocol(Protocol):
    def get_archive_link(self, ref: str, archive_format: ArchiveFormat = "tarball") -> ActionResult[ArchiveLink]: ...


@runtime_checkable
class DownloadArchiveProtocol(Protocol):
    def download_archive(self, ref: str, archive_format: ArchiveFormat = "tarball") -> bytes: ...


# Check Run Protocols


@runtime_checkable
class GetCheckRunProtocol(Protocol):
    def get_check_run(
        self,
        check_run_id: ResourceId,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[CheckRun]: ...


@runtime_checkable
class CreateCheckRunProtocol(Protocol):
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
    ) -> ActionResult[CheckRun]: ...


@runtime_checkable
class UpdateCheckRunProtocol(Protocol):
    def update_check_run(
        self,
        check_run_id: ResourceId,
        status: BuildStatus | None = None,
        conclusion: BuildConclusion | None = None,
        output: CheckRunOutput | None = None,
    ) -> ActionResult[CheckRun]: ...


# Review Protocols


@runtime_checkable
class CreateReviewCommentFileProtocol(Protocol):
    def create_review_comment_file(
        self,
        pull_request_id: str,
        commit_id: SHA,
        body: str,
        path: str,
        side: ReviewSide,
    ) -> ActionResult[ReviewComment]: ...


@runtime_checkable
class CreateReviewCommentLineProtocol(Protocol):
    def create_review_comment_line(
        self,
        pull_request_id: str,
        commit_id: SHA,
        body: str,
        path: str,
        side: ReviewSide,
        line: int,
    ) -> ActionResult[ReviewComment]: ...


@runtime_checkable
class CreateReviewCommentMultilineProtocol(Protocol):
    def create_review_comment_multiline(
        self,
        pull_request_id: str,
        commit_id: SHA,
        body: str,
        path: str,
        side: ReviewSide,
        start_line: int,
        end_line: int,
    ) -> ActionResult[ReviewComment]: ...


@runtime_checkable
class CreateReviewCommentReplyProtocol(Protocol):
    def create_review_comment_reply(
        self,
        pull_request_id: str,
        body: str,
        comment_id: str,
    ) -> ActionResult[ReviewComment]: ...


@runtime_checkable
class CreateReviewProtocol(Protocol):
    def create_review(
        self,
        pull_request_id: str,
        commit_sha: SHA,
        event: ReviewEvent,
        comments: list[ReviewCommentInput],
        body: str | None = None,
    ) -> ActionResult[Review]: ...


# Moderation Protocols


@runtime_checkable
class MinimizeCommentProtocol(Protocol):
    def minimize_comment(self, comment_node_id: str, reason: str) -> None: ...


@runtime_checkable
class ResolveReviewThreadProtocol(Protocol):
    def resolve_review_thread(self, thread_node_id: str) -> None: ...


ALL_PROTOCOLS = (
    CompareCommitsProtocol,
    CreateBranchProtocol,
    CreateCheckRunProtocol,
    CreateGitBlobProtocol,
    CreateGitCommitProtocol,
    CreateGitTreeProtocol,
    CreateIssueCommentProtocol,
    CreateIssueCommentReactionProtocol,
    CreateIssueProtocol,
    CreateIssueReactionProtocol,
    CreatePullRequestCommentProtocol,
    CreatePullRequestCommentReactionProtocol,
    CreatePullRequestDraftProtocol,
    CreatePullRequestProtocol,
    CreatePullRequestReactionProtocol,
    CreateReviewCommentFileProtocol,
    CreateReviewCommentLineProtocol,
    CreateReviewCommentMultilineProtocol,
    CreateReviewCommentReplyProtocol,
    CreateReviewProtocol,
    DeleteIssueCommentProtocol,
    DeleteIssueCommentReactionProtocol,
    DeleteIssueReactionProtocol,
    DeletePullRequestCommentProtocol,
    DeletePullRequestCommentReactionProtocol,
    DeletePullRequestReactionProtocol,
    DownloadArchiveProtocol,
    GetArchiveLinkProtocol,
    GetBranchProtocol,
    GetCheckRunProtocol,
    GetCommitProtocol,
    GetCommitsByPathProtocol,
    GetCommitsProtocol,
    GetFileContentProtocol,
    GetGitCommitProtocol,
    GetIssueCommentReactionsProtocol,
    GetIssueCommentsProtocol,
    GetIssueProtocol,
    GetIssueReactionsProtocol,
    GetPullRequestCommentReactionsProtocol,
    GetPullRequestCommentsProtocol,
    GetPullRequestCommitsProtocol,
    GetPullRequestDiffProtocol,
    GetPullRequestFilesProtocol,
    GetPullRequestProtocol,
    GetPullRequestReactionsProtocol,
    GetPullRequestsProtocol,
    GetRepositoryAssigneesProtocol,
    GetRepositoryLabelsProtocol,
    GetRepositoryProtocol,
    GetTreeProtocol,
    MinimizeCommentProtocol,
    RequestReviewProtocol,
    ResolveReviewThreadProtocol,
    UpdateBranchProtocol,
    UpdateCheckRunProtocol,
    UpdatePullRequestProtocol,
)


class ApiClient[T](Protocol):
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
    ) -> T | requests.Response: ...


class Provider(ApiClient, Protocol):
    """
    Providers abstract over an integration. They map generic commands to service-provider specific
    commands and they map the results of those commands to generic result-types.

    Providers necessarily offer a larger API surface than what is available in an integration. Some
    methods may be duplicates in some providers. This is intentional. Providers capture programmer
    intent and translate it into a concrete interface. Therefore, providers provide a large range
    of behaviors which may or may not be explicitly defined on a service-provider.

    Providers, also by necessity, offer a smaller API surface than what the SCM platform can
    maximally provide. There are simply some operations which can not be adequately translated
    between providers. None the less, we want to have a service-agnostic interface. This problem
    is solved with capability-object-like system. Capabilities are progressively opted into using
    structural sub-typing. As a provider's surface area expands the SourceCodeManager class will
    automatically recognize that the provider has a particular capability and return "true" when
    handling "can" requests.
    """

    organization_id: int
    repository: Repository

    def is_rate_limited(self, referrer: Referrer) -> bool: ...
