import datetime
from typing import Literal

import msgspec

from scm.types import (
    CheckRunAction,
    CommentAction,
    IssueState,
    PullRequestAction,
    PullRequestState,
    Reaction,
    TreeEntryMode,
    TreeEntryType,
)

type GitHubFileStatus = Literal["added", "removed", "modified", "renamed", "copied", "changed", "unchanged"]
type GitHubFileContentType = Literal["file", "dir", "symlink", "submodule"]


class GitHubUser(msgspec.Struct):
    id: int
    login: str
    type: str | None = None


class GitHubCheckRun(msgspec.Struct):
    external_id: str
    html_url: str


class GitHubIssueComment(msgspec.Struct):
    id: int
    user: GitHubUser | None
    body: str | None = None


class GitHubIssueCommentPullRequest(msgspec.Struct):
    pass


class GitHubIssue(msgspec.Struct):
    number: int
    pull_request: GitHubIssueCommentPullRequest | None = None


class GitHubPullRequest(msgspec.Struct):
    body: str | None
    head: "GitHubPullRequestHead"
    base: "GitHubPullRequestBase"
    merge_commit_sha: str | None
    title: str
    user: GitHubUser
    merged: bool | None = None


class GitHubPullRequestBase(msgspec.Struct):
    ref: str
    repo: "GitHubPullRequestRepo"
    sha: str


class GitHubPullRequestRepo(msgspec.Struct):
    private: bool


class GitHubPullRequestHead(msgspec.Struct):
    ref: str
    repo: GitHubPullRequestRepo | None
    sha: str


class GitHubPullRequestReviewComment(msgspec.Struct):
    id: int
    node_id: str
    pull_request_review_id: int
    author_association: str
    body: str
    commit_id: str
    diff_hunk: str
    html_url: str
    original_commit_id: str
    path: str
    user: GitHubUser | None
    created_at: datetime.datetime
    updated_at: datetime.datetime


# Remaining types in use:
#   * "installation"
#   * "installation_repositories"
#   * "issues"
#   * "pull_request_review"
#   * "pull_request_review_comment"
#   * "push"


class GitHubCheckRunEvent(msgspec.Struct):
    action: CheckRunAction
    check_run: GitHubCheckRun


class GitHubIssueCommentEvent(msgspec.Struct):
    action: CommentAction
    comment: GitHubIssueComment
    issue: GitHubIssue


class GitHubPullRequestEvent(msgspec.Struct):
    action: PullRequestAction
    number: int
    pull_request: GitHubPullRequest


class GitHubAppInstallationPermissions(msgspec.Struct):
    contents: str | None = None
    pull_requests: str | None = None


class GitHubAppInstallationResponse(msgspec.Struct):
    permissions: GitHubAppInstallationPermissions = msgspec.field(default_factory=GitHubAppInstallationPermissions)


class GitHubRepositoryResponse(msgspec.Struct):
    full_name: str
    default_branch: str
    clone_url: str
    private: bool
    size: int
    description: str | None = None
    topics: list[str] = msgspec.field(default_factory=list)


class GitHubCommentResponse(msgspec.Struct):
    id: int
    body: str | None = None
    user: GitHubUser | None = None


class GitHubLabelResponse(msgspec.Struct):
    id: int
    name: str
    color: str
    description: str | None = None


class GitHubReactionResponse(msgspec.Struct):
    id: int
    content: Reaction
    user: GitHubUser | None = None


class GitHubGitBlobResponse(msgspec.Struct):
    sha: str


class GitHubFileContentResponse(msgspec.Struct):
    path: str
    sha: str
    size: int
    content: str = ""
    encoding: str = ""
    type: GitHubFileContentType = "file"


class GitHubCommitAuthorDetail(msgspec.Struct):
    name: str = ""
    email: str = ""
    date: str | None = None


class GitHubCommitFileResponse(msgspec.Struct):
    filename: str
    status: GitHubFileStatus = "modified"
    patch: str | None = None
    additions: int | None = None
    deletions: int | None = None
    previous_filename: str | None = None


class GitHubCommitDetail(msgspec.Struct):
    message: str = ""
    author: GitHubCommitAuthorDetail | None = None


class GitHubCommitStats(msgspec.Struct):
    additions: int | None = None
    deletions: int | None = None


class GitHubCommitResponse(msgspec.Struct):
    sha: str
    commit: GitHubCommitDetail = msgspec.field(default_factory=GitHubCommitDetail)
    files: list[GitHubCommitFileResponse] = msgspec.field(default_factory=list)
    stats: GitHubCommitStats | None = None


class GitHubTreeEntryResponse(msgspec.Struct):
    path: str
    mode: TreeEntryMode
    type: TreeEntryType
    sha: str
    size: int | None = None


class GitHubGitTreeResponse(msgspec.Struct):
    sha: str
    tree: list[GitHubTreeEntryResponse]
    truncated: bool


class GitHubGitCommitTreeResponse(msgspec.Struct):
    sha: str


class GitHubGitCommitObjectResponse(msgspec.Struct):
    sha: str
    tree: GitHubGitCommitTreeResponse
    message: str = ""


class GitHubReviewResponse(msgspec.Struct):
    id: int
    html_url: str


class GitHubCheckRunResponse(msgspec.Struct):
    id: int
    name: str = ""
    status: str = ""
    conclusion: str | None = None
    html_url: str | None = None


class GitHubPullRequestFileResponse(msgspec.Struct):
    filename: str
    status: GitHubFileStatus = "modified"
    patch: str | None = None
    changes: int = 0
    sha: str | None = None
    previous_filename: str | None = None


class GitHubPullRequestCommitResponse(msgspec.Struct):
    sha: str
    commit: GitHubCommitDetail = msgspec.field(default_factory=GitHubCommitDetail)


class GitHubPullRequestBranchResponse(msgspec.Struct):
    sha: str | None
    ref: str


class GitHubPullRequestResponse(msgspec.Struct):
    id: int
    number: int
    title: str
    state: PullRequestState
    head: GitHubPullRequestBranchResponse
    base: GitHubPullRequestBranchResponse
    user: GitHubUser
    html_url: str = ""
    body: str | None = None
    merged_at: str | None = None


class GitHubIssueResponse(msgspec.Struct):
    number: int
    title: str
    state: IssueState
    body: str | None = None
    html_url: str = ""


class GitHubTopicsResponse(msgspec.Struct):
    names: list[str] = msgspec.field(default_factory=list)


class GitHubBranchCommit(msgspec.Struct):
    sha: str


class GitHubBranchResponse(msgspec.Struct):
    name: str
    commit: GitHubBranchCommit


class GitHubGitRefObject(msgspec.Struct):
    sha: str


class GitHubGitRefResponse(msgspec.Struct):
    ref: str
    object: GitHubGitRefObject


class GitHubCommitComparisonResponse(msgspec.Struct):
    commits: list[GitHubCommitResponse]
