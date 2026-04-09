import datetime

import msgspec

from scm.types import CheckRunAction, CommentAction, PullRequestAction


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
