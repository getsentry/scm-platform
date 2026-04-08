import datetime

import msgspec


class GitHubUser(msgspec.Struct, gc=False):
    id: int
    login: str
    type: str | None = None


class GitHubCheckRun(msgspec.Struct, gc=False):
    external_id: str
    html_url: str


class GitHubIssueComment(msgspec.Struct, gc=False):
    id: int
    user: GitHubUser | None
    body: str | None = None


class GitHubIssueCommentPullRequest(msgspec.Struct, gc=False):
    pass


class GitHubIssue(msgspec.Struct, gc=False):
    number: int
    pull_request: GitHubIssueCommentPullRequest | None = None


class GitHubPullRequest(msgspec.Struct, gc=False):
    body: str | None
    head: "GitHubPullRequestHead"
    base: "GitHubPullRequestBase"
    merge_commit_sha: str | None
    title: str
    user: GitHubUser
    merged: bool | None = None


class GitHubPullRequestBase(msgspec.Struct, gc=False):
    ref: str
    repo: "GitHubPullRequestRepo"
    sha: str


class GitHubPullRequestRepo(msgspec.Struct, gc=False):
    private: bool


class GitHubPullRequestHead(msgspec.Struct, gc=False):
    ref: str
    repo: GitHubPullRequestRepo | None
    sha: str


class GitHubPullRequestReviewComment(msgspec.Struct, gc=False):
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
