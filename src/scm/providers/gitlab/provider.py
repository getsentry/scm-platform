import datetime
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from typing import Any
from urllib.parse import quote

import requests

from scm.errors import ErrorCode, SCMCodedError
from scm.helpers import iter_all_pages
from scm.types import (
    SHA,
    ActionResult,
    ApiClient,
    AppInstallation,
    ArchiveFormat,
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
    CommitComparison,
    CommitFile,
    CommitWithChanges,
    CoPilotChatExtension,
    CredentialsSet,
    DeleteCommitAction,
    Encoding,
    FileContent,
    FileContentType,
    FileStatus,
    GitCommitObject,
    GitCommitTree,
    GitRef,
    GitRepository,
    GitTree,
    Issue,
    IssueState,
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

API_VERSION = "/api/v4"

VALID_README_FILES = {"readme", "readme.md", "readme.txt", "readme.rst"}

PULL_REQUEST_TEMPLATE_DIR = ".gitlab/merge_request_templates"


class GitLab:
    oauth_token = "/oauth/token"
    blame = "/projects/{project}/repository/files/{path}/blame"
    commit = "/projects/{project}/repository/commits/{sha}"
    commits = "/projects/{project}/repository/commits"
    commit_merge_requests = "/projects/{project}/repository/commits/{sha}/merge_requests"
    compare = "/projects/{project}/repository/compare"
    diff = "/projects/{project}/repository/commits/{sha}/diff"
    file = "/projects/{project}/repository/files/{path}"
    file_raw = "/projects/{project}/repository/files/{path}/raw"
    group = "/groups/{group}"
    group_projects = "/groups/{group}/projects"
    hooks = "/hooks"
    issue = "/projects/{project}/issues/{issue}"
    issues = "/projects/{project}/issues"
    project_users = "/projects/{project_id}/users"
    project_labels = "/projects/{project_id}/labels"
    issue_awards = "/projects/{project_id}/issues/{issue_id}/award_emoji"
    issue_award = "/projects/{project_id}/issues/{issue_id}/award_emoji/{award_id}"
    issue_notes = "/projects/{project_id}/issues/{issue_id}/notes"
    issue_note = "/projects/{project_id}/issues/{issue_id}/notes/{note_id}"
    issue_note_awards = "/projects/{project_id}/issues/{issue_id}/notes/{note_id}/award_emoji"
    issue_note_award = "/projects/{project_id}/issues/{issue_id}/notes/{note_id}/award_emoji/{award_id}"
    merge_requests = "/projects/{project_id}/merge_requests"
    merge_request = "/projects/{project_id}/merge_requests/{pr_key}"
    merge_request_commits = "/projects/{project_id}/merge_requests/{pr_key}/commits"
    merge_request_awards = "/projects/{project_id}/merge_requests/{pr_key}/award_emoji"
    merge_request_award = "/projects/{project_id}/merge_requests/{pr_key}/award_emoji/{award_id}"
    merge_request_notes = "/projects/{project_id}/merge_requests/{pr_key}/notes"
    merge_request_note = "/projects/{project_id}/merge_requests/{pr_key}/notes/{note_id}"
    merge_request_note_awards = "/projects/{project_id}/merge_requests/{pr_key}/notes/{note_id}/award_emoji"
    merge_request_note_award = "/projects/{project_id}/merge_requests/{pr_key}/notes/{note_id}/award_emoji/{award_id}"
    merge_request_versions = "/projects/{project_id}/merge_requests/{pr_key}/versions"
    merge_request_discussions = "/projects/{project_id}/merge_requests/{pr_key}/discussions"
    merge_request_discussion = "/projects/{project_id}/merge_requests/{pr_key}/discussions/{discussion_id}"
    merge_request_discussion_notes = "/projects/{project_id}/merge_requests/{pr_key}/discussions/{discussion_id}/notes"
    merge_request_discussion_note = (
        "/projects/{project_id}/merge_requests/{pr_key}/discussions/{discussion_id}/notes/{note_id}"
    )
    merge_request_approve = "/projects/{project_id}/merge_requests/{pr_key}/approve"
    pr_diffs = "/projects/{project}/merge_requests/{pr_key}/diffs"
    pr_raw_diffs = "/projects/{project}/merge_requests/{pr_key}/raw_diffs"
    project = "/projects/{project}"
    project_issues = "/projects/{project}/issues"
    project_hooks = "/projects/{project}/hooks"
    project_hook = "/projects/{project}/hooks/{hook_id}"
    projects = "/projects"
    statuses = "/projects/{project}/statuses/{sha}"
    commit_statuses = "/projects/{project}/repository/commits/{sha}/statuses"
    archive = "/projects/{project}/repository/archive{format}"
    tree = "/projects/{project}/repository/tree"
    branches = "/projects/{project_id}/repository/branches"
    branch = "/projects/{project_id}/repository/branches/{branch}"
    user = "/user"
    users = "/users"

    @staticmethod
    def build_api_url(base_url, path) -> str:
        return f"{base_url.rstrip('/')}{API_VERSION}{path}"


AWARD_NAME_BY_REACTION: dict[Reaction, str] = {
    "+1": "thumbsup",
    "-1": "thumbsdown",
    "laugh": "laughing",
    "confused": "confused",
    "heart": "heart",
    "hooray": "tada",
    "rocket": "rocket",
    "eyes": "eyes",
}

REACTION_BY_AWARD_NAME: dict[str, Reaction] = {award: reaction for reaction, award in AWARD_NAME_BY_REACTION.items()}

GITLAB_ARCHIVE_FORMAT_MAP: dict[ArchiveFormat, str] = {
    "tarball": ".tar.gz",
    "zip": ".zip",
}

GITLAB_ENCODING_MAP: dict[Encoding, str] = {
    "utf-8": "text",
    "base64": "base64",
}

PULL_REQUEST_STATE_RETRIEVE_MAP: dict[PullRequestState, list[str]] = {
    "open": ["opened"],
    "closed": ["closed", "merged"],
}

PULL_REQUEST_STATE_UPDATE_MAP: dict[PullRequestState, str] = {
    "open": "reopen",
    "closed": "close",
}

ISSUE_STATE_UPDATE_MAP: dict[IssueState, str] = {
    "open": "reopen",
    "closed": "close",
}

# GitLab description field length limit on the commit status API.
GITLAB_STATUS_DESCRIPTION_MAX_LENGTH = 255

# GitLab commit status states map to (BuildStatus, BuildConclusion).
# In-progress states (manual/scheduled/etc.) collapse to "pending" since the
# generic model has no finer-grained "waiting" representation.
GITLAB_STATUS_READ_MAP: dict[str, tuple[BuildStatus, BuildConclusion | None]] = {
    "created": ("pending", None),
    "pending": ("pending", None),
    "manual": ("pending", None),
    "scheduled": ("pending", None),
    "waiting_for_resource": ("pending", None),
    "preparing": ("pending", None),
    "running": ("running", None),
    "canceling": ("running", None),
    "success": ("completed", "success"),
    "failed": ("completed", "failure"),
    "canceled": ("completed", "cancelled"),
    "skipped": ("completed", "skipped"),
}

# Reverse map for writing: collapses (BuildStatus, BuildConclusion) into one of
# the six writable GitLab states (pending, running, success, failed, canceled,
# skipped). Conclusions without a direct equivalent ("timed_out",
# "action_required", "unknown") fall back to "failed"; "neutral" maps to
# "success" since GitLab has no neutral terminal state.
GITLAB_BUILD_CONCLUSION_WRITE_MAP: dict[BuildConclusion, str] = {
    "success": "success",
    "failure": "failed",
    "cancelled": "canceled",
    "skipped": "skipped",
    "timed_out": "failed",
    "neutral": "success",
    "action_required": "failed",
    "unknown": "failed",
}


class GitLabProvider:
    def __init__(self, client: ApiClient, organization_id: int, repository: Repository) -> None:
        self.client = client
        self.organization_id = organization_id
        self.repository = repository

        # External ID format is "{netloc}:{repo_id}", where netloc might contain a colon before a port number
        if repository["external_id"] is None or ":" not in repository["external_id"]:
            raise SCMCodedError(code="malformed_external_id")

        netloc, self.project_id = repository["external_id"].rsplit(":", maxsplit=1)
        self.web_base_url = f"https://{netloc}"

    def is_rate_limited(self, referrer: Referrer) -> bool:
        return False

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
    ) -> requests.Response:
        headers = {}
        headers.update(extra_headers or {})

        options = request_options or {}

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
            timeout=options.get("timeout"),
        )

    def post(
        self,
        path: str,
        data: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> requests.Response:
        return self.request("POST", path=path, data=data, headers=headers)

    def put(
        self,
        path: str,
        data: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> requests.Response:
        return self.request("PUT", path=path, data=data, headers=headers)

    def delete(self, path: str) -> requests.Response:
        return self.request("DELETE", path=path)

    def get_app_installation(self) -> ActionResult[AppInstallation]:
        response = self.get(GitLab.project.format(project=self.project_id), params={"statistics": "true"})
        return make_result(map_app_installation, response.json())

    def get_repository(self) -> ActionResult[GitRepository]:
        response = self.get(GitLab.project.format(project=self.project_id), params={"statistics": "true"})
        return make_result(map_repository, response.json())

    def get_repository_assignees(
        self,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[Author]]:
        response = self.get(
            GitLab.project_users.format(project_id=self.project_id),
            pagination=pagination,
            request_options=request_options,
        )
        return make_paginated_result(map_author, response, response.json())

    def get_repository_labels(
        self,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[Label]]:
        response = self.get(
            GitLab.project_labels.format(project_id=self.project_id),
            pagination=pagination,
            request_options=request_options,
        )
        return make_paginated_result(map_label, response, response.json())

    def get_repository_topics(
        self,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[list[str]]:
        response = self.get(
            GitLab.project.format(project=self.project_id),
            request_options=request_options,
        )
        raw = response.json()
        return ActionResult(
            data=list(raw.get("topics", [])),
            type="gitlab",
            raw={"data": raw, "headers": None},
            meta={},
        )

    def get_issue_comments(
        self,
        issue_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[Comment]]:
        response = self.get(
            GitLab.issue_notes.format(project_id=self.project_id, issue_id=issue_id),
            pagination=pagination,
            request_options=request_options,
        )
        return make_paginated_result(map_comment, response, response.json())

    def create_issue_comment(self, issue_id: str, body: str) -> ActionResult[Comment]:
        response = self.post(
            GitLab.issue_notes.format(project_id=self.project_id, issue_id=issue_id),
            data={"body": body},
        )
        return make_result(map_comment, response.json())

    def delete_issue_comment(self, issue_id: str, comment_id: str) -> None:
        self.delete(
            GitLab.issue_note.format(project_id=self.project_id, issue_id=issue_id, note_id=comment_id),
        )

    def get_issue(
        self,
        issue_id: str,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[Issue]:
        response = self.get(
            GitLab.issue.format(project=self.project_id, issue=issue_id),
            request_options=request_options,
        )
        return make_result(map_issue, response.json())

    def create_issue(
        self,
        title: str,
        body: str,
        assignees: list[str] | None = None,
        labels: list[str] | None = None,
    ) -> ActionResult[Issue]:
        data: dict[str, Any] = {"title": title, "description": body}
        if assignees is not None:
            data["assignee_ids"] = [int(a) for a in assignees]
        if labels is not None:
            data["labels"] = labels
        response = self.post(
            GitLab.issues.format(project=self.project_id),
            data=data,
        )
        return make_result(map_issue, response.json())

    def update_issue(
        self,
        issue_id: str,
        state: IssueState | None = None,
        assignees: list[str] | None = None,
        labels: list[str] | None = None,
    ) -> ActionResult[Issue]:
        data: dict[str, Any] = {}
        if state is not None:
            data["state_event"] = ISSUE_STATE_UPDATE_MAP[state]
        if assignees is not None:
            data["assignee_ids"] = [int(a) for a in assignees]
        if labels is not None:
            data["labels"] = labels
        response = self.put(
            GitLab.issue.format(project=self.project_id, issue=issue_id),
            data=data,
        )
        return make_result(map_issue, response.json())

    def get_pull_request(
        self,
        pull_request_id: str,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[PullRequest]:
        response = self.get(
            GitLab.merge_request.format(project_id=self.project_id, pr_key=pull_request_id),
            request_options=request_options,
        )
        return make_result(map_pull_request, response.json())

    def get_pull_request_comments(
        self,
        pull_request_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[Comment]]:
        """
        To achieve a behavior consistent with GitHub, we filter out:

        1) GitLab's "system notes"
        They are auto-generated comments for events like "Someone resolved all threads".
        They don't exist on GitHub and have little use outside GitLab's UI.

        2) GitLab's review comments
        They correspond to GitHub's review comments, which are not returned by GitHub's
        "list review comments" endpoint, used to to implement `get_pull_request_comments`.
        """
        response = self.get(
            GitLab.merge_request_notes.format(project_id=self.project_id, pr_key=pull_request_id),
            pagination=pagination,
            request_options=request_options,
        )
        raw = response.json()
        return make_paginated_result(
            map_comment,
            response,
            raw,
            raw_items=(
                note
                for note in raw
                if (
                    not note["system"]  # Filter out system notes
                    and note.get("position") is None  # Filter out review comments
                )
            ),
        )

    def create_pull_request_comment(
        self,
        pull_request_id: str,
        body: str,
        extensions: list[CoPilotChatExtension] | None = None,
    ) -> ActionResult[Comment]:
        response = self.post(
            GitLab.merge_request_notes.format(project_id=self.project_id, pr_key=pull_request_id),
            data={"body": body},
        )
        return make_result(map_comment, response.json())

    def delete_pull_request_comment(self, pull_request_id: str, comment_id: str) -> None:
        self.delete(
            GitLab.merge_request_note.format(project_id=self.project_id, pr_key=pull_request_id, note_id=comment_id),
        )

    def get_issue_comment_reactions(
        self,
        issue_id: str,
        comment_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[ReactionResult]]:
        response = self.get(
            GitLab.issue_note_awards.format(project_id=self.project_id, issue_id=issue_id, note_id=comment_id),
            pagination=pagination,
            request_options=request_options,
        )
        raw = response.json()
        return make_paginated_result(
            map_reaction_result,
            response,
            raw,
            raw_items=(award for award in raw if award["name"] in REACTION_BY_AWARD_NAME),
        )

    def create_issue_comment_reaction(
        self,
        issue_id: str,
        comment_id: str,
        reaction: Reaction,
    ) -> ActionResult[ReactionResult]:
        response = self.post(
            GitLab.issue_note_awards.format(project_id=self.project_id, issue_id=issue_id, note_id=comment_id),
            data={"name": AWARD_NAME_BY_REACTION[reaction]},
        )
        return make_result(map_reaction_result, response.json())

    def delete_issue_comment_reaction(
        self,
        issue_id: str,
        comment_id: str,
        reaction_id: str,
    ) -> None:
        self.delete(
            GitLab.issue_note_award.format(
                project_id=self.project_id, issue_id=issue_id, note_id=comment_id, award_id=reaction_id
            ),
        )

    def get_pull_request_comment_reactions(
        self,
        pull_request_id: str,
        comment_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[ReactionResult]]:
        response = self.get(
            GitLab.merge_request_note_awards.format(
                project_id=self.project_id, pr_key=pull_request_id, note_id=comment_id
            ),
            pagination=pagination,
            request_options=request_options,
        )
        raw = response.json()
        return make_paginated_result(
            map_reaction_result,
            response,
            raw,
            raw_items=(award for award in raw if award["name"] in REACTION_BY_AWARD_NAME),
        )

    def create_pull_request_comment_reaction(
        self,
        pull_request_id: str,
        comment_id: str,
        reaction: Reaction,
    ) -> ActionResult[ReactionResult]:
        response = self.post(
            GitLab.merge_request_note_awards.format(
                project_id=self.project_id, pr_key=pull_request_id, note_id=comment_id
            ),
            data={"name": AWARD_NAME_BY_REACTION[reaction]},
        )
        return make_result(map_reaction_result, response.json())

    def delete_pull_request_comment_reaction(self, pull_request_id: str, comment_id: str, reaction_id: str) -> None:
        self.delete(
            GitLab.merge_request_note_award.format(
                project_id=self.project_id, pr_key=pull_request_id, note_id=comment_id, award_id=reaction_id
            ),
        )

    def get_issue_reactions(
        self,
        issue_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[ReactionResult]]:
        response = self.get(
            GitLab.issue_awards.format(project_id=self.project_id, issue_id=issue_id),
            pagination=pagination,
            request_options=request_options,
        )
        raw = response.json()
        return make_paginated_result(
            map_reaction_result,
            response,
            raw,
            raw_items=(award for award in raw if award["name"] in REACTION_BY_AWARD_NAME),
        )

    def create_issue_reaction(self, issue_id: str, reaction: Reaction) -> ActionResult[ReactionResult]:
        response = self.post(
            GitLab.issue_awards.format(project_id=self.project_id, issue_id=issue_id),
            data={"name": AWARD_NAME_BY_REACTION[reaction]},
        )
        return make_result(map_reaction_result, response.json())

    def delete_issue_reaction(self, issue_id: str, reaction_id: str) -> None:
        self.delete(
            GitLab.issue_award.format(project_id=self.project_id, issue_id=issue_id, award_id=reaction_id),
        )

    def get_pull_request_reactions(
        self,
        pull_request_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[ReactionResult]]:
        response = self.get(
            GitLab.merge_request_awards.format(project_id=self.project_id, pr_key=pull_request_id),
            pagination=pagination,
            request_options=request_options,
        )
        raw = response.json()
        return make_paginated_result(
            map_reaction_result,
            response,
            raw,
            raw_items=(award for award in raw if award["name"] in REACTION_BY_AWARD_NAME),
        )

    def create_pull_request_reaction(self, pull_request_id: str, reaction: Reaction) -> ActionResult[ReactionResult]:
        response = self.post(
            GitLab.merge_request_awards.format(project_id=self.project_id, pr_key=pull_request_id),
            data={"name": AWARD_NAME_BY_REACTION[reaction]},
        )
        return make_result(map_reaction_result, response.json())

    def delete_pull_request_reaction(self, pull_request_id: str, reaction_id: str) -> None:
        self.delete(
            GitLab.merge_request_award.format(project_id=self.project_id, pr_key=pull_request_id, award_id=reaction_id),
        )

    def get_branch(
        self,
        branch: BranchName,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[GitRef]:
        response = self.get(
            GitLab.branch.format(project_id=self.project_id, branch=quote(branch, safe="")),
            request_options=request_options,
        )
        return make_result(map_git_ref, response.json())

    def create_branch(self, branch: BranchName, sha: SHA) -> ActionResult[GitRef]:
        response = self.post(
            GitLab.branches.format(project_id=self.project_id),
            data={"branch": branch, "ref": sha},
        )
        return make_result(map_git_ref, response.json())

    def delete_branch(self, branch: BranchName) -> None:
        self.delete(GitLab.branch.format(project_id=self.project_id, branch=quote(branch, safe="")))

    def get_file_url(
        self,
        file_path: str,
        sha: SHA,
        start_line: int | None = None,
        end_line: int | None = None,
    ) -> str:
        url = f"{self.web_base_url}/{self.repository['name']}/-/blob/{sha}/{file_path}"
        if start_line:
            url += f"#L{start_line}"
        if start_line and end_line:
            url += f"-L{end_line}"
        elif end_line:
            url += f"#L{end_line}"
        return url

    def get_commit_url(self, commit_sha: SHA) -> str:
        return f"{self.web_base_url}/{self.repository['name']}/-/commit/{commit_sha}"

    def get_pull_request_url(self, pull_request_id: str) -> str:
        return f"{self.web_base_url}/{self.repository['name']}/-/merge_requests/{pull_request_id}"

    def get_tree(
        self,
        tree_sha: SHA,
        recursive: bool = True,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[GitTree]:
        """List the repository tree at a given ref.

        GitLab's tree API takes a ref (commit SHA, branch, tag) rather than a
        tree-object SHA.  We treat ``tree_sha`` as a ref so callers can pass a
        commit SHA obtained from ``get_git_commit``.
        """
        params: dict[str, str] = {"ref": tree_sha}
        if recursive:
            params["recursive"] = "true"
        response = self.get(
            GitLab.tree.format(project=self.project_id),
            params=params,
        )
        raw = response.json()
        return ActionResult(
            data=GitTree(
                sha=tree_sha,
                tree=[map_tree_entry(e) for e in raw],
                truncated=False,
            ),
            type="gitlab",
            raw={"data": raw, "headers": None},
            meta={},
        )

    def get_git_commit(
        self,
        sha: SHA,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[GitCommitObject]:
        """Get a commit as a git object.

        GitLab's commit endpoint does not expose the tree-object SHA.  We set
        ``tree.sha`` to the commit SHA so that downstream code can pass it to
        ``get_tree`` (which accepts any ref).
        """
        response = self.get(
            GitLab.commit.format(project=self.project_id, sha=sha),
            request_options=request_options,
        )
        return make_result(map_git_commit_object, response.json())

    def get_file_content(
        self,
        path: str,
        ref: str,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[FileContent]:
        response = self.get(
            GitLab.file.format(project=self.project_id, path=quote(path, safe="")),
            params={"ref": ref},
            request_options=request_options,
        )
        return make_result(map_file_content, response.json())

    def get_readme(
        self,
        ref: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[FileContent]:
        iter_kwargs: dict[str, Any] = {}
        if pagination is not None:
            if "per_page" in pagination:
                iter_kwargs["per_page"] = pagination["per_page"]
            if "cursor" in pagination:
                iter_kwargs["cursor"] = pagination["cursor"]

        for page in iter_all_pages(
            lambda p: self.get_directory_contents("/", ref, p, request_options),
            **iter_kwargs,
        ):
            for entry in page["data"]:
                if entry["type"] == "file" and entry["path"].lower() in VALID_README_FILES:
                    return self.get_file_content(entry["path"], ref=ref, request_options=request_options)
        raise SCMCodedError(code="readme_not_found")

    def get_pull_request_template(
        self,
        ref: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> Iterator[ActionResult[FileContent]]:
        # The official endpoint (GET /projects/:id/templates/merge_requests to list, then GET
        # /projects/:id/templates/merge_requests/:name to fetch each) is cleaner and is GitLab's blessed path, but
        # it has two notable differences from the current behavior:
        #
        # 1. No ref support — the templates API always reads from the default branch, so the ref parameter on the
        # action would become advisory/ignored on GitLab.
        # 2. Response shape — it returns {name, content} only (no path, sha, size), so we'd need to synthesize a
        # FileContent (e.g. set path to .gitlab/merge_request_templates/{name}.md) or accept losing some fields.
        #
        # It's also still N+1 (1 list call + 1 per template), same order as today.
        iter_kwargs: dict[str, Any] = {}
        if pagination is not None:
            if "per_page" in pagination:
                iter_kwargs["per_page"] = pagination["per_page"]
            if "cursor" in pagination:
                iter_kwargs["cursor"] = pagination["cursor"]

        try:
            pages = iter_all_pages(
                lambda p: self.get_directory_contents(PULL_REQUEST_TEMPLATE_DIR, ref, p, request_options),
                **iter_kwargs,
            )
            for page in pages:
                for entry in page["data"]:
                    if entry["type"] == "file" and entry["path"].lower().endswith(".md"):
                        yield self.get_file_content(entry["path"], ref=ref, request_options=request_options)
        except SCMCodedError as e:
            if e.code in ("resource_not_found", "path_is_not_directory"):
                return
            raise

    def get_directory_contents(
        self,
        path: str,
        ref: str | None = None,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[FileContent]]:
        params: dict[str, str] = {"path": path}
        if ref:
            params["ref"] = ref
        try:
            response = self.get(
                GitLab.tree.format(project=self.project_id),
                params=params,
                pagination=pagination,
                request_options=request_options,
            )
        except SCMCodedError as e:
            # GitLab returns 404 "not treeish" when the path resolves to a file.
            if e.code == "resource_not_found" and e.detail and "not treeish" in e.detail:
                raise SCMCodedError(code="path_is_not_directory", detail=path) from e
            raise
        return make_paginated_result(map_tree_entry_to_file_content, response, response.json())

    def get_commit(
        self,
        sha: SHA,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[CommitWithChanges]:
        response = self.get(
            GitLab.commit.format(project=self.project_id, sha=sha),
            request_options=request_options,
        )
        return make_result(map_commit_with_changes, response.json())

    def get_commit_changes(
        self,
        sha: SHA,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[CommitFile]]:
        response = self.get(
            GitLab.diff.format(project=self.project_id, sha=sha),
            pagination=pagination,
            request_options=request_options,
        )
        return make_paginated_result(map_commit_diff, response, response.json())

    def get_commits(
        self,
        ref: str | None = None,
        pagination: PaginationParams | None = None,
        since: datetime.datetime | None = None,
        until: datetime.datetime | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[Commit]]:
        params: dict[str, str] = {"with_stats": "true"}
        if ref:
            params["ref_name"] = ref
        if since:
            params["since"] = since.isoformat()
        if until:
            params["until"] = until.isoformat()
        response = self.get(
            GitLab.commits.format(project=self.project_id),
            params=params,
            pagination=pagination,
            request_options=request_options,
        )
        return make_paginated_result(map_commit, response, response.json())

    def get_commits_by_path(
        self,
        path: str,
        ref: str | None = None,
        pagination: PaginationParams | None = None,
        since: datetime.datetime | None = None,
        until: datetime.datetime | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[Commit]]:
        params: dict[str, str] = {"path": path, "with_stats": "true"}
        if ref:
            params["ref_name"] = ref
        if since:
            params["since"] = since.isoformat()
        if until:
            params["until"] = until.isoformat()
        response = self.get(
            GitLab.commits.format(project=self.project_id),
            params=params,
            pagination=pagination,
            request_options=request_options,
        )
        return make_paginated_result(map_commit, response, response.json())

    def compare_commits(
        self,
        start_sha: SHA,
        end_sha: SHA,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[CommitComparison]:
        response = self.get(
            GitLab.compare.format(project=self.project_id),
            params={"from": start_sha, "to": end_sha},
            pagination=pagination,
            request_options=request_options,
        )
        raw = response.json()
        return make_paginated_result_single(map_commit_comparison, response, raw)

    def create_commit(
        self,
        branch: BranchName,
        parent_sha: SHA,
        message: str,
        actions: list[ChmodCommitAction | DeleteCommitAction | MoveCommitAction | WriteCommitAction],
        force: bool = False,
    ) -> ActionResult[Commit]:
        response = self.post(
            GitLab.commits.format(project=self.project_id),
            data={
                "branch": branch,
                "commit_message": message,
                "start_sha": parent_sha,
                "force": force,
                "actions": [map_commit_action(a) for a in actions],
            },
        )
        return make_result(map_commit, response.json())

    def get_pull_request_files(
        self,
        pull_request_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[PullRequestFile]]:
        response = self.get(
            GitLab.pr_diffs.format(project=self.project_id, pr_key=pull_request_id),
            pagination=pagination,
            request_options=request_options,
        )
        return make_paginated_result(map_pull_request_file, response, response.json())

    def get_pull_request_commits(
        self,
        pull_request_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[PullRequestCommit]]:
        response = self.get(
            GitLab.merge_request_commits.format(project_id=self.project_id, pr_key=pull_request_id),
            pagination=pagination,
            request_options=request_options,
        )
        raw = response.json()
        return make_paginated_result(map_pull_request_commit, response, raw, raw_items=reversed(raw))

    def get_pull_request_diff(
        self,
        pull_request_id: str,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[str]:
        response = self.get(
            GitLab.pr_raw_diffs.format(project=self.project_id, pr_key=pull_request_id),
            request_options=request_options,
        )
        return ActionResult(
            data=response.text,
            type="gitlab",
            raw={"data": response.text, "headers": None},
            meta={},
        )

    def get_pull_requests(
        self,
        state: PullRequestState | None = "open",
        # @todo The 'head' parameter has very ad-hoc behavior on GitHub; we should consider removing it entirely.
        head: BranchName | None = None,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[PullRequest]]:
        raw = []
        gitlab_states: list[str] | list[None]
        if state:
            gitlab_states = PULL_REQUEST_STATE_RETRIEVE_MAP[state]
        else:
            gitlab_states = [None]
        for gitlab_state in gitlab_states:
            params: dict[str, str] = {}
            if gitlab_state:
                params["state"] = gitlab_state
            response = self.get(
                GitLab.merge_requests.format(project_id=self.project_id),
                params=params,
                pagination=pagination,
                request_options=request_options,
            )
            raw.extend(response.json())
        return make_paginated_result(map_pull_request, response, raw)

    def create_pull_request(
        self,
        title: str,
        body: str,
        head: str,
        base: str,
    ) -> ActionResult[PullRequest]:
        data = {
            "title": title,
            "description": body,
            "source_branch": head,
            "target_branch": base,
        }
        response = self.post(
            GitLab.merge_requests.format(project_id=self.project_id),
            data=data,
        )
        return make_result(map_pull_request, response.json())

    def create_pull_request_draft(
        self,
        title: str,
        body: str,
        head: str,
        base: str,
    ) -> ActionResult[PullRequest]:
        # GitLab has no draft flag on the create-MR endpoint. The documented
        # mechanism is a "Draft:" (or "[Draft]" / "(Draft)") title prefix.
        # See https://docs.gitlab.com/user/project/merge_requests/drafts/.
        return self.create_pull_request(
            title=_with_draft_prefix(title),
            body=body,
            head=head,
            base=base,
        )

    def update_pull_request(
        self,
        pull_request_id: str,
        title: str | None = None,
        body: str | None = None,
        state: PullRequestState | None = None,
    ) -> ActionResult[PullRequest]:
        data = {}
        if title is not None:
            data["title"] = title
        if body is not None:
            data["description"] = body
        if state is not None:
            data["state_event"] = PULL_REQUEST_STATE_UPDATE_MAP[state]
        response = self.put(
            GitLab.merge_request.format(project_id=self.project_id, pr_key=pull_request_id),
            data=data,
        )
        return make_result(map_pull_request, response.json())

    def _fetch_mr_versions(self, pull_request_id: str) -> list[dict[str, Any]]:
        return self.get(
            GitLab.merge_request_versions.format(project_id=self.project_id, pr_key=pull_request_id),
        ).json()

    def _post_review_discussion(
        self,
        pull_request_id: str,
        body: str,
        path: str,
        versions: list[dict[str, Any]],
        *,
        position_type: str = "text",
        line: int | None = None,
        side: ReviewSide = "head",
        start_line: int | None = None,
        start_side: ReviewSide = "head",
    ) -> dict[str, Any]:
        position: dict[str, Any] = {
            "position_type": position_type,
            "base_sha": versions[0]["base_commit_sha"],
            "head_sha": versions[0]["head_commit_sha"],
            "start_sha": versions[0]["start_commit_sha"],
            "new_path": path,
            "old_path": path,
        }
        if line is not None:
            position["new_line" if side == "head" else "old_line"] = line
        if start_line is not None:
            line_key = "new_line" if start_side == "head" else "old_line"
            range_type = "new" if start_side == "head" else "old"
            position["line_range"] = {
                "start": {line_key: start_line, "type": range_type},
                "end": {line_key: line, "type": range_type},
            }
        return self.post(
            GitLab.merge_request_discussions.format(project_id=self.project_id, pr_key=pull_request_id),
            data={"body": body, "position": position},
        ).json()

    def create_review_comment_file(
        self,
        pull_request_id: str,
        commit_id: SHA,
        body: str,
        path: str,
        side: ReviewSide,
    ) -> ActionResult[ReviewComment]:
        """
        GitLab's "notes" are similar to GitHub's "comments".
        Additionally, each note belongs to a "discussion".

        On GitLab, one replies to a discussion. On GitHub, one replies to a comment.

        To allow replying to review comments in a consistent way across providers,
        we build a comment ID made of the GitLab's discussion ID and comment ID.
        It can be passed to `create_review_comment_reply`, and uniquely identifies a note.
        """
        raw = self._post_review_discussion(
            pull_request_id,
            body,
            path,
            self._fetch_mr_versions(pull_request_id),
            position_type="file",
        )
        return make_result(
            map_review_comment(raw["id"]),
            raw,
            raw_item=raw["notes"][0],
        )

    def create_review_comment_line(
        self,
        pull_request_id: str,
        commit_id: SHA,
        body: str,
        path: str,
        side: ReviewSide,
        line: int,
    ) -> ActionResult[ReviewComment]:
        """Leave a review comment on a single line of a merge request diff."""
        raw = self._post_review_discussion(
            pull_request_id,
            body,
            path,
            self._fetch_mr_versions(pull_request_id),
            line=line,
            side=side,
        )
        return make_result(
            map_review_comment(raw["id"]),
            raw,
            raw_item=raw["notes"][0],
        )

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
        """Leave a review comment on a span of lines of a merge request diff."""
        raw = self._post_review_discussion(
            pull_request_id,
            body,
            path,
            self._fetch_mr_versions(pull_request_id),
            line=end_line,
            side=side,
            start_line=start_line,
            start_side=start_side,
        )
        return make_result(
            map_review_comment(raw["id"]),
            raw,
            raw_item=raw["notes"][0],
        )

    def create_review_comment_reply(
        self,
        pull_request_id: str,
        body: str,
        comment_id: str,
    ) -> ActionResult[ReviewComment]:
        """
        The comment_id must have the format returned by `create_review_comment_file`.
        The newly created comment's ID will have the same format.
        """
        discussion_id = comment_id.split(":")[0]
        response = self.post(
            GitLab.merge_request_discussion_notes.format(
                project_id=self.project_id, pr_key=pull_request_id, discussion_id=discussion_id
            ),
            data={"body": body},
        )
        raw = response.json()
        return make_result(
            map_review_comment(discussion_id),
            raw,
        )

    def update_review_comment(
        self,
        pull_request_id: str,
        comment_id: str,
        body: str,
    ) -> ActionResult[ReviewComment]:
        discussion_id, note_id = comment_id.split(":")
        response = self.put(
            GitLab.merge_request_discussion_note.format(
                project_id=self.project_id,
                pr_key=pull_request_id,
                discussion_id=discussion_id,
                note_id=note_id,
            ),
            data={"body": body},
        )
        raw = response.json()
        return make_result(
            map_review_comment(discussion_id),
            raw,
        )

    def create_review(
        self,
        pull_request_id: str,
        commit_sha: SHA,
        event: ReviewEvent,
        comments: list[ReviewCommentInput],
        body: str | None = None,
    ) -> ActionResult[Review]:
        """Create a review on a GitLab merge request.

        Unlike GitHub's atomic review API, GitLab has no single endpoint that
        bundles inline comments with an approval action.  This method emulates
        that by creating each inline comment as a separate MR discussion and,
        when *event* is ``"approve"``, calling the MR approve endpoint.  The
        operation is therefore **not atomic**: individual API calls may succeed
        independently.
        """
        versions = self._fetch_mr_versions(pull_request_id)

        def _create_review_comment(comment: ReviewCommentInput) -> str:
            kwargs: dict[str, Any] = {}
            if "line" in comment:
                kwargs["line"] = comment["line"]
                kwargs["side"] = comment.get("side", "head")
            if "start_line" in comment:
                kwargs["start_line"] = comment["start_line"]
                kwargs["start_side"] = comment.get("start_side", "head")
            raw = self._post_review_discussion(pull_request_id, comment["body"], comment["path"], versions, **kwargs)
            return raw["id"]

        def _create_review_body() -> None:
            self.post(
                GitLab.merge_request_notes.format(project_id=self.project_id, pr_key=pull_request_id),
                data={"body": body},
            )

        def _approve_pull_request() -> None:
            self.post(
                GitLab.merge_request_approve.format(project_id=self.project_id, pr_key=pull_request_id),
                data={"sha": commit_sha},
            )

        discussion_ids: list[str | None] = [None] * len(comments)
        with ThreadPoolExecutor() as executor:
            comment_futures = {
                executor.submit(_create_review_comment, comment): idx for idx, comment in enumerate(comments)
            }
            action_futures: list[Future[None]] = []
            if body is not None:
                action_futures.append(executor.submit(_create_review_body))
            if event == "approve":
                action_futures.append(executor.submit(_approve_pull_request))

            for comment_future in as_completed(comment_futures):
                idx = comment_futures[comment_future]
                discussion_ids[idx] = comment_future.result()

            for action_future in action_futures:
                action_future.result()

        return ActionResult(
            data=Review(
                id="unset",
                html_url=f"{self.web_base_url}/{self.repository['name']}/-/merge_requests/{pull_request_id}",
            ),
            type="gitlab",
            raw={"data": {}, "headers": None},
            meta={},
        )

    def download_archive(
        self,
        ref: str,
        archive_format: ArchiveFormat = "tarball",
        request_options: RequestOptions | None = None,
    ) -> requests.Response:
        return self.get(
            GitLab.archive.format(project=self.project_id, format=GITLAB_ARCHIVE_FORMAT_MAP[archive_format]),
            params={"sha": ref},
            request_options=request_options,
        )

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
        """Create a commit status on GitLab, mapped to a check run.

        GitLab's commit status API is simpler than GitHub's checks API:
        ``external_id``, ``started_at``, and ``completed_at`` are not
        accepted by the endpoint and are ignored. ``output.text`` (annotations)
        and Markdown-formatted summaries are likewise unsupported; only the
        title is forwarded as the status ``description`` (truncated to 255
        chars).

        Raises ``invalid_check_run_state_transition`` if GitLab's state machine
        rejects the (state, prior-state) combination — e.g. transitioning to
        ``skipped`` from ``running``.
        """
        data: dict[str, Any] = {
            "name": name,
            "state": _gitlab_state_for(status, conclusion),
        }
        description = _description_from_output(output)
        if description is not None:
            data["description"] = description
        response = self._post_check_run(head_sha, data)
        return make_result(_make_map_check_run(self, head_sha, name), response.json())

    def get_check_run(
        self,
        check_run_id: ResourceId,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[CheckRun]:
        """Get the latest commit status matching the given check run id.

        ``check_run_id`` is the ``"{sha}:{name}"`` value returned by
        ``create_check_run``. GitLab records every state transition as a
        separate row; this returns the most recent row for that ``(sha, name)``
        pair.
        """
        sha, name = _split_check_run_id(check_run_id)
        response = self.get(
            GitLab.commit_statuses.format(project=self.project_id, sha=sha),
            params={"name": name},
            request_options=request_options,
        )
        raw = response.json()
        latest = _latest_status(raw, name)
        if latest is None:
            raise SCMCodedError(code="resource_not_found", detail=f"No commit status named {name!r} on {sha}.")
        return make_result(_make_map_check_run(self, sha, name), raw, raw_item=latest)

    def update_check_run(
        self,
        check_run_id: ResourceId,
        status: BuildStatus | None = None,
        conclusion: BuildConclusion | None = None,
        output: CheckRunOutput | None = None,
    ) -> ActionResult[CheckRun]:
        """Append a new commit status row for the given check run id.

        GitLab's commit status API has no PATCH endpoint; an "update" is
        another POST with the same ``name`` on the same ``sha``, which
        appends a new transition row that becomes the latest state. The
        returned check run id is unchanged.

        Unlike GitHub, GitLab requires every POST to be a *state transition*:
        the API rejects same-state POSTs, so output cannot be updated without
        also changing state. Callers must therefore pass ``status`` or
        ``conclusion``; if both are omitted, ``resource_bad_request`` is
        raised.

        Raises ``invalid_check_run_state_transition`` if GitLab's state machine
        rejects the requested transition (e.g. ``skipped`` from ``running``).
        """
        sha, name = _split_check_run_id(check_run_id)
        if status is None and conclusion is None:
            raise SCMCodedError(
                code="resource_bad_request",
                detail="GitLab does not support output-only updates; pass 'status' or 'conclusion'.",
            )
        data: dict[str, Any] = {"name": name, "state": _gitlab_state_for(status, conclusion)}
        description = _description_from_output(output)
        if description is not None:
            data["description"] = description
        response = self._post_check_run(sha, data)
        return make_result(_make_map_check_run(self, sha, name), response.json())

    def _post_check_run(self, sha: SHA, data: dict[str, Any]) -> requests.Response:
        """POST a commit status, translating GitLab state-machine errors.

        GitLab rejects illegal state transitions (e.g. ``skipped`` from
        ``running``, or any same-state no-op) with a 400 whose body matches
        ``Cannot transition status``. We re-raise these as
        ``invalid_check_run_state_transition`` so callers can branch on a
        typed code instead of substring-matching error text.
        """
        try:
            return self.post(GitLab.statuses.format(project=self.project_id, sha=sha), data=data)
        except SCMCodedError as e:
            if e.detail and "Cannot transition status" in e.detail:
                raise SCMCodedError(code="invalid_check_run_state_transition", detail=e.detail) from e
            raise

    def resolve_review_thread(self, pull_request_id: str, thread_id: str) -> None:
        self.put(
            GitLab.merge_request_discussion.format(
                project_id=self.project_id, pr_key=pull_request_id, discussion_id=thread_id
            ),
            data={"resolved": True},
        )

    def get_pull_request_review_threads(
        self,
        pull_request_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[ReviewThread]]:
        """List merge request review threads (GitLab discussions with a diff position).

        GitLab returns discussions inline — non-positioned discussions are ordinary
        MR comments and are filtered out here. Reactions are not returned by the
        discussions endpoint; per-comment reactions would require a separate
        award_emoji call per note, so they are surfaced as empty lists.
        """
        response = self.get(
            GitLab.merge_request_discussions.format(project_id=self.project_id, pr_key=pull_request_id),
            pagination=pagination,
            request_options=request_options,
        )
        raw = response.json()
        return make_paginated_result(
            map_review_thread,
            response,
            raw,
            raw_items=(d for d in raw if _is_review_thread_discussion(d)),
        )

    def get_thread_id_from_review_comment_unique_id(
        self, pull_request_id: str, review_comment_unique_id: str
    ) -> str | None:
        discussion_id, _, _ = review_comment_unique_id.partition(":")
        return discussion_id or None


def make_paginated_result[T](
    map_item: Callable[[dict[str, Any]], T],
    response: requests.Response,
    raw: Any,
    *,
    raw_items: Iterable[dict[str, Any]] | None = None,
) -> PaginatedActionResult[list[T]]:
    if raw_items is None:
        assert isinstance(raw, list)
        raw_items = raw

    next_cursor = response.headers.get("X-Next-Page")

    return PaginatedActionResult(
        data=[map_item(item) for item in raw_items],
        type="gitlab",
        raw={"data": raw, "headers": None},
        meta=PaginatedResponseMeta(next_cursor=next_cursor),
    )


def make_paginated_result_single[T](
    map_item: Callable[[dict[str, Any]], T],
    response: requests.Response,
    raw: Any,
) -> PaginatedActionResult[T]:
    next_cursor = response.headers.get("X-Next-Page")
    return PaginatedActionResult(
        data=map_item(raw),
        type="gitlab",
        raw={"data": raw, "headers": None},
        meta=PaginatedResponseMeta(next_cursor=next_cursor),
    )


def make_result[T](
    map_item: Callable[[dict[str, Any]], T],
    raw: Any,
    *,
    raw_item: dict[str, Any] | None = None,
) -> ActionResult[T]:
    if raw_item is None:
        assert isinstance(raw, dict)
        raw_item = raw
    return ActionResult(
        data=map_item(raw_item),
        type="gitlab",
        raw={"data": raw, "headers": None},
        meta={},
    )


def map_author(raw: dict[str, Any]) -> Author:
    return Author(
        id=str(raw["id"]),
        username=raw["username"],
    )


def map_comment(raw: dict[str, Any]) -> Comment:
    return Comment(
        id=str(raw["id"]),
        body=raw["body"],
        author=map_author(raw["author"]),
    )


def map_commit_action(
    action: ChmodCommitAction | DeleteCommitAction | MoveCommitAction | WriteCommitAction,
) -> dict[str, Any]:
    if isinstance(action, ChmodCommitAction):
        return {
            "action": "chmod",
            "file_path": action.filename,
            "execute_filemode": action.executable,
        }
    if isinstance(action, DeleteCommitAction):
        return {
            "action": "delete",
            "file_path": action.filename,
        }
    if isinstance(action, MoveCommitAction):
        return {
            "action": "move",
            "file_path": action.new_filename,
            "previous_path": action.old_filename,
        }
    return {
        "action": action.action,
        "file_path": action.filename,
        "content": action.content,
        "encoding": GITLAB_ENCODING_MAP[action.encoding],
    }


def map_commit_comparison(raw: dict[str, Any]) -> CommitComparison:
    return CommitComparison(
        ahead_by=len(raw.get("commits", [])),
        commits=[map_commit(c) for c in raw.get("commits", [])],
        diff=[map_commit_diff(d) for d in raw.get("diffs", [])],
    )


def map_commit(raw: dict[str, Any]) -> Commit:
    stats = raw.get("stats") or {}
    return Commit(
        id=str(raw["id"]),
        message=raw["message"],
        author=CommitAuthor(
            name=raw["author_name"],
            email=raw["author_email"],
            date=datetime.datetime.fromisoformat(raw["created_at"]),
        ),
        additions=stats.get("additions"),
        deletions=stats.get("deletions"),
    )


def map_commit_with_changes(raw: dict[str, Any]) -> CommitWithChanges:
    stats = raw.get("stats") or {}
    return CommitWithChanges(
        id=str(raw["id"]),
        message=raw["message"],
        author=CommitAuthor(
            name=raw["author_name"],
            email=raw["author_email"],
            date=datetime.datetime.fromisoformat(raw["created_at"]),
        ),
        files=None,
        additions=stats.get("additions"),
        deletions=stats.get("deletions"),
    )


def map_file_content(raw: dict[str, Any]) -> FileContent:
    return FileContent(
        path=raw["file_path"],
        sha=raw["blob_id"],
        content=raw["content"],
        encoding=raw["encoding"],
        size=raw["size"],
        type="file",
    )


_GITLAB_TREE_ENTRY_TYPES: dict[str, FileContentType] = {
    "blob": "file",
    "tree": "directory",
    "commit": "submodule",
}


def map_tree_entry_to_file_content(raw: dict[str, Any]) -> FileContent:
    return FileContent(
        path=raw["path"],
        sha=raw["id"],
        content="",
        encoding="",
        size=0,
        type=_GITLAB_TREE_ENTRY_TYPES.get(raw["type"], "file"),
    )


def map_git_ref(raw: dict[str, Any]) -> GitRef:
    return GitRef(ref=raw["name"], sha=raw["commit"]["id"])


# Prefixes GitLab recognizes as marking a merge request as draft.
# Reference: https://docs.gitlab.com/user/project/merge_requests/drafts/
_DRAFT_TITLE_PREFIXES = ("draft:", "[draft]", "(draft)")


def _with_draft_prefix(title: str) -> str:
    if title.lstrip().lower().startswith(_DRAFT_TITLE_PREFIXES):
        return title
    return f"Draft: {title}"


def map_pull_request(raw: dict[str, Any]) -> PullRequest:
    return PullRequest(
        internal_id=str(raw["id"]),
        id=str(raw["iid"]),
        title=raw["title"],
        body=raw["description"] or None,
        state="open" if raw["state"] == "opened" else "closed",
        base=PullRequestBranch(ref=raw["target_branch"], sha=None),
        head=PullRequestBranch(
            ref=raw["source_branch"],
            sha=raw["sha"],
        ),
        merged=raw["merged_at"] is not None,
        html_url=raw["web_url"],
        author=map_author(raw["author"]),
    )


def map_issue(raw: dict[str, Any]) -> Issue:
    return Issue(
        id=str(raw["iid"]),
        title=raw["title"],
        body=raw.get("description") or None,
        state="open" if raw["state"] == "opened" else "closed",
        html_url=raw["web_url"],
    )


def map_pull_request_commit(raw: dict[str, Any]) -> PullRequestCommit:
    return PullRequestCommit(
        sha=raw["id"],
        message=raw["message"],
        author=CommitAuthor(
            name=raw["author_name"],
            email=raw["author_email"],
            date=datetime.datetime.fromisoformat(raw["authored_date"]),
        ),
    )


def map_commit_diff(raw: dict[str, Any]) -> CommitFile:
    status: FileStatus
    if raw.get("new_file"):
        status = "added"
    elif raw.get("deleted_file"):
        status = "removed"
    elif raw.get("renamed_file"):
        status = "renamed"
    else:
        status = "modified"
    old_path = raw.get("old_path")
    new_path = raw.get("new_path") or old_path or ""
    return CommitFile(
        filename=new_path,
        status=status,
        patch=raw.get("diff"),
        additions=None,
        deletions=None,
        previous_filename=old_path if raw.get("renamed_file") else None,
    )


def map_pull_request_file(raw: dict[str, Any]) -> PullRequestFile:
    return PullRequestFile(
        filename=raw["new_path"],
        previous_filename=(raw["old_path"] if raw["old_path"] != raw["new_path"] else None),
        status=("added" if raw["new_file"] else "removed" if raw["deleted_file"] else "modified"),
        changes=0,
        patch=raw.get("diff"),
        sha="",
    )


def map_app_installation(raw: dict[str, Any]) -> AppInstallation:
    permissions = raw.get("permissions", {})
    project_access_level = (permissions.get("project_access") or {}).get("access_level", 0)
    group_access_level = (permissions.get("group_access") or {}).get("access_level", 0)
    access_level = max(project_access_level, group_access_level)
    # Numerical levels are described e.g. in https://docs.gitlab.com/api/access_requests/#approve-an-access-request
    # Roles associated to levels are described in https://docs.gitlab.com/user/permissions/#default-roles
    return AppInstallation(
        has_read_access=access_level >= 15,  # Planner
        has_write_access=access_level >= 30,  # Developer
    )


def map_repository(raw: dict[str, Any]) -> GitRepository:
    statistics = raw.get("statistics")
    repo_size = statistics.get("repository_size", 0) if statistics else 0
    return GitRepository(
        full_name=raw["path_with_namespace"],
        default_branch=raw["default_branch"],
        clone_url=raw["http_url_to_repo"],
        private=raw["visibility"] != "public",
        # GitLab returns size in bytes. We convert to kB to match GitHub
        size=repo_size // 1000,
        description=raw.get("description"),
        topics=list(raw.get("topics", [])),
    )


def map_label(raw: dict[str, Any]) -> Label:
    color = raw["color"]
    assert isinstance(color, str)
    return Label(
        id=str(raw["id"]),
        name=raw["name"],
        color=color.removeprefix("#"),
        description=raw.get("description"),
    )


def map_reaction_result(raw: dict[str, Any]) -> ReactionResult:
    return ReactionResult(
        id=str(raw["id"]),
        content=REACTION_BY_AWARD_NAME[raw["name"]],
        author=map_author(raw["user"]),
    )


def map_git_commit_object(raw: dict[str, Any]) -> GitCommitObject:
    return GitCommitObject(
        sha=raw["id"],
        # GitLab's commit API does not return a tree-object SHA.  We use the
        # commit SHA so callers can pass it to get_tree (which accepts any ref).
        tree=GitCommitTree(sha=raw["id"]),
        message=raw["message"],
    )


def map_tree_entry(raw: dict[str, Any]) -> TreeEntry:
    return TreeEntry(
        path=raw["path"],
        mode=raw["mode"],
        type=raw["type"],
        sha=raw["id"],
        size=None,
    )


def _split_check_run_id(check_run_id: ResourceId) -> tuple[str, str]:
    """Parse a ``"{sha}:{name}"`` check run id. ``name`` may contain colons."""
    sha, sep, name = check_run_id.partition(":")
    if not sep or not sha or not name:
        raise SCMCodedError(code="resource_bad_request", detail=f"Expected '<sha>:<name>', got {check_run_id!r}.")
    return sha, name


def _gitlab_state_for(status: BuildStatus | None, conclusion: BuildConclusion | None) -> str:
    if conclusion is not None:
        return GITLAB_BUILD_CONCLUSION_WRITE_MAP[conclusion]
    if status == "completed":
        raise SCMCodedError(
            code="resource_bad_request",
            detail="A 'conclusion' is required when 'status' is 'completed'.",
        )
    if status == "running":
        return "running"
    return "pending"


def _description_from_output(output: CheckRunOutput | None) -> str | None:
    """Reduce a check run output to a single description string.

    GitLab commit statuses have no equivalent of GitHub's rich Markdown summary
    or annotations; only the title is forwarded, truncated to GitLab's 255-char
    description limit.
    """
    if output is None:
        return None
    title = output.get("title")
    if not title:
        return None
    return title[:GITLAB_STATUS_DESCRIPTION_MAX_LENGTH]


def _latest_status(raw: list[dict[str, Any]], name: str) -> dict[str, Any] | None:
    """Pick the most recent commit status row for ``name``.

    GitLab's API returns rows in reverse-chronological order (latest first), but
    we filter and pick the first match defensively in case ordering changes or
    the caller supplied unfiltered data.
    """
    for entry in raw:
        if entry.get("name") == name:
            return entry
    return None


def _make_map_check_run(provider: "GitLabProvider", sha: SHA, name: str) -> Callable[[dict[str, Any]], CheckRun]:
    def _map(raw: dict[str, Any]) -> CheckRun:
        gitlab_state = raw.get("status", "")
        status, conclusion = GITLAB_STATUS_READ_MAP.get(gitlab_state, ("pending", None))
        target_url = raw.get("target_url")
        return CheckRun(
            id=f"{sha}:{name}",
            name=raw.get("name") or name,
            status=status,
            conclusion=conclusion,
            html_url=target_url or provider.get_commit_url(sha),
        )

    return _map


def _is_review_thread_discussion(discussion: dict[str, Any]) -> bool:
    """A GitLab discussion is a review thread iff its first note has a ``position`` —
    those are inline diff notes. Plain MR comments come back with ``position`` unset."""
    notes = discussion.get("notes") or []
    return bool(notes) and notes[0].get("position") is not None


def map_review_thread_comment(raw: dict[str, Any]) -> ReviewThreadComment:
    author_raw = raw.get("author") or {}
    if author_raw:
        author: Author | None = Author(id=str(author_raw["id"]), username=author_raw["username"])
    else:
        author = None
    return ReviewThreadComment(
        id=str(raw["id"]),
        unique_id=str(raw["id"]),
        body=raw.get("body", ""),
        author=author,
        # GitLab marks system actors via ``bot`` on the user object.
        is_bot=bool(author_raw.get("bot")),
        created_at=raw.get("created_at"),
        updated_at=raw.get("updated_at"),
    )


def map_review_thread(raw: dict[str, Any]) -> ReviewThread:
    notes = raw.get("notes") or []
    head_note = notes[0] if notes else {}
    position = head_note.get("position") or {}
    line_range = position.get("line_range") or {}
    end_pos = line_range.get("end") or {}
    start_pos = line_range.get("start") or {}

    def _line(p: dict[str, Any]) -> int | None:
        return p.get("new_line") if p.get("new_line") is not None else p.get("old_line")

    line = _line(end_pos) if end_pos else (position.get("new_line") or position.get("old_line"))
    start_line = _line(start_pos) if start_pos else None

    return ReviewThread(
        id=str(raw["id"]),
        is_resolved=bool(head_note.get("resolved", False)),
        # GitLab does not expose an "outdated" flag on discussions; an outdated
        # discussion can be inferred from position.line_range vs the latest diff
        # but the API surfaces no boolean. Report False conservatively.
        is_outdated=False,
        file_path=position.get("new_path") or position.get("old_path"),
        line=line,
        start_line=start_line,
        comments=[map_review_thread_comment(n) for n in notes],
    )


def map_review_comment(discussion_id: str) -> Callable[[dict[str, Any]], ReviewComment]:
    def _map_review_comment(raw: dict[str, Any]) -> ReviewComment:
        author_raw = raw.get("author")
        return ReviewComment(
            id=f"{discussion_id}:{raw['id']}",
            unique_id=f"{discussion_id}:{raw['id']}",
            url=None,
            file_path=raw.get("position", {}).get("new_path"),
            body=raw["body"],
            author=Author(id=str(author_raw["id"]), username=author_raw["username"]) if author_raw else None,
            created_at=raw.get("created_at"),
            diff_hunk=None,
            review_id=None,
            author_association=None,
            commit_sha=None,
            head=None,
            thread_id=discussion_id,
        )

    return _map_review_comment
