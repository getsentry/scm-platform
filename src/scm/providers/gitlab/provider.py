import datetime
from collections.abc import Callable, Iterable
from typing import Any

import requests

from scm.errors import SCMCodedError
from scm.types import (
    SHA,
    ActionResult,
    ApiClient,
    ArchiveFormat,
    Author,
    BranchName,
    Comment,
    Commit,
    CommitAuthor,
    FileContent,
    GitCommitObject,
    GitCommitTree,
    GitRef,
    GitRepository,
    GitTree,
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
    ReviewComment,
    ReviewSide,
    TreeEntry,
)

API_VERSION = "/api/v4"


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
    pr_diffs = "/projects/{project}/merge_requests/{pr_key}/diffs"
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

PULL_REQUEST_STATE_RETRIEVE_MAP: dict[PullRequestState, list[str]] = {
    "open": ["opened"],
    "closed": ["closed", "merged"],
}

PULL_REQUEST_STATE_UPDATE_MAP: dict[PullRequestState, str] = {
    "open": "reopen",
    "closed": "close",
}


class GitLabProvider:
    def __init__(self, client: ApiClient, organization_id: int, repository: Repository) -> None:
        self.client = client
        self.organization_id = organization_id
        self.repository = repository

        # External ID format is "{netloc}:{repo_id}", where netloc might contain a colon before a port number
        if repository["external_id"] is None or ":" not in repository["external_id"]:
            raise SCMCodedError(code="malformed_external_id")

        self.project_id = repository["external_id"].rsplit(":", maxsplit=1)[1]

    def is_rate_limited(self, referrer: Referrer) -> bool:
        return False

    def _request(
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
        try:
            response = self.client._request(
                method=method,
                path=path,
                headers=headers,
                data=data,
                params=params,
                raw_response=raw_response,
                allow_redirects=allow_redirects,
                stream=stream,
            )
            if response.status_code == 403:
                raise SCMCodedError(code="resource_forbidden")
            elif response.status_code == 404:
                raise SCMCodedError(code="resource_not_found")

            try:
                response.raise_for_status()
            except Exception as e:
                raise SCMCodedError(code="unhandled_exception") from e

            return response
        except Exception as e:
            raise SCMCodedError(code="unhandled_exception") from e

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

        params = params or {}
        if pagination:
            params["per_page"] = str(pagination["per_page"])
            params["page"] = str(pagination["cursor"])

        return self._request(
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
        return self._request("POST", path=path, data=data, headers=headers)

    def patch(
        self,
        path: str,
        data: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> requests.Response:
        return self._request("PATCH", path=path, data=data, headers=headers)

    def delete(self, path: str) -> requests.Response:
        return self._request("DELETE", path=path)

    def get_repository(self) -> ActionResult[GitRepository]:
        response = self.get(GitLab.project.format(project=self.project_id), params={"statistics": "true"})
        return make_result(map_repository, response.json())

    def get_issue_comments(
        self,
        issue_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[Comment]:
        response = self.get(
            GitLab.issue_notes.format(project_id=self.project_id, issue_id=issue_id),
            pagination=pagination,
            request_options=request_options,
        )
        return make_paginated_result(map_comment, response.json())

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
    ) -> PaginatedActionResult[Comment]:
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

    def create_pull_request_comment(self, pull_request_id: str, body: str) -> ActionResult[Comment]:
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
    ) -> PaginatedActionResult[ReactionResult]:
        response = self.get(
            GitLab.issue_note_awards.format(project_id=self.project_id, issue_id=issue_id, note_id=comment_id),
            pagination=pagination,
            request_options=request_options,
        )
        raw = response.json()
        return make_paginated_result(
            map_reaction_result,
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
    ) -> PaginatedActionResult[ReactionResult]:
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
    ) -> PaginatedActionResult[ReactionResult]:
        response = self.get(
            GitLab.issue_awards.format(project_id=self.project_id, issue_id=issue_id),
            pagination=pagination,
            request_options=request_options,
        )
        raw = response.json()
        return make_paginated_result(
            map_reaction_result,
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
    ) -> PaginatedActionResult[ReactionResult]:
        response = self.get(
            GitLab.merge_request_awards.format(project_id=self.project_id, pr_key=pull_request_id),
            pagination=pagination,
            request_options=request_options,
        )
        raw = response.json()
        return make_paginated_result(
            map_reaction_result,
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
            GitLab.branch.format(project_id=self.project_id, branch=branch),
            request_options=request_options,
        )
        return make_result(map_git_ref, response.json())

    def create_branch(self, branch: BranchName, sha: SHA) -> ActionResult[GitRef]:
        response = self.post(
            GitLab.branches.format(project_id=self.project_id),
            data={"branch": branch, "ref": sha},
        )
        return make_result(map_git_ref, response.json())

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
        ref: str | None = None,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[FileContent]:
        params: dict[str, str] = {}
        if ref:
            params["ref"] = ref
        response = self.get(
            GitLab.file.format(project=self.project_id, path=path),
            params=params,
            request_options=request_options,
        )
        return make_result(map_file_content, response.json())

    def get_commit(
        self,
        sha: SHA,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[Commit]:
        response = self.get(
            GitLab.commit.format(project=self.project_id, sha=sha),
            request_options=request_options,
        )
        return make_result(map_commit, response.json())

    def get_commits(
        self,
        ref: str | None = None,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[Commit]:
        params: dict[str, str] = {}
        if ref:
            params["ref_name"] = ref
        response = self.get(
            GitLab.commits.format(project=self.project_id),
            params=params,
            pagination=pagination,
            request_options=request_options,
        )
        return make_paginated_result(map_commit, response.json())

    def get_commits_by_path(
        self,
        path: str,
        ref: str | None = None,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[Commit]:
        params: dict[str, str] = {"path": path}
        if ref:
            params["ref_name"] = ref
        response = self.get(
            GitLab.commits.format(project=self.project_id),
            params=params,
            pagination=pagination,
            request_options=request_options,
        )
        return make_paginated_result(map_commit, response.json())

    def compare_commits(
        self,
        start_sha: SHA,
        end_sha: SHA,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[Commit]:
        response = self.get(
            GitLab.compare.format(project=self.project_id),
            params={"from": start_sha, "to": end_sha},
            pagination=pagination,
            request_options=request_options,
        )
        raw = response.json()
        return make_paginated_result(map_commit, raw, raw_items=raw["commits"])

    def get_pull_request_files(
        self,
        pull_request_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[PullRequestFile]:
        response = self.get(
            GitLab.pr_diffs.format(project=self.project_id, pr_key=pull_request_id),
            pagination=pagination,
            request_options=request_options,
        )
        return make_paginated_result(map_pull_request_file, response.json())

    def get_pull_request_commits(
        self,
        pull_request_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[PullRequestCommit]:
        response = self.get(
            GitLab.merge_request_commits.format(project_id=self.project_id, pr_key=pull_request_id),
            pagination=pagination,
            request_options=request_options,
        )
        raw = response.json()
        return make_paginated_result(map_pull_request_commit, raw, raw_items=reversed(raw))

    def get_pull_requests(
        self,
        state: PullRequestState | None = "open",
        # @todo The 'head' parameter has very ad-hoc behavior on GitHub; we should consider removing it entirely.
        head: BranchName | None = None,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[PullRequest]:
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
        return make_paginated_result(map_pull_request, raw)

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
        response = self.patch(
            GitLab.merge_request.format(project_id=self.project_id, pr_key=pull_request_id),
            data=data,
        )
        return make_result(map_pull_request, response.json())

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
        versions_response = self.get(
            GitLab.merge_request_versions.format(project_id=self.project_id, pr_key=pull_request_id),
        )
        versions = versions_response.json()

        response = self.post(
            GitLab.merge_request_discussions.format(project_id=self.project_id, pr_key=pull_request_id),
            data={
                "body": body,
                "position": {
                    "position_type": "file",
                    "base_sha": versions[0]["base_commit_sha"],
                    "head_sha": versions[0]["head_commit_sha"],
                    "start_sha": versions[0]["start_commit_sha"],
                    "new_path": path,
                    "old_path": path,
                },
            },
        )
        raw = response.json()
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


def make_paginated_result[T](
    map_item: Callable[[dict[str, Any]], T],
    raw: Any,
    *,
    raw_items: Iterable[dict[str, Any]] | None = None,
) -> PaginatedActionResult[T]:
    if raw_items is None:
        assert isinstance(raw, list)
        raw_items = raw
    return PaginatedActionResult(
        data=[map_item(item) for item in raw_items],
        type="gitlab",
        raw={"data": raw, "headers": None},
        # No actual pagination for now
        meta=PaginatedResponseMeta(next_cursor=None),
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


def map_commit(raw: dict[str, Any]) -> Commit:
    return Commit(
        id=str(raw["id"]),
        message=raw["message"],
        author=CommitAuthor(
            name=raw["author_name"],
            email=raw["author_email"],
            date=datetime.datetime.fromisoformat(raw["created_at"]),
        ),
        files=None,
    )


def map_file_content(raw: dict[str, Any]) -> FileContent:
    return FileContent(
        path=raw["file_path"],
        sha=raw["blob_id"],
        content=raw["content"],
        encoding=raw["encoding"],
        size=raw["size"],
    )


def map_git_ref(raw: dict[str, Any]) -> GitRef:
    return GitRef(ref=raw["name"], sha=raw["commit"]["id"])


def map_pull_request(raw: dict[str, Any]) -> PullRequest:
    return PullRequest(
        id=str(raw["id"]),
        number=str(raw["iid"]),
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


def map_pull_request_file(raw: dict[str, Any]) -> PullRequestFile:
    return PullRequestFile(
        filename=raw["new_path"],
        previous_filename=(raw["old_path"] if raw["old_path"] != raw["new_path"] else None),
        status=("added" if raw["new_file"] else "removed" if raw["deleted_file"] else "modified"),
        changes=0,
        patch=raw.get("diff"),
        sha="",
    )


def map_repository(raw: dict[str, Any]) -> GitRepository:
    statistics = raw.get("statistics")
    repo_size = statistics.get("repository_size", 0) if statistics else 0
    return GitRepository(
        full_name=raw["path_with_namespace"],
        default_branch=raw.get("default_branch"),
        clone_url=raw["http_url_to_repo"],
        private=raw["visibility"] != "public",
        # GitLab returns size in bytes. We convert to kB to match GitHub
        size=repo_size // 1000,
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


def map_review_comment(discussion_id: str) -> Callable[[dict[str, Any]], ReviewComment]:
    def _map_review_comment(raw: dict[str, Any]) -> ReviewComment:
        author_raw = raw.get("author")
        return ReviewComment(
            id=f"{discussion_id}:{raw['id']}",
            unique_id=f"{discussion_id}:{raw['id']}",
            url=None,
            file_path=raw["position"]["new_path"],
            body=raw["body"],
            author=Author(id=str(author_raw["id"]), username=author_raw["username"]) if author_raw else None,
            created_at=raw.get("created_at"),
            diff_hunk=None,
            review_id=None,
            author_association=None,
            commit_sha=None,
            head=None,
        )

    return _map_review_comment
