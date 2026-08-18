"""Gitea provider.

Gitea's REST API is modeled closely on GitHub's -- repositories are addressed as
``{owner}/{name}`` path segments and the routes are near-identical
(``/repos/{repo}/contents/{path}``, ``/repos/{repo}/compare/{basehead}``,
``/repos/{repo}/pulls/{index}``). So the request/pagination scaffolding here
follows :mod:`scm.providers.github.provider` rather than GitLab's.

Three subsystems break that pattern and follow GitLab's playbook instead,
because Gitea genuinely lacks GitHub's machinery:

- **Check runs.** Gitea has commit *statuses*, not a Checks API, so a check run
  is synthesized from a status the way GitLab's provider does it, with a
  composite ``{sha}:{context}`` id.
- **Draft pull requests.** There is no ``draft`` field on Gitea's pull-request
  *edit* payload (only on the read model), so draft state is written with the
  ``WIP:`` title prefix -- the same trick GitLab's ``Draft:`` prefix needs.
- **No GraphQL.** Gitea exposes no GraphQL endpoint at all, so every capability
  GitHub implements through it (review threads, comment minimization, thread
  resolution) is absent here.

Capabilities are opted into structurally: a protocol this class does not
implement is a capability the facade reports as unavailable, and callers guard
with ``isinstance`` before using it. Three protocols are deliberately omitted
because Gitea has no endpoint behind them -- see ``UNSUPPORTED_CAPABILITIES``.
"""

from collections.abc import Callable
from datetime import datetime
from email.utils import format_datetime, parsedate_to_datetime
from typing import Any, Literal
from urllib.parse import quote

import requests

from scm.errors import (
    MalformedExternalId,
    ResourceNotFound,
    error_class_for_status,
)
from scm.types import (
    ActionResult,
    ApiClient,
    Author,
    BranchName,
    BuildConclusion,
    BuildStatus,
    CheckRun,
    Comment,
    Commit,
    CommitAuthor,
    CredentialsSet,
    FileStatus,
    GitRef,
    GitRepository,
    Issue,
    IssueState,
    PaginatedActionResult,
    PaginatedResponseMeta,
    PaginationParams,
    PullRequest,
    PullRequestBranch,
    PullRequestFile,
    PullRequestReviewState,
    Reaction,
    ReactionResult,
    Referrer,
    Repository,
    RepositoryPermission,
    RequestOptions,
    ResourceId,
    ResponseMeta,
    Review,
    ReviewComment,
    UserPermissions,
)

API_VERSION = "/api/v1"

# Gitea caps every list response at `[api] MAX_RESPONSE_ITEMS`, which defaults
# to 50, and silently clamps a larger `limit` rather than erroring. Asking for
# more than the cap makes a pagination loop read the clamped page as a short
# final page and stop early, so requests are pinned at the cap instead. This
# mirrors the monolith's `GiteaApiClient.page_size`.
GITEA_MAX_PAGE_SIZE = 50

# Gitea's default work-in-progress prefixes are configurable per instance
# (`[repository.pull-request] WORK_IN_PROGRESS_PREFIXES`), but every stock
# install ships these two. The first is what we write; both are recognized when
# reading so a PR marked draft through the web UI is understood.
WIP_PREFIXES = ("WIP:", "[WIP]")

# Gitea's commit-status states, mapped onto the generic build model. Gitea has
# no "running" state -- a status is pending until it reaches a terminal state.
# "warning" has no generic equivalent and lands on "neutral", matching how the
# GitHub provider treats a conclusion it cannot classify.
GITEA_STATUS_READ_MAP: dict[str, tuple[BuildStatus, BuildConclusion | None]] = {
    "pending": ("pending", None),
    "success": ("completed", "success"),
    "error": ("completed", "failure"),
    "failure": ("completed", "failure"),
    "warning": ("completed", "neutral"),
    "skipped": ("completed", "skipped"),
}

# Gitea's review states are upper-case; normalize to the generic literals.
# REQUEST_REVIEW is a *solicited* review that has not been written yet, which is
# the same thing the generic model calls "pending".
GITEA_REVIEW_STATE_MAP: dict[str, PullRequestReviewState] = {
    "APPROVED": "approved",
    "REQUEST_CHANGES": "changes_requested",
    "COMMENT": "commented",
    "PENDING": "pending",
    "REQUEST_REVIEW": "pending",
}

# Gitea's collaborator permission levels. "owner" is strictly above "admin" but
# the generic model tops out at "admin", so both collapse there.
GITEA_PERMISSION_MAP: dict[str, RepositoryPermission] = {
    "owner": "admin",
    "admin": "admin",
    "write": "write",
    "read": "read",
    "none": "none",
}

GITEA_FILE_STATUS_MAP: dict[str, FileStatus] = {
    "added": "added",
    "removed": "removed",
    "deleted": "removed",
    "modified": "modified",
    "changed": "changed",
    "renamed": "renamed",
    "copied": "copied",
    "unchanged": "unchanged",
}

# Protocols intentionally left unimplemented because Gitea exposes no endpoint
# for them. Gitea has reactions on issues/pull requests
# (`/issues/{index}/reactions`) and on issue comments
# (`/issues/comments/{id}/reactions`) but none on *review* comments -- GitHub's
# `/pulls/comments/{id}/reactions` has no Gitea counterpart. Callers guard with
# `isinstance` and skip the behavior, which is the intended degradation.
UNSUPPORTED_CAPABILITIES = (
    "GetReviewCommentReactionsProtocol",
    "CreateReviewCommentReactionProtocol",
    "DeleteReviewCommentReactionProtocol",
)


def _extract_response_meta(response: requests.Response) -> ResponseMeta:
    meta: ResponseMeta = {}
    if etag := response.headers.get("ETag"):
        meta["etag"] = etag
    if last_modified := response.headers.get("Last-Modified"):
        meta["last_modified"] = parsedate_to_datetime(last_modified)
    return meta


def _strip_wip_prefix(title: str) -> str:
    """Return ``title`` without a leading work-in-progress marker."""
    for prefix in WIP_PREFIXES:
        if title.upper().startswith(prefix.upper()):
            return title[len(prefix) :].lstrip()
    return title


def _has_wip_prefix(title: str) -> bool:
    return any(title.upper().startswith(prefix.upper()) for prefix in WIP_PREFIXES)


class GiteaProvider:
    def __init__(
        self,
        client: ApiClient,
        organization_id: int,
        repository: Repository,
        web_base_url: str,
    ) -> None:
        self.client = client
        self.organization_id = organization_id
        self.repository = repository

        # Gitea's ROOT_URL is free-form and may include a sub-path
        # (`https://example.com/gitea/`), so the full base URL is carried on the
        # repository rather than being rebuilt from a hostname the way the
        # GitLab provider rebuilds `https://{netloc}`.
        if not web_base_url:
            raise MalformedExternalId(detail="web_base_url is required for gitea repositories")
        self.web_base_url = web_base_url.rstrip("/")

        # Gitea addresses repositories by `{owner}/{name}` path segments, so the
        # repository name has to be that pair -- a bare name would silently
        # build a URL one segment short and hit an unrelated route.
        name = repository["name"]
        if name.count("/") != 1 or not all(part and part.strip() for part in name.split("/")):
            raise MalformedExternalId(detail=f"gitea repository name must be 'owner/name', got {name!r}")
        self.repo_path = name

    def is_rate_limited(self, referrer: Referrer) -> bool:
        # Stock Gitea has no rate limiting. Hosted instances sit behind proxies
        # that emit IETF `RateLimit-*` headers, but those are advisory and
        # absent on most installs, so nothing is pre-emptively blocked here.
        return False

    def build_url(self, path: str) -> str:
        return f"{self.web_base_url}{API_VERSION}{path}"

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
            error_cls = error_class_for_status(response.status_code)
            raise error_cls(
                detail=response.content.decode("utf-8"),
                status_code=response.status_code,
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
        headers: dict[str, str] = {}
        options = request_options or {}

        if_none_match = options.get("if_none_match")
        if if_none_match is not None:
            headers["If-None-Match"] = if_none_match

        if_modified_since = options.get("if_modified_since")
        if if_modified_since is not None:
            headers["If-Modified-Since"] = format_datetime(if_modified_since)

        if extra_headers:
            headers.update(extra_headers)

        params = dict(params or {})
        if pagination:
            # Gitea names the page-size parameter `limit`, not `per_page`, and
            # clamps it at GITEA_MAX_PAGE_SIZE regardless of what is asked for.
            params["limit"] = str(min(pagination["per_page"], GITEA_MAX_PAGE_SIZE))
            params["page"] = str(pagination["cursor"])

        return self.request(
            "GET",
            path=path,
            params=params,
            headers=headers,
            allow_redirects=allow_redirects,
            timeout=options.get("timeout"),
        )

    def post(self, path: str, data: dict[str, Any], headers: dict[str, str] | None = None) -> requests.Response:
        return self.request("POST", path=path, data=data, headers=headers)

    def patch(self, path: str, data: dict[str, Any], headers: dict[str, str] | None = None) -> requests.Response:
        return self.request("PATCH", path=path, data=data, headers=headers)

    def delete(self, path: str, data: dict[str, Any] | None = None) -> requests.Response:
        return self.request("DELETE", path=path, data=data)

    # Repository

    def get_repository(self) -> ActionResult[GitRepository]:
        response = self.get(f"/repos/{self.repo_path}")
        return map_action(response, map_repository)

    def get_authenticated_actor(self) -> ActionResult[Author]:
        # Gitea has no app identity: every API action attributes to the
        # authorizing user, so the authenticated actor is simply that user.
        response = self.get("/user")
        return map_action(response, map_author)

    def get_repository_user_permission(
        self,
        username: str,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[UserPermissions]:
        response = self.get(
            f"/repos/{self.repo_path}/collaborators/{quote(username, safe='')}/permission",
            request_options=request_options,
        )
        return map_action(response, map_collaborator_permission)

    # Branches

    def get_branch(
        self,
        branch: BranchName,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[GitRef]:
        response = self.get(
            f"/repos/{self.repo_path}/branches/{quote(branch, safe='')}",
            request_options=request_options,
        )
        return map_action(response, map_branch)

    # Commits

    def get_commits_by_path(
        self,
        path: str,
        ref: str | None = None,
        pagination: PaginationParams | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[Commit]]:
        params: dict[str, Any] = {"path": path}
        if ref is not None:
            params["sha"] = ref
        if since is not None:
            params["since"] = since.isoformat()
        if until is not None:
            params["until"] = until.isoformat()

        response = self.get(
            f"/repos/{self.repo_path}/commits",
            params=params,
            pagination=pagination,
            request_options=request_options,
        )
        return map_paginated_action(pagination, response, lambda raw: [map_commit(c) for c in raw])

    # Pull requests

    def get_pull_request(
        self,
        pull_request_id: str,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[PullRequest]:
        response = self.get(
            f"/repos/{self.repo_path}/pulls/{pull_request_id}",
            request_options=request_options,
        )
        return map_action(response, map_pull_request)

    def get_pull_request_files(
        self,
        pull_request_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[PullRequestFile]]:
        response = self.get(
            f"/repos/{self.repo_path}/pulls/{pull_request_id}/files",
            pagination=pagination,
            request_options=request_options,
        )
        return map_paginated_action(pagination, response, lambda raw: [map_pull_request_file(f) for f in raw])

    def request_review(self, pull_request_id: str, reviewers: list[str]) -> None:
        self.post(
            f"/repos/{self.repo_path}/pulls/{pull_request_id}/requested_reviewers",
            data={"reviewers": reviewers},
        )

    def mark_pull_request_ready_for_review(self, pull_request_id: str) -> None:
        self._set_pull_request_draft(pull_request_id, draft=False)

    def mark_pull_request_as_draft(self, pull_request_id: str) -> None:
        self._set_pull_request_draft(pull_request_id, draft=True)

    def _set_pull_request_draft(self, pull_request_id: str, *, draft: bool) -> None:
        """Toggle draft state through the title prefix.

        Gitea's pull-request *read* model carries a real ``draft`` boolean, but
        its edit payload has no such field -- draft state is derived from the
        title. So this reads the current title, rewrites the prefix, and PATCHes
        it back. The read is what makes the operation idempotent: without it a
        repeated call would stack prefixes.
        """
        current = self.get(f"/repos/{self.repo_path}/pulls/{pull_request_id}").json()
        title = current.get("title") or ""

        if draft == _has_wip_prefix(title):
            return

        new_title = f"{WIP_PREFIXES[0]} {title}" if draft else _strip_wip_prefix(title)
        self.patch(f"/repos/{self.repo_path}/pulls/{pull_request_id}", data={"title": new_title})

    # Pull request comments
    #
    # Gitea models pull requests as issues, so a PR's conversation comments live
    # on the issue comment routes -- the same `{index}` addresses both.

    def create_pull_request_comment(
        self,
        pull_request_id: str,
        body: str,
        extensions: list[Any] | None = None,
    ) -> ActionResult[Comment]:
        # `extensions` carries GitHub Copilot chat affordances, which have no
        # Gitea counterpart; the comment body is posted unchanged.
        response = self.post(
            f"/repos/{self.repo_path}/issues/{pull_request_id}/comments",
            data={"body": body},
        )
        return map_action(response, map_comment)

    # Reviews

    def get_pull_request_review(
        self,
        pull_request_id: str,
        review_id: str,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[Review]:
        response = self.get(
            f"/repos/{self.repo_path}/pulls/{pull_request_id}/reviews/{review_id}",
            request_options=request_options,
        )
        return map_action(response, map_review)

    def get_review_comments(
        self,
        pull_request_id: str,
        review_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[ReviewComment]]:
        response = self.get(
            f"/repos/{self.repo_path}/pulls/{pull_request_id}/reviews/{review_id}/comments",
            pagination=pagination,
            request_options=request_options,
        )
        return map_paginated_action(
            pagination,
            response,
            lambda raw: [map_review_comment(c, review_id) for c in raw],
        )

    # Issues

    def update_issue(
        self,
        issue_id: str,
        state: IssueState | None = None,
        assignees: list[str] | None = None,
        labels: list[str] | None = None,
    ) -> ActionResult[Issue]:
        data: dict[str, Any] = {}
        if state is not None:
            data["state"] = state
        if assignees is not None:
            data["assignees"] = assignees
        if labels is not None:
            # Gitea's issue-edit payload takes label *ids*, not names, so a
            # caller-supplied name is resolved against the repository's labels
            # first. An unknown name is dropped rather than failing the edit.
            data["labels"] = self._resolve_label_ids(labels)

        response = self.patch(f"/repos/{self.repo_path}/issues/{issue_id}", data=data)
        return map_action(response, map_issue)

    def _resolve_label_ids(self, labels: list[str]) -> list[int]:
        response = self.get(f"/repos/{self.repo_path}/labels", params={"limit": str(GITEA_MAX_PAGE_SIZE)})
        by_name = {label["name"]: label["id"] for label in response.json()}
        return [by_name[name] for name in labels if name in by_name]

    # Reactions
    #
    # Gitea has no per-reaction identifier: its Reaction object is
    # `{content, user, created_at}` and the delete endpoint takes the *content*
    # in a request body rather than an id in the path. So `ResourceId` for a
    # Gitea reaction is the content string itself, which is what makes the
    # generic `delete_*_reaction(..., reaction_id)` signature satisfiable.

    def get_pull_request_reactions(
        self,
        pull_request_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[ReactionResult]]:
        response = self.get(
            f"/repos/{self.repo_path}/issues/{pull_request_id}/reactions",
            pagination=pagination,
            request_options=request_options,
        )
        return map_paginated_action(pagination, response, lambda raw: [map_reaction(r) for r in raw])

    def create_pull_request_reaction(self, pull_request_id: str, reaction: Reaction) -> ActionResult[ReactionResult]:
        response = self.post(
            f"/repos/{self.repo_path}/issues/{pull_request_id}/reactions",
            data={"content": reaction},
        )
        return map_action(response, map_reaction)

    def delete_pull_request_reaction(self, pull_request_id: str, reaction_id: str) -> None:
        self.delete(
            f"/repos/{self.repo_path}/issues/{pull_request_id}/reactions",
            data={"content": reaction_id},
        )

    def get_pull_request_comment_reactions(
        self,
        pull_request_id: str,
        comment_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[ReactionResult]]:
        response = self.get(
            f"/repos/{self.repo_path}/issues/comments/{comment_id}/reactions",
            pagination=pagination,
            request_options=request_options,
        )
        return map_paginated_action(pagination, response, lambda raw: [map_reaction(r) for r in raw])

    def create_pull_request_comment_reaction(
        self, pull_request_id: str, comment_id: str, reaction: Reaction
    ) -> ActionResult[ReactionResult]:
        response = self.post(
            f"/repos/{self.repo_path}/issues/comments/{comment_id}/reactions",
            data={"content": reaction},
        )
        return map_action(response, map_reaction)

    def delete_pull_request_comment_reaction(self, pull_request_id: str, comment_id: str, reaction_id: str) -> None:
        self.delete(
            f"/repos/{self.repo_path}/issues/comments/{comment_id}/reactions",
            data={"content": reaction_id},
        )

    # Check runs
    #
    # Gitea has no Checks API. A check run is synthesized from a commit status
    # the way the GitLab provider does it: the status `context` supplies the
    # name and the id is the composite `{sha}:{context}`, since a status carries
    # no identifier stable across updates.

    def list_check_runs_for_ref(
        self,
        ref: str,
        check_name: str | None = None,
        status: Literal["queued", "in_progress", "completed"] | None = None,
        timestamp_filter: Literal["latest", "all"] = "latest",
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[CheckRun]]:
        response = self.get(
            f"/repos/{self.repo_path}/commits/{quote(ref, safe='')}/statuses",
            params={"sort": "recentupdate"},
            pagination=pagination,
            request_options=request_options,
        )

        def _map(raw: list[dict[str, Any]]) -> list[CheckRun]:
            statuses = raw
            if timestamp_filter == "latest":
                statuses = _latest_status_per_context(statuses)
            if check_name is not None:
                statuses = [s for s in statuses if s.get("context") == check_name]
            runs = [map_check_run(s, ref) for s in statuses]
            if status is not None:
                wanted = _CHECK_RUN_STATUS_FILTER[status]
                runs = [r for r in runs if r["status"] == wanted]
            return runs

        return map_paginated_action(pagination, response, _map)


# The caller-facing status filter uses GitHub's vocabulary; translate it to the
# generic BuildStatus the mapper produces.
_CHECK_RUN_STATUS_FILTER: dict[str, BuildStatus] = {
    "queued": "pending",
    "in_progress": "running",
    "completed": "completed",
}


def _latest_status_per_context(statuses: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep only the most recently updated status for each context.

    Gitea appends a new row on every status update rather than mutating the
    existing one, so a context that ran three times appears three times. The
    request asks for `sort=recentupdate`, so the first occurrence of a context
    is its newest.
    """
    seen: dict[str, dict[str, Any]] = {}
    for status in statuses:
        context = status.get("context") or ""
        if context not in seen:
            seen[context] = status
    return list(seen.values())


# Mappers


def map_action[T](response: requests.Response, fn: Callable[[Any], T]) -> ActionResult[T]:
    raw = response.json()
    return {
        "data": fn(raw),
        "type": "gitea",
        "raw": {"data": raw, "headers": dict(response.headers)},
        "meta": _extract_response_meta(response),
    }


def map_paginated_action[T](
    pagination: PaginationParams | None,
    response: requests.Response,
    fn: Callable[[Any], T],
) -> PaginatedActionResult[T]:
    raw = response.json()
    # Gitea sends no next-page header (no `X-Next-Page`, and its `Link` header
    # is not universally present), so the cursor is advanced blindly the way the
    # GitHub provider does it. `iter_all_pages` stops on the first empty page.
    meta: PaginatedResponseMeta = {
        **_extract_response_meta(response),
        "next_cursor": str(int(pagination["cursor"]) + 1 if pagination else 2),
    }
    return {
        "data": fn(raw),
        "type": "gitea",
        "raw": {"data": raw, "headers": dict(response.headers)},
        "meta": meta,
    }


def map_author(raw: dict[str, Any] | None) -> Author:
    if not raw:
        return Author(id="", username="")
    # Gitea's User model names the handle `login`; there is no `username` field.
    return Author(id=str(raw.get("id", "")), username=raw.get("login") or "")


def map_optional_author(raw: dict[str, Any] | None) -> Author | None:
    return map_author(raw) if raw else None


def map_repository(raw: dict[str, Any]) -> GitRepository:
    return GitRepository(
        full_name=raw["full_name"],
        default_branch=raw.get("default_branch") or "",
        clone_url=raw.get("clone_url") or "",
        private=bool(raw.get("private")),
        size=raw.get("size") or 0,
        description=raw.get("description") or None,
        topics=raw.get("topics") or [],
    )


def map_collaborator_permission(raw: dict[str, Any]) -> UserPermissions:
    user = raw.get("user") or {}
    return UserPermissions(
        login=user.get("login") or "",
        id=str(user.get("id", "")),
        perms=GITEA_PERMISSION_MAP.get(raw.get("permission") or "", "none"),
    )


def map_branch(raw: dict[str, Any]) -> GitRef:
    commit = raw.get("commit") or {}
    return GitRef(ref=raw["name"], sha=commit.get("id") or commit.get("sha") or "")


def map_commit_author(raw: dict[str, Any]) -> CommitAuthor | None:
    """Map the *git* identity carried on a Gitea commit.

    This is the self-asserted `commit.author` block, not the account Gitea
    attributed the commit to -- that is mapped separately onto `author_login`.
    """
    inner = (raw.get("commit") or {}).get("author") or {}
    if not inner:
        return None

    raw_date = inner.get("date")
    date: datetime | None = None
    if raw_date:
        try:
            date = datetime.fromisoformat(raw_date.replace("Z", "+00:00"))
        except ValueError:
            date = None

    return CommitAuthor(name=inner.get("name") or "", email=inner.get("email") or "", date=date)


def map_commit(raw: dict[str, Any]) -> Commit:
    stats = raw.get("stats") or {}
    commit = Commit(
        id=raw["sha"],
        message=(raw.get("commit") or {}).get("message") or "",
        author=map_commit_author(raw),
        additions=stats.get("additions"),
        deletions=stats.get("deletions"),
    )

    # Unlike GitLab, Gitea attaches the resolved *accounts* alongside the git
    # identities, so the login keys can be populated -- these are what a caller
    # tests to tell a bot's commits from a human's. They are absent when the
    # commit email matches no account.
    if (author := raw.get("author")) and author.get("login"):
        commit["author_login"] = author["login"]
    if (committer := raw.get("committer")) and committer.get("login"):
        commit["committer_login"] = committer["login"]

    return commit


def map_pull_request(raw: dict[str, Any]) -> PullRequest:
    head = raw.get("head") or {}
    base = raw.get("base") or {}
    return PullRequest(
        # Gitea exposes both a global `id` and the per-repo `number`; every
        # pull-request route addresses the latter, so it is the caller-facing id.
        id=str(raw["number"]),
        internal_id=str(raw.get("id", "")),
        title=_strip_wip_prefix(raw.get("title") or ""),
        body=raw.get("body"),
        state="open" if raw.get("state") == "open" else "closed",
        merged=bool(raw.get("merged")),
        html_url=raw.get("html_url") or "",
        head=PullRequestBranch(sha=head.get("sha"), ref=head.get("ref") or ""),
        base=PullRequestBranch(sha=base.get("sha"), ref=base.get("ref") or ""),
        author=map_author(raw.get("user")),
    )


def map_pull_request_file(raw: dict[str, Any]) -> PullRequestFile:
    return PullRequestFile(
        filename=raw.get("filename") or "",
        status=GITEA_FILE_STATUS_MAP.get(raw.get("status") or "", "unknown"),
        # Gitea's changed-file entry carries counts and URLs but no diff body
        # and no blob sha, so both are empty here. `sha=""` follows the GitLab
        # provider, whose diff entries have the same gap.
        patch=None,
        changes=raw.get("changes") or 0,
        sha="",
        previous_filename=raw.get("previous_filename") or None,
    )


def map_comment(raw: dict[str, Any]) -> Comment:
    return Comment(
        id=str(raw["id"]),
        body=raw.get("body"),
        author=map_optional_author(raw.get("user")),
        created_at=raw.get("created_at"),
        # Gitea has no author-association concept, and reactions are not
        # surfaced inline on a comment payload.
        author_association=None,
    )


def map_review(raw: dict[str, Any]) -> Review:
    return Review(
        id=str(raw["id"]),
        html_url=raw.get("html_url") or "",
        state=GITEA_REVIEW_STATE_MAP.get(raw.get("state") or "", "commented"),
        author=map_optional_author(raw.get("user")),
        body=raw.get("body"),
        submitted_at=raw.get("submitted_at"),
        commit_id=raw.get("commit_id"),
    )


def map_review_comment(raw: dict[str, Any], review_id: str) -> ReviewComment:
    # Gitea anchors a review comment with `position`/`original_position`, which
    # is an index into the diff hunk rather than a file line number, so it
    # cannot be translated into the generic DiffLine (base/head line numbers)
    # without re-reading the diff. Left None rather than reported wrongly.
    return ReviewComment(
        id=str(raw["id"]),
        unique_id=str(raw["id"]),
        url=raw.get("html_url"),
        file_path=raw.get("path"),
        body=raw.get("body") or "",
        author=map_optional_author(raw.get("user")),
        created_at=raw.get("created_at"),
        diff_hunk=raw.get("diff_hunk"),
        line=None,
        start_line=None,
        review_id=str(raw.get("pull_request_review_id") or review_id),
        author_association=None,
        commit_sha=raw.get("commit_id"),
        head=raw.get("commit_id"),
        # Gitea has no review-thread entity; a comment's `resolver` records who
        # resolved it but there is no thread id to resolve against.
        thread_id=None,
    )


def map_reaction(raw: dict[str, Any]) -> ReactionResult:
    content = raw.get("content") or ""
    # See the reaction note on GiteaProvider: Gitea assigns no reaction id, so
    # the content doubles as the identifier the delete endpoint needs.
    return ReactionResult(
        id=content,
        content=content,  # type: ignore[typeddict-item]  # validated by the instance's reaction set
        author=map_optional_author(raw.get("user")),
    )


def map_issue(raw: dict[str, Any]) -> Issue:
    return Issue(
        id=str(raw["number"]),
        title=raw.get("title") or "",
        body=raw.get("body"),
        state="open" if raw.get("state") == "open" else "closed",
        html_url=raw.get("html_url") or "",
    )


def map_check_run(raw: dict[str, Any], ref: str) -> CheckRun:
    context = raw.get("context") or ""
    status, conclusion = GITEA_STATUS_READ_MAP.get(raw.get("status") or "", ("pending", None))
    return CheckRun(
        # A Gitea commit status has an `id`, but it changes on every update
        # while the context is what identifies the check across runs -- so the
        # composite is the stable handle, matching the GitLab provider.
        id=f"{ref}:{context}",
        name=context,
        status=status,
        conclusion=conclusion,
        html_url=raw.get("target_url") or "",
    )


__all__ = (
    "GITEA_MAX_PAGE_SIZE",
    "ApiClient",
    "GiteaProvider",
    "ResourceId",
    "ResourceNotFound",
    "UNSUPPORTED_CAPABILITIES",
)
