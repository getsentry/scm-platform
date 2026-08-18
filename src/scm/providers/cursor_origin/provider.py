"""Cursor Origin provider.

Origin's REST API is GitHub-shaped but much smaller, and the differences are not
where you would guess. The three that cost the most time:

1. **Contents is a query parameter.** ``GET /repos/{o}/{r}/contents?path=X&ref=Y``.
   The documented ``/contents/{path}`` form returns a 404 whose body reads
   ``"Route GET:... not found"`` -- which looks like an auth or permission problem
   but is a routing one.
2. **Pagination is cursor-based** (``pageSize`` + ``pageToken`` -> ``nextPageToken``),
   not GitHub's ``per_page``/``page``. A page token also *encodes* the filters it
   was issued for, so ``sha``/``state``/``pageSize`` are ignored on a follow-up
   request that carries one.
3. **Unknown query parameters are silently ignored.** ``?ref=`` on ``/commits`` does
   nothing (the parameter is ``sha``), and ``since``/``until``/``path`` do nothing at
   all. A caller would get a plausible-looking page of the wrong commits, so the
   filters Origin does not implement raise rather than lie.

The write surface is small: pull requests, pull-request comments, reviews, and check
runs. There is no way to write a commit, branch, ref, blob, or tree over REST -- those
go over Git HTTPS with the installation token. See ``limitations.md`` for the full list
and ``actions-quick-ref.md`` for the action -> endpoint mapping.

**Scope.** This is written to what Origin's OpenAPI document publishes and we verified on
the wire, and nothing else. A feature visible in Origin's web UI but absent from the API
-- reactions, today -- stays unimplemented rather than reverse-engineered: an endpoint
found by probing is one Cursor never promised and can withdraw without notice. The API is
``v1alpha1``, so every gap here is provisional; ``limitations.md`` keeps a "Revisit when
Origin supports it" list so the next person reads them as pending, not settled.
"""

from collections.abc import Callable, Iterator
from datetime import UTC, date, datetime
from email.utils import format_datetime, parsedate_to_datetime
from typing import Any, Literal, cast

import requests

from scm.errors import (
    PathIsDirectory,
    PathIsNotDirectory,
    ReadmeNotFound,
    ResourceBadRequest,
    UnexpectedResponseFormat,
    error_class_for_status,
)
from scm.types import (
    SHA,
    ActionResult,
    ApiClient,
    AppInstallation,
    Author,
    BranchName,
    BuildConclusion,
    BuildStatus,
    CheckRun,
    CheckRunOutput,
    Comment,
    Commit,
    CommitAuthor,
    CommitComparison,
    CommitFile,
    CommitWithChanges,
    CoPilotChatExtension,
    CredentialsSet,
    DiffLine,
    FileContent,
    FileContentType,
    FileStatus,
    GitCommitObject,
    GitCommitTree,
    GitRef,
    GitRepository,
    GitTree,
    PaginatedActionResult,
    PaginatedResponseMeta,
    PaginationParams,
    ProviderName,
    PullRequest,
    PullRequestBranch,
    PullRequestCommit,
    PullRequestFile,
    PullRequestReviewState,
    PullRequestState,
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
    TreeEntryMode,
    TreeEntryType,
)

PROVIDER_NAME: ProviderName = "cursor_origin"

# No Origin resource carries a web URL -- not in a REST response, not in a webhook
# payload -- so every link this provider produces is assembled locally. The repository
# prefix `https://cursor.com/codebase/{owner}/{repo}` is documented, and `/pull/{n}` is
# confirmed against the web UI; the commit and blob suffixes are still GitHub-shaped
# guesses. See limitations.md.
#
# This is the *codebase root*, so a repository sits directly beneath it. Sentry defines
# the same value as `CURSOR_ORIGIN_WEB_BASE_URL` in
# `sentry/integrations/cursor_origin/constants.py`; the two must agree, because whatever
# Sentry stores on the repository is handed to this provider as `web_base_url`.
CURSOR_ORIGIN_WEB_BASE_URL = "https://cursor.com/codebase"

# Origin's page size is clamped server-side to 100.
CURSOR_ORIGIN_MAX_PAGE_SIZE = 100

# `scm.helpers.iter_all_pages` seeds its first request with cursor="1" -- a GitHub page
# number. Origin's `pageToken` is an opaque cursor and rejects an unrecognized one with
# a 400, so the sentinel is dropped and the first page is simply requested without a
# token. Real Origin tokens are long opaque strings, never "1".
_FIRST_PAGE_SENTINEL = "1"

CURSOR_ORIGIN_STATUS_MAP: dict[str, BuildStatus] = {
    "queued": "pending",
    "in_progress": "running",
    "completed": "completed",
}

CURSOR_ORIGIN_STATUS_WRITE_MAP: dict[BuildStatus, str] = {
    "pending": "queued",
    "running": "in_progress",
    "completed": "completed",
}

# Origin's conclusions match GitHub's, plus "stale", which has no generic counterpart.
CURSOR_ORIGIN_CONCLUSION_MAP: dict[str, BuildConclusion] = {
    "success": "success",
    "failure": "failure",
    "neutral": "neutral",
    "cancelled": "cancelled",
    "skipped": "skipped",
    "timed_out": "timed_out",
    "action_required": "action_required",
    "stale": "unknown",
}

CURSOR_ORIGIN_CONCLUSION_WRITE_MAP: dict[BuildConclusion, str] = {
    "success": "success",
    "failure": "failure",
    "neutral": "neutral",
    "cancelled": "cancelled",
    "skipped": "skipped",
    "timed_out": "timed_out",
    "action_required": "action_required",
    "unknown": "neutral",
}

CURSOR_ORIGIN_REVIEW_VERDICT_MAP: dict[ReviewEvent, str] = {
    "approve": "approve",
    "change_request": "request_changes",
    "comment": "comment",
}

CURSOR_ORIGIN_REVIEW_STATE_MAP: dict[str, PullRequestReviewState] = {
    "approve": "approved",
    "request_changes": "changes_requested",
    "comment": "commented",
}

# Filenames recognized as a README, lowercased. Origin has no /readme endpoint, so the
# repository root is listed and matched against this set (as GitLab and Bitbucket do).
README_FILENAMES = frozenset({"readme", "readme.md", "readme.rst", "readme.txt"})

_VALID_FILE_STATUSES: frozenset[str] = frozenset(
    {"added", "removed", "modified", "renamed", "copied", "changed", "unchanged"}
)

_CONTENT_TYPES: dict[str, FileContentType] = {
    "file": "file",
    "dir": "directory",
    "symlink": "symlink",
    "submodule": "submodule",
}


def _extract_response_meta(response: requests.Response) -> ResponseMeta:
    """Origin sends an ETag on single-resource reads -- the object's sha -- and honors
    it on a conditional request (verified: a matching If-None-Match returns 304). It
    sends no Last-Modified, and list endpoints carry no ETag at all."""
    meta: ResponseMeta = {}
    if etag := response.headers.get("ETag"):
        meta["etag"] = etag
    if last_modified := response.headers.get("Last-Modified"):
        meta["last_modified"] = parsedate_to_datetime(last_modified)
    return meta


class CursorOriginProvider:
    def __init__(
        self,
        client: ApiClient,
        organization_id: int,
        repository: Repository,
        web_base_url: str = CURSOR_ORIGIN_WEB_BASE_URL,
    ) -> None:
        self.client = client
        self.organization_id = organization_id
        self.repository = repository
        self._web_base_url = web_base_url.rstrip("/")

    # ------------------------------------------------------------------ transport

    @property
    def _repo(self) -> str:
        """The repository path prefix. ``repository["name"]`` is Origin's ``fullName``."""
        return f"/repos/{self.repository['name']}"

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
        credentials_set: CredentialsSet = "installation",
    ) -> requests.Response:
        headers: dict[str, str] = {}
        options = request_options or {}

        # Origin serves 304 for a matching If-None-Match on single-resource reads; its
        # ETag is the object's sha. List endpoints send no ETag, and Last-Modified is
        # not sent at all, but the header is forwarded for parity with the other
        # providers in case that changes.
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
            if per_page := pagination.get("per_page"):
                params["pageSize"] = str(min(per_page, CURSOR_ORIGIN_MAX_PAGE_SIZE))
            cursor = pagination.get("cursor")
            if cursor and cursor != _FIRST_PAGE_SENTINEL:
                params["pageToken"] = cursor

        return self.request(
            "GET",
            path=path,
            params=params,
            headers=headers,
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

    # ---------------------------------------------------------------- repository

    def get_repository(self) -> ActionResult[GitRepository]:
        return map_action(self.get(self._repo), map_repository)

    def list_repositories(
        self,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[GitRepository]]:
        response = self.get("/installation/repos", pagination=pagination, request_options=request_options)
        return map_paginated_action(response, lambda r: [map_repository(repo) for repo in r.get("repositories") or []])

    def get_authenticated_actor(self) -> ActionResult[Author]:
        """The app itself -- Origin has no ``/user`` endpoint and no bot user record.

        ``GET /app`` needs the app JWT rather than the installation token, and returns
        the app's own id and slug. The slug is the login Origin attributes the app's
        writes to, which is what callers use to recognize their own comments.
        """
        response = self.get("/app", credentials_set="application")
        return map_action(response, lambda r: Author(id=str(r["id"]), username=r["slug"]))

    def get_app_installation(self) -> ActionResult[AppInstallation]:
        """The granted scopes of the installation covering this repository.

        Origin has no per-repository installation route -- both
        ``GET /repos/{owner}/{repo}/installation`` and ``GET /installation`` 404 with a
        route-not-found body. The only place granted scopes are reported is the
        app-scoped installation resource, so this lists installations with the app JWT
        and matches on ``target.slug``, the codebase that prefixes every repository's
        ``fullName``.

        Consequences worth knowing:

        - It costs the app-JWT credential rather than the installation token, like
          ``get_authenticated_actor``.
        - An app installed on several codebases gets one installation per codebase, and
          scopes are per-installation, so matching on the owner slug is required --
          taking the first entry would report another codebase's grant.
        - ``has_write_access`` requires *both* ``contents:write`` and
          ``pull_requests:write``, mirroring GitHub's mapping. Note that
          ``contents:write`` does not buy a REST content write on Origin (there is no
          such route) -- it is the scope that authorizes the Git-over-HTTPS push.
        """
        response = self.get("/app/installations", credentials_set="application")
        owner = self.repository["name"].split("/")[0]
        return map_action(response, lambda r: map_app_installation(r, owner))

    # -------------------------------------------------------------------- refs

    def get_branch(
        self,
        branch: BranchName,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[GitRef]:
        """Origin has no ``/branches/{name}`` route; the git-ref endpoint serves the same purpose.

        ``GET /branches`` exists but only as an unfiltered list -- it ignores a ``name``
        query parameter rather than rejecting it.
        """
        response = self.get(f"{self._repo}/git/ref/heads/{branch}", request_options=request_options)
        return map_action(response, lambda r: GitRef(ref=r["ref"].removeprefix("refs/heads/"), sha=r["object"]["sha"]))

    def get_git_ref(
        self,
        ref: str,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[GitRef]:
        response = self.get(f"{self._repo}/git/ref/{ref}", request_options=request_options)
        return map_action(response, lambda r: GitRef(ref=r["ref"], sha=r["object"]["sha"]))

    # ---------------------------------------------------------------- contents

    def get_file_content(
        self,
        path: str,
        ref: str,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[FileContent]:
        response = self.get(
            f"{self._repo}/contents",
            params={"path": path, "ref": ref},
            request_options=request_options,
        )
        raw = response.json()
        if raw.get("type") == "dir":
            raise PathIsDirectory(detail=path)
        return map_action(response, map_file_content)

    def get_directory_contents(
        self,
        path: str,
        ref: str | None = None,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[FileContent]]:
        """List a directory's direct children.

        The contents endpoint returns a whole directory in one response, so
        ``pagination`` is accepted for protocol conformance only and
        ``meta["next_cursor"]`` is always ``None``. Entries carry no size -- use
        :meth:`get_tree` when byte counts matter.
        """
        params: dict[str, str] = {}
        if path:
            params["path"] = path
        if ref:
            params["ref"] = ref
        response = self.get(f"{self._repo}/contents", params=params, request_options=request_options)
        raw = response.json()
        if raw.get("type") != "dir":
            raise PathIsNotDirectory(detail=path)
        return {
            "data": [map_file_content(entry) for entry in raw.get("entries") or []],
            "type": PROVIDER_NAME,
            "raw": {"data": raw, "headers": dict(response.headers)},
            "meta": {**_extract_response_meta(response), "next_cursor": None},
        }

    def get_readme(
        self,
        ref: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[FileContent]:
        """Origin has no ``/readme`` endpoint, so the root listing is scanned by name."""
        root = self.get_directory_contents("", ref=ref, request_options=request_options)
        for entry in root["data"]:
            if entry["type"] == "file" and entry["path"].rsplit("/", 1)[-1].lower() in README_FILENAMES:
                return self.get_file_content(entry["path"], ref=ref, request_options=request_options)
        raise ReadmeNotFound()

    # ------------------------------------------------------------------- trees

    def _fetch_tree(
        self,
        tree_sha: SHA,
        recursive: bool,
        request_options: RequestOptions | None,
    ) -> requests.Response:
        # Origin treats *any* non-empty value of `recursive` as true -- including the
        # string "false" -- so the parameter is omitted entirely when not recursing.
        params: dict[str, Any] = {"recursive": "1"} if recursive else {}
        return self.get(f"{self._repo}/git/trees/{tree_sha}", params=params, request_options=request_options)

    def get_tree(
        self,
        tree_sha: SHA,
        recursive: bool = True,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[GitTree]:
        """Fetch the repository tree.

        Like GitHub, Origin returns the whole tree in one response, so ``pagination``
        is accepted for protocol conformance only. Unlike GitHub it sends no
        ``truncated`` flag, so ``truncated`` is always ``False``; whether Origin caps
        very large trees is unknown.
        """
        response = self._fetch_tree(tree_sha, recursive, request_options)
        raw = response.json()
        return {
            "data": map_git_tree(raw),
            "type": PROVIDER_NAME,
            "raw": {"data": raw, "headers": dict(response.headers)},
            "meta": {**_extract_response_meta(response), "next_cursor": None},
        }

    def get_full_tree(
        self,
        tree_sha: SHA,
        recursive: bool = True,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[GitTree]:
        return map_action(self._fetch_tree(tree_sha, recursive, request_options), map_git_tree)

    # ----------------------------------------------------------------- commits

    def get_commit(
        self,
        sha: SHA,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[CommitWithChanges]:
        """Fetch a commit and its changed files.

        Origin splits what GitHub returns in one response across two endpoints, and the
        file list is paginated, so this costs 1 + ceil(files/100) requests.
        """
        response = self.get(f"{self._repo}/commits/{sha}", request_options=request_options)
        raw = response.json()
        files = list(self._iter_commit_files(sha, request_options))
        return {
            "data": map_commit_with_changes(raw, files),
            "type": PROVIDER_NAME,
            "raw": {"data": {**raw, "files": files}, "headers": dict(response.headers)},
            "meta": _extract_response_meta(response),
        }

    def _iter_commit_files(self, sha: SHA, request_options: RequestOptions | None) -> Iterator[dict[str, Any]]:
        cursor: str | None = None
        while True:
            pagination: PaginationParams = {"per_page": CURSOR_ORIGIN_MAX_PAGE_SIZE}
            if cursor:
                pagination["cursor"] = cursor
            raw = self.get(
                f"{self._repo}/commits/{sha}/files",
                pagination=pagination,
                request_options=request_options,
            ).json()
            yield from raw.get("files") or []
            cursor = raw.get("nextPageToken") or None
            if cursor is None:
                return

    def get_commit_changes(
        self,
        sha: SHA,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[CommitFile]]:
        response = self.get(
            f"{self._repo}/commits/{sha}/files",
            pagination=pagination,
            request_options=request_options,
        )
        return map_paginated_action(response, lambda r: [map_commit_file(f) for f in r.get("files") or []])

    def get_commits(
        self,
        ref: str | None = None,
        pagination: PaginationParams | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[Commit]]:
        """List commits, newest first, starting at ``ref``.

        Origin has no date filtering. It also *ignores* unknown query parameters rather
        than rejecting them, so passing ``since``/``until`` through would silently
        return the unfiltered list; they raise instead.
        """
        if since is not None or until is not None:
            raise ResourceBadRequest(
                detail="Cursor Origin does not support filtering commits by date; "
                "'since' and 'until' would be ignored by the API.",
            )
        params: dict[str, str] = {}
        if ref:
            # The parameter is `sha`, as on GitHub. `ref` is accepted and ignored.
            params["sha"] = ref
        response = self.get(
            f"{self._repo}/commits",
            params=params,
            pagination=pagination,
            request_options=request_options,
        )
        return map_paginated_action(response, lambda r: [map_commit(c) for c in r.get("commits") or []])

    def get_git_commit(
        self,
        sha: SHA,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[GitCommitObject]:
        response = self.get(f"{self._repo}/git/commits/{sha}", request_options=request_options)
        return map_action(response, map_git_commit_object)

    def compare_commits(
        self,
        start_sha: SHA,
        end_sha: SHA,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
        *,
        include_behind: bool = False,
    ) -> PaginatedActionResult[CommitComparison]:
        """Compare two commits.

        Origin's compare endpoint returns only the counts and the three boundary
        commits -- no commit list and no file list. So ``ahead_by``/``behind_by`` are
        read off the response (``include_behind`` is a no-op, both are always present),
        ``commits`` is assembled by walking ``/commits?sha={end_sha}`` back to the merge
        base, and ``diff`` is always empty: there is no endpoint that reports the files
        changed between two arbitrary commits.

        The commit walk honors ``pagination``. A page that reaches the merge base ends
        the comparison, so ``next_cursor`` is ``None`` even when Origin offers one.
        """
        comparison_response = self.get(
            f"{self._repo}/compare/{start_sha}...{end_sha}",
            request_options=request_options,
        )
        comparison = comparison_response.json()
        merge_base = (comparison.get("mergeBaseCommit") or {}).get("sha")

        commits_response = self.get(
            f"{self._repo}/commits",
            params={"sha": end_sha},
            pagination=pagination,
            request_options=request_options,
        )
        raw_commits = commits_response.json()

        commits: list[Commit] = []
        reached_merge_base = False
        for raw_commit in raw_commits.get("commits") or []:
            if raw_commit.get("sha") == merge_base:
                reached_merge_base = True
                break
            commits.append(map_commit(raw_commit))

        next_cursor = None if reached_merge_base else (raw_commits.get("nextPageToken") or None)
        data = CommitComparison(
            ahead_by=comparison.get("aheadBy", 0),
            behind_by=comparison.get("behindBy", 0),
            commits=commits,
            diff=[],
        )
        return {
            "data": data,
            "type": PROVIDER_NAME,
            "raw": {
                "data": {"comparison": comparison, "commits": raw_commits},
                "headers": dict(comparison_response.headers),
            },
            "meta": {**_extract_response_meta(comparison_response), "next_cursor": next_cursor},
        }

    # ----------------------------------------------------------- pull requests

    def get_pull_request(
        self,
        pull_request_id: str,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[PullRequest]:
        response = self.get(f"{self._repo}/pulls/{pull_request_id}", request_options=request_options)
        return map_action(response, self._map_pull_request)

    def get_pull_requests(
        self,
        state: PullRequestState | None = "open",
        head: BranchName | None = None,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[PullRequest]]:
        params: dict[str, str] = {"state": state if state is not None else "all"}
        if head:
            params["head"] = _normalize_head(head)
        response = self.get(
            f"{self._repo}/pulls",
            params=params,
            pagination=pagination,
            request_options=request_options,
        )
        return map_paginated_action(
            response, lambda r: [self._map_pull_request(pr) for pr in r.get("pullRequests") or []]
        )

    def get_pull_request_files(
        self,
        pull_request_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[PullRequestFile]]:
        response = self.get(
            f"{self._repo}/pulls/{pull_request_id}/files",
            pagination=pagination,
            request_options=request_options,
        )
        return map_paginated_action(response, lambda r: [map_pull_request_file(f) for f in r.get("files") or []])

    def get_pull_request_commits(
        self,
        pull_request_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[PullRequestCommit]]:
        response = self.get(
            f"{self._repo}/pulls/{pull_request_id}/commits",
            pagination=pagination,
            request_options=request_options,
        )
        return map_paginated_action(response, lambda r: [map_pull_request_commit(c) for c in r.get("commits") or []])

    def create_pull_request(
        self,
        title: str,
        body: str,
        head: BranchName,
        base: BranchName,
    ) -> ActionResult[PullRequest]:
        response = self.post(
            f"{self._repo}/pulls",
            data={"title": title, "body": body, "head": head, "base": base},
        )
        return map_action(response, self._map_pull_request)

    def create_pull_request_draft(
        self,
        title: str,
        body: str,
        head: BranchName,
        base: BranchName,
    ) -> ActionResult[PullRequest]:
        response = self.post(
            f"{self._repo}/pulls",
            data={"title": title, "body": body, "head": head, "base": base, "draft": True},
        )
        return map_action(response, self._map_pull_request)

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
        response = self.patch(f"{self._repo}/pulls/{pull_request_id}", data=data)
        return map_action(response, self._map_pull_request)

    def mark_pull_request_ready_for_review(self, pull_request_id: str) -> None:
        """Draft state is a plain field on the PR, so this is one PATCH (no GraphQL, no read-first)."""
        self.patch(f"{self._repo}/pulls/{pull_request_id}", data={"draft": False})

    def mark_pull_request_as_draft(self, pull_request_id: str) -> None:
        self.patch(f"{self._repo}/pulls/{pull_request_id}", data={"draft": True})

    # -------------------------------------------------- pull request comments

    def get_pull_request_comments(
        self,
        pull_request_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[Comment]]:
        response = self.get(
            f"{self._repo}/pulls/{pull_request_id}/comments",
            pagination=pagination,
            request_options=request_options,
        )
        return map_paginated_action(response, lambda r: [map_comment(c) for c in r.get("comments") or []])

    def create_pull_request_comment(
        self,
        pull_request_id: str,
        body: str,
        extensions: list[CoPilotChatExtension] | None = None,
    ) -> ActionResult[Comment]:
        """Post a general-discussion comment.

        Omitting a thread id starts a new thread. ``extensions`` is GitHub's
        Copilot-chat affordance and has no Origin counterpart, so it is rejected rather
        than dropped -- a caller that asked for interactive actions should not silently
        get a plain comment.
        """
        if extensions:
            raise ResourceBadRequest(detail="Cursor Origin comments do not support Copilot chat extensions.")
        response = self.post(f"{self._repo}/pulls/{pull_request_id}/comments", data={"body": body})
        return map_action(response, map_comment)

    # --------------------------------------------- review comments (degraded)

    def _post_located_comment(
        self,
        pull_request_id: str,
        body: str,
        path: str,
        line: DiffLine | None,
        start_line: DiffLine | None = None,
        thread_id: str | None = None,
    ) -> requests.Response:
        """Post a general comment carrying its own location header.

        Origin has no diff-anchored comments, so an inline comment degrades to a
        general-discussion one that *states* where it belongs. The finding still
        reaches the pull request; it just is not attached to the diff.
        """
        data: dict[str, Any] = {"body": f"{format_comment_location(path, line, start_line)}\n\n{body}"}
        if thread_id is not None:
            data["threadId"] = thread_id
        return self.post(f"{self._repo}/pulls/{pull_request_id}/comments", data=data)

    def create_review_comment(
        self,
        pull_request_id: str,
        commit_id: SHA,
        body: str,
        path: str,
        line: DiffLine,
        start_line: DiffLine | None = None,
    ) -> ActionResult[ReviewComment]:
        """Leave a review comment on a diff line -- **best-effort**.

        Origin has no inline review comments, so this posts a general-discussion
        comment prefixed with the file and line it refers to. The returned
        ``ReviewComment`` reports the ``file_path``/``line`` the caller *asked* for, not
        an anchor Origin is holding: nothing in Origin knows this comment belongs to a
        diff position, so it will not move with the diff, cannot be resolved, and will
        not appear in the file view. ``commit_id`` is unused.
        """
        response = self._post_located_comment(pull_request_id, body, path, line, start_line)
        return map_action(
            response,
            lambda r: map_review_comment(r, file_path=path, line=line, start_line=start_line, head=commit_id),
        )

    def create_review_comment_file(
        self,
        pull_request_id: str,
        commit_id: SHA,
        body: str,
        path: str,
        side: ReviewSide,
    ) -> ActionResult[ReviewComment]:
        """Leave a file-level review comment -- **best-effort**, as above, with no line."""
        response = self._post_located_comment(pull_request_id, body, path, None)
        return map_action(response, lambda r: map_review_comment(r, file_path=path, head=commit_id))

    def create_review_comment_reply(
        self,
        pull_request_id: str,
        body: str,
        comment_id: str,
    ) -> ActionResult[ReviewComment]:
        """Reply within the thread that ``comment_id`` belongs to.

        Origin threads replies by ``threadId`` rather than by parent comment, so the
        comment is read first to recover its thread -- two requests. This is the one
        piece of review-comment behavior Origin supports natively.
        """
        parent = self.get(f"{self._repo}/pulls/comments/{comment_id}").json()
        thread_id = (parent.get("thread") or {}).get("id")
        if not thread_id:
            raise UnexpectedResponseFormat(detail="Pull request comment response is missing its thread id.")
        response = self.post(
            f"{self._repo}/pulls/{pull_request_id}/comments",
            data={"body": body, "threadId": thread_id},
        )
        return map_action(response, map_review_comment)

    def update_review_comment(
        self,
        pull_request_id: str,
        comment_id: str,
        body: str,
    ) -> ActionResult[ReviewComment]:
        """Edit a comment in place. Origin allows this only for the comment's own author.

        With no way to resolve or collapse a superseded comment, editing is the only
        means of retracting a finding without leaving the stale text behind.
        """
        response = self.patch(f"{self._repo}/pulls/comments/{comment_id}", data={"body": body})
        return map_action(response, map_review_comment)

    # ----------------------------------------------------------------- reviews

    def create_review(
        self,
        pull_request_id: str,
        commit_sha: SHA,
        event: ReviewEvent,
        comments: list[ReviewCommentInput],
        body: str | None = None,
    ) -> ActionResult[Review]:
        """Submit a review verdict, with inline comments degraded to general ones.

        Origin's review endpoint takes a verdict and a body and nothing else, so this is
        **not atomic**: the verdict is submitted first, then each inline comment is
        posted separately as a located general comment (see
        :meth:`create_review_comment`). A failure part-way leaves a submitted verdict
        with some of its findings missing.

        The verdict goes first deliberately. If it went last, a failure would leave
        findings posted under no review at all, which reads as an unattributed drive-by;
        this way the review exists and the gap is in its detail.

        ``commit_sha`` is unused -- Origin pins a review to a pull request *version*.
        """
        data: dict[str, Any] = {"verdict": CURSOR_ORIGIN_REVIEW_VERDICT_MAP[event]}
        if body is not None:
            data["body"] = body
        response = self.post(f"{self._repo}/pulls/{pull_request_id}/reviews", data=data)

        for comment in comments:
            self._post_located_comment(
                pull_request_id,
                comment["body"],
                comment["path"],
                comment.get("line"),
                comment.get("start_line"),
            )

        return map_action(response, lambda r: self._map_review(pull_request_id, r))

    def list_pull_request_reviews(
        self,
        pull_request_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[Review]]:
        response = self.get(
            f"{self._repo}/pulls/{pull_request_id}/reviews",
            pagination=pagination,
            request_options=request_options,
        )
        return map_paginated_action(
            response,
            lambda r: [self._map_review(pull_request_id, review) for review in r.get("reviews") or []],
        )

    # -------------------------------------------------------------- check runs

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
        """Create (or upsert) a check run.

        Origin requires a check *suite* alongside every run, and identifies both by a
        caller-chosen ``key``. We derive the key from ``external_id`` (falling back to
        ``name``) so that repeated calls for the same logical check land on the same
        run. ``externalUpdatedAt`` is mandatory and orders concurrent writes; it is
        stamped with the current time.
        """
        key = external_id or name
        response = self.post(
            f"{self._repo}/check-runs",
            data={
                "headSha": head_sha,
                "checkSuite": {"key": key, "name": name, "externalId": key},
                "checkRun": _check_run_input(
                    key=key,
                    name=name,
                    external_id=key,
                    status=status,
                    conclusion=conclusion,
                    started_at=started_at,
                    completed_at=completed_at,
                    output=output,
                ),
            },
        )
        return map_action(response, lambda r: map_check_run(r["checkRun"]))

    def get_check_run(
        self,
        check_run_id: ResourceId,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[CheckRun]:
        response = self.get(f"{self._repo}/check-runs/{check_run_id}", request_options=request_options)
        return map_action(response, map_check_run)

    def update_check_run(
        self,
        check_run_id: ResourceId,
        status: BuildStatus | None = None,
        conclusion: BuildConclusion | None = None,
        output: CheckRunOutput | None = None,
    ) -> ActionResult[CheckRun]:
        """Update a check run by re-upserting it.

        Origin has no PATCH for check runs; a POST with the same ``(key, externalId)``
        replaces the previous state. The existing run and its suite have to be read
        first to recover the key, name, and head sha the upsert needs, so an update
        costs three requests.
        """
        existing = self.get(f"{self._repo}/check-runs/{check_run_id}").json()
        suite_id = (existing.get("checkSuite") or {}).get("id")
        if not suite_id:
            raise UnexpectedResponseFormat(detail="Check run response is missing its check suite id.")
        suite = self.get(f"{self._repo}/check-suites/{suite_id}").json()

        response = self.post(
            f"{self._repo}/check-runs",
            data={
                "headSha": existing["sha"],
                "checkSuite": {
                    "key": suite["key"],
                    "name": suite.get("name", suite["key"]),
                    "externalId": suite.get("externalId", suite["key"]),
                },
                "checkRun": _check_run_input(
                    key=existing["key"],
                    name=existing.get("name", existing["key"]),
                    external_id=existing.get("externalId", existing["key"]),
                    status=status if status is not None else CURSOR_ORIGIN_STATUS_MAP.get(existing.get("status", "")),
                    conclusion=conclusion,
                    started_at=existing.get("startedAt"),
                    completed_at=existing.get("completedAt"),
                    output=output,
                ),
            },
        )
        return map_action(response, lambda r: map_check_run(r["checkRun"]))

    def list_check_runs_in_check_suite(
        self,
        check_suite_id: ResourceId,
        check_name: str | None = None,
        status: Literal["queued", "in_progress", "completed"] | None = None,
        timestamp_filter: Literal["latest", "all"] = "latest",
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[CheckRun]]:
        response = self.get(
            f"{self._repo}/check-suites/{check_suite_id}/check-runs",
            pagination=pagination,
            request_options=request_options,
        )
        return map_paginated_action(response, lambda r: _filter_check_runs(r, check_name, status))

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
            f"{self._repo}/commits/{ref}/check-runs",
            pagination=pagination,
            request_options=request_options,
        )
        return map_paginated_action(response, lambda r: _filter_check_runs(r, check_name, status))

    # -------------------------------------------------------------- web links

    def _repo_web_url(self) -> str:
        return f"{self._web_base_url}/{self.repository['name']}"

    def get_file_url(
        self,
        file_path: str,
        sha: SHA,
        start_line: int | None = None,
        end_line: int | None = None,
    ) -> str:
        url = f"{self._repo_web_url()}/blob/{sha}/{file_path}"
        if start_line:
            url += f"#L{start_line}"
        if start_line and end_line:
            url += f"-L{end_line}"
        elif end_line:
            url += f"#L{end_line}"
        return url

    def get_commit_url(self, commit_sha: SHA) -> str:
        return f"{self._repo_web_url()}/commit/{commit_sha}"

    def get_commits_url(
        self,
        commit_sha: SHA,
        *,
        file_path: str | None = None,
        since: date | None = None,
        until: date | None = None,
    ) -> str:
        url = f"{self._repo_web_url()}/commits/{commit_sha}"
        if file_path is not None:
            url += f"/{file_path}"
        return url

    def get_pull_request_url(self, pull_request_id: str) -> str:
        return f"{self._repo_web_url()}/pull/{pull_request_id}"

    # ------------------------------------------------------- instance mappers

    def _map_pull_request(self, raw: dict[str, Any]) -> PullRequest:
        """Origin returns no web URL on any resource, so the link is built locally.

        ``number`` arrives as a string: Origin serializes every 64-bit integer that way.
        """
        number = str(raw["number"])
        head = raw.get("head") or {}
        base = raw.get("base") or {}
        return PullRequest(
            internal_id=str(raw["id"]),
            id=number,
            title=raw.get("title", ""),
            body=raw.get("body"),
            state=cast(PullRequestState, raw.get("state", "open")),
            merged=bool(raw.get("merged")),
            html_url=self.get_pull_request_url(number),
            head=PullRequestBranch(sha=head.get("sha"), ref=head.get("ref", "")),
            base=PullRequestBranch(sha=base.get("sha"), ref=base.get("ref", "")),
            author=map_actor(raw.get("author")) or Author(id="", username=""),
        )

    def _map_review(self, pull_request_id: str, raw: dict[str, Any]) -> Review:
        verdict = raw.get("verdict", "comment")
        version = raw.get("pullRequestVersion") or {}
        return Review(
            id=str(raw["id"]),
            html_url=self.get_pull_request_url(pull_request_id),
            state=CURSOR_ORIGIN_REVIEW_STATE_MAP.get(verdict, "commented"),
            author=map_actor(raw.get("author")),
            body=raw.get("body") or None,
            submitted_at=raw.get("submittedAt"),
            commit_id=version.get("headSha"),
        )


# ------------------------------------------------------------------ mappers


def map_app_installation(raw: dict[str, Any], owner: str) -> AppInstallation:
    """Map ``GET /app/installations`` to the installation covering ``owner``.

    An installation with no match reports no access rather than raising: the caller
    (``check_repo_access``) treats an exception and a no-access answer the same way, and
    "the app is not installed on this codebase" is a legitimate answer to the question
    rather than a failure to answer it.
    """
    installations = raw.get("installations") or []
    scopes: set[str] = set()
    for installation in installations:
        if (installation.get("target") or {}).get("slug") == owner:
            scopes = set(installation.get("scopes") or [])
            break

    return AppInstallation(
        has_read_access="repository:metadata:read" in scopes,
        has_write_access=("repository:contents:write" in scopes and "repository:pull_requests:write" in scopes),
        has_check_run_write_access="repository:checks:write" in scopes,
    )


def _normalize_head(head: str) -> str:
    """Normalize a GitHub-shaped ``head`` filter to the bare branch name Origin wants.

    Callers pass ``head`` in GitHub's shape: an optional ``owner:`` prefix and
    sometimes a ``refs/heads/`` ref prefix (e.g. ``"acme:refs/heads/feature"``).
    Seer does exactly this -- it looks for an existing open PR with
    ``head=f"{owner}:{branch}"`` before opening a new one.

    Origin's ``head`` filter is a real server-side filter (verified: a branch that
    does not exist returns zero pull requests rather than the unfiltered list), but
    it accepts only a plain git ref. The qualified form is rejected outright:

        GET /pulls?state=open&head=sentry:some-branch
        -> 400 {"code": 3, "message": "Git ref is invalid"}

    So this is not one of Origin's silently-ignored parameters -- forwarding the
    GitHub shape unchanged is a loud 400, which is why it is normalized here rather
    than in the caller. GitLab's provider does the same thing for the same reason
    (``_head_to_source_branch``).

    A ``refs/heads/`` prefix is in fact accepted by Origin (verified: it returns the
    same single pull request as the bare name). It is stripped anyway, so that one
    normalization covers both spellings and matches GitLab's behaviour.
    """
    branch = head.split(":", 1)[-1]
    return branch.removeprefix("refs/heads/")


def _check_run_input(
    *,
    key: str,
    name: str,
    external_id: str,
    status: BuildStatus | None,
    conclusion: BuildConclusion | None,
    started_at: str | None,
    completed_at: str | None,
    output: CheckRunOutput | None,
) -> dict[str, Any]:
    data: dict[str, Any] = {
        "key": key,
        "name": name,
        "externalId": external_id,
        # Mandatory, and used by Origin to order concurrent writes to the same run.
        "externalUpdatedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "status": CURSOR_ORIGIN_STATUS_WRITE_MAP[status] if status is not None else "queued",
    }
    if conclusion is not None:
        data["conclusion"] = CURSOR_ORIGIN_CONCLUSION_WRITE_MAP[conclusion]
    if started_at is not None:
        data["startedAt"] = started_at
    if completed_at is not None:
        data["completedAt"] = completed_at
    if output is not None:
        data["output"] = output
    return data


def _filter_check_runs(
    raw: dict[str, Any],
    check_name: str | None,
    status: Literal["queued", "in_progress", "completed"] | None,
) -> list[CheckRun]:
    """Apply the protocol's filters locally -- Origin's check-run lists take none.

    Filtering after the fact means a page can come back short (or empty) while later
    pages still hold matches, so callers must walk to the end rather than stop at the
    first empty page.
    """
    runs = raw.get("checkRuns") or []
    if check_name is not None:
        runs = [r for r in runs if r.get("name") == check_name]
    if status is not None:
        runs = [r for r in runs if r.get("status") == status]
    return [map_check_run(r) for r in runs]


def map_actor(raw: dict[str, Any] | None) -> Author | None:
    """Map an ``OriginActor`` -- a tagged union of user, app, and service account.

    Only apps carry a human-readable name (their slug); users are identified by email
    and service accounts by id alone, so those fall back to the id for ``username``.
    """
    if not raw:
        return None
    if app := raw.get("app"):
        return Author(id=str(app.get("id", "")), username=app.get("slug", ""))
    if user := raw.get("user"):
        return Author(id=str(user.get("id", "")), username=user.get("email") or str(user.get("id", "")))
    if service_account := raw.get("serviceAccount"):
        account_id = str(service_account.get("id", ""))
        return Author(id=account_id, username=account_id)
    return None


def map_repository(raw: dict[str, Any]) -> GitRepository:
    """Origin reports no visibility, size, description, or topics.

    ``private=True`` is the safe reading: Origin codebases are not public artifacts, and
    treating an unknown as public is the mistake with consequences. ``size=0`` is why
    language detection has to weigh tree blob sizes instead of asking the repository.
    """
    return GitRepository(
        full_name=raw["fullName"],
        default_branch=raw.get("defaultBranch", ""),
        clone_url=raw.get("cloneUrl", ""),
        private=True,
        size=0,
        description=None,
        topics=[],
    )


def map_file_content(raw: dict[str, Any]) -> FileContent:
    """Map a file or directory entry.

    ``size`` arrives as a *string* on the contents endpoint (Origin serializes 64-bit
    integers as JSON strings) and is absent on directory entries.
    """
    return FileContent(
        path=raw["path"],
        sha=raw.get("sha", ""),
        content=raw.get("content", ""),
        encoding=raw.get("encoding", ""),
        size=int(raw.get("size") or 0),
        type=_CONTENT_TYPES.get(raw.get("type", "file"), "file"),
    )


def map_commit_author(raw: dict[str, Any] | None) -> CommitAuthor | None:
    if raw is None:
        return None
    raw_date = raw.get("date")
    return CommitAuthor(
        name=raw.get("name", ""),
        email=raw.get("email", ""),
        date=datetime.fromisoformat(raw_date) if raw_date else None,
    )


def map_commit_file(raw: dict[str, Any]) -> CommitFile:
    """Map a changed file. Zero-valued counts are omitted by Origin, not sent as 0."""
    raw_status = raw.get("status", "modified")
    return CommitFile(
        filename=raw["filename"],
        status=cast(FileStatus, raw_status if raw_status in _VALID_FILE_STATUSES else "unknown"),
        patch=raw.get("patch"),
        additions=raw.get("additions", 0),
        deletions=raw.get("deletions", 0),
        previous_filename=raw.get("previousFilename"),
    )


def map_commit(raw: dict[str, Any]) -> Commit:
    """Map a commit.

    ``author_login``/``committer_login`` are never set: Origin's commit payload carries
    only the git identities, with no account attribution to go with them.
    """
    commit = raw.get("commit") or {}
    stats = raw.get("stats") or {}
    return Commit(
        id=raw["sha"],
        message=commit.get("message", ""),
        author=map_commit_author(commit.get("author")),
        additions=stats.get("additions"),
        deletions=stats.get("deletions"),
    )


def map_commit_with_changes(raw: dict[str, Any], raw_files: list[dict[str, Any]]) -> CommitWithChanges:
    commit = raw.get("commit") or {}
    stats = raw.get("stats") or {}
    return CommitWithChanges(
        id=raw["sha"],
        message=commit.get("message", ""),
        author=map_commit_author(commit.get("author")),
        files=[map_commit_file(f) for f in raw_files],
        additions=stats.get("additions"),
        deletions=stats.get("deletions"),
    )


def map_pull_request_commit(raw: dict[str, Any]) -> PullRequestCommit:
    commit = raw.get("commit") or {}
    return PullRequestCommit(
        sha=raw["sha"],
        message=commit.get("message", ""),
        author=map_commit_author(commit.get("author")),
    )


def map_pull_request_file(raw: dict[str, Any]) -> PullRequestFile:
    """Map a changed file on a pull request. Origin reports no blob sha for it."""
    raw_status = raw.get("status", "modified")
    return PullRequestFile(
        filename=raw["filename"],
        status=cast(FileStatus, raw_status if raw_status in _VALID_FILE_STATUSES else "unknown"),
        patch=raw.get("patch"),
        changes=raw.get("changes", 0),
        sha="",
        previous_filename=raw.get("previousFilename"),
    )


def map_tree_entry(raw: dict[str, Any]) -> TreeEntry:
    return TreeEntry(
        path=raw["path"],
        mode=cast(TreeEntryMode, raw["mode"]),
        type=cast(TreeEntryType, raw["type"]),
        sha=raw["sha"],
        size=raw.get("size"),
    )


def map_git_tree(raw: dict[str, Any]) -> GitTree:
    """Map a tree. Origin sends no ``truncated`` flag, so it defaults to ``False``."""
    return GitTree(
        sha=raw["sha"],
        tree=[map_tree_entry(e) for e in raw.get("tree") or []],
        truncated=bool(raw.get("truncated", False)),
    )


def map_git_commit_object(raw: dict[str, Any]) -> GitCommitObject:
    return GitCommitObject(
        sha=raw["sha"],
        tree=GitCommitTree(sha=(raw.get("tree") or {}).get("sha", "")),
        message=raw.get("message", ""),
    )


def _line_label(line: DiffLine) -> str:
    """Describe a diff line in prose. The base side is named because a reader looking at
    the current file will not find the line there."""
    head = line.get("head")
    if head is not None:
        return str(head)
    base = line.get("base")
    return f"{base} (before)" if base is not None else "?"


def format_comment_location(path: str, line: DiffLine | None, start_line: DiffLine | None = None) -> str:
    """Render the header that stands in for a diff anchor.

    Deliberately plain text rather than a link: the blob URL shape is still unverified
    (see limitations.md), and a dead link is worse than a path a reader can search for.
    """
    if line is None:
        return f"**`{path}`**"
    if start_line is not None:
        return f"**`{path}`** lines {_line_label(start_line)}-{_line_label(line)}"
    return f"**`{path}`** line {_line_label(line)}"


def map_review_comment(
    raw: dict[str, Any],
    *,
    file_path: str | None = None,
    line: DiffLine | None = None,
    start_line: DiffLine | None = None,
    head: str | None = None,
) -> ReviewComment:
    """Map a general-discussion comment into the review-comment shape.

    ``file_path``/``line``/``start_line`` are echoed from the caller's request rather
    than read from the response: Origin stores no diff position, so these describe what
    the comment *says* about itself, not an anchor the service is maintaining. They are
    ``None`` when the comment is read back rather than just written, because at that
    point the location exists only inside the body text.
    """
    return ReviewComment(
        id=str(raw["id"]),
        unique_id=str(raw["id"]),
        url=None,
        file_path=file_path,
        body=raw.get("body", ""),
        author=map_actor(raw.get("author")),
        created_at=raw.get("createdAt"),
        diff_hunk=None,
        line=line,
        start_line=start_line,
        review_id=None,
        author_association=None,
        commit_sha=None,
        head=head,
        thread_id=(raw.get("thread") or {}).get("id"),
    )


def map_comment(raw: dict[str, Any]) -> Comment:
    """Map a general-discussion pull request comment.

    Origin has no reactions and no author-association concept, so those keys are left
    unset rather than filled with a placeholder.
    """
    return Comment(
        id=str(raw["id"]),
        body=raw.get("body"),
        author=map_actor(raw.get("author")),
        created_at=raw.get("createdAt"),
    )


def map_check_run(raw: dict[str, Any]) -> CheckRun:
    raw_conclusion = raw.get("conclusion")
    return CheckRun(
        id=str(raw["id"]),
        name=raw.get("name", ""),
        status=CURSOR_ORIGIN_STATUS_MAP.get(raw.get("status", ""), "pending"),
        conclusion=CURSOR_ORIGIN_CONCLUSION_MAP.get(raw_conclusion) if raw_conclusion else None,
        html_url=raw.get("detailsUrl") or "",
    )


def map_action[T](response: requests.Response, fn: Callable[[dict[str, Any]], T]) -> ActionResult[T]:
    raw = response.json()
    return {
        "data": fn(raw),
        "type": PROVIDER_NAME,
        "raw": {"data": raw, "headers": dict(response.headers)},
        "meta": _extract_response_meta(response),
    }


def map_paginated_action[T](
    response: requests.Response,
    fn: Callable[[Any], T],
) -> PaginatedActionResult[T]:
    """Wrap a paginated response.

    The cursor comes from the body's ``nextPageToken``, which is absent (not empty) on
    the last page. An empty collection also arrives as ``{}`` rather than as an empty
    array, so every extractor reads its key defensively.
    """
    raw = response.json()
    meta: PaginatedResponseMeta = {
        **_extract_response_meta(response),
        "next_cursor": raw.get("nextPageToken") or None,
    }
    return {
        "data": fn(raw),
        "type": PROVIDER_NAME,
        "raw": {"data": raw, "headers": dict(response.headers)},
        "meta": meta,
    }
