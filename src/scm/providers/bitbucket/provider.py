import base64
import datetime
import hashlib
import re
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from urllib.parse import parse_qs, quote, urlparse

import requests

from scm.errors import (
    ResourceBadRequest,
    SCMCodedError,
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
    FileContent,
    FileStatus,
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
    Referrer,
    Repository,
    RequestOptions,
    ResourceId,
    Review,
    ReviewComment,
    ReviewCommentInput,
    ReviewEvent,
    ReviewSide,
    TreeEntry,
    TreeEntryMode,
    TreeEntryType,
)

# Bitbucket Cloud reads a single PR template from this path on the source branch.
PULL_REQUEST_TEMPLATE_PATH = ".bitbucket/pull_request_template.md"

# Depth passed to the /src listing's ``max_depth`` for a recursive tree. Bitbucket
# does a breadth-first walk and returns 555 if the value is "too large" for the
# repo, so this is a best-effort ceiling covering realistic directory nesting;
# trees nested deeper than this are not fully descended into.
TREE_RECURSION_MAX_DEPTH = 100

# Bitbucket pull request states, grouped to match our binary open/closed model.
# "closed" collapses every non-open terminal state Bitbucket exposes.
PULL_REQUEST_STATE_RETRIEVE_MAP: dict[PullRequestState, list[str]] = {
    "open": ["OPEN"],
    "closed": ["MERGED", "DECLINED", "SUPERSEDED"],
}

# Bitbucket build states are INPROGRESS/SUCCESSFUL/FAILED/STOPPED. This collapses
# our (status, conclusion) model into one of those on write. Conclusions without
# a direct equivalent fall back to FAILED; "neutral" maps to SUCCESSFUL.
BITBUCKET_BUILD_CONCLUSION_WRITE_MAP: dict[BuildConclusion, str] = {
    "success": "SUCCESSFUL",
    "failure": "FAILED",
    "cancelled": "STOPPED",
    "skipped": "STOPPED",
    "timed_out": "FAILED",
    "neutral": "SUCCESSFUL",
    "action_required": "FAILED",
    "unknown": "FAILED",
}

# Reverse map for reads: Bitbucket build state -> (BuildStatus, BuildConclusion).
BITBUCKET_BUILD_STATE_READ_MAP: dict[str, tuple[BuildStatus, BuildConclusion | None]] = {
    "INPROGRESS": ("running", None),
    "SUCCESSFUL": ("completed", "success"),
    "FAILED": ("completed", "failure"),
    "STOPPED": ("completed", "cancelled"),
}


class BitbucketProvider:
    def __init__(self, client: ApiClient, organization_id: int, repository: Repository) -> None:
        self.client = client
        self.organization_id = organization_id
        self.repository = repository

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
        headers = {}
        headers.update(extra_headers or {})

        options = request_options or {}

        params = params or {}
        if pagination:
            if "per_page" in pagination:
                params["pagelen"] = str(pagination["per_page"])
            # The first page carries no cursor; only forward one when present. This
            # also matters because /src pages by an opaque token (not a page number),
            # so a synthetic "page=1" is rejected with "Invalid page".
            if "cursor" in pagination:
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

    def get_authenticated_actor(self) -> ActionResult[Author]:
        response = self.get("/user")
        return make_result(map_author, response.json())

    def get_app_installation(self) -> ActionResult[AppInstallation]:
        # Bitbucket has no "app installation"; the closest signal is the authenticated
        # user's workspace-level role. (The per-repository permission endpoint was
        # deprecated -- CHANGE-2770.)
        workspace = self.repository["name"].split("/", 1)[0]
        response = self.get(f"/user/workspaces/{quote(workspace, safe='')}/permission")
        return make_result(map_app_installation, response.json())

    def get_repository(self) -> ActionResult[GitRepository]:
        response = self.get(f"/repositories/{self.repository['name']}")
        return make_result(map_repository, response.json())

    def get_repository_assignees(
        self,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[Author]]:
        # Bitbucket has no assignee list; the closest analog is the set of users with an
        # explicit permission on the repository. Requires admin access on the repository.
        workspace, _, repo_slug = self.repository["name"].partition("/")
        response = self.get(
            f"/workspaces/{quote(workspace, safe='')}/permissions/repositories/{quote(repo_slug, safe='')}",
            pagination=pagination,
            request_options=request_options,
        )
        return make_paginated_result(map_repository_permission_author, response.json())

    def get_branch(
        self,
        branch: BranchName,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[GitRef]:
        response = self.get(
            f"/repositories/{self.repository['name']}/refs/branches/{quote(branch, safe='/')}",
            request_options=request_options,
        )
        return make_result(map_git_ref, response.json())

    def create_branch(self, branch: BranchName, sha: SHA) -> ActionResult[GitRef]:
        response = self.post(
            f"/repositories/{self.repository['name']}/refs/branches",
            data={"name": branch, "target": {"hash": sha}},
        )
        return make_result(map_git_ref, response.json())

    def delete_branch(self, branch: BranchName) -> None:
        self.delete(f"/repositories/{self.repository['name']}/refs/branches/{quote(branch, safe='/')}")

    def _ref_to_commit(self, ref: str, request_options: RequestOptions | None) -> str:
        """Resolve a ref to a slash-free commit hash when it contains a ``/``.

        Bitbucket's ``/src`` endpoint parses the commit only up to the first ``/``,
        so a branch/tag name containing ``/`` (e.g. ``topics/templates``) is truncated
        to ``topics`` -- even when the slash is URL-encoded as ``%2F``. We resolve such
        refs to their commit hash first. Slash-free refs (commit SHAs and simple
        branch/tag names, which ``/src`` handles fine) pass through unchanged.
        """
        if "/" not in ref:
            return ref
        quoted = quote(ref, safe="/")
        try:
            response = self.get(
                f"/repositories/{self.repository['name']}/refs/branches/{quoted}",
                request_options=request_options,
            )
        except SCMCodedError as e:
            if e.code != "resource_not_found":
                raise
            response = self.get(
                f"/repositories/{self.repository['name']}/refs/tags/{quoted}",
                request_options=request_options,
            )
        return response.json()["target"]["hash"]

    def get_file_content(
        self,
        path: str,
        ref: str,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[FileContent]:
        """Fetch a file's contents at a ref.

        Bitbucket's ``/src`` endpoint returns raw bytes (no blob SHA and no
        line/size metadata in the body), so we base64-encode the response and
        report ``size`` from its byte length. Since Bitbucket exposes no git
        blob id, we compute one locally so ``sha`` matches the blob SHA that
        GitHub and GitLab report.
        """
        commit = self._ref_to_commit(ref, request_options)
        response = self.get(
            f"/repositories/{self.repository['name']}/src/{quote(commit, safe='')}/{quote(path, safe='/')}",
            request_options=request_options,
        )
        raw_content = response.content
        content = base64.b64encode(raw_content).decode("ascii")
        return ActionResult(
            data=FileContent(
                path=path,
                sha=_git_blob_sha1(raw_content),
                content=content,
                encoding="base64",
                size=len(raw_content),
                type="file",
            ),
            type="bitbucket",
            raw={"data": content, "headers": None},
            meta={},
        )

    def get_tree(
        self,
        tree_sha: SHA,
        recursive: bool = True,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[GitTree]:
        """List a single page of the repository tree at a given ref.

        Bitbucket has no tree-object endpoint; we list the repository root via
        the ``/src`` browsing endpoint, which (like GitLab) takes a ref/commit
        rather than a tree-object SHA -- so ``tree_sha`` is treated as a ref and
        a slash-containing one is resolved to a commit hash first (see
        ``_ref_to_commit``). Listing the root requires a trailing slash.

        ``/src`` paginates in-body (``values`` + ``next``), so this returns one
        page and ``meta["next_cursor"]`` carries the next; ``truncated`` mirrors
        it. When ``recursive`` we pass ``max_depth`` for a breadth-first walk;
        Bitbucket exposes no git blob/tree object id, so every entry's ``sha`` is
        empty (like ``get_pull_request_files``).
        """
        commit = self._ref_to_commit(tree_sha, request_options)
        params: dict[str, Any] = {}
        if recursive:
            params["max_depth"] = str(TREE_RECURSION_MAX_DEPTH)
        response = self.get(
            f"/repositories/{self.repository['name']}/src/{quote(commit, safe='')}/",
            params=params,
            pagination=pagination,
            request_options=request_options,
        )
        raw = response.json()
        next_cursor = _next_cursor(raw)
        return PaginatedActionResult(
            data=GitTree(
                sha=tree_sha,
                tree=[map_git_tree_entry(e) for e in raw.get("values", [])],
                truncated=bool(next_cursor),
            ),
            type="bitbucket",
            raw={"data": raw, "headers": None},
            meta=PaginatedResponseMeta(next_cursor=next_cursor),
        )

    def get_full_tree(
        self,
        tree_sha: SHA,
        recursive: bool = True,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[GitTree]:
        """List the complete repository tree, walking every page.

        Follows ``/src``'s in-body ``next`` cursor to exhaustion, so this can be
        expensive on large repositories; prefer :meth:`get_tree` when manual
        pagination is acceptable. The first page carries no cursor (``/src`` pages
        by an opaque token), so we omit it and forward each subsequent one.
        """
        entries: list[TreeEntry] = []
        raw_entries: list[dict[str, Any]] = []
        pagination: PaginationParams = {"per_page": 100}
        while True:
            page = self.get_tree(
                tree_sha,
                recursive=recursive,
                pagination=pagination,
                request_options=request_options,
            )
            entries.extend(page["data"]["tree"])
            raw_entries.extend(page["raw"]["data"].get("values", []))

            next_cursor = page["meta"]["next_cursor"]
            if not next_cursor:
                break
            pagination = {"per_page": 100, "cursor": next_cursor}

        return ActionResult(
            data=GitTree(sha=tree_sha, tree=entries, truncated=False),
            type="bitbucket",
            raw={"data": raw_entries, "headers": None},
            meta={},
        )

    def get_pull_request(
        self,
        pull_request_id: str,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[PullRequest]:
        response = self.get(
            f"/repositories/{self.repository['name']}/pullrequests/{pull_request_id}",
            request_options=request_options,
        )
        return make_result(map_pull_request, response.json())

    def get_pull_requests(
        self,
        state: PullRequestState | None = "open",
        # 'head' is GitHub-shaped (optional ``owner:`` prefix, optional
        # ``refs/heads/`` ref prefix). We map it to a Bitbucket ``q`` filter on
        # the source branch so the open-PR-for-this-branch lookup stays a
        # server-side query rather than a fetch-all-and-filter.
        head: BranchName | None = None,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[PullRequest]]:
        params: dict[str, Any] = {}
        if state is not None:
            # Bitbucket accepts a repeatable ``state`` param; requests serializes
            # a list value as repeated query parameters.
            params["state"] = PULL_REQUEST_STATE_RETRIEVE_MAP[state]
        if head:
            params["q"] = f'source.branch.name="{_head_to_source_branch(head)}"'
        response = self.get(
            f"/repositories/{self.repository['name']}/pullrequests",
            params=params,
            pagination=pagination,
            request_options=request_options,
        )
        return make_paginated_result(map_pull_request, response.json())

    def create_pull_request(
        self,
        title: str,
        body: str,
        head: BranchName,
        base: BranchName,
    ) -> ActionResult[PullRequest]:
        return self._create_pull_request(title, body, head, base, draft=False)

    def create_pull_request_draft(
        self,
        title: str,
        body: str,
        head: BranchName,
        base: BranchName,
    ) -> ActionResult[PullRequest]:
        """Create a draft pull request via Bitbucket's ``draft`` flag."""
        return self._create_pull_request(title, body, head, base, draft=True)

    def _create_pull_request(
        self,
        title: str,
        body: str,
        head: BranchName,
        base: BranchName,
        *,
        draft: bool,
    ) -> ActionResult[PullRequest]:
        data: dict[str, Any] = {
            "title": title,
            "description": body,
            "source": {"branch": {"name": head}},
            "destination": {"branch": {"name": base}},
        }
        if draft:
            data["draft"] = True
        response = self.post(f"/repositories/{self.repository['name']}/pullrequests", data=data)
        return make_result(map_pull_request, response.json())

    def update_pull_request(
        self,
        pull_request_id: str,
        title: str | None = None,
        body: str | None = None,
        state: PullRequestState | None = None,
    ) -> ActionResult[PullRequest]:
        """Update a pull request's title, body, and/or state.

        Bitbucket's PUT endpoint edits fields like title and description but has
        no state field. Closing a pull request therefore goes through the
        separate ``/decline`` endpoint, which we call after the PUT when
        ``state`` is ``"closed"``. Bitbucket cannot reopen a declined pull
        request via the API, so ``state="open"`` is rejected.
        """
        if state == "open":
            raise ResourceBadRequest(
                detail="Bitbucket cannot reopen a pull request via the API.",
            )

        data: dict[str, Any] = {}
        if title is not None:
            data["title"] = title
        if body is not None:
            data["description"] = body

        pr_path = f"/repositories/{self.repository['name']}/pullrequests/{pull_request_id}"

        response = self.put(pr_path, data=data)
        if state == "closed":
            response = self.post(f"{pr_path}/decline", data={})

        return make_result(map_pull_request, response.json())

    def get_commit(
        self,
        sha: SHA,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[CommitWithChanges]:
        """Get a single commit.

        Bitbucket's commit endpoint returns commit metadata without the file
        diff, so ``files`` is always ``None`` (use ``get_commit_changes`` for
        the changed files).
        """
        response = self.get(
            f"/repositories/{self.repository['name']}/commit/{sha}",
            request_options=request_options,
        )
        return make_result(map_commit_with_changes, response.json())

    def get_commits(
        self,
        ref: str | None = None,
        pagination: PaginationParams | None = None,
        since: datetime.datetime | None = None,
        until: datetime.datetime | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[Commit]]:
        """List repository commits, most recent first.

        ``ref`` (branch, tag, or SHA) is passed as Bitbucket's ``include``
        filter; without it, Bitbucket lists commits from the main branch.

        Bitbucket's commits endpoint has no date filtering (no ``since``/
        ``until`` params and no ``q`` support), so passing either is rejected
        rather than silently returning unfiltered results.
        """
        if since is not None or until is not None:
            raise ResourceBadRequest(
                detail="Bitbucket's commits endpoint does not support date filtering (since/until).",
            )
        params: dict[str, Any] = {}
        if ref:
            params["include"] = ref
        response = self.get(
            f"/repositories/{self.repository['name']}/commits",
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
        since: datetime.datetime | None = None,
        until: datetime.datetime | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[Commit]]:
        """List commits that touched ``path``, most recent first.

        Uses Bitbucket's file-history endpoint, whose ``{commit}`` path segment
        is required; when ``ref`` is omitted we resolve the repository's default
        branch. The ``{commit}`` segment is parsed only up to the first ``/`` (see
        ``_ref_to_commit``), so a slash-containing ref is resolved to a commit hash
        first. As with ``get_commits``, Bitbucket has no date filtering here, so
        ``since``/``until`` are rejected.

        File-history entries embed only an abbreviated commit (hash + links), so
        we follow up with one ``get_commit`` call per entry to hydrate the full
        commit. These N calls are issued concurrently via a thread pool, but it
        is still an N+1 fan-out bounded by the page size.
        """
        if since is not None or until is not None:
            raise ResourceBadRequest(
                detail="Bitbucket's file-history endpoint does not support date filtering (since/until).",
            )
        commit = self._ref_to_commit(ref or self.get_repository()["data"]["default_branch"], request_options)
        response = self.get(
            f"/repositories/{self.repository['name']}/filehistory/{commit}/{quote(path, safe='/')}",
            pagination=pagination,
            request_options=request_options,
        )
        raw = response.json()
        hashes = [entry["commit"]["hash"] for entry in raw.get("values", [])]
        # executor.map preserves input order, so the commits stay newest-first.
        with ThreadPoolExecutor() as executor:
            full_commits = list(executor.map(self._fetch_commit, hashes))
        return make_paginated_result(map_commit, raw, raw_items=full_commits)

    def _fetch_commit(self, commit_hash: SHA) -> dict[str, Any]:
        """Fetch a single commit's full representation by hash."""
        return self.get(f"/repositories/{self.repository['name']}/commit/{commit_hash}").json()

    def compare_commits(
        self,
        start_sha: SHA,
        end_sha: SHA,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[CommitComparison]:
        """Compare two commits, ``start_sha`` (base) to ``end_sha`` (head).

        Bitbucket has no single compare endpoint, so we combine two: the commits
        reachable from ``end_sha`` but not ``start_sha`` (the "ahead" commits),
        and the diffstat. The commit list is paginated (the cursor pages it);
        the diffstat is walked in full so the changed-file list is complete.

        Note Bitbucket's diffstat ``spec`` orders commits opposite to git: the
        first is the changes to preview, the second the baseline, so we pass
        ``end..start`` to mirror git's ``start..end``.
        """
        commits_response = self.get(
            f"/repositories/{self.repository['name']}/commits/{end_sha}",
            params={"exclude": start_sha},
            pagination=pagination,
            request_options=request_options,
        )
        commits_raw = commits_response.json()
        commits = [map_commit(c) for c in commits_raw.get("values", [])]
        diff = self._fetch_all_diffstat(f"{end_sha}..{start_sha}", request_options)
        return PaginatedActionResult(
            data=CommitComparison(
                ahead_by=len(commits),
                commits=commits,
                diff=diff,
            ),
            type="bitbucket",
            raw={"data": commits_raw, "headers": None},
            meta=PaginatedResponseMeta(next_cursor=_next_cursor(commits_raw)),
        )

    def _fetch_all_diffstat(self, spec: str, request_options: RequestOptions | None) -> list[CommitFile]:
        """Walk every diffstat page for ``spec`` and return all changed files."""
        files: list[CommitFile] = []
        page = "1"
        while True:
            raw = self.get(
                f"/repositories/{self.repository['name']}/diffstat/{spec}",
                params={"page": page},
                request_options=request_options,
            ).json()
            files.extend(map_diffstat(d) for d in raw.get("values", []))
            cursor = _next_cursor(raw)
            if not cursor:
                return files
            page = cursor

    def get_pull_request_comments(
        self,
        pull_request_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[Comment]]:
        """List a pull request's conversation comments.

        Bitbucket returns inline (review) comments and deleted tombstones from
        the same endpoint. To stay consistent with GitHub — whose "list issue
        comments" excludes review comments — we drop entries carrying an
        ``inline`` anchor as well as deleted ones.
        """
        response = self.get(
            f"/repositories/{self.repository['name']}/pullrequests/{pull_request_id}/comments?q=deleted=false",
            pagination=pagination,
            request_options=request_options,
        )
        raw = response.json()
        return make_paginated_result(
            map_comment,
            raw,
            raw_items=(c for c in raw.get("values", []) if "inline" not in c),
        )

    def create_review_comment_file(
        self,
        pull_request_id: str,
        commit_id: SHA,
        body: str,
        path: str,
        side: ReviewSide,
    ) -> ActionResult[ReviewComment]:
        """Leave a review comment anchored to a whole file.

        A Bitbucket inline comment with only a ``path`` (no line) attaches to the
        file. Bitbucket anchors inline comments to the pull request rather than a
        commit or side, so ``commit_id`` and ``side`` are not used.
        """
        response = self.post(
            f"/repositories/{self.repository['name']}/pullrequests/{pull_request_id}/comments",
            data={"content": {"raw": body}, "inline": {"path": path}},
        )
        return make_result(map_review_comment, response.json())

    def create_review_comment_reply(
        self,
        pull_request_id: str,
        body: str,
        comment_id: str,
    ) -> ActionResult[ReviewComment]:
        """Reply to an existing review comment.

        Bitbucket threads replies by referencing the parent comment's id; the
        reply inherits the parent's inline anchor.
        """
        response = self.post(
            f"/repositories/{self.repository['name']}/pullrequests/{pull_request_id}/comments",
            data={"content": {"raw": body}, "parent": {"id": int(comment_id)}},
        )
        return make_result(map_review_comment, response.json())

    def create_review_comment_line(
        self,
        pull_request_id: str,
        commit_id: SHA,
        body: str,
        path: str,
        side: ReviewSide,
        line: int,
    ) -> ActionResult[ReviewComment]:
        """Leave a review comment on a single line of the diff."""
        response = self.post(
            f"/repositories/{self.repository['name']}/pullrequests/{pull_request_id}/comments",
            data={"content": {"raw": body}, "inline": _inline_anchor(path, line, side)},
        )
        return make_result(map_review_comment, response.json())

    def create_review(
        self,
        pull_request_id: str,
        commit_sha: SHA,
        event: ReviewEvent,
        comments: list[ReviewCommentInput],
        body: str | None = None,
    ) -> ActionResult[Review]:
        """Create a review on a Bitbucket pull request.

        Bitbucket has no atomic review endpoint (unlike GitHub's single reviews
        call): the inline comments, the review body, and the approval verdict
        are each separate requests. Like the GitLab implementation, we fan these
        out concurrently, so the operation is **not atomic** — individual calls
        may succeed independently. Only ``event == "approve"`` maps to an action
        (the approve endpoint); ``"comment"`` and ``"change_request"`` post the
        comments/body without a verdict.
        """
        base = f"/repositories/{self.repository['name']}/pullrequests/{pull_request_id}"
        comments_path = f"{base}/comments"

        def _create_review_comment(comment: ReviewCommentInput) -> None:
            if "line" in comment:
                side: ReviewSide = comment["side"] if "side" in comment else "head"
                inline = _inline_anchor(comment["path"], comment["line"], side)
            else:
                inline = {"path": comment["path"]}
            self.post(comments_path, data={"content": {"raw": comment["body"]}, "inline": inline})

        def _create_review_body() -> None:
            self.post(comments_path, data={"content": {"raw": body}})

        def _approve_pull_request() -> None:
            self.post(f"{base}/approve", data={})

        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(_create_review_comment, comment) for comment in comments]
            if body is not None:
                futures.append(executor.submit(_create_review_body))
            if event == "approve":
                futures.append(executor.submit(_approve_pull_request))
            for future in futures:
                future.result()

        return ActionResult(
            data=Review(
                id="unset",
                html_url=f"https://bitbucket.org/{self.repository['name']}/pull-requests/{pull_request_id}",
            ),
            type="bitbucket",
            raw={"data": {}, "headers": None},
            meta={},
        )

    def create_pull_request_comment(
        self,
        pull_request_id: str,
        body: str,
        extensions: list[CoPilotChatExtension] | None = None,
    ) -> ActionResult[Comment]:
        response = self.post(
            f"/repositories/{self.repository['name']}/pullrequests/{pull_request_id}/comments",
            data={"content": {"raw": body}},
        )
        return make_result(map_comment, response.json())

    def delete_pull_request_comment(self, pull_request_id: str, comment_id: str) -> None:
        self.delete(
            f"/repositories/{self.repository['name']}/pullrequests/{pull_request_id}/comments/{comment_id}",
        )

    def get_thread_id_from_review_comment_unique_id(
        self, pull_request_id: str, review_comment_unique_id: str
    ) -> str | None:
        # A Bitbucket comment roots its own thread (``map_review_comment`` sets
        # ``thread_id`` == the comment id), so the unique id already is the thread id.
        return review_comment_unique_id or None

    def resolve_review_thread(self, pull_request_id: str, thread_id: str) -> None:
        # The thread is identified by its root comment's id.
        self.post(
            f"/repositories/{self.repository['name']}/pullrequests/{pull_request_id}/comments/{thread_id}/resolve",
            data={},
        )

    def collapse_pull_request_comment(
        self,
        pull_request_id: str,
        thread_id: str,
        comment_node_id: str,
        reason: str = "OUTDATED",
    ) -> None:
        # Bitbucket has no minimize-with-reason concept, so ``reason`` and the
        # GitHub-only ``comment_node_id`` are ignored.
        self.resolve_review_thread(pull_request_id, thread_id)

    def update_review_comment(
        self,
        pull_request_id: str,
        comment_id: str,
        body: str,
    ) -> ActionResult[ReviewComment]:
        response = self.put(
            f"/repositories/{self.repository['name']}/pullrequests/{pull_request_id}/comments/{comment_id}",
            data={"content": {"raw": body}},
        )
        return make_result(map_review_comment, response.json())

    def update_and_collapse_pull_request_comment(
        self,
        pull_request_id: str,
        thread_id: str,
        comment_id: str,
        comment_node_id: str,
        body: str,
        reason: str = "OUTDATED",
    ) -> ActionResult[ReviewComment]:
        # Bitbucket has no atomic edit-and-resolve; update the comment, then resolve.
        result = self.update_review_comment(pull_request_id, comment_id, body)
        self.collapse_pull_request_comment(pull_request_id, thread_id, comment_node_id, reason)
        return result

    def get_pull_request_template(
        self,
        ref: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> Iterator[ActionResult[FileContent]]:
        # Bitbucket supports a single template file, read from the source branch (``ref``).
        # There is no directory of templates, so ``pagination`` is unused.
        try:
            yield self.get_file_content(PULL_REQUEST_TEMPLATE_PATH, ref=ref, request_options=request_options)
        except SCMCodedError as e:
            if e.code == "resource_not_found":
                return
            raise

    def get_pull_request_diff(
        self,
        pull_request_id: str,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[str]:
        response = self.get(
            f"/repositories/{self.repository['name']}/pullrequests/{pull_request_id}/diff",
            request_options=request_options,
        )
        return ActionResult(
            data=response.text,
            type="bitbucket",
            raw={"data": response.text, "headers": None},
            meta={},
        )

    def get_pull_request_files(
        self,
        pull_request_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[PullRequestFile]]:
        """List the files changed in a pull request.

        Backed by Bitbucket's diffstat endpoint, which reports per-file line
        counts but no patch text, so ``patch`` is ``None`` and ``sha`` is empty.
        """
        response = self.get(
            f"/repositories/{self.repository['name']}/pullrequests/{pull_request_id}/diffstat",
            pagination=pagination,
            request_options=request_options,
        )
        return make_paginated_result(map_pull_request_file, response.json())

    def get_pull_request_commits(
        self,
        pull_request_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[PullRequestCommit]]:
        """List a pull request's commits.

        Bitbucket returns commits newest-first; we reverse the page to match the
        oldest-first order GitHub and GitLab produce.
        """
        response = self.get(
            f"/repositories/{self.repository['name']}/pullrequests/{pull_request_id}/commits",
            pagination=pagination,
            request_options=request_options,
        )
        raw = response.json()
        return make_paginated_result(
            map_pull_request_commit,
            raw,
            raw_items=reversed(raw.get("values", [])),
        )

    def _commit_web_url(self, sha: SHA) -> str:
        return f"https://bitbucket.org/{self.repository['name']}/commits/{sha}"

    def get_commit_url(self, commit_sha: SHA) -> str:
        return self._commit_web_url(commit_sha)

    def get_pull_request_url(self, pull_request_id: str) -> str:
        return f"https://bitbucket.org/{self.repository['name']}/pull-requests/{pull_request_id}"

    def get_file_url(
        self,
        file_path: str,
        sha: SHA,
        start_line: int | None = None,
        end_line: int | None = None,
    ) -> str:
        url = f"https://bitbucket.org/{self.repository['name']}/src/{sha}/{file_path}"
        # Bitbucket anchors source lines as ``#lines-N`` (single) or ``#lines-N:M`` (range).
        if start_line and end_line:
            url += f"#lines-{start_line}:{end_line}"
        elif start_line:
            url += f"#lines-{start_line}"
        elif end_line:
            url += f"#lines-{end_line}"
        return url

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
        """Create a Bitbucket commit build status, mapped to a check run.

        Bitbucket keys a build status by ``key`` (unique per commit); we use
        ``external_id`` when given, otherwise ``name``. The check run id is
        ``"{sha}:{key}"``. ``started_at``/``completed_at`` have no Bitbucket
        equivalent and are ignored; of ``output`` only the title is forwarded
        (as the status ``description``). Bitbucket requires a link, so ``url``
        defaults to the commit page.
        """
        key = external_id or name
        data: dict[str, Any] = {
            "key": key,
            "state": _bitbucket_build_state(status, conclusion),
            "name": name,
            "url": self._commit_web_url(head_sha),
        }
        description = _description_from_output(output)
        if description is not None:
            data["description"] = description
        response = self.post(
            f"/repositories/{self.repository['name']}/commit/{head_sha}/statuses/build",
            data=data,
        )
        return make_result(_make_map_check_run(self, head_sha, key), response.json())

    def get_check_run(
        self,
        check_run_id: ResourceId,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[CheckRun]:
        """Get a commit build status. ``check_run_id`` is ``"{sha}:{key}"``."""
        sha, key = _split_check_run_id(check_run_id)
        response = self.get(
            f"/repositories/{self.repository['name']}/commit/{sha}/statuses/build/{key}",
            request_options=request_options,
        )
        return make_result(_make_map_check_run(self, sha, key), response.json())

    def update_check_run(
        self,
        check_run_id: ResourceId,
        status: BuildStatus | None = None,
        conclusion: BuildConclusion | None = None,
        output: CheckRunOutput | None = None,
    ) -> ActionResult[CheckRun]:
        """Update a commit build status via Bitbucket's PUT endpoint.

        Unlike GitLab's append-only statuses, Bitbucket has a real PUT keyed by
        ``key``, so a state change is not required; the title (from ``output``)
        can be updated on its own.
        """
        sha, key = _split_check_run_id(check_run_id)
        data: dict[str, Any] = {}
        if status is not None or conclusion is not None:
            data["state"] = _bitbucket_build_state(status, conclusion)
        description = _description_from_output(output)
        if description is not None:
            data["description"] = description
        response = self.put(
            f"/repositories/{self.repository['name']}/commit/{sha}/statuses/build/{key}",
            data=data,
        )
        return make_result(_make_map_check_run(self, sha, key), response.json())


def _head_to_source_branch(head: str) -> str:
    """Normalize a GitHub-style ``head`` filter to a bare Bitbucket branch name.

    Callers pass ``head`` in GitHub's shape — an optional ``owner:`` prefix and
    sometimes a ``refs/heads/`` ref prefix (e.g. ``"acme:refs/heads/feature"``).
    Bitbucket's ``source.branch.name`` filter wants the bare branch name.
    """
    branch = head.split(":", 1)[-1]
    return branch.removeprefix("refs/heads/")


def _git_blob_sha1(content: bytes) -> str:
    """Compute a git blob object id for ``content``.

    Git names a blob by ``sha1("blob <bytelen>\\0" + content)``. Bitbucket does
    not expose this id, so we recompute it to match the blob SHA GitHub and
    GitLab return for the same file.
    """
    header = f"blob {len(content)}\0".encode()
    return hashlib.sha1(header + content, usedforsecurity=False).hexdigest()


def _parse_raw_author(raw: str) -> tuple[str, str]:
    """Split Bitbucket's git-style author string into ``(name, email)``.

    Bitbucket encodes commit authors as ``"Name <email>"`` in the ``raw`` field.
    When no angle-bracketed email is present we return the whole string as the
    name and an empty email.
    """
    match = re.match(r"^(.*?)\s*<(.*)>\s*$", raw)
    if match:
        return match.group(1), match.group(2)
    return raw, ""


def _commit_author(raw: dict[str, Any]) -> CommitAuthor:
    """Build a CommitAuthor from a Bitbucket commit object.

    The git-style author string lives under ``author.raw``; the commit date is
    the top-level ``date``.
    """
    name, email = _parse_raw_author((raw.get("author") or {}).get("raw", ""))
    date = raw.get("date")
    return CommitAuthor(
        name=name,
        email=email,
        date=datetime.datetime.fromisoformat(date) if date else None,
    )


def map_commit(raw: dict[str, Any]) -> Commit:
    return Commit(
        id=raw["hash"],
        message=raw["message"],
        author=_commit_author(raw),
        # Bitbucket's commit list carries no per-commit line stats.
        additions=None,
        deletions=None,
    )


def map_pull_request_commit(raw: dict[str, Any]) -> PullRequestCommit:
    return PullRequestCommit(
        sha=raw["hash"],
        message=raw["message"],
        author=_commit_author(raw),
    )


_BITBUCKET_DIFFSTAT_STATUS: dict[str, FileStatus] = {
    "added": "added",
    "removed": "removed",
    "modified": "modified",
    "renamed": "renamed",
}


def map_diffstat(raw: dict[str, Any]) -> CommitFile:
    new = raw.get("new") or {}
    old = raw.get("old") or {}
    status = _BITBUCKET_DIFFSTAT_STATUS.get(raw.get("status", ""), "modified")
    return CommitFile(
        filename=new.get("path") or old.get("path") or "",
        status=status,
        # Diffstat carries only counts, not the patch text.
        patch=None,
        additions=raw.get("lines_added"),
        deletions=raw.get("lines_removed"),
        previous_filename=old.get("path") if status == "renamed" else None,
    )


def map_commit_with_changes(raw: dict[str, Any]) -> CommitWithChanges:
    return CommitWithChanges(
        id=raw["hash"],
        message=raw["message"],
        author=_commit_author(raw),
        # Bitbucket's commit endpoint carries no diff or line stats.
        files=None,
        additions=None,
        deletions=None,
    )


def map_pull_request_file(raw: dict[str, Any]) -> PullRequestFile:
    new = raw.get("new") or {}
    old = raw.get("old") or {}
    status = _BITBUCKET_DIFFSTAT_STATUS.get(raw.get("status", ""), "modified")
    return PullRequestFile(
        filename=new.get("path") or old.get("path") or "",
        status=status,
        # Diffstat has no patch text.
        patch=None,
        changes=(raw.get("lines_added") or 0) + (raw.get("lines_removed") or 0),
        # Bitbucket exposes no blob SHA on diffstat entries.
        sha="",
        previous_filename=old.get("path") if status == "renamed" else None,
    )


def _bitbucket_build_state(status: BuildStatus | None, conclusion: BuildConclusion | None) -> str:
    if conclusion is not None:
        return BITBUCKET_BUILD_CONCLUSION_WRITE_MAP[conclusion]
    if status == "completed":
        raise ResourceBadRequest(detail="A 'conclusion' is required when 'status' is 'completed'.")
    # Bitbucket has no distinct pending/running; both are INPROGRESS.
    return "INPROGRESS"


def _description_from_output(output: CheckRunOutput | None) -> str | None:
    """Bitbucket build statuses have only a short description; forward the title."""
    if output is None:
        return None
    return output.get("title") or None


def _split_check_run_id(check_run_id: ResourceId) -> tuple[str, str]:
    """Parse a ``"{sha}:{key}"`` check run id."""
    sha, sep, key = check_run_id.partition(":")
    if not sep or not sha or not key:
        raise ResourceBadRequest(detail=f"Expected '<sha>:<key>', got {check_run_id!r}.")
    return sha, key


def _make_map_check_run(provider: "BitbucketProvider", sha: SHA, key: str) -> Callable[[dict[str, Any]], CheckRun]:
    def _map(raw: dict[str, Any]) -> CheckRun:
        status, conclusion = BITBUCKET_BUILD_STATE_READ_MAP.get(raw.get("state", ""), ("running", None))
        return CheckRun(
            id=f"{sha}:{key}",
            name=raw.get("name") or key,
            status=status,
            conclusion=conclusion,
            html_url=raw.get("url") or provider._commit_web_url(sha),
        )

    return _map


def _inline_anchor(path: str, line: int, side: ReviewSide) -> dict[str, Any]:
    """Build a Bitbucket ``inline`` anchor for a diff line.

    Bitbucket keys the line by side: ``to`` is the line in the new/destination
    file, ``from`` the line in the old/source file.
    """
    return {"path": path, "to" if side == "head" else "from": line}


def map_review_comment(raw: dict[str, Any]) -> ReviewComment:
    user = raw.get("user")
    inline = raw.get("inline") or {}
    comment_id = str(raw["id"])
    return ReviewComment(
        id=comment_id,
        # Bitbucket has a single comment id; replies reference it via `parent`.
        unique_id=comment_id,
        url=((raw.get("links") or {}).get("html") or {}).get("href"),
        file_path=inline.get("path"),
        body=(raw.get("content") or {}).get("raw", ""),
        author=map_author(user) if user else None,
        created_at=raw.get("created_on"),
        # Bitbucket inline comments carry no diff hunk, review grouping, or
        # author association, and anchor to the PR rather than a commit.
        diff_hunk=None,
        review_id=None,
        author_association=None,
        commit_sha=None,
        head=None,
        # A top-level comment roots its own thread; replies point back via parent.
        thread_id=comment_id,
    )


def map_git_ref(raw: dict[str, Any]) -> GitRef:
    return GitRef(ref=raw["name"], sha=raw["target"]["hash"])


def map_comment(raw: dict[str, Any]) -> Comment:
    user = raw.get("user")
    return Comment(
        id=str(raw["id"]),
        body=(raw.get("content") or {}).get("raw"),
        author=map_author(user) if user else None,
        created_at=raw.get("created_on"),
        # Bitbucket has no author-association concept.
        author_association=None,
    )


def map_app_installation(raw: dict[str, Any]) -> AppInstallation:
    # Bitbucket workspace roles, from most to least privileged: "owner" (admin) >
    # "member" (write) > "collaborator" (read-only external user). This is coarser
    # than a per-repo permission, but the per-repo endpoint was deprecated.
    permission = raw.get("permission")
    has_write = permission in ("owner", "member")
    return AppInstallation(
        has_read_access=permission in ("owner", "member", "collaborator"),
        has_write_access=has_write,
        # Bitbucket has no check-run permission concept; commit build statuses only
        # require repository write access.
        has_check_run_write_access=has_write,
    )


def map_repository_permission_author(raw: dict[str, Any]) -> Author:
    # Each entry is a ``repository_permission`` wrapping the actual ``user`` account.
    return map_author(raw["user"])


def map_author(raw: dict[str, Any]) -> Author:
    # Bitbucket removed usernames for privacy; ``nickname`` is the closest
    # display handle, and ``uuid`` is the stable identifier used in API paths.
    return Author(
        id=raw.get("uuid") or raw.get("account_id") or "",
        username=raw.get("nickname") or raw.get("display_name") or "",
    )


def map_pull_request(raw: dict[str, Any]) -> PullRequest:
    source = raw["source"]
    destination = raw["destination"]
    return PullRequest(
        # Bitbucket exposes a single repo-scoped id, so internal_id mirrors id.
        internal_id=str(raw["id"]),
        id=str(raw["id"]),
        title=raw["title"],
        body=raw.get("description") or None,
        state="open" if raw["state"] == "OPEN" else "closed",
        base=PullRequestBranch(
            ref=destination["branch"]["name"],
            sha=(destination.get("commit") or {}).get("hash"),
        ),
        head=PullRequestBranch(
            ref=source["branch"]["name"],
            sha=(source.get("commit") or {}).get("hash"),
        ),
        merged=raw["state"] == "MERGED",
        html_url=raw["links"]["html"]["href"],
        author=map_author(raw.get("author") or {}),
    )


def map_repository(raw: dict[str, Any]) -> GitRepository:
    return GitRepository(
        full_name=raw["full_name"],
        default_branch=raw["mainbranch"]["name"],
        clone_url=raw["links"]["clone"][0]["href"],
        private=raw["is_private"],
        # Bitbucket returns size in bytes. We convert to kB to match GitHub
        size=raw["size"] // 1000,
        description=raw["description"],
        topics=[],
    )


def map_git_tree_entry(raw: dict[str, Any]) -> TreeEntry:
    """Map a Bitbucket ``/src`` entry to a git tree entry.

    Bitbucket reports no git blob/tree object id in listings, so ``sha`` is
    always empty. A directory is a ``commit_directory``; a file's mode is
    derived from its ``attributes`` (a symlink, submodule, or the executable
    bit), defaulting to a regular file.
    """
    if raw["type"] == "commit_directory":
        return TreeEntry(path=raw["path"], mode="040000", type="tree", sha="", size=None)
    attributes = raw.get("attributes") or []
    if "subrepository" in attributes:
        entry_type: TreeEntryType = "commit"
        mode: TreeEntryMode = "160000"
    elif "link" in attributes:
        entry_type, mode = "blob", "120000"
    elif "executable" in attributes:
        entry_type, mode = "blob", "100755"
    else:
        entry_type, mode = "blob", "100644"
    return TreeEntry(path=raw["path"], mode=mode, type=entry_type, sha="", size=raw.get("size"))


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
        type="bitbucket",
        raw={"data": raw, "headers": None},
        meta={},
    )


def _next_cursor(raw: dict[str, Any]) -> str | None:
    """Extract the next page number from a Bitbucket paginated response.

    Unlike GitLab (which paginates via response headers), Bitbucket returns a
    full ``next`` URL in the body when more pages exist. We pull the ``page``
    query parameter out of it so callers can pass it back as ``cursor``.
    """
    next_url = raw.get("next")
    if not next_url:
        return None
    page = parse_qs(urlparse(next_url).query).get("page")
    return page[0] if page else None


def make_paginated_result[T](
    map_item: Callable[[dict[str, Any]], T],
    raw: dict[str, Any],
    *,
    raw_items: Iterable[dict[str, Any]] | None = None,
) -> PaginatedActionResult[list[T]]:
    items: Iterable[dict[str, Any]] = raw.get("values", []) if raw_items is None else raw_items

    return PaginatedActionResult(
        data=[map_item(item) for item in items],
        type="bitbucket",
        raw={"data": raw, "headers": None},
        meta=PaginatedResponseMeta(next_cursor=_next_cursor(raw)),
    )
