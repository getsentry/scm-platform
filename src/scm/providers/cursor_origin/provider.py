from collections.abc import Callable
from datetime import UTC, date, datetime
from typing import Any, Literal
from urllib.parse import quote

import requests

from scm.errors import (
    PathIsDirectory,
    PathIsNotDirectory,
    ResourceBadRequest,
    ResourceConflict,
    ResourceNotFound,
    SCMCodedError,
    StaleBranchHead,
    UnexpectedResponseFormat,
    error_class_for_status,
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
    CommitAuthorParam,
    CommitComparison,
    CommitFile,
    CredentialsSet,
    DeleteCommitAction,
    FileContent,
    FileContentType,
    FileStatus,
    GitCommitObject,
    GitCommitTree,
    GitRef,
    GitRepository,
    GitTree,
    MoveCommitAction,
    PaginatedActionResult,
    PaginationParams,
    ProviderName,
    PullRequest,
    PullRequestBranch,
    PullRequestCommit,
    PullRequestFile,
    PullRequestState,
    Repository,
    RequestOptions,
    ResourceId,
    ReviewThread,
    ReviewThreadComment,
    TreeEntry,
    WriteCommitAction,
)

PROVIDER_TYPE: ProviderName = "cursor_origin"
CURSOR_ORIGIN_WEB_BASE_URL = "https://cursor.com/codebase"
# Origin has no no-reply address for an app.
CURSOR_ORIGIN_APP_COMMIT_EMAIL = "noreply@sentry.io"
PAGE_TOKEN_PARAM = "pageToken"
PAGE_SIZE_PARAM = "pageSize"
# The cursor `iter_all_pages` sends for the first page. The first page takes no token.
FIRST_PAGE_CURSOR = "1"
MAX_PAGE_SIZE = 100
# Origin's visibilities.
_PRIVATE_VISIBILITIES = {"internal", "private"}


CURSOR_ORIGIN_FILE_TYPE_MAP: dict[str, FileContentType] = {"file": "file", "dir": "directory"}


CURSOR_ORIGIN_STATUS_WRITE_MAP: dict[BuildStatus, str] = {
    "pending": "queued",
    "running": "in_progress",
    "completed": "completed",
}
CURSOR_ORIGIN_STATUS_MAP: dict[str, BuildStatus] = {
    **{origin: status for status, origin in CURSOR_ORIGIN_STATUS_WRITE_MAP.items()},
    "rerequested": "pending",
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
CURSOR_ORIGIN_CONCLUSION_MAP: dict[str, BuildConclusion] = {
    **{
        origin: conclusion
        for conclusion, origin in CURSOR_ORIGIN_CONCLUSION_WRITE_MAP.items()
        if conclusion != "unknown"
    },
    "stale": "unknown",
}


COMPARE_MAX_PAGES = 3

CURSOR_ORIGIN_FILE_STATUS_MAP: dict[str, FileStatus] = {
    "added": "added",
    "removed": "removed",
    "modified": "modified",
    "renamed": "renamed",
    "copied": "copied",
}


class CursorOriginProvider:
    def __init__(
        self,
        client: ApiClient,
        organization_id: int,
        repository: Repository,
        installation_id: str,
        web_base_url: str = CURSOR_ORIGIN_WEB_BASE_URL,
    ) -> None:
        self.client = client
        self.organization_id = organization_id
        self.repository = repository
        self.installation_id = installation_id
        self._web_base_url = web_base_url

    @property
    def repository_path(self) -> str:
        return self.repository["name"]

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
        allow_redirects: bool | None = None,
        credentials_set: CredentialsSet = "installation",
    ) -> requests.Response:
        options = request_options or {}
        params = dict(params or {})
        if pagination is not None:
            if per_page := pagination.get("per_page"):
                params[PAGE_SIZE_PARAM] = str(per_page)
            cursor = pagination.get("cursor")
            if cursor and cursor != FIRST_PAGE_CURSOR:
                params[PAGE_TOKEN_PARAM] = cursor

        return self.request(
            "GET",
            path=path,
            params=params,
            allow_redirects=allow_redirects,
            credentials_set=credentials_set,
            timeout=options.get("timeout"),
        )

    def post(self, path: str, data: dict[str, Any]) -> requests.Response:
        return self.request("POST", path=path, data=data)

    def patch(self, path: str, data: dict[str, Any]) -> requests.Response:
        return self.request("PATCH", path=path, data=data)

    def delete(self, path: str) -> requests.Response:
        return self.request("DELETE", path=path)

    def get_app_installation(self) -> ActionResult[AppInstallation]:
        response = self.get(f"/app/installations/{self.installation_id}", credentials_set="application")
        return map_action(response, map_app_installation)

    def get_repository(self) -> ActionResult[GitRepository]:
        response = self.get(f"/repos/{self.repository_path}")
        return map_action(response, map_repository)

    def get_pull_request(
        self,
        pull_request_id: str,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[PullRequest]:
        response = self.get(
            f"/repos/{self.repository_path}/pulls/{pull_request_id}",
            request_options=request_options,
        )
        return map_action(response, self._map_pull_request)

    def get_pull_request_comments(
        self,
        pull_request_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[Comment]]:
        """Return general discussion comments from the pull request.

        Origin returns general discussion and review threads together, so all pages must be
        read even when a page has no general comments. ``pagination`` is unused.
        """
        comments, response = self._all_comments(pull_request_id, request_options)
        general = [map_comment(comment) for comment in comments if _is_general_discussion(comment)]
        return map_comments_page(response, comments, general)

    def get_branch(
        self,
        branch: BranchName,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[GitRef]:
        response = self.get(
            f"/repos/{self.repository_path}/git/ref/heads/{branch}",
            request_options=request_options,
        )
        return map_action(response, map_git_ref)

    def create_branch(self, branch: BranchName, sha: SHA) -> ActionResult[GitRef]:
        response = self.post(
            f"/repos/{self.repository_path}/git/refs",
            data={"ref": f"refs/heads/{branch}", "sha": sha},
        )
        return map_action(response, map_git_ref)

    def delete_branch(self, branch: BranchName) -> None:
        self.delete(f"/repos/{self.repository_path}/git/refs/heads/{branch}")

    def get_file_url(
        self,
        file_path: str,
        sha: SHA,
        start_line: int | None = None,
        end_line: int | None = None,
    ) -> str:
        url = f"{self._web_base_url}/{self.repository_path}/blob/{quote(sha, safe='')}/{quote(file_path)}"
        if start_line:
            url += f"#L{start_line}"
        if start_line and end_line:
            url += f"-L{end_line}"
        elif end_line:
            url += f"#L{end_line}"
        return url

    def get_commit_url(self, commit_sha: SHA) -> str:
        return f"{self._web_base_url}/{self.repository_path}/commit/{commit_sha}"

    def get_commits_url(
        self,
        commit_sha: SHA,
        *,
        file_path: str | None = None,
        since: date | None = None,
        until: date | None = None,
    ) -> str:
        """Only the unfiltered history page is supported.

        The ref is one path segment, so branch slashes must be encoded.
        """
        if file_path is not None or since is not None or until is not None:
            raise ResourceBadRequest(detail="Origin's commit history page takes no file or date filter.")
        return f"{self._web_base_url}/{self.repository_path}/commits/{quote(commit_sha, safe='')}"

    def get_pull_request_url(self, pull_request_id: str) -> str:
        return f"{self._web_base_url}/{self.repository_path}/pull/{pull_request_id}"

    def get_file_content(
        self,
        path: str,
        ref: str,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[FileContent]:
        response = self.get(
            f"/repos/{self.repository_path}/contents",
            params={"path": path, "ref": ref},
            request_options=request_options,
        )
        raw = response.json()
        if "entries" in raw:
            raise PathIsDirectory(detail=path)
        return map_action(response, map_file_content, raw)

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
        response = self.get(
            f"/repos/{self.repository_path}/contents",
            params=params,
            request_options=request_options,
        )
        raw = response.json()
        if "entries" not in raw:
            raise PathIsNotDirectory(detail=path)
        return {
            "data": [map_file_content(entry) for entry in raw["entries"]],
            "type": PROVIDER_TYPE,
            "raw": {"data": raw, "headers": dict(response.headers)},
            "meta": {"next_cursor": None},
        }

    def create_commit(
        self,
        branch: BranchName,
        parent_sha: SHA,
        message: str,
        actions: list[ChmodCommitAction | DeleteCommitAction | MoveCommitAction | WriteCommitAction],
        force: bool = False,
        create_branch: bool = False,
        author: CommitAuthorParam | None = None,
        *,
        expected_head_sha: SHA | None = None,
    ) -> ActionResult[Commit]:
        """Commit ``actions`` onto ``branch``. See :func:`scm.actions.create_commit`.

        Origin only commits at the branch head, so ``parent_sha`` must match it. New branches are
        created at ``parent_sha`` first.
        """
        if force:
            raise ResourceBadRequest(detail="Origin cannot force a branch to a new commit.")
        if expected_head_sha is not None:
            if create_branch:
                raise ResourceBadRequest(
                    detail="'expected_head_sha' cannot be combined with 'create_branch': the branch has no head yet.",
                )
            if expected_head_sha != parent_sha:
                raise ResourceBadRequest(detail="Origin commits onto the branch head, so it must be 'parent_sha'.")
        author = author or self._app_author()

        files = [change for action in actions for change in self._file_changes(action, parent_sha)]
        if not create_branch:
            return self._commit_files(branch, parent_sha, message, author, files, expected_head_sha)

        try:
            self.get_branch(branch)
        except ResourceNotFound:
            pass
        else:
            raise ResourceConflict(detail=f"Branch '{branch}' already exists.")
        self.create_branch(branch, parent_sha)
        try:
            return self._commit_files(branch, parent_sha, message, author, files, expected_head_sha)
        except SCMCodedError:
            # A timeout can hide a commit that landed, and a second writer can share the branch.
            if self.get_branch(branch)["data"]["sha"] == parent_sha:
                self.delete_branch(branch)
            raise

    def _commit_files(
        self,
        branch: BranchName,
        parent_sha: SHA,
        message: str,
        author: CommitAuthorParam,
        files: list[dict[str, Any]],
        expected_head_sha: SHA | None,
    ) -> ActionResult[Commit]:
        try:
            response = self.post(
                f"/repos/{self.repository_path}/git/commits:createFromFiles",
                data={
                    "targetBranch": branch,
                    "expectedHeadSha": parent_sha,
                    "message": message,
                    "author": {"name": author["name"], "email": author["email"]},
                    "files": files,
                },
            )
        except ResourceBadRequest as e:
            if expected_head_sha is None:
                raise
            current_head = self.get_branch(branch)["data"]["sha"]
            if current_head == expected_head_sha:
                raise
            raise StaleBranchHead(
                detail=f"Branch '{branch}' is at {current_head}, expected {expected_head_sha}.",
            ) from e

        commit_author = CommitAuthor(name=author["name"], email=author["email"], date=None)
        return map_action(
            response,
            lambda raw: Commit(id=raw["sha"], message=message, author=commit_author, additions=None, deletions=None),
        )

    def _app_author(self) -> CommitAuthorParam:
        app = self.get("/app", credentials_set="application").json()
        return CommitAuthorParam(name=app["displayName"], email=CURSOR_ORIGIN_APP_COMMIT_EMAIL)

    def _file_changes(
        self,
        action: ChmodCommitAction | DeleteCommitAction | MoveCommitAction | WriteCommitAction,
        parent_sha: SHA,
    ) -> list[dict[str, Any]]:
        """Origin takes whole files, so a move or a mode change rewrites the file."""
        if isinstance(action, WriteCommitAction):
            return [{"path": action.filename, "content": action.content, "encoding": action.encoding}]
        if isinstance(action, DeleteCommitAction):
            return [{"path": action.filename, "delete": True}]
        if isinstance(action, MoveCommitAction):
            existing = self.get_file_content(action.old_filename, ref=parent_sha)["data"]
            return [
                {"path": action.old_filename, "delete": True},
                {"path": action.new_filename, "content": existing["content"], "encoding": existing["encoding"]},
            ]
        existing = self.get_file_content(action.filename, ref=parent_sha)["data"]
        return [
            {
                "path": action.filename,
                "content": existing["content"],
                "encoding": existing["encoding"],
                "mode": "executable" if action.executable else "file",
            }
        ]

    def _fetch_tree(self, tree_sha: SHA, recursive: bool, request_options: RequestOptions | None) -> requests.Response:
        return self.get(
            f"/repos/{self.repository_path}/git/trees/{tree_sha}",
            params={"recursive": "true"} if recursive else {},
            request_options=request_options,
        )

    def get_tree(
        self,
        tree_sha: SHA,
        recursive: bool = True,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[GitTree]:
        response = self._fetch_tree(tree_sha, recursive, request_options)
        raw = response.json()
        return {
            "data": map_git_tree(raw),
            "type": PROVIDER_TYPE,
            "raw": {"data": raw, "headers": dict(response.headers)},
            "meta": {"next_cursor": None},
        }

    def get_archive_link(
        self,
        ref: str,
        archive_format: ArchiveFormat = "tarball",
        request_options: RequestOptions | None = None,
    ) -> ActionResult[ArchiveLink]:
        """A signed URL for the repository archive at `ref`.

        The first request builds the archive and returns 200 without a body we need.
        Later requests return a signed URL with a 15-minute expiry.
        """
        _require_tarball(archive_format)
        response = self._get_archive_location(ref, request_options)
        if response.status_code == 200:
            response.close()
            response = self._get_archive_location(ref, request_options)
        response.close()

        location = response.headers.get("Location")
        if response.status_code != 302 or not location:
            raise UnexpectedResponseFormat(detail="Could not extract 'Location' header.")

        return {
            "data": ArchiveLink(url=location, headers={}),
            "type": PROVIDER_TYPE,
            "raw": {"data": location, "headers": dict(response.headers)},
            "meta": {},
        }

    def download_archive(
        self,
        ref: str,
        archive_format: ArchiveFormat = "tarball",
        request_options: RequestOptions | None = None,
    ) -> requests.Response:
        _require_tarball(archive_format)
        return self.get(
            f"/repos/{self.repository_path}/tarball",
            params={"ref": ref},
            request_options=request_options,
        )

    def _get_archive_location(self, ref: str, request_options: RequestOptions | None) -> requests.Response:
        return self.get(
            f"/repos/{self.repository_path}/tarball",
            params={"ref": ref},
            request_options=request_options,
            allow_redirects=False,
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
        """Create or update a check run for a commit.

        Origin keys runs by repository, commit, suite key, and run key. The run name is used
        for both keys, so posting the same name for a commit updates the existing run.
        """
        return self._post_check_run(
            head_sha,
            key=name,
            name=name,
            external_id=external_id or f"{name}:{head_sha}",
            status=CURSOR_ORIGIN_STATUS_WRITE_MAP[status or "pending"],
            conclusion=CURSOR_ORIGIN_CONCLUSION_WRITE_MAP[conclusion] if conclusion else None,
            started_at=started_at,
            completed_at=completed_at,
            output=output,
            details_url=None,
        )

    def get_check_run(
        self,
        check_run_id: ResourceId,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[CheckRun]:
        response = self.get(
            f"/repos/{self.repository_path}/check-runs/{check_run_id}",
            request_options=request_options,
        )
        return map_action(response, map_check_run)

    def update_check_run(
        self,
        check_run_id: ResourceId,
        status: BuildStatus | None = None,
        conclusion: BuildConclusion | None = None,
        output: CheckRunOutput | None = None,
    ) -> ActionResult[CheckRun]:
        """Update a check run by replacing it with a new post.

        Origin has no update route, so read the existing run and carry over fields the caller
        does not provide. Rerequested runs cannot be posted, so they are posted as queued.
        """
        stored = self.get(f"/repos/{self.repository_path}/check-runs/{check_run_id}").json()
        posted_status = CURSOR_ORIGIN_STATUS_WRITE_MAP[status or CURSOR_ORIGIN_STATUS_MAP[stored["status"]]]
        if conclusion:
            posted_conclusion: str | None = CURSOR_ORIGIN_CONCLUSION_WRITE_MAP[conclusion]
        elif posted_status == "completed" and stored["status"] == "completed":
            posted_conclusion = stored["conclusion"]
        else:
            posted_conclusion = None

        return self._post_check_run(
            stored["sha"],
            key=stored["key"],
            name=stored["name"],
            external_id=stored["externalId"],
            status=posted_status,
            conclusion=posted_conclusion,
            started_at=stored.get("startedAt"),
            completed_at=stored.get("completedAt"),
            output=output if output is not None else stored.get("output"),
            details_url=stored["detailsUrl"],
        )

    def _post_check_run(
        self,
        head_sha: SHA,
        *,
        key: str,
        name: str,
        external_id: str,
        status: str,
        conclusion: str | None,
        started_at: str | None,
        completed_at: str | None,
        output: CheckRunOutput | None,
        details_url: str | None,
    ) -> ActionResult[CheckRun]:
        check_run: dict[str, Any] = {
            "key": key,
            "name": name,
            "status": status,
            "externalId": external_id,
            "externalUpdatedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        }
        optional = {
            "conclusion": conclusion,
            "startedAt": started_at,
            "completedAt": completed_at,
            "output": output,
            "detailsUrl": details_url,
        }
        check_run.update({field: value for field, value in optional.items() if value})
        response = self.post(
            f"/repos/{self.repository_path}/check-runs",
            data={
                "headSha": head_sha,
                "checkSuite": {"key": key, "name": name, "externalId": external_id},
                "checkRun": check_run,
            },
        )
        return map_action(response, lambda raw: map_check_run(raw["checkRun"]))

    def list_check_runs_for_ref(
        self,
        ref: str,
        check_name: str | None = None,
        status: Literal["queued", "in_progress", "completed"] | None = None,
        timestamp_filter: Literal["latest", "all"] = "latest",
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[CheckRun]]:
        params: dict[str, str] = {}
        if check_name:
            params["checkName"] = check_name
        if status:
            params["status"] = status
        response = self.get(
            f"/repos/{self.repository_path}/commits/{ref}/check-runs",
            params=params,
            pagination=pagination,
            request_options=request_options,
        )
        return map_paginated_action(response, lambda raw: [map_check_run(run) for run in raw["checkRuns"]])

    def get_pull_requests(
        self,
        state: PullRequestState | None = "open",
        head: BranchName | None = None,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[PullRequest]]:
        params: dict[str, str] = {"state": state or "all"}
        if head:
            params["head"] = head.split(":", 1)[-1].removeprefix("refs/heads/")
        response = self.get(
            f"/repos/{self.repository_path}/pulls",
            params=params,
            pagination=pagination,
            request_options=request_options,
        )
        return map_paginated_action(response, lambda raw: [self._map_pull_request(pr) for pr in raw["pullRequests"]])

    def create_pull_request(self, title: str, body: str, head: str, base: str) -> ActionResult[PullRequest]:
        return self._create_pull_request({"title": title, "body": body, "head": head, "base": base})

    def create_pull_request_draft(self, title: str, body: str, head: str, base: str) -> ActionResult[PullRequest]:
        return self._create_pull_request({"title": title, "body": body, "head": head, "base": base, "draft": True})

    def _create_pull_request(self, data: dict[str, Any]) -> ActionResult[PullRequest]:
        response = self.post(f"/repos/{self.repository_path}/pulls", data=data)
        return map_action(response, self._map_pull_request)

    def update_pull_request(
        self,
        pull_request_id: str,
        title: str | None = None,
        body: str | None = None,
        state: PullRequestState | None = None,
    ) -> ActionResult[PullRequest]:
        data = {"title": title, "body": body, "state": state}
        response = self.patch(
            f"/repos/{self.repository_path}/pulls/{pull_request_id}",
            data={key: value for key, value in data.items() if value is not None},
        )
        return map_action(response, self._map_pull_request)

    def _map_pull_request(self, raw: dict[str, Any]) -> PullRequest:
        """Origin sends no web link, so it is built from the number."""
        return PullRequest(
            id=raw["number"],
            internal_id=raw["id"],
            title=raw["title"],
            body=raw["body"] or None,
            state=raw["state"],
            merged=raw["merged"],
            html_url=f"{self._web_base_url}/{self.repository_path}/pull/{raw['number']}",
            head=map_pull_request_branch(raw["head"]),
            base=map_pull_request_branch(raw["base"]),
            author=map_author(raw["author"]),
        )

    def get_full_tree(
        self,
        tree_sha: SHA,
        recursive: bool = True,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[GitTree]:
        response = self._fetch_tree(tree_sha, recursive, request_options)
        return map_action(response, map_git_tree)

    def get_git_commit(
        self,
        sha: SHA,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[GitCommitObject]:
        response = self.get(
            f"/repos/{self.repository_path}/git/commits/{sha}",
            request_options=request_options,
        )
        return map_action(response, map_git_commit_object)

    def get_commits(
        self,
        ref: str | None = None,
        pagination: PaginationParams | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[Commit]]:
        if since or until:
            raise ResourceBadRequest(detail="Origin cannot list commits by date")
        response = self.get(
            f"/repos/{self.repository_path}/commits",
            params={"sha": ref} if ref else {},
            pagination=pagination,
            request_options=request_options,
        )
        return map_paginated_action(response, lambda raw: [map_commit(commit) for commit in raw["commits"]])

    def compare_commits(
        self,
        start_sha: SHA,
        end_sha: SHA,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
        *,
        include_behind: bool = False,
    ) -> PaginatedActionResult[CommitComparison]:
        path = f"/repos/{self.repository_path}/compare/{start_sha}...{end_sha}"
        summary = self.get(path, request_options=request_options).json()
        page: PaginationParams = pagination or {"per_page": MAX_PAGE_SIZE}
        max_pages = 1 if pagination else COMPARE_MAX_PAGES
        files: list[dict[str, Any]] = []
        for _ in range(max_pages):
            response = self.get(f"{path}/files", pagination=page, request_options=request_options)
            raw = response.json()
            files.extend(raw["files"])
            if not raw["nextPageToken"]:
                break
            page = {**page, "cursor": raw["nextPageToken"]}
        return {
            "data": CommitComparison(
                ahead_by=summary["aheadBy"],
                behind_by=summary["behindBy"],
                commits=[],
                diff=[map_commit_file(file) for file in files],
            ),
            "type": PROVIDER_TYPE,
            "raw": {"data": {**summary, "files": files}, "headers": dict(response.headers)},
            "meta": {"next_cursor": raw["nextPageToken"] or None},
        }

    def get_pull_request_files(
        self,
        pull_request_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[PullRequestFile]]:
        response = self.get(
            f"/repos/{self.repository_path}/pulls/{pull_request_id}/files",
            pagination=pagination,
            request_options=request_options,
        )
        return map_paginated_action(response, lambda raw: [map_pull_request_file(file) for file in raw["files"]])

    def get_pull_request_commits(
        self,
        pull_request_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[PullRequestCommit]]:
        response = self.get(
            f"/repos/{self.repository_path}/pulls/{pull_request_id}/commits",
            pagination=pagination,
            request_options=request_options,
        )
        return map_paginated_action(
            response, lambda raw: [map_pull_request_commit(commit) for commit in raw["commits"]]
        )

    def get_pull_request_review_threads(
        self,
        pull_request_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
        *,
        include_reactions: bool = False,
    ) -> PaginatedActionResult[list[ReviewThread]]:
        """Return review threads from the pull request.

        Origin returns comments in a flat list, so a thread can span pages and all
        pages must be read. ``pagination`` and ``include_reactions`` are unused
        because Origin provides neither.
        """
        comments, response = self._all_comments(pull_request_id, request_options)
        return map_comments_page(response, comments, map_review_threads(comments))

    def _all_comments(
        self, pull_request_id: str, request_options: RequestOptions | None
    ) -> tuple[list[dict[str, Any]], requests.Response]:
        path = f"/repos/{self.repository_path}/pulls/{pull_request_id}/comments"
        comments: list[dict[str, Any]] = []
        page: PaginationParams = {"per_page": MAX_PAGE_SIZE}
        while True:
            response = self.get(path, pagination=page, request_options=request_options)
            raw = response.json()
            comments.extend(raw["comments"])
            if not raw["nextPageToken"]:
                return comments, response
            page = {"per_page": MAX_PAGE_SIZE, "cursor": raw["nextPageToken"]}


def map_app_installation(raw: dict[str, Any]) -> AppInstallation:
    """A write scope also grants its read scope."""
    scopes = set(raw["scopes"])
    return AppInstallation(
        has_read_access=bool(scopes & {"repository:contents:read", "repository:contents:write"}),
        has_write_access={"repository:contents:write", "repository:pull_requests:write"} <= scopes,
        has_check_run_write_access="repository:checks:write" in scopes,
    )


def map_author(raw: dict[str, Any]) -> Author:
    if user := raw.get("user"):
        return Author(id=user["id"], username=user.get("handle") or user.get("displayName", ""))
    if app := raw.get("app"):
        return Author(id=app["id"], username=app.get("displayName", ""))
    return Author(id=raw["serviceAccount"]["id"], username="")


def map_repository(raw: dict[str, Any]) -> GitRepository:
    return GitRepository(
        full_name=raw["fullName"],
        default_branch=raw["defaultBranch"],
        clone_url=raw["cloneUrl"],
        private=raw["visibility"] in _PRIVATE_VISIBILITIES,
        size=None,
        description=None,
        topics=[],
    )


def map_git_ref(raw: dict[str, Any]) -> GitRef:
    return GitRef(ref=raw["ref"].removeprefix("refs/heads/"), sha=raw["object"]["sha"])


def map_file_content(raw: dict[str, Any]) -> FileContent:
    return FileContent(
        path=raw["path"],
        sha=raw["sha"],
        content=raw.get("content", ""),
        encoding=raw.get("encoding", ""),
        size=int(raw["size"]),
        type=CURSOR_ORIGIN_FILE_TYPE_MAP[raw["type"]],
    )


def map_tree_entry(raw: dict[str, Any]) -> TreeEntry:
    size = raw.get("size")
    return TreeEntry(
        path=raw["path"],
        mode=raw["mode"],
        type=raw["type"],
        sha=raw["sha"],
        size=int(size) if size is not None else None,
    )


def map_git_tree(raw: dict[str, Any]) -> GitTree:
    return GitTree(
        sha=raw["sha"],
        tree=[map_tree_entry(entry) for entry in raw["tree"]],
        truncated=raw["truncated"],
    )


def _require_tarball(archive_format: ArchiveFormat) -> None:
    if archive_format != "tarball":
        raise ResourceBadRequest(detail=f"Origin archives are tarballs, not {archive_format}")


def map_check_run(raw: dict[str, Any]) -> CheckRun:
    completed = raw["status"] == "completed"
    return CheckRun(
        id=raw["id"],
        name=raw["name"],
        status=CURSOR_ORIGIN_STATUS_MAP[raw["status"]],
        conclusion=CURSOR_ORIGIN_CONCLUSION_MAP[raw["conclusion"]] if completed else None,
        html_url=raw["detailsUrl"],
    )


def map_pull_request_branch(raw: dict[str, Any]) -> PullRequestBranch:
    return PullRequestBranch(sha=raw["sha"] or None, ref=raw["ref"].removeprefix("refs/heads/"))


def map_git_commit_object(raw: dict[str, Any]) -> GitCommitObject:
    return GitCommitObject(
        sha=raw["sha"],
        tree=GitCommitTree(sha=raw["tree"]["sha"]),
        message=raw["message"],
    )


def map_commit_author(raw: dict[str, Any]) -> CommitAuthor:
    return CommitAuthor(
        name=raw["name"],
        email=raw["email"],
        date=datetime.fromisoformat(raw["date"]) if raw["date"] else None,
    )


def map_commit(raw: dict[str, Any]) -> Commit:
    return Commit(
        id=raw["sha"],
        message=raw["commit"]["message"],
        author=map_commit_author(raw["commit"]["author"]),
        additions=None,
        deletions=None,
    )


def map_commit_file(raw: dict[str, Any]) -> CommitFile:
    return CommitFile(
        filename=raw["filename"],
        status=CURSOR_ORIGIN_FILE_STATUS_MAP.get(raw["status"], "unknown"),
        patch=raw["patch"] or None,
        additions=raw["additions"],
        deletions=raw["deletions"],
        previous_filename=raw.get("previousFilename"),
    )


def map_pull_request_file(raw: dict[str, Any]) -> PullRequestFile:
    return PullRequestFile(
        filename=raw["filename"],
        status=CURSOR_ORIGIN_FILE_STATUS_MAP.get(raw["status"], "unknown"),
        patch=raw["patch"] or None,
        changes=raw["changes"],
        sha="",
        previous_filename=raw.get("previousFilename"),
    )


def map_pull_request_commit(raw: dict[str, Any]) -> PullRequestCommit:
    return PullRequestCommit(
        sha=raw["sha"],
        message=raw["commit"]["message"],
        author=map_commit_author(raw["commit"]["author"]),
    )


def map_comment(raw: dict[str, Any]) -> Comment:
    return Comment(
        id=raw["id"],
        body=raw["body"],
        author=map_author(raw["author"]),
        created_at=raw["createdAt"],
        author_association=None,
    )


def _is_general_discussion(raw: dict[str, Any]) -> bool:
    return not raw["thread"]["path"]


def map_review_thread_comment(raw: dict[str, Any]) -> ReviewThreadComment:
    return ReviewThreadComment(
        id=raw["id"],
        unique_id=raw["id"],
        body=raw["body"],
        author=map_author(raw["author"]),
        is_bot="user" not in raw["author"],
        created_at=raw["createdAt"],
        updated_at=raw["updatedAt"],
        is_minimized=False,
        commit_sha=raw["thread"]["version"]["headSha"],
    )


def map_review_threads(comments: list[dict[str, Any]]) -> list[ReviewThread]:
    """Map Origin's flat comments into review threads.

    The first comment contains the thread state.
    """
    grouped: dict[str, list[dict[str, Any]]] = {}
    for raw in comments:
        if not _is_general_discussion(raw):
            grouped.setdefault(raw["thread"]["id"], []).append(raw)

    threads = []
    for thread_id, thread_comments in grouped.items():
        thread = thread_comments[0]["thread"]
        start, end = thread["startLine"], thread["endLine"]
        threads.append(
            ReviewThread(
                id=thread_id,
                is_resolved=bool(thread.get("resolvedAt")),
                is_outdated=False,
                file_path=thread["path"],
                line=(end or start) or None,
                start_line=start if end else None,
                comments=[map_review_thread_comment(raw) for raw in thread_comments],
            )
        )
    return threads


def map_action[T](
    response: requests.Response,
    fn: Callable[[dict[str, Any]], T],
    raw: dict[str, Any] | None = None,
) -> ActionResult[T]:
    raw = response.json() if raw is None else raw
    return {
        "data": fn(raw),
        "type": PROVIDER_TYPE,
        "raw": {"data": raw, "headers": dict(response.headers)},
        "meta": {},
    }


def map_paginated_action[T](response: requests.Response, fn: Callable[[dict[str, Any]], T]) -> PaginatedActionResult[T]:
    raw = response.json()
    return {
        "data": fn(raw),
        "type": PROVIDER_TYPE,
        "raw": {"data": raw, "headers": dict(response.headers)},
        "meta": {"next_cursor": raw["nextPageToken"] or None},
    }


def map_comments_page[T](
    response: requests.Response, comments: list[dict[str, Any]], data: T
) -> PaginatedActionResult[T]:
    """Build a paginated result containing all comments read for the pull request."""
    return {
        "data": data,
        "type": PROVIDER_TYPE,
        "raw": {"data": {"comments": comments}, "headers": dict(response.headers)},
        "meta": {"next_cursor": None},
    }
