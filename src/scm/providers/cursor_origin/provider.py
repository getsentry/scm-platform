from collections.abc import Callable
from typing import Any

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
    ArchiveFormat,
    ArchiveLink,
    Author,
    BranchName,
    ChmodCommitAction,
    Commit,
    CommitAuthor,
    CommitAuthorParam,
    CredentialsSet,
    DeleteCommitAction,
    FileContent,
    FileContentType,
    GitRef,
    GitRepository,
    GitTree,
    MoveCommitAction,
    PaginatedActionResult,
    PaginationParams,
    ProviderName,
    PullRequest,
    PullRequestBranch,
    PullRequestState,
    Repository,
    RequestOptions,
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

    def get_tree(
        self,
        tree_sha: SHA,
        recursive: bool = True,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[GitTree]:
        params = {"recursive": "true"} if recursive else {}
        response = self.get(
            f"/repos/{self.repository_path}/git/trees/{tree_sha}",
            params=params,
            request_options=request_options,
        )
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


def map_pull_request_branch(raw: dict[str, Any]) -> PullRequestBranch:
    return PullRequestBranch(sha=raw["sha"] or None, ref=raw["ref"].removeprefix("refs/heads/"))


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
