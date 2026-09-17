"""Perforce (Helix Core) provider, read-only.

Perforce speaks the P4 RPC protocol, not HTTP, so this provider addresses
synthetic routes that the injected ``ApiClient`` turns into ``p4`` commands.
Tagged routes return a JSON list of p4 records; ``/print`` returns
``{"stat": ..., "content_base64": ...}``; ``/archive`` streams gzip.

A changelist number (as a string) is both commit SHA and tree SHA. A branch is
a depot path or a stream beneath it.
"""

import base64
import datetime
import re
from collections import defaultdict
from collections.abc import Callable
from typing import Any
from urllib.parse import quote

import requests

from scm.errors import MalformedExternalId, error_class_for_status
from scm.types import (
    SHA,
    ActionResult,
    ApiClient,
    AppInstallation,
    ArchiveFormat,
    BranchName,
    Commit,
    CommitAuthor,
    CommitFile,
    CommitWithChanges,
    CredentialsSet,
    FileContent,
    FileStatus,
    GitCommitObject,
    GitCommitTree,
    GitRef,
    GitRepository,
    GitTree,
    PaginatedActionResult,
    PaginatedResponseMeta,
    PaginationParams,
    Repository,
    RequestOptions,
    TreeEntry,
    TreeEntryMode,
)

_INDEXED_FIELD = re.compile(r"^(?P<name>.*?)(?P<index>\d+)$")

_SYMLINK_BASE_TYPES = frozenset({"symlink"})

# Applied server-side by ``p4 fstat -F``, so binary assets never cross the wire.
TEXT_TYPE_FILTER = "headType=text*"

_ACTION_TO_STATUS: dict[str, FileStatus] = {
    "add": "added",
    "edit": "modified",
    "delete": "removed",
    "branch": "added",
    "integrate": "modified",
    "import": "added",
    "purge": "removed",
    "archive": "unchanged",
    "move/add": "renamed",
    "move/delete": "removed",
}


class Perforce:
    archive = "/archive"
    changes = "/changes"
    describe = "/describe"
    files = "/files"
    fstat = "/fstat"
    print_file = "/print"


def split_base_type(head_type: str) -> str:
    return head_type.split("+", 1)[0].lower()


def map_tree_entry_mode(head_type: str) -> TreeEntryMode:
    # Perforce has no mode bits; executability and symlink-ness live in the type.
    if split_base_type(head_type) in _SYMLINK_BASE_TYPES:
        return "120000"
    modifiers = head_type.split("+", 1)[1] if "+" in head_type else ""
    return "100755" if "x" in modifiers else "100644"


def map_action_to_status(action: str) -> FileStatus:
    return _ACTION_TO_STATUS.get(action.lower(), "unknown")


def parse_p4_time(raw: str | None) -> datetime.datetime | None:
    if not raw:
        return None
    try:
        return datetime.datetime.fromtimestamp(int(raw), tz=datetime.UTC)
    except (TypeError, ValueError):
        return None


def map_commit_author(raw: dict[str, Any]) -> CommitAuthor:
    # p4 reports only a login; the client enriches where it can, and ``email`` is required.
    login = raw.get("user", "")
    return CommitAuthor(
        name=raw.get("userFullName") or login,
        email=raw.get("userEmail") or login,
        date=parse_p4_time(raw.get("time")),
    )


def map_commit(raw: dict[str, Any]) -> Commit:
    commit = Commit(
        id=str(raw.get("change", "")),
        message=raw.get("desc", ""),
        author=map_commit_author(raw),
        additions=None,
        deletions=None,
    )
    if login := raw.get("user"):
        commit["author_login"] = login
    return commit


def iter_indexed_records(raw: dict[str, Any], prefix: str) -> list[dict[str, Any]]:
    """Regroup ``p4 describe``'s flattened ``depotFile0``/``action0``/... fields by index."""
    grouped: dict[int, dict[str, Any]] = defaultdict(dict)
    for key, value in raw.items():
        # Non-greedy name, so the index takes every trailing digit: "depotFile10" is 10, not 0.
        if match := _INDEXED_FIELD.match(key):
            grouped[int(match.group("index"))][match.group("name")] = value
    return [grouped[index] for index in sorted(grouped) if prefix in grouped[index]]


def map_commit_file(raw: dict[str, Any]) -> CommitFile:
    return CommitFile(
        filename=raw.get("depotFile", ""),
        status=map_action_to_status(raw.get("action", "")),
        patch=None,
        additions=None,
        deletions=None,
        previous_filename=None,
    )


def map_tree_entry(raw: dict[str, Any]) -> TreeEntry:
    file_size = raw.get("fileSize")
    return TreeEntry(
        path=raw.get("depotFile", ""),
        mode=map_tree_entry_mode(raw.get("headType", "")),
        type="blob",
        sha=raw.get("digest", ""),
        size=int(file_size) if file_size is not None else None,
    )


def make_result[T](map_item: Callable[[dict[str, Any]], T], raw: dict[str, Any]) -> ActionResult[T]:
    return ActionResult(data=map_item(raw), type="perforce", raw={"data": raw, "headers": None}, meta={})


def make_paginated_result[T](
    map_item: Callable[[dict[str, Any]], T], raw: list[dict[str, Any]]
) -> PaginatedActionResult[list[T]]:
    # ``p4 changes -m N`` caps server-side and offers no cursor to resume from.
    return PaginatedActionResult(
        data=[map_item(item) for item in raw],
        type="perforce",
        raw={"data": raw, "headers": None},
        meta=PaginatedResponseMeta(next_cursor=None),
    )


class PerforceProvider:
    def __init__(self, client: ApiClient, organization_id: int, repository: Repository) -> None:
        self.client = client
        self.organization_id = organization_id
        self.repository = repository

        external_id = repository["external_id"]
        if not external_id or not external_id.startswith("//"):
            raise MalformedExternalId()

        self.depot_path = external_id.rstrip("/")
        self.web_base_url = repository["web_base_url"]

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
            # A synthesized Response may carry no PreparedRequest.
            request = getattr(response, "request", None)
            raise error_class_for_status(response.status_code)(
                detail=response.content.decode("utf-8", errors="replace"),
                status_code=response.status_code,
                response_content=response.content.decode("utf-8", errors="replace"),
                request_headers=getattr(request, "headers", None),
                request_body=getattr(request, "body", None),
                request_url=getattr(request, "url", None),
                request_method=getattr(request, "method", None),
            )
        return response

    def get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        request_options: RequestOptions | None = None,
        stream: bool = True,
    ) -> requests.Response:
        options = request_options or {}
        return self.request("GET", path=path, params=params or {}, stream=stream, timeout=options.get("timeout"))

    def contain(self, depot_path: str) -> str:
        """Refuse an absolute depot path outside this repository.

        Branch names and file paths both reach the provider from outside -- user
        settings and agent tool calls -- so an absolute path elsewhere on the
        server would otherwise read a depot the organization never connected. The
        trailing separator is what stops "//SentryDemoEvil" matching "//SentryDemo".
        """
        if depot_path != self.depot_path and not depot_path.startswith(f"{self.depot_path}/"):
            raise MalformedExternalId()
        return depot_path

    def depot_scope(self, branch: BranchName | None = None) -> str:
        if not branch:
            return f"{self.depot_path}/..."
        if branch.startswith("//"):
            return f"{self.contain(branch.rstrip('/'))}/..."
        return f"{self.depot_path}/{branch.strip('/')}/..."

    def resolve_path(self, path: str) -> str:
        if path.startswith("//"):
            return self.contain(path)
        return f"{self.depot_path}/{path.lstrip('/')}"

    def get_app_installation(self) -> ActionResult[AppInstallation]:
        # Probe the depot: authenticating is not the same as being allowed to read it.
        self.get(Perforce.files, params={"path": self.depot_scope(), "max": "1"})
        return ActionResult(
            data=AppInstallation(has_read_access=True, has_write_access=False, has_check_run_write_access=False),
            type="perforce",
            raw={"data": None, "headers": None},
            meta={},
        )

    def get_repository(self) -> ActionResult[GitRepository]:
        # ``size`` would cost a whole-depot ``p4 sizes`` per construction; nothing needs it.
        return ActionResult(
            data=GitRepository(
                full_name=self.depot_path,
                default_branch=self.depot_path,
                clone_url=self.depot_path,
                private=True,
                size=0,
                description=None,
                topics=[],
            ),
            type="perforce",
            raw={"data": None, "headers": None},
            meta={},
        )

    def get_branch(self, branch: BranchName, request_options: RequestOptions | None = None) -> ActionResult[GitRef]:
        response = self.get(
            Perforce.changes,
            params={"path": self.depot_scope(branch), "max": "1"},
            request_options=request_options,
        )
        records = response.json()
        return ActionResult(
            data=GitRef(ref=branch, sha=str(records[0]["change"]) if records else ""),
            type="perforce",
            raw={"data": records, "headers": None},
            meta={},
        )

    def get_file_content(
        self, path: str, ref: str, request_options: RequestOptions | None = None
    ) -> ActionResult[FileContent]:
        depot_file = self.resolve_path(path)
        response = self.get(
            Perforce.print_file,
            params={"path": f"{depot_file}@{ref}" if ref else depot_file},
            request_options=request_options,
        )
        return make_result(self.build_file_content, response.json())

    def build_file_content(self, raw: dict[str, Any]) -> FileContent:
        stat = raw.get("stat") or {}
        content = raw.get("content_base64") or ""
        file_size = stat.get("fileSize")
        head_type = stat.get("type") or stat.get("headType") or ""
        return FileContent(
            path=stat.get("depotFile", ""),
            sha=stat.get("digest", ""),
            content=content,
            encoding="base64",
            size=int(file_size) if file_size is not None else len(base64.b64decode(content)),
            type="symlink" if split_base_type(head_type) in _SYMLINK_BASE_TYPES else "file",
        )

    def fetch_tree_entries(self, tree_sha: SHA, request_options: RequestOptions | None = None) -> list[dict[str, Any]]:
        response = self.get(
            Perforce.fstat,
            params={
                "path": f"{self.depot_scope()}@{tree_sha}" if tree_sha else self.depot_scope(),
                "filter": TEXT_TYPE_FILTER,
                "fields": "depotFile,headType,headAction,fileSize,digest",
            },
            request_options=request_options,
        )
        # A file deleted at this changelist has no content to print.
        return [record for record in response.json() if record.get("headAction") != "delete"]

    def get_tree(
        self,
        tree_sha: SHA,
        recursive: bool = True,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[GitTree]:
        # One fstat returns the whole flat manifest. ``truncated`` must stay False:
        # consumers otherwise fall back to a subtree walk that has no p4 analogue.
        entries = self.fetch_tree_entries(tree_sha, request_options)
        return PaginatedActionResult(
            data=GitTree(sha=tree_sha, tree=[map_tree_entry(entry) for entry in entries], truncated=False),
            type="perforce",
            raw={"data": entries, "headers": None},
            meta=PaginatedResponseMeta(next_cursor=None),
        )

    def get_full_tree(
        self, tree_sha: SHA, recursive: bool = True, request_options: RequestOptions | None = None
    ) -> ActionResult[GitTree]:
        entries = self.fetch_tree_entries(tree_sha, request_options)
        return ActionResult(
            data=GitTree(sha=tree_sha, tree=[map_tree_entry(entry) for entry in entries], truncated=False),
            type="perforce",
            raw={"data": entries, "headers": None},
            meta={},
        )

    def get_git_commit(self, sha: SHA, request_options: RequestOptions | None = None) -> ActionResult[GitCommitObject]:
        response = self.get(Perforce.describe, params={"change": sha}, request_options=request_options)
        records = response.json()
        raw = records[0] if records else {}
        return ActionResult(
            data=GitCommitObject(sha=sha, tree=GitCommitTree(sha=sha), message=raw.get("desc", "")),
            type="perforce",
            raw={"data": records, "headers": None},
            meta={},
        )

    def get_commit(self, sha: SHA, request_options: RequestOptions | None = None) -> ActionResult[CommitWithChanges]:
        response = self.get(Perforce.describe, params={"change": sha}, request_options=request_options)
        records = response.json()
        raw = records[0] if records else {}
        files = [map_commit_file(record) for record in iter_indexed_records(raw, "depotFile")]
        return ActionResult(
            data=CommitWithChanges(**map_commit(raw), files=files),
            type="perforce",
            raw={"data": records, "headers": None},
            meta={},
        )

    def get_commit_changes(
        self,
        sha: SHA,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[CommitFile]]:
        response = self.get(Perforce.describe, params={"change": sha}, request_options=request_options)
        records = response.json()
        raw = records[0] if records else {}
        return make_paginated_result(map_commit_file, iter_indexed_records(raw, "depotFile"))

    def get_commits_by_path(
        self,
        path: str,
        ref: str | None = None,
        pagination: PaginationParams | None = None,
        since: datetime.datetime | None = None,
        until: datetime.datetime | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[Commit]]:
        params: dict[str, Any] = {"path": self.resolve_path(path)}
        if pagination:
            params["max"] = str(pagination["per_page"])
        if ref:
            params["ref"] = ref
        if since:
            params["since"] = since.date().isoformat()
        if until:
            params["until"] = until.date().isoformat()
        response = self.get(Perforce.changes, params=params, request_options=request_options)
        return make_paginated_result(map_commit, response.json())

    def get_file_url(self, file_path: str, sha: SHA, start_line: int | None = None, end_line: int | None = None) -> str:
        depot_file = self.resolve_path(file_path)
        if not self.web_base_url:
            # No web UI configured; a depot path can at least be pasted into p4.
            return f"{depot_file}@{sha}" if sha else depot_file

        url = f"{self.web_base_url.rstrip('/')}/files/{quote(depot_file.lstrip('/'), safe='/')}"
        if sha:
            url = f"{url}?v=@{sha}"
        if start_line is not None:
            url = f"{url}#L{start_line}" + (f"-L{end_line}" if end_line is not None else "")
        return url

    def get_commit_url(self, commit_sha: SHA) -> str:
        if not self.web_base_url:
            return f"{self.depot_path}@{commit_sha}"
        return f"{self.web_base_url.rstrip('/')}/changes/{commit_sha}"

    def get_commits_url(
        self,
        commit_sha: SHA,
        *,
        file_path: str | None = None,
        since: datetime.date | None = None,
        until: datetime.date | None = None,
    ) -> str:
        # Swarm's file view already renders a path's revision history.
        if file_path:
            return self.get_file_url(file_path, commit_sha)
        return self.get_commit_url(commit_sha)

    def download_archive(
        self,
        ref: str,
        archive_format: ArchiveFormat = "tarball",
        request_options: RequestOptions | None = None,
    ) -> requests.Response:
        # Returned unread so the caller streams it.
        return self.get(
            Perforce.archive,
            params={"path": self.depot_scope(), "change": ref, "filter": TEXT_TYPE_FILTER},
            request_options=request_options,
            stream=True,
        )
