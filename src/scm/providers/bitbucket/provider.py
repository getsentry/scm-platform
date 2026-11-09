import base64
import binascii
import hashlib
import posixpath
import re
from collections.abc import Callable, Iterator
from datetime import UTC, date, datetime
from typing import Any
from urllib.parse import parse_qs, quote, unquote, urlparse

import requests
from urllib3 import encode_multipart_formdata

from scm.errors import (
    PathIsDirectory,
    PathIsNotDirectory,
    ReadmeNotFound,
    ResourceBadRequest,
    ResourceConflict,
    ResourceNotFound,
    SCMCodedError,
    StaleBranchHead,
    error_class_for_status,
)
from scm.helpers import iter_all_pages
from scm.types import (
    SHA,
    ActionResult,
    ApiClient,
    AppInstallation,
    BranchName,
    ChmodCommitAction,
    Commit,
    CommitAuthor,
    CommitAuthorParam,
    CommitComparison,
    CommitFile,
    CommitWithChanges,
    CredentialsSet,
    DeleteCommitAction,
    FileContent,
    FileContentType,
    FileStatus,
    GitRemote,
    GitRepository,
    MoveCommitAction,
    PaginatedActionResult,
    PaginationParams,
    ProviderName,
    Repository,
    RequestOptions,
    WriteCommitAction,
)

PROVIDER_TYPE: ProviderName = "bitbucket"
BITBUCKET_WEB_BASE_URL = "https://bitbucket.org"
# The cursor `iter_all_pages` sends for the first page. Endpoints that page by an opaque
# token reject `page=1`, so the first page takes no `page` at all.
FIRST_PAGE_CURSOR = "1"
MAX_PAGE_SIZE = 100

# Granted scopes come back in the `x-oauth-scopes` response header. Connect apps and OAuth
# consumers use the short names; API tokens use the `<action>:<resource>:bitbucket` names.
# A write scope also grants its read scope.
_REPOSITORY_READ_SCOPES = {
    "repository",
    "repository:write",
    "repository:admin",
    "read:repository:bitbucket",
    "write:repository:bitbucket",
    "admin:repository:bitbucket",
}
_REPOSITORY_WRITE_SCOPES = {
    "repository:write",
    "repository:admin",
    "write:repository:bitbucket",
    "admin:repository:bitbucket",
}
_PULL_REQUEST_WRITE_SCOPES = {"pullrequest:write", "write:pullrequest:bitbucket"}

VALID_README_FILES = {"readme", "readme.md", "readme.txt", "readme.rst"}
# Bitbucket has no pull request template file of its own, so follow GitHub's layout.
PULL_REQUEST_TEMPLATE_PARENT_DIRS = (".github", "", "docs")
PULL_REQUEST_TEMPLATE_FILENAME = "pull_request_template.md"
PULL_REQUEST_TEMPLATE_DIRNAME = "pull_request_template"
_FILE_STATUSES: dict[str, FileStatus] = {
    "added": "added",
    "removed": "removed",
    "modified": "modified",
    "renamed": "renamed",
}
_RAW_AUTHOR = re.compile(r"^(?P<name>.*?)\s*<(?P<email>[^>]*)>\s*$")


class BitbucketProvider:
    def __init__(
        self,
        client: ApiClient,
        organization_id: int,
        repository: Repository,
        web_base_url: str = BITBUCKET_WEB_BASE_URL,
    ) -> None:
        self.client = client
        self.organization_id = organization_id
        self.repository = repository
        self._web_base_url = web_base_url

    @property
    def repository_path(self) -> str:
        """The repository's `{workspace}/{repo_slug}`."""
        return self.repository["name"]

    def request(
        self,
        method: str,
        path: str,
        headers: dict[str, str] | None = None,
        data: dict[str, Any] | bytes | None = None,
        params: dict[str, str] | None = None,
        allow_redirects: bool | None = None,
        stream: bool = True,
        raw_response: bool = True,
        credentials_set: CredentialsSet = "installation",
        timeout: float | tuple[float, float] | None = None,
    ) -> requests.Response:
        # `requests` collapses dot segments, which could point the call at another repository.
        if any(unquote(segment) in {".", ".."} for segment in path.split("/")):
            raise ResourceBadRequest(detail=path)

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
    ) -> requests.Response:
        options = request_options or {}
        params = dict(params or {})
        if pagination is not None:
            if per_page := pagination.get("per_page"):
                params["pagelen"] = str(min(per_page, MAX_PAGE_SIZE))
            cursor = pagination.get("cursor")
            if cursor and cursor != FIRST_PAGE_CURSOR:
                params["page"] = cursor

        return self.request(
            "GET",
            path=path,
            params=params,
            allow_redirects=allow_redirects,
            timeout=options.get("timeout"),
        )

    def post(self, path: str, data: dict[str, Any]) -> requests.Response:
        return self.request("POST", path=path, data=data)

    def put(self, path: str, data: dict[str, Any]) -> requests.Response:
        return self.request("PUT", path=path, data=data)

    def delete(self, path: str) -> requests.Response:
        return self.request("DELETE", path=path)

    def get_repository(self) -> ActionResult[GitRepository]:
        response = self.get(f"/2.0/repositories/{self.repository_path}")
        return map_action(response, map_repository)

    def get_app_installation(self) -> ActionResult[AppInstallation]:
        """Report the access the integration's credentials were granted on this repository.

        Bitbucket has no installation resource, so read the scopes off a repository request.
        """
        response = self.get(f"/2.0/repositories/{self.repository_path}")
        scopes = parse_scopes(response.headers.get("x-oauth-scopes", ""))
        return map_action(response, lambda _: map_app_installation(scopes))

    def get_git_remote(self) -> GitRemote:
        return GitRemote(url=f"{self._web_base_url}/{self.repository_path}.git", basic_auth_username="x-token-auth")

    def get_file_url(
        self,
        file_path: str,
        sha: SHA,
        start_line: int | None = None,
        end_line: int | None = None,
    ) -> str:
        url = f"{self._web_base_url}/{self.repository_path}/src/{sha}/{quote(file_path)}"
        if start_line and end_line:
            url += f"#lines-{start_line}:{end_line}"
        elif start_line or end_line:
            url += f"#lines-{start_line or end_line}"
        return url

    def get_file_content(
        self,
        path: str,
        ref: str,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[FileContent]:
        commit = self._resolve_ref(ref, request_options)
        response = self.get(self._src_path(commit, path), request_options=request_options)
        if _is_listing(response):
            raise PathIsDirectory(detail=path)
        return {
            "data": map_file_content(path, response.content),
            "type": PROVIDER_TYPE,
            "raw": {"data": None, "headers": dict(response.headers)},
            "meta": {},
        }

    def get_directory_contents(
        self,
        path: str,
        ref: str | None = None,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[FileContent]]:
        commit = self._resolve_ref(ref, request_options) if ref else "HEAD"
        response = self.get(
            self._src_path(commit, path).rstrip("/") + "/",
            pagination=pagination,
            request_options=request_options,
        )
        if not _is_listing(response):
            raise PathIsNotDirectory(detail=path)
        return map_paginated_action(response, map_directory_entry)

    def get_readme(
        self,
        ref: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[FileContent]:
        commit = self._resolve_ref(ref, request_options)
        for entry in self._walk_directory("", commit, pagination, request_options):
            if entry["type"] == "file" and entry["path"].lower() in VALID_README_FILES:
                return self.get_file_content(entry["path"], commit, request_options)
        raise ReadmeNotFound()

    def get_pull_request_template(
        self,
        ref: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> Iterator[ActionResult[FileContent]]:
        commit = self._resolve_ref(ref, request_options)
        for parent in PULL_REQUEST_TEMPLATE_PARENT_DIRS:
            for entry in self._walk_directory(parent, commit, pagination, request_options):
                basename = entry["path"].rsplit("/", 1)[-1].lower()
                if entry["type"] == "file" and basename == PULL_REQUEST_TEMPLATE_FILENAME:
                    yield self.get_file_content(entry["path"], commit, request_options)
                elif entry["type"] == "directory" and basename == PULL_REQUEST_TEMPLATE_DIRNAME:
                    for child in self._walk_directory(entry["path"], commit, pagination, request_options):
                        if child["type"] == "file" and child["path"].lower().endswith(".md"):
                            yield self.get_file_content(child["path"], commit, request_options)

    def _walk_directory(
        self,
        path: str,
        commit: str,
        pagination: PaginationParams | None,
        request_options: RequestOptions | None,
    ) -> Iterator[FileContent]:
        try:
            for page in iter_all_pages(
                lambda p: self.get_directory_contents(path, commit, p, request_options),
                **(pagination or {}),
            ):
                yield from page["data"]
        except SCMCodedError as e:
            if e.code not in ("resource_not_found", "path_is_not_directory"):
                raise

    def _resolve_ref(self, ref: str, request_options: RequestOptions | None) -> str:
        """Look up the commit for a ref with a slash, since `src` takes everything after the first slash as the path."""
        if "/" not in ref:
            return ref
        response = self.get(
            f"/2.0/repositories/{self.repository_path}/commits/{quote(ref)}",
            params={"pagelen": "1", "fields": "values.hash"},
            request_options=request_options,
        )
        if not (values := response.json()["values"]):
            raise ResourceNotFound(detail=ref)
        return values[0]["hash"]

    def _src_path(self, commit: str, path: str) -> str:
        return f"/2.0/repositories/{self.repository_path}/src/{quote(commit, safe='')}/{quote(path.strip('/'))}"

    def get_commit_url(self, commit_sha: SHA) -> str:
        return f"{self._web_base_url}/{self.repository_path}/commits/{commit_sha}"

    def get_commits_url(
        self,
        commit_sha: SHA,
        *,
        file_path: str | None = None,
        since: date | None = None,
        until: date | None = None,
    ) -> str:
        """Link to a file's history at the commit, or else the repository's commits page.

        Bitbucket's history pages take no dates, and its commits page can't start at a commit.
        """
        if file_path is None:
            return f"{self._web_base_url}/{self.repository_path}/commits"
        return f"{self._web_base_url}/{self.repository_path}/history-node/{commit_sha}/{quote(file_path)}"

    def get_commit(
        self,
        sha: SHA,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[CommitWithChanges]:
        """Return a commit with its first page of changed files."""
        response = self.get(f"/2.0/repositories/{self.repository_path}/commit/{sha}", request_options=request_options)
        changes = self.get_commit_changes(sha, {"per_page": MAX_PAGE_SIZE}, request_options)
        files = changes["data"]
        complete = changes["meta"]["next_cursor"] is None
        additions = sum(f["additions"] or 0 for f in files) if complete else None
        deletions = sum(f["deletions"] or 0 for f in files) if complete else None
        return map_action(response, lambda raw: CommitWithChanges(**map_commit(raw, additions, deletions), files=files))

    def get_commit_changes(
        self,
        sha: SHA,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[CommitFile]]:
        """Read one page of a diffstat, with each file's patch cut from the full diff."""
        # Filtering the diff by the page's paths fails: Bitbucket rejects query strings over about 4KB.
        repository_api = f"/2.0/repositories/{self.repository_path}"
        response = self.get(f"{repository_api}/diffstat/{sha}", pagination=pagination, request_options=request_options)
        patches = split_patches(self.get(f"{repository_api}/diff/{sha}", request_options=request_options).text)
        return map_paginated_action(response, lambda raw: map_commit_file(raw, patches))

    def get_commits(
        self,
        ref: str | None = None,
        pagination: PaginationParams | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[Commit]]:
        return self._get_commits(ref, {}, pagination, since, until, request_options)

    def get_commits_by_path(
        self,
        path: str,
        ref: str | None = None,
        pagination: PaginationParams | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[Commit]]:
        return self._get_commits(ref, {"path": path}, pagination, since, until, request_options)

    def compare_commits(
        self,
        start_sha: SHA,
        end_sha: SHA,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
        *,
        include_behind: bool = False,
    ) -> PaginatedActionResult[CommitComparison]:
        """Compare two commits against their merge base.

        Bitbucket returns no counts, so they're the length of a commit list and cap at one page.
        `behind_by` takes a second request and is only counted when the commits aren't paged.
        """
        if include_behind and pagination is not None:
            raise ResourceBadRequest(
                detail="'include_behind' cannot be combined with 'pagination': 'ahead_by' would count one page "
                "while 'behind_by' counts the whole range.",
            )
        commits_path = f"/2.0/repositories/{self.repository_path}/commits"
        response = self.get(
            commits_path,
            params={"include": end_sha, "exclude": start_sha},
            pagination=pagination or {"per_page": MAX_PAGE_SIZE},
            request_options=request_options,
        )
        raw = response.json()
        commits = [map_commit(commit) for commit in raw["values"]]
        # A range spec diffs the first commit against the merge base, so the head goes first.
        spec = f"{end_sha}..{start_sha}"
        repository_api = f"/2.0/repositories/{self.repository_path}"
        patches = split_patches(self.get(f"{repository_api}/diff/{spec}", request_options=request_options).text)
        diffstats = list(
            iter_all_pages(
                lambda page: map_paginated_action(
                    self.get(f"{repository_api}/diffstat/{spec}", pagination=page, request_options=request_options),
                    lambda file: map_commit_file(file, patches),
                ),
                per_page=MAX_PAGE_SIZE,
            )
        )
        diff = [file for page in diffstats for file in page["data"]]
        comparison = CommitComparison(ahead_by=len(commits), commits=commits, diff=diff)
        if pagination is None:
            behind = self.get(
                commits_path,
                params={"include": start_sha, "exclude": end_sha},
                pagination={"per_page": MAX_PAGE_SIZE},
                request_options=request_options,
            )
            comparison["behind_by"] = len(behind.json()["values"])
        return {
            "data": comparison,
            "type": PROVIDER_TYPE,
            "raw": {
                "data": {**raw, "diffstat": [file for page in diffstats for file in page["raw"]["data"]["values"]]},
                "headers": dict(response.headers),
            },
            "meta": {"next_cursor": next_cursor(raw)},
        }

    def _get_commits(
        self,
        ref: str | None,
        params: dict[str, str],
        pagination: PaginationParams | None,
        since: datetime | None,
        until: datetime | None,
        request_options: RequestOptions | None,
    ) -> PaginatedActionResult[list[Commit]]:
        # Without a revision Bitbucket lists commits on every branch; HEAD is the main branch.
        revision = quote(ref or "HEAD", safe="/")
        while True:
            response = self.get(
                f"/2.0/repositories/{self.repository_path}/commits/{revision}",
                params=params,
                pagination=pagination,
                request_options=request_options,
            )
            result = map_paginated_action(response, map_commit)
            if since is None and until is None:
                return result
            # Commits aren't strictly date-ordered, so only a page that's entirely past `since` ends the walk.
            # Bitbucket's commit `date` is the committer date; the API has no author date.
            dates = [datetime.fromisoformat(raw["date"]) for raw in result["raw"]["data"].get("values", [])]
            start, end = as_utc(since or datetime.min), as_utc(until or datetime.max)
            result["data"] = [commit for commit, d in zip(result["data"], dates, strict=True) if start <= d <= end]
            if dates and all(d < start for d in dates):
                result["meta"]["next_cursor"] = None
            # Callers stop at an empty page, so skip the ones that only hold commits after `until`.
            if result["data"] or not (cursor := result["meta"]["next_cursor"]):
                return result
            pagination = {**(pagination or {}), "cursor": cursor}

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
        """Commit `actions` onto `branch`. See :func:`scm.actions.create_commit`.

        Bitbucket only commits at the branch head, so `parent_sha` must match it.
        """
        if force:
            raise ResourceBadRequest(detail="Bitbucket cannot force a branch to a new commit.")
        if expected_head_sha is not None:
            if create_branch:
                raise ResourceBadRequest(
                    detail="'expected_head_sha' cannot be combined with 'create_branch': the branch has no head yet.",
                )
            if expected_head_sha != parent_sha:
                raise ResourceBadRequest(detail="Bitbucket commits onto the branch head, so it must be 'parent_sha'.")

        # Bitbucket creates a missing branch from `parents` rather than failing.
        exists = self._branch_exists(branch)
        if create_branch and exists:
            raise ResourceConflict(detail=f"Branch '{branch}' already exists.")
        if not create_branch and not exists:
            raise ResourceNotFound(detail=f"Branch '{branch}' does not exist.")

        fields: list[tuple[str, str | tuple[str, bytes, str]]] = [
            ("message", message),
            ("branch", branch),
            ("parents", parent_sha),
        ]
        if author is not None:
            fields.append(("author", f"{author['name']} <{author['email']}>"))
        for action in actions:
            if isinstance(action, WriteCommitAction):
                fields.append(file_part(action.filename, decode_content(action)))
            elif isinstance(action, DeleteCommitAction):
                fields.append(("files", action.filename))
            elif isinstance(action, MoveCommitAction):
                fields.append(("files", action.old_filename))
                fields.append(file_part(action.new_filename, self._file_content(action.old_filename, parent_sha)))
            else:
                raise ResourceBadRequest(detail="Bitbucket cannot change a file's mode through its commit API.")

        body, content_type = encode_multipart_formdata(fields)
        try:
            response = self.request(
                "POST",
                f"/2.0/repositories/{self.repository_path}/src",
                headers={"Content-Type": content_type},
                data=body,
            )
        except ResourceBadRequest as e:
            if "is not the head of branch" not in e.detail:
                raise
            raise StaleBranchHead(detail=f"Branch '{branch}' is not at {parent_sha}.") from e

        sha = response.headers["Location"].rstrip("/").rsplit("/", 1)[-1]
        commit_author = CommitAuthor(name=author["name"], email=author["email"], date=None) if author else None
        return {
            "data": Commit(id=sha, message=message, author=commit_author, additions=None, deletions=None),
            "type": PROVIDER_TYPE,
            "raw": {"data": None, "headers": dict(response.headers)},
            "meta": {},
        }

    def _branch_exists(self, branch: BranchName) -> bool:
        try:
            self.get(f"/2.0/repositories/{self.repository_path}/refs/branches/{quote(branch, safe='/')}")
        except ResourceNotFound:
            return False
        return True

    def _file_content(self, path: str, ref: SHA) -> bytes:
        """Read a file to rewrite it elsewhere, refusing what an upload can't recreate."""
        file_path = f"/2.0/repositories/{self.repository_path}/src/{ref}/{quote(path)}"
        # An upload can't set attributes, so a moved executable or symlink would lose them.
        # `binary` only describes the content, which the upload keeps.
        attributes = set(self.get(file_path, params={"format": "meta"}).json().get("attributes", [])) - {"binary"}
        if attributes:
            raise ResourceBadRequest(
                detail=f"Bitbucket cannot move '{path}' and keep its {', '.join(sorted(attributes))} attribute."
            )
        return self.get(file_path).content


def parse_scopes(header: str) -> set[str]:
    return {scope.strip() for scope in header.split(",") if scope.strip()}


def map_app_installation(scopes: set[str]) -> AppInstallation:
    can_write_repository = bool(scopes & _REPOSITORY_WRITE_SCOPES)
    return AppInstallation(
        has_read_access=bool(scopes & _REPOSITORY_READ_SCOPES),
        has_write_access=can_write_repository and bool(scopes & _PULL_REQUEST_WRITE_SCOPES),
        has_check_run_write_access=can_write_repository,
    )


def map_repository(raw: dict[str, Any]) -> GitRepository:
    return GitRepository(
        full_name=raw["full_name"],
        # Empty repositories have no main branch.
        default_branch=(raw.get("mainbranch") or {}).get("name", ""),
        # The `clone` links carry the requesting user's name, so don't use them.
        clone_url=f"{BITBUCKET_WEB_BASE_URL}/{raw['full_name']}.git",
        private=raw["is_private"],
        # Bitbucket reports bytes; GitHub reports kB.
        size=raw["size"] // 1000,
        description=raw.get("description") or None,
        topics=[],
    )


def _is_listing(response: requests.Response) -> bool:
    # File bodies come back as text/plain or their own media type, even for JSON files.
    return response.headers.get("content-type", "").startswith("application/json")


def map_file_content(path: str, content: bytes) -> FileContent:
    """Bitbucket doesn't expose blob ids, so hash the content the way git does."""
    return FileContent(
        path=path,
        sha=hashlib.sha1(b"blob %d\0" % len(content) + content).hexdigest(),
        content=base64.b64encode(content).decode("ascii"),
        encoding="base64",
        size=len(content),
        type="file",
    )


def map_directory_entry(raw: dict[str, Any]) -> FileContent:
    """Listings carry no blob ids or contents, so `sha` and `content` are empty."""
    attributes = raw.get("attributes") or []
    file_type: FileContentType = "file"
    if raw["type"] == "commit_directory":
        file_type = "directory"
    elif "subrepository" in attributes:
        file_type = "submodule"
    elif "link" in attributes:
        file_type = "symlink"
    return FileContent(
        path=raw["path"],
        sha="",
        content="",
        encoding="",
        size=raw.get("size", 0),
        type=file_type,
    )


def map_commit_author(raw: dict[str, Any], raw_date: str) -> CommitAuthor:
    """Parse the git author, which is a raw `Name <email>` string whether or not it links to a Bitbucket user."""
    if match := _RAW_AUTHOR.match(raw["raw"]):
        name, email = match["name"], match["email"]
    else:
        name, email = raw["raw"], ""
    return CommitAuthor(name=name, email=email, date=datetime.fromisoformat(raw_date))


def as_utc(value: datetime) -> datetime:
    return value if value.tzinfo else value.replace(tzinfo=UTC)


def map_commit(raw: dict[str, Any], additions: int | None = None, deletions: int | None = None) -> Commit:
    return Commit(
        id=raw["hash"],
        message=raw["message"],
        author=map_commit_author(raw["author"], raw["date"]),
        additions=additions,
        deletions=deletions,
    )


def map_commit_file(raw: dict[str, Any], patches: dict[str, str]) -> CommitFile:
    old, new = raw["old"], raw["new"]
    filename = (new or old)["path"]
    return CommitFile(
        filename=filename,
        status=_FILE_STATUSES.get(raw["status"], "unknown"),
        patch=patches.get(filename),
        additions=raw["lines_added"],
        deletions=raw["lines_removed"],
        previous_filename=old["path"] if raw["status"] == "renamed" else None,
    )


def split_patches(diff: str) -> dict[str, str]:
    """Split a unified diff into each file's hunks, keyed by the file's new path."""
    patches = {}
    for section in re.split(r"^(?=diff --git )", diff, flags=re.MULTILINE):
        header, _, body = section.partition("\n")
        if header and (hunks := re.search(r"^@@", body, flags=re.MULTILINE)):
            patches[header.rpartition(" b/")[2]] = body[hunks.start() :]
    return patches


def decode_content(action: WriteCommitAction) -> bytes:
    if action.encoding == "base64":
        try:
            return base64.b64decode(action.content, validate=True)
        except binascii.Error as e:
            raise ResourceBadRequest(detail=f"'{action.filename}' is not valid base64.") from e
    return action.content.encode("utf-8")


def file_part(path: str, content: bytes) -> tuple[str, tuple[str, bytes, str]]:
    """A form part that Bitbucket writes to `path`.

    The leading slash keeps a file named like a form field, such as `message`, from being read as that field.
    """
    # Bitbucket decodes a part without a filename as text, which mangles binary content.
    return f"/{path.lstrip('/')}", (posixpath.basename(path), content, "application/octet-stream")


def next_cursor(raw: dict[str, Any]) -> str | None:
    """Read the `page` value out of the `next` link, which is a page number or an opaque token."""
    if not (next_url := raw.get("next")):
        return None
    page = parse_qs(urlparse(next_url).query).get("page")
    return page[0] if page else None


def map_action[T](response: requests.Response, fn: Callable[[dict[str, Any]], T]) -> ActionResult[T]:
    raw = response.json()
    return {
        "data": fn(raw),
        "type": PROVIDER_TYPE,
        "raw": {"data": raw, "headers": dict(response.headers)},
        "meta": {},
    }


def map_paginated_action[T](
    response: requests.Response, fn: Callable[[dict[str, Any]], T]
) -> PaginatedActionResult[list[T]]:
    raw = response.json()
    return {
        "data": [fn(item) for item in raw.get("values", [])],
        "type": PROVIDER_TYPE,
        "raw": {"data": raw, "headers": dict(response.headers)},
        "meta": {"next_cursor": next_cursor(raw)},
    }
