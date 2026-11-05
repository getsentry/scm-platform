import base64
import hashlib
from collections.abc import Callable, Iterator
from typing import Any
from urllib.parse import parse_qs, quote, urlparse

import requests

from scm.errors import (
    PathIsDirectory,
    PathIsNotDirectory,
    ReadmeNotFound,
    ResourceNotFound,
    SCMCodedError,
    error_class_for_status,
)
from scm.helpers import iter_all_pages
from scm.types import (
    SHA,
    ActionResult,
    ApiClient,
    AppInstallation,
    CredentialsSet,
    FileContent,
    FileContentType,
    GitRemote,
    GitRepository,
    PaginatedActionResult,
    PaginationParams,
    ProviderName,
    Repository,
    RequestOptions,
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
            return quote(ref)
        response = self.get(
            f"/2.0/repositories/{self.repository_path}/commits/{quote(ref)}",
            params={"pagelen": "1", "fields": "values.hash"},
            request_options=request_options,
        )
        if not (values := response.json()["values"]):
            raise ResourceNotFound(detail=ref)
        return values[0]["hash"]

    def _src_path(self, commit: str, path: str) -> str:
        return f"/2.0/repositories/{self.repository_path}/src/{commit}/{quote(path.strip('/'))}"


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
