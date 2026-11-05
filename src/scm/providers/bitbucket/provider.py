from collections.abc import Callable
from typing import Any
from urllib.parse import parse_qs, quote, urlparse

import requests

from scm.errors import PathIsNotDirectory, ResourceNotFound, error_class_for_status
from scm.helpers import iter_all_pages
from scm.types import (
    SHA,
    ActionResult,
    ApiClient,
    AppInstallation,
    CredentialsSet,
    GitCommitObject,
    GitCommitTree,
    GitRepository,
    GitTree,
    PaginatedActionResult,
    PaginationParams,
    ProviderName,
    Repository,
    RequestOptions,
    TreeEntry,
    TreeEntryMode,
    TreeEntryType,
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

_MAX_TREE_DEPTH = str(2**31 - 1)
_TREE_FIELDS = "next,values.path,values.type,values.size,values.attributes,values.commit.hash"


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

    def get_git_commit(
        self,
        sha: SHA,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[GitCommitObject]:
        """Get a commit as a git object.

        Bitbucket doesn't expose tree SHAs, so the tree is named by the commit SHA, which `get_tree` takes.
        """
        response = self.get(f"/2.0/repositories/{self.repository_path}/commit/{sha}", request_options=request_options)
        return map_action(response, map_git_commit_object)

    def get_tree(
        self,
        tree_sha: SHA,
        recursive: bool = True,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[GitTree]:
        """List one page of the tree named by `<commit>` or `<commit>:<path>`.

        Bitbucket has no tree SHAs, so each entry's `sha` is its `<commit>:<path>` git revision.
        """
        commit, path = self._split_tree_sha(tree_sha, request_options)
        return self._get_tree_page(commit, path, tree_sha, recursive, pagination, request_options)

    def get_full_tree(
        self,
        tree_sha: SHA,
        recursive: bool = True,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[GitTree]:
        """List a whole tree, walking every page."""
        commit, path = self._split_tree_sha(tree_sha, request_options)
        entries: list[TreeEntry] = []
        raw_entries: list[dict[str, Any]] = []
        for page in iter_all_pages(
            lambda pagination: self._get_tree_page(commit, path, tree_sha, recursive, pagination, request_options),
            per_page=MAX_PAGE_SIZE,
        ):
            entries.extend(page["data"]["tree"])
            raw_entries.extend(page["raw"]["data"]["values"])
        return {
            "data": GitTree(sha=tree_sha, tree=entries, truncated=False),
            "type": PROVIDER_TYPE,
            "raw": {"data": raw_entries, "headers": None},
            "meta": {},
        }

    def _split_tree_sha(self, tree_sha: SHA, request_options: RequestOptions | None) -> tuple[SHA, str]:
        ref, _, path = tree_sha.partition(":")
        if "/" not in ref:
            return ref, path
        # `src` reads the first path segment as the revision, so resolve refs like `feature/x` first.
        response = self.get(
            f"/2.0/repositories/{self.repository_path}/commits/{quote(ref)}",
            params={"pagelen": "1", "fields": "values.hash"},
            request_options=request_options,
        )
        if not (values := response.json()["values"]):
            raise ResourceNotFound(detail=f"No commit for {ref!r}")
        return values[0]["hash"], path

    def _get_tree_page(
        self,
        commit: SHA,
        path: str,
        tree_sha: SHA,
        recursive: bool,
        pagination: PaginationParams | None,
        request_options: RequestOptions | None,
    ) -> PaginatedActionResult[GitTree]:
        params = {"fields": _TREE_FIELDS}
        if recursive:
            params["max_depth"] = _MAX_TREE_DEPTH
        response = self.get(
            f"/2.0/repositories/{self.repository_path}/src/{quote(commit)}/{quote(f'{path}/' if path else '')}",
            params=params,
            pagination=pagination or {"per_page": MAX_PAGE_SIZE},
            request_options=request_options,
        )
        if not _is_listing(response):
            raise PathIsNotDirectory(detail=path)
        raw = response.json()
        cursor = next_cursor(raw)
        return {
            "data": GitTree(
                sha=tree_sha,
                tree=[map_tree_entry(entry, path) for entry in raw["values"]],
                truncated=cursor is not None,
            ),
            "type": PROVIDER_TYPE,
            "raw": {"data": raw, "headers": dict(response.headers)},
            "meta": {"next_cursor": cursor},
        }


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


def map_git_commit_object(raw: dict[str, Any]) -> GitCommitObject:
    return GitCommitObject(sha=raw["hash"], tree=GitCommitTree(sha=raw["hash"]), message=raw["message"])


def map_tree_entry(raw: dict[str, Any], tree_path: str) -> TreeEntry:
    """Map a `src` listing entry, with its path relative to the listed directory."""
    mode, entry_type = tree_entry_kind(raw)
    return TreeEntry(
        path=raw["path"].removeprefix(f"{tree_path}/") if tree_path else raw["path"],
        mode=mode,
        type=entry_type,
        sha=f"{raw['commit']['hash']}:{raw['path']}",
        # A submodule's size is the length of its commit SHA.
        size=raw["size"] if entry_type == "blob" else None,
    )


def tree_entry_kind(raw: dict[str, Any]) -> tuple[TreeEntryMode, TreeEntryType]:
    if raw["type"] == "commit_directory":
        return "040000", "tree"
    attributes = raw.get("attributes", [])
    if "subrepository" in attributes:
        return "160000", "commit"
    if "link" in attributes:
        return "120000", "blob"
    if "executable" in attributes:
        return "100755", "blob"
    return "100644", "blob"


def _is_listing(response: requests.Response) -> bool:
    # File bodies come back as text/plain or their own media type, even for JSON files.
    return response.headers.get("content-type", "").startswith("application/json")


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
