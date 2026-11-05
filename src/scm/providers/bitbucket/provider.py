from collections.abc import Callable
from typing import Any
from urllib.parse import parse_qs, quote, urlparse

import requests

from scm.errors import ResourceNotFound, error_class_for_status
from scm.types import (
    SHA,
    ActionResult,
    ApiClient,
    AppInstallation,
    BranchName,
    CredentialsSet,
    GitRef,
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

_REF_KINDS = {"heads": "branches", "tags": "tags"}


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

    def get_branch(
        self,
        branch: BranchName,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[GitRef]:
        response = self.get(
            f"/2.0/repositories/{self.repository_path}/refs/branches/{quote_ref(branch)}",
            request_options=request_options,
        )
        return map_action(response, map_branch)

    def create_branch(self, branch: BranchName, sha: SHA) -> ActionResult[GitRef]:
        response = self.post(
            f"/2.0/repositories/{self.repository_path}/refs/branches",
            data={"name": branch, "target": {"hash": sha}},
        )
        return map_action(response, map_branch)

    def delete_branch(self, branch: BranchName) -> None:
        self.delete(f"/2.0/repositories/{self.repository_path}/refs/branches/{quote_ref(branch)}")

    def get_git_ref(
        self,
        ref: str,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[GitRef]:
        """Get a `heads/<branch>` or `tags/<tag>` ref."""
        kind, _, name = ref.partition("/")
        if kind not in _REF_KINDS or not name:
            raise ResourceNotFound(detail=f"Unsupported ref '{ref}'.")
        response = self.get(
            f"/2.0/repositories/{self.repository_path}/refs/{_REF_KINDS[kind]}/{quote_ref(name)}",
            request_options=request_options,
        )
        return map_action(response, lambda raw: GitRef(ref=f"refs/{kind}/{raw['name']}", sha=raw["target"]["hash"]))


def quote_ref(name: str) -> str:
    """Percent-encode a branch or tag name for a URL path, keeping `/` literal as Bitbucket expects."""
    return quote(name, safe="/")


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


def map_branch(raw: dict[str, Any]) -> GitRef:
    return GitRef(ref=raw["name"], sha=raw["target"]["hash"])


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
