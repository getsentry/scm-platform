from collections.abc import Callable
from typing import Any

import requests

from scm.errors import (
    error_class_for_status,
)
from scm.types import (
    ActionResult,
    ApiClient,
    Author,
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

PROVIDER_TYPE: ProviderName = "cursor_origin"
CURSOR_ORIGIN_WEB_BASE_URL = "https://cursor.com/codebase"
PAGE_TOKEN_PARAM = "pageToken"
PAGE_SIZE_PARAM = "pageSize"
# The cursor `iter_all_pages` sends for the first page. The first page takes no token.
FIRST_PAGE_CURSOR = "1"
MAX_PAGE_SIZE = 100
# Origin's visibilities.
_PRIVATE_VISIBILITIES = {"internal", "private"}


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


def map_action[T](response: requests.Response, fn: Callable[[dict[str, Any]], T]) -> ActionResult[T]:
    raw = response.json()
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
