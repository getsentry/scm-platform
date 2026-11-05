from collections.abc import Callable
from typing import Any
from urllib.parse import parse_qs, quote, urlparse

import requests

from scm.errors import (
    ResourceBadRequest,
    ResourceConflict,
    ResourceNotFound,
    StaleBranchHead,
    error_class_for_status,
)
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
    CredentialsSet,
    DeleteCommitAction,
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
        json: bool = True,
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
            json=json,
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

        Bitbucket only commits at the branch head, so `parent_sha` must match it. Only UTF-8 text can be written.
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

        data: dict[str, Any] = {"message": message, "branch": branch, "parents": parent_sha}
        if author is not None:
            data["author"] = f"{author['name']} <{author['email']}>"
        deleted: list[str] = []
        for action in actions:
            if isinstance(action, WriteCommitAction):
                if action.encoding != "utf-8":
                    raise ResourceBadRequest(detail=f"Bitbucket can only commit UTF-8 text, not '{action.filename}'.")
                data[file_field(action.filename)] = action.content
            elif isinstance(action, DeleteCommitAction):
                deleted.append(action.filename)
            elif isinstance(action, MoveCommitAction):
                deleted.append(action.old_filename)
                data[file_field(action.new_filename)] = self._text_file(action.old_filename, parent_sha)
            else:
                raise ResourceBadRequest(detail="Bitbucket cannot change a file's mode through its commit API.")
        if deleted:
            data["files"] = deleted

        try:
            response = self.request("POST", f"/2.0/repositories/{self.repository_path}/src", data=data, json=False)
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

    def _text_file(self, path: str, ref: SHA) -> str:
        """Read a file to rewrite it elsewhere, refusing what a plain-text upload can't recreate."""
        file_path = f"/2.0/repositories/{self.repository_path}/src/{ref}/{quote(path)}"
        # A text upload can't set attributes, so a moved executable or symlink would lose them.
        if attributes := self.get(file_path, params={"format": "meta"}).json().get("attributes"):
            raise ResourceBadRequest(
                detail=f"Bitbucket cannot move '{path}' and keep its {', '.join(attributes)} attribute."
            )
        try:
            return self.get(file_path).content.decode("utf-8")
        except UnicodeDecodeError as e:
            raise ResourceBadRequest(detail=f"Bitbucket can only commit UTF-8 text, not '{path}'.") from e


def parse_scopes(header: str) -> set[str]:
    return {scope.strip() for scope in header.split(",") if scope.strip()}


def map_app_installation(scopes: set[str]) -> AppInstallation:
    can_write_repository = bool(scopes & _REPOSITORY_WRITE_SCOPES)
    return AppInstallation(
        has_read_access=bool(scopes & _REPOSITORY_READ_SCOPES),
        has_write_access=can_write_repository and bool(scopes & _PULL_REQUEST_WRITE_SCOPES),
        has_check_run_write_access=can_write_repository,
    )


def file_field(path: str) -> str:
    """A leading slash keeps a file named like a form field, such as `message`, from being read as that field."""
    return f"/{path.lstrip('/')}"


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
