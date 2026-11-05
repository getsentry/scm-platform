import json
from collections.abc import Callable
from datetime import datetime
from email.utils import parseaddr
from typing import Any
from urllib.parse import parse_qs, urlparse

import requests

from scm.errors import ResourceForbidden, ResourceNotFound, UnexpectedResponseFormat, error_class_for_status
from scm.types import (
    ActionResult,
    ApiClient,
    AppInstallation,
    Author,
    BranchName,
    Comment,
    CommitAuthor,
    CoPilotChatExtension,
    CredentialsSet,
    FileStatus,
    GitRepository,
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
    ReviewThread,
    ReviewThreadComment,
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

_PULL_REQUEST_STATE_QUERIES: dict[PullRequestState | None, str] = {
    "open": 'state="OPEN"',
    "closed": '(state="MERGED" OR state="DECLINED" OR state="SUPERSEDED")',
    None: '(state="OPEN" OR state="MERGED" OR state="DECLINED" OR state="SUPERSEDED")',
}
_FILE_STATUSES: dict[str, FileStatus] = {
    "added": "added",
    "removed": "removed",
    "modified": "modified",
    "renamed": "renamed",
}


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

    def get_authenticated_actor(self) -> ActionResult[Author]:
        response = self.get("/2.0/user")
        return map_action(response, map_authenticated_actor)

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

    def get_pull_request(
        self,
        pull_request_id: str,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[PullRequest]:
        response = self.get(self._pull_request_path(pull_request_id), request_options=request_options)
        result = map_action(response, map_pull_request)
        raw = result["raw"]["data"]
        for side, branch in (("source", result["data"]["head"]), ("destination", result["data"]["base"])):
            if branch["sha"] and (repository := raw[side].get("repository")):
                branch["sha"] = self._full_hash(repository["full_name"], branch["sha"], request_options)
        return result

    def get_pull_requests(
        self,
        state: PullRequestState | None = "open",
        head: BranchName | None = None,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[PullRequest]]:
        # Without a `state` term, Bitbucket lists only open pull requests.
        query = _PULL_REQUEST_STATE_QUERIES[state]
        if head:
            branch = head.split(":", 1)[-1].removeprefix("refs/heads/")
            query = f"source.branch.name={json.dumps(branch, ensure_ascii=False)} AND {query}"
        response = self.get(
            f"/2.0/repositories/{self.repository_path}/pullrequests",
            params={"q": query},
            pagination=pagination,
            request_options=request_options,
        )
        return map_paginated_action(response, map_pull_request)

    def get_pull_request_files(
        self,
        pull_request_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[PullRequestFile]]:
        # Bitbucket answers with a redirect to the repository diffstat endpoint.
        response = self.get(
            f"{self._pull_request_path(pull_request_id)}/diffstat",
            pagination=pagination,
            request_options=request_options,
            allow_redirects=True,
        )
        return map_paginated_action(response, map_pull_request_file)

    def get_pull_request_commits(
        self,
        pull_request_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[PullRequestCommit]]:
        """List every commit on the pull request, oldest first. ``pagination`` is unused.

        Bitbucket lists newest first, so all pages are read to return them oldest first.
        """
        path = f"{self._pull_request_path(pull_request_id)}/commits"
        values: list[dict[str, Any]] = []
        page: PaginationParams = {"per_page": MAX_PAGE_SIZE}
        while True:
            response = self.get(path, pagination=page, request_options=request_options)
            raw = response.json()
            values.extend(raw.get("values", []))
            if not (cursor := next_cursor(raw)):
                break
            page = {"per_page": MAX_PAGE_SIZE, "cursor": cursor}
        # Once merged, Bitbucket lists its own merge commit first. Other providers leave it out.
        # A squash merge lists only the squash commit, which has one parent, so it stays.
        if values and len(values[0]["parents"]) > 1:
            pull_request = self.get(self._pull_request_path(pull_request_id), request_options=request_options).json()
            merge_hash = (pull_request.get("merge_commit") or {}).get("hash")
            if merge_hash and values[0]["hash"].startswith(merge_hash):
                values.pop(0)
        return {
            "data": [map_pull_request_commit(value) for value in reversed(values)],
            "type": PROVIDER_TYPE,
            "raw": {"data": values, "headers": dict(response.headers)},
            "meta": {"next_cursor": None},
        }

    def get_pull_request_diff(
        self,
        pull_request_id: str,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[str]:
        # Bitbucket answers with a redirect to the repository diff endpoint.
        response = self.get(
            f"{self._pull_request_path(pull_request_id)}/diff",
            request_options=request_options,
            allow_redirects=True,
        )
        return {
            "data": response.text,
            "type": PROVIDER_TYPE,
            "raw": {"data": response.text, "headers": dict(response.headers)},
            "meta": {},
        }

    def get_pull_request_comments(
        self,
        pull_request_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[Comment]]:
        """Return the general comments, leaving out inline ones."""
        response = self.get(
            self._comments_path(pull_request_id),
            # Filter on the server so a page of inline comments doesn't come back empty.
            params={"q": "inline.path = null AND deleted = false"},
            pagination=pagination,
            request_options=request_options,
        )
        # Pending comments are the caller's own unpublished drafts, and Bitbucket can't filter on them.
        return map_paginated_action(response, map_comment, keep=lambda raw: not raw.get("pending"))

    def create_pull_request_comment(
        self,
        pull_request_id: str,
        body: str,
        extensions: list[CoPilotChatExtension] | None = None,
    ) -> ActionResult[Comment]:
        response = self.post(self._comments_path(pull_request_id), data={"content": {"raw": body}})
        return map_action(response, map_comment)

    def delete_pull_request_comment(self, pull_request_id: str, comment_id: str) -> None:
        self.delete(f"{self._comments_path(pull_request_id)}/{comment_id}")

    def get_pull_request_review_threads(
        self,
        pull_request_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
        *,
        include_reactions: bool = False,
    ) -> PaginatedActionResult[list[ReviewThread]]:
        """Return the inline comment threads, reading every page since a thread can span pages.

        ``pagination`` and ``include_reactions`` are unused; Bitbucket has no comment reactions.
        """
        comments: list[dict[str, Any]] = []
        page: PaginationParams = {"per_page": MAX_PAGE_SIZE}
        while True:
            # The list leaves out resolution and outdated unless asked for.
            response = self.get(
                self._comments_path(pull_request_id),
                params={"q": "inline.path != null", "fields": "+values.resolution.type,+values.inline.*"},
                pagination=page,
                request_options=request_options,
            )
            raw = response.json()
            comments.extend(raw["values"])
            if not (cursor := next_cursor(raw)):
                break
            page = {"per_page": MAX_PAGE_SIZE, "cursor": cursor}
        return {
            "data": map_review_threads(comments),
            "type": PROVIDER_TYPE,
            "raw": {"data": {"values": comments}, "headers": dict(response.headers)},
            "meta": {"next_cursor": None},
        }

    def get_pull_request_url(self, pull_request_id: str) -> str:
        return f"{self._web_base_url}/{self.repository_path}/pull-requests/{pull_request_id}"

    def _pull_request_path(self, pull_request_id: str) -> str:
        return f"/2.0/repositories/{self.repository_path}/pullrequests/{pull_request_id}"

    def _comments_path(self, pull_request_id: str) -> str:
        return f"{self._pull_request_path(pull_request_id)}/comments"

    def _full_hash(self, repository_path: str, short_hash: str, request_options: RequestOptions | None) -> str:
        """Expand one of the 12-character hashes Bitbucket puts on pull requests."""
        try:
            response = self.get(
                f"/2.0/repositories/{repository_path}/commit/{short_hash}",
                params={"fields": "hash"},
                request_options=request_options,
            )
        except (ResourceForbidden, ResourceNotFound):
            # The commit can live in a fork we can't read, or be gone with a deleted branch.
            return short_hash
        return response.json()["hash"]


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


def map_author(raw: dict[str, Any]) -> Author:
    # App users have no nickname.
    return Author(id=raw["uuid"], username=raw.get("nickname") or raw.get("display_name") or "")


def map_user(raw: dict[str, Any] | None) -> Author | None:
    return map_author(raw) if raw and raw.get("uuid") else None


def map_authenticated_actor(raw: dict[str, Any]) -> Author:
    if not (author := map_user(raw)):
        raise UnexpectedResponseFormat(detail="Bitbucket /2.0/user response has no uuid")
    return author


def map_pull_request_branch(raw: dict[str, Any]) -> PullRequestBranch:
    # Bitbucket abbreviates these hashes to 12 characters. Only get_pull_request expands them.
    return PullRequestBranch(ref=raw["branch"]["name"], sha=(raw.get("commit") or {}).get("hash"))


def map_pull_request(raw: dict[str, Any]) -> PullRequest:
    return PullRequest(
        id=str(raw["id"]),
        internal_id=str(raw["id"]),
        title=raw["title"],
        body=raw["description"] or None,
        state="open" if raw["state"] == "OPEN" else "closed",
        merged=raw["state"] == "MERGED",
        html_url=raw["links"]["html"]["href"],
        head=map_pull_request_branch(raw["source"]),
        base=map_pull_request_branch(raw["destination"]),
        author=map_author(raw["author"]),
    )


def map_pull_request_file(raw: dict[str, Any]) -> PullRequestFile:
    status = _FILE_STATUSES.get(raw["status"], "unknown")
    return PullRequestFile(
        filename=(raw["new"] or raw["old"])["path"],
        status=status,
        patch=None,
        changes=raw["lines_added"] + raw["lines_removed"],
        sha="",
        previous_filename=raw["old"]["path"] if status == "renamed" else None,
    )


def map_pull_request_commit(raw: dict[str, Any]) -> PullRequestCommit:
    name, email = parseaddr(raw["author"]["raw"])
    return PullRequestCommit(
        sha=raw["hash"],
        message=raw["message"],
        author=CommitAuthor(name=name, email=email, date=datetime.fromisoformat(raw["date"])),
    )


def map_comment(raw: dict[str, Any]) -> Comment:
    return Comment(
        id=str(raw["id"]),
        body=raw["content"]["raw"],
        author=map_user(raw.get("user")),
        created_at=raw["created_on"],
        author_association=None,
    )


def map_review_thread_comment(raw: dict[str, Any]) -> ReviewThreadComment:
    return ReviewThreadComment(
        id=str(raw["id"]),
        unique_id=str(raw["id"]),
        body=raw["content"]["raw"],
        author=map_user(raw.get("user")),
        is_bot=(raw.get("user") or {}).get("type") == "app_user",
        created_at=raw["created_on"],
        updated_at=raw["updated_on"],
        is_minimized=False,
        commit_sha=raw["inline"].get("src_rev") or "",
    )


def map_review_threads(comments: list[dict[str, Any]]) -> list[ReviewThread]:
    """Group inline comments under their top-level comment, which holds the thread's state."""
    by_id = {raw["id"]: raw for raw in comments}

    def root(raw: dict[str, Any]) -> dict[str, Any]:
        while (parent := raw.get("parent")) and parent["id"] in by_id:
            raw = by_id[parent["id"]]
        return raw

    # A deleted top-level comment still anchors the replies under it.
    grouped: dict[int, list[dict[str, Any]]] = {}
    for raw in comments:
        if not raw.get("deleted") and not raw.get("pending"):
            grouped.setdefault(root(raw)["id"], []).append(raw)

    threads = []
    for root_id, thread_comments in grouped.items():
        head = by_id[root_id]
        inline = head["inline"]
        threads.append(
            ReviewThread(
                id=str(root_id),
                is_resolved=bool(head.get("resolution")),
                is_outdated=bool(inline.get("outdated")),
                file_path=inline["path"],
                line=inline.get("to") or inline.get("from"),
                start_line=inline.get("start_to") or inline.get("start_from"),
                comments=[map_review_thread_comment(raw) for raw in thread_comments],
            )
        )
    return threads


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
    response: requests.Response,
    fn: Callable[[dict[str, Any]], T],
    keep: Callable[[dict[str, Any]], bool] = lambda _: True,
) -> PaginatedActionResult[list[T]]:
    raw = response.json()
    return {
        "data": [fn(item) for item in raw.get("values", []) if keep(item)],
        "type": PROVIDER_TYPE,
        "raw": {"data": raw, "headers": dict(response.headers)},
        "meta": {"next_cursor": next_cursor(raw)},
    }
