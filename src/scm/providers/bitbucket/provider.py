import base64
import datetime
import hashlib
import re
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from urllib.parse import parse_qs, quote, urlparse

import requests

from scm.errors import (
    ResourceBadRequest,
    error_class_for_status,
)
from scm.types import (
    SHA,
    ActionResult,
    ApiClient,
    Author,
    BranchName,
    Comment,
    Commit,
    CommitAuthor,
    CoPilotChatExtension,
    CredentialsSet,
    FileContent,
    GitRef,
    GitRepository,
    PaginatedActionResult,
    PaginatedResponseMeta,
    PaginationParams,
    PullRequest,
    PullRequestBranch,
    PullRequestState,
    Referrer,
    Repository,
    RequestOptions,
)

# Bitbucket pull request states, grouped to match our binary open/closed model.
# "closed" collapses every non-open terminal state Bitbucket exposes.
PULL_REQUEST_STATE_RETRIEVE_MAP: dict[PullRequestState, list[str]] = {
    "open": ["OPEN"],
    "closed": ["MERGED", "DECLINED", "SUPERSEDED"],
}


class BitbucketProvider:
    def __init__(self, client: ApiClient, organization_id: int, repository: Repository) -> None:
        self.client = client
        self.organization_id = organization_id
        self.repository = repository

    def is_rate_limited(self, referrer: Referrer) -> bool:
        return False

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
        extra_headers: dict[str, str] | None = None,
        allow_redirects: bool | None = None,
    ) -> requests.Response:
        headers = {}
        headers.update(extra_headers or {})

        options = request_options or {}

        params = params or {}
        if pagination:
            params["pagelen"] = str(pagination["per_page"])
            params["page"] = str(pagination["cursor"])

        return self.request(
            "GET",
            path=path,
            params=params,
            headers=headers,
            allow_redirects=allow_redirects,
            timeout=options.get("timeout"),
        )

    def post(
        self,
        path: str,
        data: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> requests.Response:
        return self.request("POST", path=path, data=data, headers=headers)

    def put(
        self,
        path: str,
        data: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> requests.Response:
        return self.request("PUT", path=path, data=data, headers=headers)

    def delete(self, path: str) -> requests.Response:
        return self.request("DELETE", path=path)

    def get_repository(self) -> ActionResult[GitRepository]:
        response = self.get(f"/repositories/{self.repository['name']}")
        return make_result(map_repository, response.json())

    def get_branch(
        self,
        branch: BranchName,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[GitRef]:
        response = self.get(
            f"/repositories/{self.repository['name']}/refs/branches/{quote(branch, safe='/')}",
            request_options=request_options,
        )
        return make_result(map_git_ref, response.json())

    def create_branch(self, branch: BranchName, sha: SHA) -> ActionResult[GitRef]:
        response = self.post(
            f"/repositories/{self.repository['name']}/refs/branches",
            data={"name": branch, "target": {"hash": sha}},
        )
        return make_result(map_git_ref, response.json())

    def delete_branch(self, branch: BranchName) -> None:
        self.delete(f"/repositories/{self.repository['name']}/refs/branches/{quote(branch, safe='/')}")

    def get_file_content(
        self,
        path: str,
        ref: str,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[FileContent]:
        """Fetch a file's contents at a ref.

        Bitbucket's ``/src`` endpoint returns raw bytes (no blob SHA and no
        line/size metadata in the body), so we base64-encode the response and
        report ``size`` from its byte length. Since Bitbucket exposes no git
        blob id, we compute one locally so ``sha`` matches the blob SHA that
        GitHub and GitLab report.
        """
        response = self.get(
            f"/repositories/{self.repository['name']}/src/{quote(ref, safe='')}/{quote(path, safe='/')}",
            request_options=request_options,
        )
        raw_content = response.content
        content = base64.b64encode(raw_content).decode("ascii")
        return ActionResult(
            data=FileContent(
                path=path,
                sha=_git_blob_sha1(raw_content),
                content=content,
                encoding="base64",
                size=len(raw_content),
                type="file",
            ),
            type="bitbucket",
            raw={"data": content, "headers": None},
            meta={},
        )

    def get_pull_request(
        self,
        pull_request_id: str,
        request_options: RequestOptions | None = None,
    ) -> ActionResult[PullRequest]:
        response = self.get(
            f"/repositories/{self.repository['name']}/pullrequests/{pull_request_id}",
            request_options=request_options,
        )
        return make_result(map_pull_request, response.json())

    def get_pull_requests(
        self,
        state: PullRequestState | None = "open",
        # 'head' is GitHub-shaped (optional ``owner:`` prefix, optional
        # ``refs/heads/`` ref prefix). We map it to a Bitbucket ``q`` filter on
        # the source branch so the open-PR-for-this-branch lookup stays a
        # server-side query rather than a fetch-all-and-filter.
        head: BranchName | None = None,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[PullRequest]]:
        params: dict[str, Any] = {}
        if state is not None:
            # Bitbucket accepts a repeatable ``state`` param; requests serializes
            # a list value as repeated query parameters.
            params["state"] = PULL_REQUEST_STATE_RETRIEVE_MAP[state]
        if head:
            params["q"] = f'source.branch.name="{_head_to_source_branch(head)}"'
        response = self.get(
            f"/repositories/{self.repository['name']}/pullrequests",
            params=params,
            pagination=pagination,
            request_options=request_options,
        )
        return make_paginated_result(map_pull_request, response.json())

    def create_pull_request(
        self,
        title: str,
        body: str,
        head: BranchName,
        base: BranchName,
    ) -> ActionResult[PullRequest]:
        data: dict[str, Any] = {
            "title": title,
            "description": body,
            "source": {"branch": {"name": head}},
            "destination": {"branch": {"name": base}},
        }
        response = self.post(f"/repositories/{self.repository['name']}/pullrequests", data=data)
        return make_result(map_pull_request, response.json())

    def update_pull_request(
        self,
        pull_request_id: str,
        title: str | None = None,
        body: str | None = None,
        state: PullRequestState | None = None,
    ) -> ActionResult[PullRequest]:
        """Update a pull request's title, body, and/or state.

        Bitbucket's PUT endpoint edits fields like title and description but has
        no state field. Closing a pull request therefore goes through the
        separate ``/decline`` endpoint, which we call after the PUT when
        ``state`` is ``"closed"``. Bitbucket cannot reopen a declined pull
        request via the API, so ``state="open"`` is rejected.
        """
        if state == "open":
            raise ResourceBadRequest(
                detail="Bitbucket cannot reopen a pull request via the API.",
            )

        data: dict[str, Any] = {}
        if title is not None:
            data["title"] = title
        if body is not None:
            data["description"] = body

        pr_path = f"/repositories/{self.repository['name']}/pullrequests/{pull_request_id}"

        response = self.put(pr_path, data=data)
        if state == "closed":
            response = self.post(f"{pr_path}/decline", data={})

        return make_result(map_pull_request, response.json())

    def get_commits(
        self,
        ref: str | None = None,
        pagination: PaginationParams | None = None,
        since: datetime.datetime | None = None,
        until: datetime.datetime | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[Commit]]:
        """List repository commits, most recent first.

        ``ref`` (branch, tag, or SHA) is passed as Bitbucket's ``include``
        filter; without it, Bitbucket lists commits from the main branch.

        Bitbucket's commits endpoint has no date filtering (no ``since``/
        ``until`` params and no ``q`` support), so passing either is rejected
        rather than silently returning unfiltered results.
        """
        if since is not None or until is not None:
            raise ResourceBadRequest(
                detail="Bitbucket's commits endpoint does not support date filtering (since/until).",
            )
        params: dict[str, Any] = {}
        if ref:
            params["include"] = ref
        response = self.get(
            f"/repositories/{self.repository['name']}/commits",
            params=params,
            pagination=pagination,
            request_options=request_options,
        )
        return make_paginated_result(map_commit, response.json())

    def get_commits_by_path(
        self,
        path: str,
        ref: str | None = None,
        pagination: PaginationParams | None = None,
        since: datetime.datetime | None = None,
        until: datetime.datetime | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[Commit]]:
        """List commits that touched ``path``, most recent first.

        Uses Bitbucket's file-history endpoint, whose ``{commit}`` path segment
        is required; when ``ref`` is omitted we resolve the repository's default
        branch. As with ``get_commits``, Bitbucket has no date filtering here,
        so ``since``/``until`` are rejected.

        File-history entries embed only an abbreviated commit (hash + links), so
        we follow up with one ``get_commit`` call per entry to hydrate the full
        commit. These N calls are issued concurrently via a thread pool, but it
        is still an N+1 fan-out bounded by the page size.
        """
        if since is not None or until is not None:
            raise ResourceBadRequest(
                detail="Bitbucket's file-history endpoint does not support date filtering (since/until).",
            )
        commit = ref or self.get_repository()["data"]["default_branch"]
        response = self.get(
            f"/repositories/{self.repository['name']}/filehistory/{commit}/{quote(path, safe='/')}",
            pagination=pagination,
            request_options=request_options,
        )
        raw = response.json()
        hashes = [entry["commit"]["hash"] for entry in raw.get("values", [])]
        # executor.map preserves input order, so the commits stay newest-first.
        with ThreadPoolExecutor() as executor:
            full_commits = list(executor.map(self._fetch_commit, hashes))
        return make_paginated_result(map_commit, raw, raw_items=full_commits)

    def _fetch_commit(self, commit_hash: SHA) -> dict[str, Any]:
        """Fetch a single commit's full representation by hash."""
        return self.get(f"/repositories/{self.repository['name']}/commit/{commit_hash}").json()

    def get_pull_request_comments(
        self,
        pull_request_id: str,
        pagination: PaginationParams | None = None,
        request_options: RequestOptions | None = None,
    ) -> PaginatedActionResult[list[Comment]]:
        """List a pull request's conversation comments.

        Bitbucket returns inline (review) comments and deleted tombstones from
        the same endpoint. To stay consistent with GitHub — whose "list issue
        comments" excludes review comments — we drop entries carrying an
        ``inline`` anchor as well as deleted ones.
        """
        response = self.get(
            f"/repositories/{self.repository['name']}/pullrequests/{pull_request_id}/comments?q=deleted=false",
            pagination=pagination,
            request_options=request_options,
        )
        raw = response.json()
        return make_paginated_result(
            map_comment,
            raw,
            raw_items=(c for c in raw.get("values", []) if "inline" not in c),
        )

    def create_pull_request_comment(
        self,
        pull_request_id: str,
        body: str,
        extensions: list[CoPilotChatExtension] | None = None,
    ) -> ActionResult[Comment]:
        response = self.post(
            f"/repositories/{self.repository['name']}/pullrequests/{pull_request_id}/comments",
            data={"content": {"raw": body}},
        )
        return make_result(map_comment, response.json())

    def delete_pull_request_comment(self, pull_request_id: str, comment_id: str) -> None:
        self.delete(
            f"/repositories/{self.repository['name']}/pullrequests/{pull_request_id}/comments/{comment_id}",
        )


def _head_to_source_branch(head: str) -> str:
    """Normalize a GitHub-style ``head`` filter to a bare Bitbucket branch name.

    Callers pass ``head`` in GitHub's shape — an optional ``owner:`` prefix and
    sometimes a ``refs/heads/`` ref prefix (e.g. ``"acme:refs/heads/feature"``).
    Bitbucket's ``source.branch.name`` filter wants the bare branch name.
    """
    branch = head.split(":", 1)[-1]
    return branch.removeprefix("refs/heads/")


def _git_blob_sha1(content: bytes) -> str:
    """Compute a git blob object id for ``content``.

    Git names a blob by ``sha1("blob <bytelen>\\0" + content)``. Bitbucket does
    not expose this id, so we recompute it to match the blob SHA GitHub and
    GitLab return for the same file.
    """
    header = f"blob {len(content)}\0".encode()
    return hashlib.sha1(header + content, usedforsecurity=False).hexdigest()


def _parse_raw_author(raw: str) -> tuple[str, str]:
    """Split Bitbucket's git-style author string into ``(name, email)``.

    Bitbucket encodes commit authors as ``"Name <email>"`` in the ``raw`` field.
    When no angle-bracketed email is present we return the whole string as the
    name and an empty email.
    """
    match = re.match(r"^(.*?)\s*<(.*)>\s*$", raw)
    if match:
        return match.group(1), match.group(2)
    return raw, ""


def map_commit(raw: dict[str, Any]) -> Commit:
    author_raw = (raw.get("author") or {}).get("raw", "")
    name, email = _parse_raw_author(author_raw)
    date = raw.get("date")
    return Commit(
        id=raw["hash"],
        message=raw["message"],
        author=CommitAuthor(
            name=name,
            email=email,
            date=datetime.datetime.fromisoformat(date) if date else None,
        ),
        # Bitbucket's commit list carries no per-commit line stats.
        additions=None,
        deletions=None,
    )


def map_git_ref(raw: dict[str, Any]) -> GitRef:
    return GitRef(ref=raw["name"], sha=raw["target"]["hash"])


def map_comment(raw: dict[str, Any]) -> Comment:
    user = raw.get("user")
    return Comment(
        id=str(raw["id"]),
        body=(raw.get("content") or {}).get("raw"),
        author=map_author(user) if user else None,
        created_at=raw.get("created_on"),
        # Bitbucket has no author-association concept.
        author_association=None,
    )


def map_author(raw: dict[str, Any]) -> Author:
    # Bitbucket removed usernames for privacy; ``nickname`` is the closest
    # display handle, and ``uuid`` is the stable identifier used in API paths.
    return Author(
        id=raw.get("uuid") or raw.get("account_id") or "",
        username=raw.get("nickname") or raw.get("display_name") or "",
    )


def map_pull_request(raw: dict[str, Any]) -> PullRequest:
    source = raw["source"]
    destination = raw["destination"]
    return PullRequest(
        # Bitbucket exposes a single repo-scoped id, so internal_id mirrors id.
        internal_id=str(raw["id"]),
        id=str(raw["id"]),
        title=raw["title"],
        body=raw.get("description") or None,
        state="open" if raw["state"] == "OPEN" else "closed",
        base=PullRequestBranch(
            ref=destination["branch"]["name"],
            sha=(destination.get("commit") or {}).get("hash"),
        ),
        head=PullRequestBranch(
            ref=source["branch"]["name"],
            sha=(source.get("commit") or {}).get("hash"),
        ),
        merged=raw["state"] == "MERGED",
        html_url=raw["links"]["html"]["href"],
        author=map_author(raw.get("author") or {}),
    )


def map_repository(raw: dict[str, Any]) -> GitRepository:
    return GitRepository(
        full_name=raw["full_name"],
        default_branch=raw["mainbranch"]["name"],
        clone_url=raw["links"]["clone"][0]["href"],
        private=raw["is_private"],
        # Bitbucket returns size in bytes. We convert to kB to match GitHub
        size=raw["size"] // 1000,
        description=raw["description"],
        topics=[],
    )


def make_result[T](
    map_item: Callable[[dict[str, Any]], T],
    raw: Any,
    *,
    raw_item: dict[str, Any] | None = None,
) -> ActionResult[T]:
    if raw_item is None:
        assert isinstance(raw, dict)
        raw_item = raw
    return ActionResult(
        data=map_item(raw_item),
        type="bitbucket",
        raw={"data": raw, "headers": None},
        meta={},
    )


def _next_cursor(raw: dict[str, Any]) -> str | None:
    """Extract the next page number from a Bitbucket paginated response.

    Unlike GitLab (which paginates via response headers), Bitbucket returns a
    full ``next`` URL in the body when more pages exist. We pull the ``page``
    query parameter out of it so callers can pass it back as ``cursor``.
    """
    next_url = raw.get("next")
    if not next_url:
        return None
    page = parse_qs(urlparse(next_url).query).get("page")
    return page[0] if page else None


def make_paginated_result[T](
    map_item: Callable[[dict[str, Any]], T],
    raw: dict[str, Any],
    *,
    raw_items: Iterable[dict[str, Any]] | None = None,
) -> PaginatedActionResult[list[T]]:
    items: Iterable[dict[str, Any]] = raw.get("values", []) if raw_items is None else raw_items

    return PaginatedActionResult(
        data=[map_item(item) for item in items],
        type="bitbucket",
        raw={"data": raw, "headers": None},
        meta=PaginatedResponseMeta(next_cursor=_next_cursor(raw)),
    )
